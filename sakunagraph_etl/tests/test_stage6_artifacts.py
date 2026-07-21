from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stderr, redirect_stdout
from io import BytesIO, StringIO
import json
from pathlib import Path
import tempfile
import unittest

from sakunagraph_etl.config import load_settings
from sakunagraph_etl.io import (
    ArtifactIntegrityError,
    LocalFileStorage,
    NetworkFileStorage,
    S3ObjectStorage,
    StorageConflictError,
    local_artifact_paths_from_manifest,
    materialize_manifest_inputs,
    record_artifact_run,
    stable_run_id,
    storage_for_profile,
    verify_run_manifest,
)
from sakunagraph_etl.io import graphdb
from sakunagraph_etl.sources.dromic.state import (
    DromicStateStore,
    EventStatus,
    EventStatusRecord,
)
from sakunagraph_etl.sources.emdat.parse import latest_workbook


def settings_for(root: Path, profile: str = "local", **extra: str):
    environ = {
        "SAKUNA_REPOSITORY_ROOT": str(root),
        "SAKUNA_ETL_PROFILE": profile,
        "SAKUNA_DATA_ROOT": str(root / "data"),
        "SAKUNA_ARTIFACT_ROOT": str(root / "artifacts"),
        **extra,
    }
    return load_settings(environ=environ)


class FakeS3Error(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakePaginator:
    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self.objects = objects

    def paginate(self, *, Bucket: str, Prefix: str):
        yield {
            "Contents": [
                {"Key": key}
                for bucket, key in self.objects
                if bucket == Bucket and key.startswith(Prefix)
            ]
        }


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def head_object(self, *, Bucket: str, Key: str):
        if (Bucket, Key) not in self.objects:
            raise FakeS3Error("404")
        return {}

    def get_object(self, *, Bucket: str, Key: str):
        if (Bucket, Key) not in self.objects:
            raise FakeS3Error("NoSuchKey")
        return {"Body": BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, **kwargs):
        object_key = (Bucket, Key)
        if kwargs.get("IfNoneMatch") == "*" and object_key in self.objects:
            raise FakeS3Error("PreconditionFailed")
        self.objects[object_key] = bytes(Body)
        return {}

    def get_paginator(self, name: str):
        if name != "list_objects_v2":
            raise AssertionError(name)
        return FakePaginator(self.objects)


class Stage6ArtifactTests(unittest.TestCase):
    def test_run_is_content_addressed_verified_and_retry_safe(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "input" / "report.xlsx"
            output = root / "output" / "emdat.ttl"
            source.parent.mkdir()
            output.parent.mkdir()
            source.write_bytes(b"source-v1")
            output.write_text("@prefix : <https://sakuna.ph/> .\n", encoding="utf-8")
            settings = settings_for(root)
            storage = LocalFileStorage(root / "artifacts")

            first = record_artifact_run(
                "emdat",
                input_paths=(source,),
                output_paths=(output,),
                validation_status="PASSED",
                settings=settings,
                storage=storage,
                parameters={"format": "turtle"},
            )
            retry = record_artifact_run(
                "emdat",
                input_paths=(source,),
                output_paths=(output,),
                validation_status="PASSED",
                settings=settings,
                storage=storage,
                parameters={"format": "turtle"},
            )

            self.assertEqual(first.run_id, retry.run_id)
            self.assertEqual(first.manifest, retry.manifest)
            self.assertTrue(first.manifest_key.startswith("runs/emdat/"))
            verify_run_manifest(first.manifest, storage)
            inputs = [a for a in first.manifest.artifacts if a.role == "input"]
            outputs = [a for a in first.manifest.artifacts if a.role == "output"]
            self.assertEqual(len(inputs), 1)
            self.assertEqual(len(outputs), 1)
            self.assertEqual(outputs[0].validation_status, "PASSED")
            self.assertEqual(outputs[0].code_version, "0.9.0")
            self.assertEqual(outputs[0].graph_context, "https://sakuna.ph/events/emdat")
            self.assertEqual(len(inputs[0].sha256 or ""), 64)
            self.assertEqual(len(outputs[0].sha256 or ""), 64)

            output.write_bytes(b"different retry")
            with self.assertRaises(StorageConflictError):
                record_artifact_run(
                    "emdat",
                    input_paths=(source,),
                    output_paths=(output,),
                    validation_status="PASSED",
                    settings=settings,
                    storage=storage,
                    parameters={"format": "turtle"},
                )

    def test_failed_validation_is_quarantined_and_not_publishable(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "psgc.xlsx"
            output = root / "psgc.staged.ttl"
            source.write_bytes(b"input")
            output.write_text("broken", encoding="utf-8")
            storage = LocalFileStorage(root / "artifacts")
            result = record_artifact_run(
                "psgc",
                input_paths=(source,),
                output_paths=(output,),
                validation_status="FAILED",
                settings=settings_for(root),
                storage=storage,
            )

            self.assertTrue(result.manifest_key.startswith("quarantine/psgc/"))
            self.assertEqual(result.manifest.status, "QUARANTINED")
            self.assertEqual(
                {a.role for a in result.manifest.artifacts if a.role != "input"},
                {"quarantine"},
            )
            manifest_path = storage.path_for(result.manifest_key)
            with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
                code = graphdb.main(["--input-manifest", str(manifest_path), "--dry-run"])
            self.assertEqual(code, 2)

    def test_manifest_materializes_directory_layout_and_detects_tampering(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            event = root / "Storm 2026"
            event.mkdir()
            (event / "metadata.json").write_text('{"event": 1}', encoding="utf-8")
            nested = event / "tables"
            nested.mkdir()
            (nested / "affected.csv").write_text("count\n1\n", encoding="utf-8")
            output = root / "dromic.ttl"
            output.write_text("@prefix : <#> .\n", encoding="utf-8")
            storage = LocalFileStorage(root / "artifacts")
            result = record_artifact_run(
                "dromic",
                input_paths=(event,),
                output_paths=(output,),
                validation_status="PASSED",
                settings=settings_for(root),
                storage=storage,
            )

            destination = root / "reproduced"
            materialized = materialize_manifest_inputs(
                result.manifest,
                storage,
                destination,
            )
            self.assertEqual(
                {path.relative_to(destination).as_posix() for path in materialized},
                {"Storm 2026/metadata.json", "Storm 2026/tables/affected.csv"},
            )

            manifest_path = storage.path_for(result.manifest_key)
            (event / "metadata.json").write_text("tampered", encoding="utf-8")
            with self.assertRaises(ArtifactIntegrityError):
                local_artifact_paths_from_manifest(manifest_path, roles=("input",))

    def test_run_identity_uses_content_code_and_parameters(self) -> None:
        inputs = (("report.xlsx", "a" * 64, 20),)
        first = stable_run_id("emdat", inputs, parameters={"rows": 10})
        same = stable_run_id("emdat", reversed(inputs), parameters={"rows": 10})
        changed = stable_run_id("emdat", inputs, parameters={"rows": 11})
        next_code = stable_run_id(
            "emdat", inputs, parameters={"rows": 10}, code_version="0.6.1"
        )
        self.assertEqual(first, same)
        self.assertNotEqual(first, changed)
        self.assertNotEqual(first, next_code)


class Stage6StorageTests(unittest.TestCase):
    def test_profile_factory_selects_reference_and_network_adapters(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.assertIsInstance(storage_for_profile(settings_for(root)), LocalFileStorage)
            self.assertIsInstance(
                storage_for_profile(settings_for(root, "onprem")),
                NetworkFileStorage,
            )
            with self.assertRaisesRegex(ValueError, "SAKUNA_OBJECT_BUCKET"):
                storage_for_profile(settings_for(root, "cloud"))

    def test_s3_conditional_create_accepts_only_identical_retry(self) -> None:
        client = FakeS3Client()
        storage = S3ObjectStorage("artifacts", prefix="sakuna", client=client)
        storage.write_once("runs/a.txt", b"one")
        storage.write_once("runs/a.txt", b"one")
        self.assertEqual(storage.read_bytes("runs/a.txt"), b"one")
        self.assertEqual(storage.uri_for("runs/a.txt"), "s3://artifacts/sakuna/runs/a.txt")
        self.assertEqual(storage.iter_files("runs"), (Path("runs/a.txt"),))
        with self.assertRaises(StorageConflictError):
            storage.write_once("runs/a.txt", b"two")


class Stage6DromicStateTests(unittest.TestCase):
    def test_producer_records_merge_concurrently_and_derive_compatibility_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            year = Path(temp) / "2026"
            year.mkdir()
            quality = EventStatusRecord.create(
                "event-a",
                EventStatus.DUPLICATE_CSV,
                "quality",
                reason="duplicate table",
                source_filename="event-a.pdf",
            )
            mapping = EventStatusRecord.create(
                "event-b",
                EventStatus.MAPPING_ERROR,
                "mapping",
                reason="invalid row",
                source_filename="event-b.pdf",
            )

            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(DromicStateStore(year).update, (record,))
                    for record in (quality, mapping)
                ]
                for future in futures:
                    future.result()

            store = DromicStateStore(year)
            manifest = store.load()
            self.assertEqual(set(manifest.events), {"event-a", "event-b"})
            self.assertEqual(
                (year / "_needs_rerun.txt").read_text(encoding="utf-8"),
                "event-a\nevent-b\n",
            )
            self.assertEqual(
                (year / "_parsed.txt").read_text(encoding="utf-8"),
                "event-a.pdf\n",
            )
            payload = json.loads((year / "_event_manifest.json").read_text(encoding="utf-8"))
            record = payload["events"]["event-b"]["mapping"]
            self.assertEqual(record["status"], "MAPPING_ERROR")
            self.assertEqual(record["reason"], "invalid row")
            self.assertTrue(record["updated_at"].endswith("+00:00"))


class Stage6InputSelectionTests(unittest.TestCase):
    def test_emdat_directory_with_multiple_workbooks_requires_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "older.xlsx").touch()
            (root / "newer.xlsx").touch()
            with self.assertRaisesRegex(ValueError, "--input-manifest"):
                latest_workbook(root)


class Stage6PackagingTests(unittest.TestCase):
    def test_images_are_split_pinned_and_non_root(self) -> None:
        etl_root = Path(__file__).resolve().parents[1]
        core = (etl_root / "deploy/docker/Dockerfile.core").read_text(encoding="utf-8")
        documents = (etl_root / "deploy/docker/Dockerfile.documents").read_text(
            encoding="utf-8"
        )
        compose = (etl_root / "docker-compose.yml").read_text(encoding="utf-8")
        self.assertIn("python:3.12.13-slim-bookworm", core)
        self.assertIn("sha256:d50fb7611f86d04a3b0471b46d7557818d88983fc3136726336b2a4c657aa30b", core)
        self.assertIn("USER 10001:10001", core)
        self.assertNotIn("chromium", core)
        self.assertIn("chromium-driver", documents)
        self.assertIn("USER 10001:10001", documents)
        self.assertIn("profiles: [documents]", compose)
        self.assertIn("e8c3b32edf5434bc2275fc9bab85f82640a19130", core)


if __name__ == "__main__":
    unittest.main()
