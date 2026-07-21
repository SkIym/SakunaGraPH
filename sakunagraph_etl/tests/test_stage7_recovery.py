from contextlib import redirect_stdout
from io import BytesIO
from io import StringIO
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from sakunagraph_etl.io import LocalFileStorage, record_artifact_run
from sakunagraph_etl.io.recovery import (
    GraphDbRecoveryClient,
    RecoveryError,
    full_rebuild,
    main,
    verify_backup,
)

from tests.test_stage7_orchestration import settings_for


def backup_archive(*, success: bool = True) -> bytes:
    buffer = BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        item = tarfile.TarInfo("backup/repositories/sakunagraph/data.bin")
        value = b"graphdb"
        item.size = len(value)
        archive.addfile(item, BytesIO(value))
        if success:
            archive.addfile(tarfile.TarInfo("backup/.success"), BytesIO(b""))
    return buffer.getvalue()


class FakeResponse:
    def __init__(self, content=b"", payload=None, text="ok") -> None:
        self.content = content
        self.payload = payload
        self.text = text
        self.headers = {"X-GraphDB-Version": "11.3"}

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size):
        for start in range(0, len(self.content), chunk_size):
            yield self.content[start : start + chunk_size]

    def json(self):
        if self.payload is None:
            raise ValueError("not json")
        return self.payload


class FakeSession:
    def __init__(self, archive: bytes) -> None:
        self.archive = archive
        self.posts = []

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        if url.endswith("/backup"):
            return FakeResponse(self.archive)
        return FakeResponse(payload={"status": "restored"})

    def get(self, url, **kwargs):
        return FakeResponse(payload={"running": False})


class Stage7RecoveryTests(unittest.TestCase):
    def test_backup_is_checksums_verified_and_restore_uses_verified_archive(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "graphdb-backup.tar.gz"
            session = FakeSession(backup_archive())
            client = GraphDbRecoveryClient(
                "http://graphdb:7200", session=session, timeout=60
            )
            metadata = client.backup(
                path,
                repositories=("sakunagraph",),
                include_system_data=True,
            )

            self.assertTrue(metadata.success_marker_verified)
            self.assertEqual(verify_backup(path).sha256, metadata.sha256)
            self.assertEqual(
                client.backup(
                    path,
                    repositories=("sakunagraph",),
                    include_system_data=True,
                ),
                metadata,
            )
            restored = client.restore(
                path,
                repositories=("sakunagraph",),
                restore_system_data=True,
                remove_stale_repositories=False,
            )
            self.assertEqual(restored, {"status": "restored"})
            restore_url, restore_kwargs = session.posts[-1]
            self.assertTrue(restore_url.endswith("/rest/recovery/restore"))
            params_part = restore_kwargs["files"]["params"]
            self.assertEqual(params_part[2], "application/json")
            self.assertIn("sakunagraph", params_part[1])
            self.assertEqual(client.status(), {"running": False})

            path.write_bytes(path.read_bytes() + b"tampered")
            with self.assertRaisesRegex(RecoveryError, "size differs"):
                verify_backup(path)

    def test_backup_without_graphdb_success_marker_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            client = GraphDbRecoveryClient(
                "http://graphdb:7200", session=FakeSession(backup_archive(success=False))
            )
            with self.assertRaisesRegex(RecoveryError, "no .success marker"):
                client.backup(
                    Path(temp) / "incomplete.tar.gz",
                    repositories=("sakunagraph",),
                    include_system_data=False,
                )
            self.assertFalse((Path(temp) / "incomplete.tar.gz").exists())

    def test_destructive_recovery_commands_require_exact_target_confirmation(self) -> None:
        with redirect_stdout(StringIO()):
            restore_code = main(
                [
                    "--repo",
                    "sakunagraph",
                    "restore",
                    "missing.tar",
                    "--confirm-repository",
                    "other",
                ]
            )
            rebuild_code = main(
                [
                    "full-rebuild",
                    "--manifest",
                    "missing.json",
                    "--target-repository",
                    "rebuild",
                    "--confirm-target",
                    "other",
                ]
            )
        self.assertEqual(restore_code, 1)
        self.assertEqual(rebuild_code, 2)

    def test_full_rebuild_preflights_all_manifests_before_clearing_graphdb(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            settings = settings_for(root)
            storage = LocalFileStorage(root / "artifacts")
            source = root / "source.csv"
            source.write_text("id\n1\n", encoding="utf-8")
            manifests = []
            for index in range(2):
                output = root / f"output-{index}.ttl"
                output.write_text(
                    f"<https://sakuna.ph/{index}> a <https://sakuna.ph/Event> .\n",
                    encoding="utf-8",
                )
                result = record_artifact_run(
                    f"source-{index}",
                    input_paths=(source,),
                    output_paths=(output,),
                    validation_status="PASSED",
                    settings=settings,
                    storage=storage,
                )
                manifests.append(storage.path_for(result.manifest_key))

            with patch("sakunagraph_etl.io.graphdb.main", return_value=0) as loader:
                code = full_rebuild(
                    manifests,
                    host="http://graphdb:7200",
                    repository="rebuild-candidate",
                    profile="local",
                    timeout=60,
                )
            self.assertEqual(code, 0)
            self.assertIn("--clear-repository", loader.call_args_list[0].args[0])
            self.assertIn("--replace", loader.call_args_list[1].args[0])

            failed_output = root / "failed.ttl"
            failed_output.write_text("not turtle", encoding="utf-8")
            failed = record_artifact_run(
                "failed",
                input_paths=(source,),
                output_paths=(failed_output,),
                validation_status="FAILED",
                settings=settings,
                storage=storage,
            )
            with patch("sakunagraph_etl.io.graphdb.main") as loader:
                with self.assertRaises(RecoveryError):
                    full_rebuild(
                        [storage.path_for(failed.manifest_key)],
                        host="http://graphdb:7200",
                        repository="rebuild-candidate",
                        profile="local",
                        timeout=60,
                    )
                loader.assert_not_called()

    def test_stage7_operations_assets_are_structurally_valid(self) -> None:
        etl_root = Path(__file__).resolve().parents[1]
        dashboard = json.loads(
            (etl_root / "deploy" / "observability" / "grafana-dashboard.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertTrue(dashboard["panels"])
        terraform = (etl_root / "deploy" / "terraform" / "workflow.tf").read_text(
            encoding="utf-8"
        )
        self.assertIn("arn:aws:states:::ecs:runTask.sync", terraform)
        self.assertIn("attribute_not_exists(LockName)", terraform)
        self.assertIn('resource "aws_dynamodb_table" "workflow_lock"', terraform)
        self.assertIn('resource "aws_scheduler_schedule" "etl"', terraform)
        systemd_root = etl_root / "deploy" / "systemd"
        self.assertTrue((systemd_root / "sakunagraph-etl@.service").is_file())
        self.assertEqual(
            {path.stem for path in systemd_root.glob("*.timer")},
            {
                "sakunagraph-etl-dromic",
                "sakunagraph-etl-emdat",
                "sakunagraph-etl-integration",
                "sakunagraph-etl-ndrrmc",
                "sakunagraph-etl-psgc",
            },
        )
        self.assertTrue((etl_root.parent / ".github" / "dependabot.yml").is_file())


if __name__ == "__main__":
    unittest.main()
