from pathlib import Path
import tempfile
import unittest

from sakunagraph_etl.io import (
    ArtifactManifest,
    JsonManifestStore,
    LocalFileStorage,
    ManifestStore,
    RunManifest,
    Storage,
)


class LocalStorageTests(unittest.TestCase):
    def test_atomic_text_write_and_file_discovery(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            storage = LocalFileStorage(temp)
            destination = storage.write_text("runs/2026/result.ttl", "@prefix : <#> .\n")

            self.assertIsInstance(storage, Storage)
            self.assertEqual(storage.read_text("runs/2026/result.ttl"), "@prefix : <#> .\n")
            self.assertEqual(storage.iter_files("runs", "**/*.ttl"), (destination,))
            self.assertEqual(list(destination.parent.glob("*.tmp")), [])

    def test_storage_rejects_parent_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            storage = LocalFileStorage(temp)

            with self.assertRaisesRegex(ValueError, "escapes configured root"):
                storage.write_text(Path("..") / "outside.txt", "unsafe")


class ManifestTests(unittest.TestCase):
    def test_json_manifest_round_trip(self) -> None:
        manifest = RunManifest(
            run_id="run-20260717",
            pipeline="dromic",
            created_at="2026-07-17T12:00:00+08:00",
            profile="onprem",
            artifacts=(
                ArtifactManifest(
                    path="rdf/events/dromic/dromic-2026.ttl",
                    media_type="text/turtle",
                    size_bytes=120,
                    sha256="abc123",
                ),
            ),
            metadata={"year": 2026},
        )

        with tempfile.TemporaryDirectory() as temp:
            store = JsonManifestStore(LocalFileStorage(temp), "manifests/dromic.json")
            self.assertIsInstance(store, ManifestStore)
            self.assertIsNone(store.load())

            store.save(manifest)

            self.assertEqual(store.load(), manifest)


if __name__ == "__main__":
    unittest.main()
