from pathlib import Path
import unittest

from sakunagraph_etl.config import DeploymentProfile, load_settings


class EtlConfigTests(unittest.TestCase):
    def test_local_defaults_are_repository_anchored(self) -> None:
        settings = load_settings(environ={})

        self.assertEqual(settings.profile, DeploymentProfile.LOCAL)
        self.assertEqual(
            settings.paths.data_root,
            settings.paths.repository_root / "data",
        )
        self.assertIsNone(settings.paths.debug_root)

    def test_cloud_profile_accepts_explicit_mounted_paths(self) -> None:
        settings = load_settings(
            environ={
                "SAKUNA_ETL_PROFILE": "cloud",
                "SAKUNA_DATA_ROOT": "runtime/data",
                "SAKUNA_LOGS_ROOT": "runtime/logs",
                "SAKUNA_DEBUG_ROOT": "runtime/debug",
                "GRAPHDB_HOST": "https://graphdb.example/",
                "GRAPHDB_REPOSITORY": "production",
            }
        )

        self.assertEqual(settings.profile, DeploymentProfile.CLOUD)
        self.assertTrue(settings.paths.data_root.is_absolute())
        self.assertEqual(settings.paths.data_root.name, "data")
        self.assertEqual(settings.paths.debug_root, Path(settings.paths.repository_root / "runtime/debug"))
        self.assertEqual(settings.graphdb_host, "https://graphdb.example")
        self.assertEqual(settings.graphdb_repository, "production")

    def test_unknown_profile_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unknown ETL profile"):
            load_settings("edge")

    def test_onprem_profile_is_available_without_cloud_services(self) -> None:
        settings = load_settings("onprem", environ={})

        self.assertEqual(settings.profile, DeploymentProfile.ONPREM)
        self.assertEqual(settings.paths.data_root, settings.paths.repository_root / "data")

    def test_quality_thresholds_are_typed_and_validated(self) -> None:
        settings = load_settings(
            environ={
                "SAKUNA_QUALITY_MINIMUM_RECORDS": "2",
                "SAKUNA_QUALITY_MAXIMUM_REJECTED_RECORDS": "3",
                "SAKUNA_QUALITY_MAXIMUM_REJECTED_RATIO": "0.25",
                "SAKUNA_QUALITY_FAIL_ON_UNEXPECTED_COLUMNS": "false",
            }
        )
        self.assertEqual(settings.quality_minimum_records, 2)
        self.assertEqual(settings.quality_maximum_rejected_records, 3)
        self.assertEqual(settings.quality_maximum_rejected_ratio, 0.25)
        self.assertFalse(settings.quality_fail_on_unexpected_columns)

        with self.assertRaises(ValueError):
            load_settings(environ={"SAKUNA_QUALITY_MAXIMUM_REJECTED_RATIO": "1.1"})


if __name__ == "__main__":
    unittest.main()
