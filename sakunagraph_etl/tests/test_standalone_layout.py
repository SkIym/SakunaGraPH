from __future__ import annotations

import ast
from pathlib import Path
import tomllib
import unittest

from sakunagraph_etl import config


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "src" / "sakunagraph_etl"
LEGACY_IMPORT_ROOTS = {
    "etl_config",
    "fetch",
    "mappings",
    "parse",
    "pipeline",
    "semantic_processing",
    "transform",
    "validate",
}


class StandaloneLayoutTests(unittest.TestCase):
    def test_package_has_no_legacy_etl_imports(self) -> None:
        violations: list[str] = []
        for path in PACKAGE_ROOT.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    roots = {alias.name.split(".", 1)[0] for alias in node.names}
                elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                    roots = {node.module.split(".", 1)[0]}
                else:
                    continue
                forbidden = roots & LEGACY_IMPORT_ROOTS
                if forbidden:
                    violations.append(f"{path.relative_to(PROJECT_ROOT)}: {sorted(forbidden)}")
        self.assertEqual(violations, [])

    def test_wheel_configuration_packages_only_standalone_modules(self) -> None:
        metadata = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        packages = set(metadata["tool"]["setuptools"]["packages"])
        self.assertIn("sakunagraph_etl.transform", packages)
        self.assertTrue(all(name.startswith("sakunagraph_etl") for name in packages))
        self.assertNotIn("py-modules", metadata["tool"]["setuptools"])

    def test_standalone_and_legacy_projects_are_siblings(self) -> None:
        repository_root = PROJECT_ROOT.parent
        legacy_root = repository_root / "etl"
        self.assertEqual(config.PROJECT_ROOT, PROJECT_ROOT)
        self.assertEqual(config.REPOSITORY_ROOT, repository_root)
        self.assertTrue((PACKAGE_ROOT / "transform" / "helpers.py").is_file())
        self.assertTrue((PACKAGE_ROOT / "transform" / "impact.py").is_file())
        self.assertTrue((PACKAGE_ROOT / "quality" / "shacl.py").is_file())
        self.assertTrue((PACKAGE_ROOT / "enrichment" / "climate_parameters.py").is_file())
        self.assertTrue((legacy_root / "transform" / "helpers.py").is_file())
        self.assertTrue((legacy_root / "transform" / "impact.py").is_file())
        self.assertTrue((legacy_root / "validate" / "validate.py").is_file())
        self.assertTrue((legacy_root / "parse" / "check_fails.py").is_file())
        self.assertTrue(
            (legacy_root / "semantic_processing" / "climate_parameter_extractor.py").is_file()
        )


if __name__ == "__main__":
    unittest.main()
