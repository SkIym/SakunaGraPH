# Ontology release manifest

SakunaGraPH releases intentionally omit `owl:versionIRI`. Release identity is the combination of:

1. `owl:versionInfo "MAJOR.MINOR.PATCH"` in the released ontology;
2. the immutable `ontology-vMAJOR.MINOR.PATCH` Git tag and commit;
3. deterministic artifact filenames; and
4. SHA-256 digests and validation evidence in the release manifest.

`manifest.schema.json` is the normative structure for
`sakunagraph-ontology-MAJOR.MINOR.PATCH.manifest.json`. It deliberately has no SakunaGraPH version
IRI field.

Create the concrete manifest only during release preparation, after all artifact bytes and the
release commit are final. Validate it against the schema, verify every checksum from a clean
checkout, and attach both the manifest and checksum file to the GitHub release. Do not commit a
manifest containing placeholders or provisional digests.

For the 2.0.0 release, `previousTag` records the historical `sakunagraphv.1.0` tag. Compatibility
notes must identify the breaking cluster-namespace correction and point to its migration guidance.

See the [namespace and versioning policy](../docs/versioning-policy.md) and
[import snapshot manifest](../imports/README.md).
