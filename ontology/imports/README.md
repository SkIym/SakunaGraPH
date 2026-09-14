# Ontology import snapshot manifest

## Purpose

This directory contains reviewed local ontology imports used for repeatable SakunaGraPH editing and
validation. `catalog-v001.xml` maps upstream ontology IRIs to local files without changing the
public `owl:imports` identifiers.

Status: Option A (complete offline import closure) was approved on 2026-09-14 and is implemented
for the current direct and transitive imports.

## Current inventory

| Import IRI | Upstream version signal | Local snapshot | SHA-256 | Offline status |
| --- | --- | --- | --- | --- |
| `https://raw.githubusercontent.com/beAWARE-project/ontology/master/beAWARE_ontology` | Embedded `owl:versionInfo` 1.0; original commit/date unknown | `beAWARE_ontology.owl` | `d63f3f666b8bc533d10edb6712f7a7d2764c8056b2a8783766216dda2439698b` | Local; Apache-2.0; checksum-pinned baseline |
| `http://www.opengis.net/ont/geosparql/1.1` | GeoSPARQL `owl:versionIRI` 1.1; file metadata reports 1.1.1 | `geosparql-1.1.ttl` | `7e6ee2d0ff5a04bd03cf849e822740204219bf11488a8036522a84d444f33619` | Local; Apache-2.0 |
| `http://www.w3.org/2004/02/skos/core` | W3C Recommendation, 18 August 2009 | `skos-2009.rdf` | `e79633b8d0564816cee8a99f5c9acf9a0e6fc7257c7209acd684ecad53a89dd6` | Local; W3C Document License |
| `http://www.w3.org/ns/prov-o-20130430` | W3C Recommendation version 2013-04-30 | `prov-o-20130430.owl` | `71ecff298c82b8c12aca0714a7bcbfd0b798cda9a7b3c89d9aeb0ef358cb79d0` | Local; W3C Document License |

The beAWARE ontology also imports SKOS. The catalog resolves all four direct imports, that
transitive SKOS import, the SakunaGraPH ontology IRI, and the applicable GeoSPARQL and PROV-O
ontology/version aliases. `THIRD_PARTY_NOTICES.md` records attribution and redistribution terms.

### Snapshot provenance

| Local snapshot | Upstream source | Upstream revision | Retrieved (UTC) |
| --- | --- | --- | --- |
| `beAWARE_ontology.owl` | beAWARE GitHub repository | Unknown historical revision; current upstream differs semantically | Original retrieval unknown; accepted as the checksum-pinned 2.0.0 baseline on 2026-09-14 |
| `geosparql-1.1.ttl` | OGC `geosemantics-semantic-resources` repository | Commit `ac303373bd0cf149d31f110cf8c1ed281ff66c60` | 2026-09-14T12:02:13Z |
| `skos-2009.rdf` | W3C SKOS Recommendation RDF/XML namespace document | Recommendation dated 2009-08-18 | 2026-09-14T12:02:13Z |
| `prov-o-20130430.owl` | W3C dated PROV-O version IRI | Recommendation dated 2013-04-30 | 2026-09-14T12:02:13Z |

## What "vendoring imports" means

Vendoring an imported ontology means keeping an exact, reviewed copy of its RDF/OWL file in this
directory and mapping the ontology's public IRI to that local copy through `catalog-v001.xml`.
SakunaGraPH would continue to use the external ontology's canonical IRI in `owl:imports`; vendoring
does not rename, fork, or claim ownership of that ontology.

The practical benefit is reproducibility. A checkout can load and validate the complete ontology
without contacting an upstream website, and a later upstream edit or outage cannot silently change
the meaning of an existing SakunaGraPH release. Each local copy must therefore record its upstream
release, tag, or commit; retrieval date; checksum; license; and reviewed semantic differences.

Vendoring also creates maintenance responsibilities:

- the files consume repository space and must not be modified after release;
- their licenses and attribution requirements must permit redistribution;
- upstream fixes are not received automatically; they require a reviewed snapshot update and an
  appropriate SakunaGraPH version change; and
- the XML catalog and offline-loading tests must cover transitive imports as well as direct imports.

SakunaGraPH now vendors the complete import closure. The existing beAWARE bytes remain pinned rather
than being silently replaced with the different current upstream file. Its original upstream
revision remains unknown, which is a documented provenance limitation rather than a reproducibility
gap: future validation can still prove it is using these exact bytes.

### Decision options for the next release

The maintainer selected option A on 2026-09-14:

- **A -- Vendor the complete import closure (recommended):** Store reviewed snapshots of beAWARE,
  GeoSPARQL 1.1, SKOS, PROV-O, and any transitive imports whose licenses permit redistribution.
  This gives the strongest offline and reproducible-build guarantee.
- **B -- Vendor beAWARE only:** Keep the project-specific dependency local but allow standards such
  as GeoSPARQL, SKOS, and PROV-O to resolve over the network. This is smaller and easier to maintain,
  but validation is not fully offline or insulated from availability problems.
- **C -- Do not vendor imports:** Resolve every external import over the network. This has the least
  repository maintenance but the weakest reproducibility and is not recommended for a release
  intended to demonstrate production ontology engineering.

For the existing beAWARE file, the project can treat the current checksum as the pinned 2.0.0
baseline while clearly recording that its original commit is unknown. Recovering the original
commit would improve provenance, but it is not necessary to prove that future releases use the
same bytes.

## Snapshot acceptance record

Every added or updated local import must record:

```text
Ontology IRI:
Version IRI:
Upstream release/tag/commit:
Retrieved at (UTC):
Local file:
SHA-256:
License and redistribution status:
Transitive imports:
Reviewed semantic diff:
First SakunaGraPH ontology version:
Last SakunaGraPH ontology version (when retired):
```

Do not replace an existing snapshot in place after it has shipped. Add a new reviewed file or cut a
new SakunaGraPH ontology version so the manifest and checksum continue to identify immutable bytes.

## Release checks for the offline import closure

There are no known missing files in the current import closure. Before releasing 2.0.0:

1. rerun the offline catalog regression test;
2. confirm the core ontology has not added another direct import;
3. keep `beAWARE_ontology.owl` unchanged unless a reviewed semantic upgrade is intentionally made;
   and
4. package `THIRD_PARTY_NOTICES.md` and `licenses/Apache-2.0.txt` with redistributed snapshots.

The automated regression test is
`sakunagraph_etl/tests/test_ontology_import_catalog.py`. It starts from the SakunaGraPH ontology,
follows every `owl:imports` statement through the XML catalog, and parses the entire closure without
network access.

See [the namespace and versioning policy](../docs/versioning-policy.md) for update and release
requirements.
