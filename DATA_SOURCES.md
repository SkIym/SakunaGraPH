# Data sources and licensing boundaries

SakunaGraPH integrates records obtained from independent data providers. The repository's MIT
license covers original project code, documentation, and ontology content authored by the project;
it does not relicense source reports, datasets, imported ontologies, logos, or trademarks.

Raw inputs are deliberately excluded from version control. Reproducing a full pipeline run
requires obtaining each source independently and accepting its current terms. This page records
the project's handling policy; it is not a substitute for the provider's terms or legal advice.

## Source register

| Source | Project use | Official access point | Repository policy |
| --- | --- | --- | --- |
| NDRRMC | Situation reports, event dates, locations, impacts, and response facts | [NDRRMC updates and incident reports](https://ndrrmc.gov.ph/index.php/8-ndrrmc-update) | Do not commit raw reports. Record the exact report URL, title, revision, and acquisition date. No blanket redistribution license is asserted by this project. |
| DROMIC | DSWD situation reports, displacement, assistance, and response details | [DROMIC situation reports](https://dromic.dswd.gov.ph/category/situation-reports/) | Do not commit downloaded reports. Retain the post URL, report revision, filename, and acquisition time in provenance. No blanket redistribution license is asserted by this project. |
| GDA | Historical disaster records supplied as a cleaned workbook | Source-specific research input | The repository does not redistribute the workbook. Confirm its owner, citation, and permission terms before sharing either the input or substantial derived extracts. |
| EM-DAT | International disaster events, impact values, and taxonomy alignment | [EM-DAT data access documentation](https://doc.emdat.be/docs/data-accessibility/) | Users must obtain their own export. Do not commit or redistribute record-level EM-DAT data unless the selected product's terms expressly permit it. Follow the [EM-DAT citation policy](https://doc.emdat.be/legal/citation-policy-2025/). |
| PSGC | Canonical Philippine administrative codes and hierarchy | [Philippine Statistics Authority PSGC](https://psa.gov.ph/classification/psgc) | Record the publication quarter and access date. PSA site content is generally marked CC BY 4.0 unless otherwise stated; check the downloaded artifact for any more specific notice and provide attribution. |

## Redistributable repository fixtures

The files under `sakunagraph_etl/tests/golden/` are small synthetic regression fixtures generated
by project tests. They use invented events and values, are covered by the repository license, and
exist to demonstrate RDF mapping behavior without exposing operational source data.

The fixture catalog in `sakunagraph_etl/tests/baselines.json` records a SHA-256 digest,
deterministic IRI, triple count, representative triple, and builder for every source. Run
`python scripts/portfolio_demo.py` after installing RDFLib to verify them.

## Derived RDF and releases

Whether derived RDF may be redistributed depends on the source and the content retained. A change
of format does not automatically remove database, copyright, attribution, or contractual
obligations.

Before publishing a graph artifact or GitHub release:

1. Inventory which source records it contains.
2. Record the source version, access date, and exact terms that applied.
3. Remove data whose terms do not permit redistribution.
4. Include required attribution close to the artifact.
5. Keep synthetic fixtures clearly separated from production-derived graphs.

In particular, verify EM-DAT permissions before distributing any release artifact containing
record-level EM-DAT material. The SakunaGraPH MIT license does not grant permission to distribute
EM-DAT data.

## Imported vocabularies and ontologies

SakunaGraPH refers to or imports GeoSPARQL, PROV-O, SKOS, QUDT, and beAWARE. Those resources remain
under their respective owners' terms. The local beAWARE copy and XML catalog exist to make
ontology tooling reproducible; their inclusion does not relicense the upstream work under MIT.

When adding an external vocabulary:

- prefer stable, versioned IRIs;
- record its authoritative URL and license;
- keep a local catalog mapping when offline ontology tooling requires it; and
- document whether the project imports, copies, maps to, or merely references it.

## Provenance expectations

Production mappings should preserve, where available:

- source organization;
- report or dataset name;
- source URL or row identifier;
- source revision and publication timestamp;
- acquisition timestamp;
- transformation activity and software version; and
- the SakunaGraPH entity derived from the source record.

PROV-O relationships carry this lineage into the graph. Missing provenance is a data-quality
problem, not a value to infer from model memory.
