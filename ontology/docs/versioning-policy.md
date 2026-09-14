# SakunaGraPH namespace and versioning policy

Status: Adopted for future ontology releases; `sakuna.ph` domain ownership remains open

Effective date: 2026-09-14

## Purpose

This policy defines stable IRIs, semantic versioning, OWL version metadata, compatibility,
deprecation, import pinning, offline resolution, and release artifact names for SakunaGraPH.

It applies to the ontology schema, disaster-type scheme, and SHACL shapes. Knowledge-graph data
snapshots, ETL, API, and frontend releases have separate versions and must state which ontology
version they use.

## Historical release anchor

The first published knowledge graph and ontology release remains exactly as released:

| Field | Historical value |
| --- | --- |
| Git tag | `sakunagraphv.1.0` |
| Date | 2026-06-19 |
| Commit | `197766f974a33b083584c905dd1a42068f235628` |
| Release scope | Combined knowledge graph and ontology baseline |
| OWL version metadata | No `owl:versionIRI` or `owl:versionInfo` was asserted |

Do not rename or move the historical tag. Future ontology releases use three-part semantic
versions. The current working ontology must not be labeled as the historical v1.0 artifact because
it contains post-release semantic changes. The maintainer has selected 2.0.0 as the next ontology
release candidate.

## Namespace registry

### Public ontology and resource IRIs

| Purpose | IRI or pattern | Stability rule |
| --- | --- | --- |
| Ontology IRI | `https://sakuna.ph/` | Historical published identifier; intended to become permanent after domain acquisition. |
| Core terms and taxonomy concepts | `https://sakuna.ph/{localName}` | Historical public namespace; never reuse a published IRI with a different meaning. |
| Source event records | `https://sakuna.ph/{source}/{uuid}` | Preserve once published; derive from stable source identity. |
| Event child records | `https://sakuna.ph/{source}/{uuid}/{segment}/{row?}` | Preserve the owning event IRI and deterministic segment identity. |
| Organizations | `https://sakuna.ph/org/{slug}` | Slug changes require an explicit replacement mapping. |
| PSGC locations | `https://sakuna.ph/{10-digit-code}` | Treat the PSGC code as identity; status/label changes do not mint a new IRI. |
| Country/island groups | `https://sakuna.ph/Philippines`, `/Luzon`, `/Visayas`, `/Mindanao` | Permanent named geography resources. |
| General SHACL shapes | `https://sakuna.ph/shapes/{localName}` | Public validation identifiers; version with the ontology package. |
| PSGC SHACL shapes | `https://sakuna.ph/shapes/psgc/{localName}` | Public validation identifiers; version with the ontology package. |
| Cluster identifiers | `https://sakuna.ph/cluster/{uuid}` | Preserve once minted; membership changes do not remint the cluster. |
| Named graph contexts | `https://sakuna.ph/events/{source}`, `/resolution`, `/orgs`, `/psgc` | Operational graph identifiers, not ontology terms. |

The ontology IRI and the core term namespace intentionally remain the same historical base. A
versioned ontology document must not put the version number into class, property, or concept IRIs.

### Namespace ownership status

As of 2026-09-14, the maintainer does not own `sakuna.ph` but plans to acquire it. Existing RDF and
the v1.0 release already publish IRIs under that domain, so those identifiers must be preserved as
historical identifiers. However, the project must not claim that they are currently dereferenceable
or under project control.

Before publishing 2.0.0 as an ontology with a controlled, dereferenceable namespace, one of these
must be true:

1. the project acquires and controls `sakuna.ph`; or
2. the maintainer approves a different controlled persistent ontology namespace and a migration
   plan.

Because 2.0.0 is already a breaking release candidate, it is the appropriate release boundary for
any unavoidable namespace migration. Acquiring `sakuna.ph` before release avoids reminting the
existing public term IRIs.

### External vocabularies

External terms retain their authoritative namespace, including GeoSPARQL, PROV-O, SKOS, QUDT, and
beAWARE. Do not copy an external term into the SakunaGraPH namespace merely to make its IRI shorter.
A local SakunaGraPH term may align to an external term only when the relationship is semantically
accurate and documented.

### Canonical-cluster namespace

ODR-0013 and ODR-0016 define clusters as collection identifiers with explicit membership.

**Maintainer decision (2026-09-14):** Every project-owned namespace must use `sakuna.ph`. The
resolver therefore mints cluster identifiers as `https://sakuna.ph/cluster/{uuid}`. Membership and
pairwise alternate-link changes from ODR-0016 must preserve the cluster IRI.

This is a breaking 2.0.0 correction to the pre-2.0 alternate cluster namespace. The UUID suffix
continues to be derived from the same sorted member-IRI key, but the full cluster IRI changes. Before
publishing 2.0.0, regenerate the alignment and deduplication registry outputs and reload the
resolution named graph. Consumers with cached cluster identifiers must refresh them. ODR-0013's
decision not to materialize a consolidated event means source event IRIs and source facts do not
need to be rewritten.

## IRI lifecycle rules

1. A published IRI identifies one continuing concept or resource.
2. Labels, comments, and spelling can improve without changing the IRI when meaning is preserved.
3. A semantic change that makes old data misleading requires a new IRI.
4. A removed or renamed public term follows the deprecation process below.
5. A deleted IRI is never reassigned to another concept.
6. Deterministic resource identifiers must not depend on mutable labels, file paths, or row order
   when a stable source identifier is available.
7. Changing the UUID namespace, normalization rule, or source identifier input requires a migration
   analysis because it can remint every affected resource.

## Ontology semantic versions

Ontology releases use `MAJOR.MINOR.PATCH` without a leading `v` inside RDF metadata.

### MAJOR

Increment MAJOR for a change that can invalidate existing RDF, alter established entailments or
query meaning, or require consumer migration. Examples include:

- removing a public term or changing its IRI;
- changing a class/property/concept to mean something materially different;
- narrowing a domain or range in a way that makes existing use inconsistent;
- adding/removing disjointness or equivalence with breaking inference consequences;
- changing a property chain, transitivity, identity, or cluster membership contract;
- restructuring the disaster taxonomy so existing classifications change meaning;
- changing the stable namespace or deterministic public resource-IRI algorithm; or
- changing a SHACL violation rule so previously accepted supported data is rejected, unless the
  earlier acceptance was an unambiguous bug with a documented exception.

### MINOR

Increment MINOR for backward-compatible semantic additions. Examples include:

- a new optional class, property, or taxonomy concept;
- a new mapping/alignment that does not change existing term meaning;
- a new non-blocking SHACL warning or informational check;
- a new source-specific SHACL profile that accepts all data valid under the applicable prior
  profile; or
- additional competency questions and examples with no contract change.

An additive OWL axiom is not automatically compatible. If it produces new types or inconsistencies
for existing data, classify it by its actual effect.

### PATCH

Increment PATCH for non-semantic corrections that preserve RDF meaning and validation outcomes.
Examples include:

- fixing grammar, formatting, examples, or documentation;
- adding labels/comments that clarify but do not redefine a term;
- serialization-only changes with an equivalent canonical graph; or
- fixing release metadata or offline catalog wiring without changing the import closure.

A changed definition that changes how a term should be used is not a patch.

## OWL version metadata

Every released `sakunagraph.ttl` after the historical v1.0 baseline must assert its human-readable
semantic version:

```turtle
<https://sakuna.ph/> a owl:Ontology ;
    owl:versionInfo "MAJOR.MINOR.PATCH" .
```

Rules:

- `owl:versionInfo` contains the same three-part version as a plain string, without `v`.
- Do not assert `owl:versionIRI`; immutable release identity is supplied by the Git tag, commit,
  release manifest, artifact filenames, and SHA-256 digests.
- Record the immediately preceding release and compatibility result in the manifest. Do not invent
  a retroactive version IRI for the historical v1.0 release.
- Record backward compatibility or intentional incompatibility in the manifest only after fixtures
  and competency queries establish the result.
- The unversioned ontology IRI always remains `https://sakuna.ph/`.
- Do not add release metadata to the working ontology until the release version and semantic diff
  are approved. Do not use placeholders such as `latest`, `dev`, or `unreleased` as
  `owl:versionInfo` values.

The next approved candidate is 2.0.0. The working ontology remains unstamped until domain control,
the 2.0.0 semantic diff, and the release artifacts are ready.

### Version-IRI decision

The [OWL 2 Structural Specification](https://www.w3.org/TR/owl2-syntax/#Ontology_IRI_and_Version_IRI)
makes `owl:versionIRI` optional. **Maintainer decision (2026-09-14):** SakunaGraPH omits
`owl:versionIRI` and relies on the stable ontology IRI `https://sakuna.ph/`,
`owl:versionInfo "MAJOR.MINOR.PATCH"`, the immutable `ontology-vMAJOR.MINOR.PATCH` Git tag, and the
checksum-bearing release manifest.

This avoids another public route while preserving a stable term namespace. A consumer that needs
an exact historical release retrieves the tagged, checksummed artifact rather than importing a
version-specific ontology IRI.

The disaster taxonomy and both shape graphs ship in the same ontology release package and use the
same semantic version in the release manifest. Their internal term IRIs remain unversioned.

## Compatibility and migration

### Backward-compatible changes

A change is backward compatible only when existing supported RDF still parses, retains its meaning,
passes the applicable blocking validation profile, and produces compatible competency-query
results under GraphDB 11.1.3 with OWL2-RL.

Compatibility must be demonstrated using the previous release artifacts and immutable fixtures;
the version number alone is not evidence.

### Deprecation

When replacing a public term:

```turtle
@prefix dcterms: <http://purl.org/dc/terms/> .

:oldTerm owl:deprecated true ;
    dcterms:isReplacedBy :newTerm ;
    rdfs:comment "Deprecated in 1.4.0; use :newTerm. Removal is permitted in 2.0.0."@en .
```

- Keep the old IRI resolvable and documented for the remainder of the current major series.
- New ETL output writes the replacement term; readers accept old and new terms during the window.
- Supply a SPARQL CONSTRUCT/update or deterministic migration script for stored RDF.
- Update competency queries, SHACL profiles, API queries, and examples in the same change.
- Use `owl:equivalentClass` or `owl:equivalentProperty` only when the old and new meanings are
  genuinely equivalent, not merely because one replaces the other.
- Remove a deprecated term only in a major release and list it in the migration guide.

The default migration window is the remainder of the current major version, rather than a fixed
number of months. Security, legal, or dangerously misleading terms may use a shorter window with an
explicit release-note justification.

## Import pinning and offline resolution

The import declaration keeps the upstream ontology IRI. Reproducible validation resolves that IRI
to a reviewed local snapshot through `ontology/imports/catalog-v001.xml`.

For every imported snapshot, record:

- canonical ontology IRI and the imported ontology's version signal, when one exists;
- upstream release, tag, or commit when available;
- retrieval date;
- local filename and SHA-256 digest;
- license/redistribution status;
- expected transitive imports; and
- the SakunaGraPH release that first/last uses it.

Import updates require a separate semantic diff and OWL/SHACL/CQ regression. Never allow a
production or release validation to fetch an unreviewed new `main`/`master` document. If upstream
publishes only a moving IRI, keep the canonical `owl:imports` IRI but pin the local bytes, checksum,
and upstream commit in the import manifest.

The current offline status and gaps are documented in
[the import snapshot manifest](../imports/README.md). A release claiming offline reproducibility
must fail if any required import cannot resolve locally.

## Release identifiers and artifact names

Starting with the next ontology release, use:

| Item | Convention |
| --- | --- |
| Git tag | `ontology-vMAJOR.MINOR.PATCH` |
| Release title | `SakunaGraPH Ontology MAJOR.MINOR.PATCH` |
| Core ontology | `sakunagraph-ontology-MAJOR.MINOR.PATCH.ttl` |
| Disaster taxonomy | `sakunagraph-disaster-types-MAJOR.MINOR.PATCH.ttl` |
| General shapes | `sakunagraph-shapes-MAJOR.MINOR.PATCH.ttl` |
| PSGC shapes | `sakunagraph-psgc-shapes-MAJOR.MINOR.PATCH.ttl` |
| Catalog | `sakunagraph-import-catalog-MAJOR.MINOR.PATCH.xml` |
| Manifest | `sakunagraph-ontology-MAJOR.MINOR.PATCH.manifest.json` |
| Checksums | `sakunagraph-ontology-MAJOR.MINOR.PATCH.sha256` |
| Migration guide, when needed | `sakunagraph-ontology-MAJOR.MINOR.PATCH-migration.md` |

The manifest records the Git commit, ontology IRI, `owl:versionInfo`, previous release tag, GraphDB
and ruleset, artifact digests,
import digests, validation reports, compatibility result, and supported data-profile versions.

Knowledge-graph snapshots are not ontology versions. Name a future snapshot such as
`sakunagraph-kg-YYYY-MM-DD-ontology-MAJOR.MINOR.PATCH.trig.gz` and record source coverage/access
dates separately. A combined release must expose both `ontologyVersion` and `dataSnapshot`; it must
not imply that updating data changes the ontology version.

Do not overwrite assets in an existing release. Corrected bytes require a new PATCH version.

**Maintainer decision (2026-09-14):** The next release uses tag `ontology-v2.0.0`, title
`SakunaGraPH Ontology 2.0.0`, and the 2.0.0 forms of the artifact names above.

## Release workflow

Before tagging an ontology release:

1. classify the semantic diff against the previous ontology release;
2. approve the next version and add `owl:versionInfo`;
3. update the import manifest/catalog and verify offline import resolution;
4. run parse, OWL2-RL, SHACL profile, competency-query, pitfall, and metrics checks;
5. compare validation and query results with the previous release;
6. prepare migrations for every deprecated/replaced/removed IRI;
7. build deterministically named artifacts and SHA-256 checksums;
8. verify the release manifest refers to the exact Git commit and artifact bytes; and
9. publish an immutable tag/release with compatibility and redistribution notes.

## Maintainer decisions recorded

The maintainer confirmed on 2026-09-14 that:

1. the next ontology release candidate is 2.0.0;
2. cluster IRIs use `https://sakuna.ph/cluster/{uuid}` so every project namespace uses
   `sakuna.ph`;
3. the ontology-only tag and artifact naming convention is approved;
4. deprecated terms remain supported until the next major release by default;
5. the complete import closure is vendored under option A; and
6. releases omit `owl:versionIRI` and rely on `owl:versionInfo`, the Git tag, and the checksum
   manifest.

The maintainer does not currently own `sakuna.ph` but plans to acquire it.

## Remaining assumptions adopted by this policy

The following defaults are implemented here but should be changed before the next release if the
maintainer disagrees:

1. After domain acquisition, `https://sakuna.ph/` becomes the permanent controlled ontology and
   term namespace.
2. Taxonomy and SHACL artifacts version in lockstep with the ontology release package.
3. Data snapshots version independently and record the ontology version they use.

## Maintainer consultation required

These points must be resolved before cutting the next ontology release:

1. **Ontology namespace control:** Acquire `sakuna.ph`, or select a different controlled persistent
   namespace and approve a 2.0.0 migration before presenting the namespace as project-controlled
   and dereferenceable.

## Step 3 completion criteria

- [x] Stable and legacy namespaces are inventoried.
- [x] Semantic-version compatibility rules are defined.
- [x] OWL version metadata and immutable tag/manifest policy are defined.
- [x] Deprecation and migration-window behavior are defined.
- [x] Import pinning, checksums, and offline catalog expectations are defined.
- [x] Release tag, artifact, manifest, and snapshot names are defined.
- [x] The historical v1.0 tag is preserved as an explicit exception.
- [x] The next release candidate, tag convention, cluster namespace, and migration window are
  approved.
- [x] All project-controlled IRI patterns use only `sakuna.ph`.
- [x] The complete offline import-closure policy (option A) is approved.
- [x] The release identity uses `owl:versionInfo`, the Git tag, and the checksum manifest without
  `owl:versionIRI`.
- [ ] Ontology domain control is resolved.
- [ ] The approved 2.0.0 value is added to `owl:versionInfo` during release preparation.
