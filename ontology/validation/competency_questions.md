# Executable competency questions

The 20 competency questions reported in the project paper are now executable. Their normative
catalog is [`competency-manifest.json`](competency-manifest.json), their SPARQL lives under
[`queries/`](queries/), and exact normalized results live under [`expected/`](expected/). All cases
use the immutable synthetic graph [`fixtures/competency-v1.ttl`](fixtures/competency-v1.ttl).

The paper reported that all 20 questions succeeded for its study snapshot (pages 12-20). That is a
historical claim. The executable suite is separately dated repository evidence for the current
ontology and fixture.

## Categories and traceability

| Questions | Paper category | Requirement IDs |
| --- | --- | --- |
| CQ01-CQ05 | Event and type classification | OR-02, OR-03, OR-08 |
| CQ06-CQ08 | Casualties and population | OR-02, OR-04, OR-05, OR-07, OR-08 |
| CQ09-CQ11 | Damage | OR-02, OR-04, OR-06, OR-08 |
| CQ12-CQ14 | Service disruptions | OR-02, OR-04, OR-07, OR-08 |
| CQ15-CQ18 | Response and preparedness | OR-02, OR-05, OR-06, OR-07 |
| CQ19-CQ20 | Provenance | OR-03, OR-05, OR-08 |

The paper text called these "five categories" but listed six. The repository consistently uses all
six. Per-question pages, requirement links, result columns, ordering behavior, and required
inference are recorded in the machine-readable manifest rather than repeated here.

## Query index

| ID | Information need | SPARQL | Expected result |
| --- | --- | --- | --- |
| CQ01 | Tropical Cyclone major events and related incidents | [query](queries/cq01.rq) | [CSV](expected/cq01.csv) |
| CQ02 | Flash/Riverine Flood events, affected locations, and PSGC regions | [query](queries/cq02.rq) | [CSV](expected/cq02.csv) |
| CQ03 | Mudslide's complete broader hierarchy and event counts at each level | [query](queries/cq03.rq) | [CSV](expected/cq03.csv) |
| CQ04 | Volcanic Activity subtypes and affected provinces in Regions V, III, and VI | [query](queries/cq04.rq) | [CSV](expected/cq04.csv) |
| CQ05 | Fire, transport, and armed-conflict events in Central Luzon/CALABARZON during 2022 | [query](queries/cq05.rq) | [CSV](expected/cq05.csv) |
| CQ06 | Displaced families and persons reported for events affecting Western Visayas | [query](queries/cq06.rq) | [CSV](expected/cq06.csv) |
| CQ07 | Dead, missing, and injured counts for Ground Movement events in CAR provinces | [query](queries/cq07.rq) | [CSV](expected/cq07.csv) |
| CQ08 | Evacuation centers and their sources for 2021 CALABARZON Tropical Cyclones | [query](queries/cq08.rq) | [CSV](expected/cq08.csv) |
| CQ09 | Infrastructure-damage cost in millions of PHP for Caraga Flood events | [query](queries/cq09.rq) | [CSV](expected/cq09.csv) |
| CQ10 | Totally versus partially damaged houses from Meteorological events in Eastern Visayas | [query](queries/cq10.rq) | [CSV](expected/cq10.csv) |
| CQ11 | Agriculture damage, production-loss cost, and loss volume for Bicol Tropical Cyclones | [query](queries/cq11.rq) | [CSV](expected/cq11.csv) |
| CQ12 | Seaport disruptions and latest reported port status from Tropical Cyclones | [query](queries/cq12.rq) | [CSV](expected/cq12.csv) |
| CQ13 | Airports disrupted by Geophysical events and cancellation duration in hours | [query](queries/cq13.rq) | [CSV](expected/cq13.csv) |
| CQ14 | NCR class suspensions and affected grade levels | [query](queries/cq14.rq) | [CSV](expected/cq14.csv) |
| CQ15 | Assistance in Isabela by source and contributing organization | [query](queries/cq15.rq) | [CSV](expected/cq15.csv) |
| CQ16 | Declarations of calamity in SOCCSKSARGEN provinces and their resolution dates | [query](queries/cq16.rq) | [CSV](expected/cq16.csv) |
| CQ17 | Mindanao rescue reports, deployed units, and equipment | [query](queries/cq17.rq) | [CSV](expected/cq17.csv) |
| CQ18 | Fourth-income-class Region IX municipalities and preemptive evacuations | [query](queries/cq18.rq) | [CSV](expected/cq18.csv) |
| CQ19 | DROMIC versus EM-DAT technological fire-record counts | [query](queries/cq19.rq) | [CSV](expected/cq19.csv) |
| CQ20 | Pairwise alternate event records reported by multiple sources | [query](queries/cq20.rq) | [CSV](expected/cq20.csv) |

## Transcription corrections

The original Markdown transcription of the paper was not a directly runnable test suite. The split
query files preserve each information need while making these explicit corrections:

- CQ01 removes a duplicate projected variable and uses a grouped optional incident assertion that
  both RDFLib and GraphDB execute.
- CQ06 removes a dangling semicolon that made its graph pattern invalid.
- CQ08 adds the 2021 filter stated by the question.
- CQ13 retains GraphDB's `ofn:asHours` function; the local preflight registers an equivalent
  duration-to-decimal-hours function solely for fixture execution.
- CQ15 removes the projected `?ic` variable, which was never bound and was not part of the
  information need.
- CQ16 normalizes the resolution timestamp to its calendar date so the two engines do not differ
  only on `Z` versus `+00:00` lexical rendering.
- Every query has a deterministic tie-breaker where result order is part of its contract.

See [`README.md`](README.md) for the offline and GraphDB commands and the evidence boundary between
their results.
