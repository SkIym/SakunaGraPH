# Ask Evaluation Baseline

Phase 0 evaluates the original text-to-SPARQL Ask pipeline without mutating
GraphDB. The evaluator submits only read queries through the API SPARQL
executor, which rejects update operations before making a GraphDB request.

## Reproduce the Evaluation

Run from `api/`:

```bash
# Validate and list all fixtures without contacting the model or GraphDB
.venv/bin/python scripts/evaluate_ask.py --dry-run

# Run the full sequential evaluation and write a timestamped report
.venv/bin/python scripts/evaluate_ask.py
```

The golden set is `tests/fixtures/ask_golden_questions.json`. It contains 81
unique questions across 23 categories. Reports are written to
`evaluation/reports/`.

## Recorded Baseline

The latest complete pre-Phase-1 report is
`reports/baseline-20260717T145102Z.json`:

| Metric | Result |
| --- | ---: |
| Cases | 81 |
| Query generation rate | 91.36% |
| Executable-query rate | 77.78% |
| Semantic-term pass rate | 17.28% |
| Expected-status match rate | 70.37% |
| Unsafe/unsupported rejection rate | 28.57% |
| Answer generation rate | 64.20% |
| Factual answer accuracy | Not yet measurable |
| Masked failure/no-data answers | 0 |
| Median end-to-end latency | 96,383.18 ms |

Executable-query rate is not treated as semantic correctness. The gap between
77.78% executable queries and 17.28% semantic-term passes is the main baseline
finding.

The current fixtures do not contain expected answer claims or a human-scored
answer rubric, so factual answer accuracy must not be inferred from answer
generation rate. Adding those expectations remains the one open Phase 0 task.

## Reproducible Failure Patterns

The case-level records in the baseline reports preserve generated SPARQL,
execution errors, missing required terms, forbidden terms, results, and answer
outcomes. The recurring patterns are:

- Seven local-model timeouts in the latest complete run.
- Invalid aggregate syntax such as `COUNT ?event` instead of `COUNT(?event)`.
- Parser failures caused by malformed filters and graph patterns.
- Casualty queries using forbidden string literals such as `"dead"`,
  `"injured"`, and `"missing"` instead of `:Dead`, `:Injured`, and `:Missing`.
- Missing QUDT value/unit patterns in damage questions.
- Missing location hierarchy, disaster taxonomy, and provenance traversal.
- Unsafe or unsupported questions accepted as ordinary answered queries in five
  of seven cases.

These reports establish Phase 0 only. Later-phase evaluations should write new
reports rather than overwrite the baseline files.

## Phase 2 Planner Evaluation

The structured planner is evaluated independently from SPARQL generation and
GraphDB execution:

```bash
.venv/bin/python scripts/evaluate_ask_planner.py
```

The command compares every field present in each fixture's `expected_plan`
against the validated `AskPlan`. It writes a timestamped `planner-*.json`
report and exits unsuccessfully when field accuracy is below the Phase 2
threshold of 80%. Use `--case`, `--category`, or `--limit` for focused runs.

This evaluator requires the configured local language model, but it never
generates SPARQL or contacts GraphDB.
