# Stage 8 parsed-data quality gates

Stage 8 adds a package-owned quality boundary before source transformation.
The gate detects missing and unexpected columns, null required values, invalid
datatypes, empty datasets, and rejected-record threshold violations. It does
not mutate accepted rows or change RDF mappings.

## Built-in source contracts

The versioned contracts in `sakunagraph_etl.quality.contracts` cover EM-DAT,
GDA, and PSGC workbook rows plus NDRRMC and DROMIC event metadata. Workbook
contracts permit source-specific optional columns but report them in
`unexpected_columns`; custom contracts can reject all unexpected columns.
NDRRMC and DROMIC checks retain missing or malformed `metadata.json` files as
rejected events instead of silently losing them from the quality report.

Every source job records its `QualityReport` in immutable artifact metadata.
The `onprem` and `cloud` profiles enforce a failed report before transformation
or RDF serialization. The `local` profile records the same report but remains
compatible with exploratory and legacy inputs.

The default production policy requires at least one record and accepts no
rejected records. Operators can configure a reviewed exception with:

```text
SAKUNA_QUALITY_MINIMUM_RECORDS
SAKUNA_QUALITY_MAXIMUM_REJECTED_RECORDS
SAKUNA_QUALITY_MAXIMUM_REJECTED_RATIO
SAKUNA_QUALITY_FAIL_ON_UNEXPECTED_COLUMNS
```

The ratio is between `0` and `1`. A record is rejected once even when it has
multiple field violations. Reports retain each reason code and up to twenty
example row numbers per field.

## CLI

Validate a built-in source contract:

```bash
sakuna-etl quality source \
  --source emdat \
  --input /data/raw/emdat/export.xlsx \
  --profile onprem \
  --report /logs/quality/emdat.json
```

Validate CSV, JSON, or JSON Lines with a custom schema:

```bash
sakuna-etl quality table \
  --schema /config/event-schema.json \
  --input /data/parsed/events.jsonl \
  --format jsonl \
  --maximum-rejected-records 2 \
  --maximum-rejected-ratio 0.001 \
  --report /logs/quality/events.json
```

The command prints the same deterministic JSON report written by `--report`
and exits `1` when the policy fails. Schema files contain `source`, `table`,
`schema_version`, `allow_unexpected_columns`, and `fields`; each field defines
`name`, `kind` (`any`, `string`, `integer`, `number`, or `date`), `required`,
and `nullable`.

## Reason codes

| Code | Meaning |
|---|---|
| `MISSING_COLUMN` | A required column is absent from the dataset. |
| `UNEXPECTED_COLUMN` | A column is outside the selected schema. |
| `NULL_REQUIRED_VALUE` | A non-nullable field is empty. |
| `INVALID_DATATYPE` | A non-null value does not match its declared kind. |
| `MINIMUM_RECORDS` | The dataset is below its nonzero-output bound. |
| `REJECTED_COUNT_THRESHOLD` | Rejected rows exceed the absolute allowance. |
| `REJECTED_RATIO_THRESHOLD` | Rejected rows exceed the proportional allowance. |

SHACL remains the RDF semantic gate after mapping. This parsed-data gate is an
earlier, complementary boundary and cannot replace SHACL validation.
