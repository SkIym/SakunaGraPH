# Workflow operations

## Local and on-premise execution

Workflow parameters may be passed individually or in a protected JSON file:

```bash
sakuna-etl workflow list
sakuna-etl workflow run source-emdat \
  --profile onprem \
  --param input=/srv/sakunagraph/data/raw/emdat/report.xlsx \
  --param output_dir=/srv/sakunagraph/data/rdf/events/emdat

sakuna-etl workflow run integration \
  --profile onprem \
  --params-file /etc/sakunagraph/workflows/integration.json
```

Run state is stored below `logs/workflows/{workflow}/{run_id}.json`. Repeating
the same scheduled time and parameters derives the same run ID. Completed task
checkpoints are checksum-verified and skipped; failed or missing checkpoints
resume from the first incomplete task.

For a backfill, execute one bounded daily interval at a time:

```bash
sakuna-etl workflow backfill source-dromic \
  --start 2026-01-01 --end 2026-01-31 \
  --params-file /etc/sakunagraph/workflows/source-dromic.json \
  --continue-on-error --profile onprem
```

Only one run of a workflow may hold the shared-filesystem lock. Do not remove a
lock while its owner is active. If a worker was terminated, verify no matching
process exists before removing a lock older than the configured 48-hour stale
window.

The managed state machine uses a conditional DynamoDB lock with the same
one-active-run policy. Normal success and failure paths delete it. If an
execution is forcibly aborted, first confirm that no execution of the state
machine is `RUNNING`, then delete only the item whose `LockName` matches the
Terraform output/environment. Never clear the lock to bypass a live execution.

## Validation failure

1. Find `workflow`, `workflow_run_id`, `task_id`, and `run_id` in JSON logs or
   `logs/alerts/workflow-alerts.jsonl`.
2. Inspect the quarantine manifest and DROMIC producer reason, if applicable.
3. Correct the input or mapping. Do not promote a quarantined object.
4. Resume using the original scheduled time and parameters. The runner skips
   unaffected immutable predecessors.

Alignment verifies every path in the `source_manifests` JSON array before it
starts. Publication is dependency-gated on alignment and production GraphDB
publication always performs SHACL validation.

## Observability

- JSON task logs contain workflow, task, attempt, workflow run ID, and artifact
  run ID where available.
- Prometheus textfile metrics are written to `logs/metrics/sakunagraph.prom`.
- Alerts are durable JSON Lines and optionally POST to `SAKUNA_ALERT_WEBHOOK`.
- OpenLineage-compatible events are in `logs/lineage/openlineage.jsonl`.
- Import `deploy/observability/grafana-dashboard.json` and load
  `prometheus-alerts.yml` into the existing monitoring stack.
