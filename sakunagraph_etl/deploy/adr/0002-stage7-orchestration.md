# ADR 0002: CLI-bound orchestration and observability

Status: Accepted

AWS Step Functions Standard workflows with the optimized ECS/Fargate `.sync`
integration are the initial managed scheduler. EventBridge Scheduler starts
scheduled executions in `Asia/Manila`; execution-status events feed SNS alerts.
On-premise deployments use systemd timers and the package-owned workflow runner.

Both schedulers invoke `sakuna-etl` commands. They pass only command arrays,
artifact/result-envelope URIs, run IDs, and data-interval metadata. Mapping,
validation, entity resolution, and GraphDB publication remain package code.

The local runner checkpoints after immutable manifests are committed. A retry
therefore verifies and skips completed task artifacts. A workflow-wide shared
filesystem lock enforces one active run, while managed executions use Step
Functions state, a conditional DynamoDB lock, and ECS task limits.

JSON logs, Prometheus textfile metrics, native CloudWatch metrics, alert events,
and OpenLineage-compatible JSON events are adopted. An OpenTelemetry SDK is
deferred until a collector/backend and sampling policy are selected; workflow,
task, run, and artifact identifiers already provide correlation fields and can
be mapped to spans without changing task boundaries.
