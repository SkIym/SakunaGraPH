# AWS managed workflow deployment

This Terraform stack selects AWS Step Functions Standard workflows with
ECS/Fargate tasks and EventBridge Scheduler. It assumes private subnets and
GraphDB connectivity already exist. Apply it only after pushing the Stage 7
core image to the immutable ECR repository and supplying its digest URI.

```bash
terraform init
terraform plan \
  -var='subnet_ids=["subnet-a","subnet-b"]' \
  -var='security_group_ids=["sg-etl"]' \
  -var='core_image_uri=ACCOUNT.dkr.ecr.ap-southeast-1.amazonaws.com/sakunagraph-etl/core@sha256:DIGEST'
terraform apply
```

The state machine input contains only CLI command arrays and small result URIs.
Each command should use `workflow task`, which writes a result envelope after
the immutable artifact manifest is committed. EventBridge Scheduler replaces
its context placeholders on each invocation, making run IDs and result keys
unique while keeping retries stable:

```json
{
  "commands": {
    "emdat": [
      "workflow", "task", "--task-id", "emdat",
      "--run-id", "<aws.scheduler.execution-id>",
      "--result-uri", "s3://BUCKET/workflow-results/<aws.scheduler.execution-id>/emdat.json",
      "--work-dir", "/tmp/sakuna-task", "--",
      "emdat", "--input", "/work/input/emdat.xlsx", "--out-dir", "/tmp/output",
      "--validate", "--profile", "cloud"
    ],
    "gda": ["workflow", "task", "..."],
    "psgc": ["workflow", "task", "..."],
    "ndrrmc": ["workflow", "task", "..."],
    "dromic": ["workflow", "task", "..."],
    "align": [
      "workflow", "task", "--task-id", "align",
      "--run-id", "<aws.scheduler.execution-id>",
      "--result-uri", "s3://BUCKET/workflow-results/<aws.scheduler.execution-id>/align.json",
      "--input-result", "emdat=s3://BUCKET/workflow-results/<aws.scheduler.execution-id>/emdat.json",
      "--input-result", "gda=s3://BUCKET/workflow-results/<aws.scheduler.execution-id>/gda.json",
      "--", "align", "--sources", "/tmp/sakuna-task/dependencies",
      "--resolution-dir", "/tmp/resolution", "--profile", "cloud"
    ],
    "publish": [
      "workflow", "task", "--task-id", "publish", "--no-artifacts",
      "--run-id", "<aws.scheduler.execution-id>",
      "--result-uri", "s3://BUCKET/workflow-results/<aws.scheduler.execution-id>/publish.json",
      "--input-result", "align=s3://BUCKET/workflow-results/<aws.scheduler.execution-id>/align.json",
      "--", "load-graphdb", "--input-manifest",
      "/tmp/sakuna-task/dependencies/align/0/manifest.json",
      "--replace", "--validate", "--profile", "cloud"
    ]
  }
}
```

Source-specific collection or raw-manifest materialization can precede each
source command. Do not put credentials, RDF, or raw files in state-machine
input. S3 keys and run metadata are sufficient.

Scheduled executions are disabled until `schedule_input` is supplied. This
prevents a newly applied stack from publishing before its commands, GraphDB
network path, and alert subscription have been tested manually.

The state machine conditionally acquires one item in the output
`workflow_lock_table`, so scheduled and manual executions cannot overlap.
Normal success and failure paths release it; follow the workflow operations
runbook after a force-aborted execution instead of deleting a live lock.
