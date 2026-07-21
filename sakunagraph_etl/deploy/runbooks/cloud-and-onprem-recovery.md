# Platform recovery checks

## On-premise

1. Restore the shared artifact and logs mount onto an isolated scheduler host.
2. Run `sakuna-etl artifacts verify` for the latest source and alignment
   manifests.
3. Run the integration workflow with `--no-resume` against a maintenance
   GraphDB repository; compare manifests and canonical RDF hashes.
4. Enable the systemd timers and confirm `systemctl list-timers` shows the next
   Asia/Manila execution.
5. Simulate a task termination after artifact commit, rerun with the same
   scheduled time, and confirm the completed task is checksum-verified/skipped.

## Cloud

1. Run `terraform plan` and confirm no artifact bucket replacement or public
   access change.
2. Restore the latest GraphDB backup into a replacement instance.
3. Start the Step Functions state machine manually with test result-envelope
   keys and a maintenance GraphDB endpoint.
4. Terminate one ECS task. Confirm the bounded retry reuses its immutable
   artifact and the result URI rather than publishing a duplicate.
5. Force a validation failure. Confirm the Parallel source state fails before
   Align and Publish, an EventBridge failure event reaches SNS, and the object
   remains under `quarantine/`.
6. Confirm CloudWatch logs contain workflow/task/run identifiers and the
   dashboard reports duration, memory, CPU, successes, and failures.

Store drill evidence outside the artifact retention bucket so a bucket-level
incident does not erase both data and proof of recoverability.
