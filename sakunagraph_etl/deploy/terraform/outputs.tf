output "artifact_bucket" { value = aws_s3_bucket.artifacts.id }
output "core_repository_url" { value = aws_ecr_repository.core.repository_url }
output "documents_repository_url" { value = aws_ecr_repository.documents.repository_url }
output "state_machine_arn" { value = aws_sfn_state_machine.etl.arn }
output "workflow_lock_table" { value = aws_dynamodb_table.workflow_lock.name }
output "alert_topic_arn" { value = aws_sns_topic.alerts.arn }
