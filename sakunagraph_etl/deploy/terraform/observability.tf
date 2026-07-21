resource "aws_sns_topic" "alerts" {
  name = "${var.project_name}-${var.environment}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_event_rule" "workflow_failure" {
  name = "${var.project_name}-${var.environment}-workflow-failure"
  event_pattern = jsonencode({
    source        = ["aws.states"]
    "detail-type" = ["Step Functions Execution Status Change"]
    detail = {
      status          = ["FAILED", "TIMED_OUT", "ABORTED"]
      stateMachineArn = [aws_sfn_state_machine.etl.arn]
    }
  })
}

resource "aws_cloudwatch_event_target" "workflow_failure" {
  rule = aws_cloudwatch_event_rule.workflow_failure.name
  arn  = aws_sns_topic.alerts.arn
}

resource "aws_sns_topic_policy" "events" {
  arn = aws_sns_topic.alerts.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sns:Publish"
      Resource  = aws_sns_topic.alerts.arn
      Condition = { ArnEquals = { "aws:SourceArn" = aws_cloudwatch_event_rule.workflow_failure.arn } }
    }]
  })
}

resource "aws_cloudwatch_dashboard" "etl" {
  dashboard_name = "${var.project_name}-${var.environment}"
  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric", width = 12, height = 6, x = 0, y = 0
        properties = {
          title = "Step Functions executions", region = var.aws_region, view = "timeSeries"
          metrics = [
            ["AWS/States", "ExecutionsFailed", "StateMachineArn", aws_sfn_state_machine.etl.arn],
            [".", "ExecutionsSucceeded", ".", "."],
            [".", "ExecutionTime", ".", "."]
          ]
        }
      },
      {
        type = "metric", width = 12, height = 6, x = 12, y = 0
        properties = {
          title = "Fargate task resources", region = var.aws_region, view = "timeSeries"
          metrics = [
            ["ECS/ContainerInsights", "CpuUtilized", "ClusterName", aws_ecs_cluster.etl.name],
            [".", "MemoryUtilized", ".", "."]
          ]
        }
      }
    ]
  })
}
