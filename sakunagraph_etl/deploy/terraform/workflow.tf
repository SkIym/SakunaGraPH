data "aws_iam_policy_document" "states_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "states" {
  name_prefix        = "${var.project_name}-states-"
  assume_role_policy = data.aws_iam_policy_document.states_assume.json
}

data "aws_iam_policy_document" "states" {
  statement {
    actions   = ["dynamodb:PutItem", "dynamodb:DeleteItem"]
    resources = [aws_dynamodb_table.workflow_lock.arn]
  }
  statement {
    actions   = ["ecs:RunTask", "ecs:StopTask", "ecs:DescribeTasks"]
    resources = [aws_ecs_task_definition.core.arn, "*"]
  }
  statement {
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.task.arn, aws_iam_role.task_execution.arn]
  }
  statement {
    actions   = ["events:PutTargets", "events:PutRule", "events:DescribeRule"]
    resources = ["arn:aws:events:${var.aws_region}:*:rule/StepFunctionsGetEventsForECSTaskRule"]
  }
  statement {
    actions = [
      "logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery", "logs:ListLogDeliveries", "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies", "logs:DescribeLogGroups"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "states" {
  role   = aws_iam_role.states.id
  policy = data.aws_iam_policy_document.states.json
}

locals {
  workflow_lock_name = "${var.project_name}-${var.environment}"

  network_configuration = {
    AwsvpcConfiguration = {
      Subnets        = var.subnet_ids
      SecurityGroups = var.security_group_ids
      AssignPublicIp = "DISABLED"
    }
  }

  ecs_task_parameters = {
    Cluster              = aws_ecs_cluster.etl.arn
    TaskDefinition       = aws_ecs_task_definition.core.arn
    LaunchType           = "FARGATE"
    NetworkConfiguration = local.network_configuration
  }

  source_branches = [
    for source in ["emdat", "gda", "psgc", "ndrrmc", "dromic"] : {
      StartAt = source
      States = {
        (source) = {
          Type     = "Task"
          Resource = "arn:aws:states:::ecs:runTask.sync"
          Parameters = merge(local.ecs_task_parameters, {
            Overrides = { ContainerOverrides = [{ Name = "etl", "Command.$" = "$.commands.${source}" }] }
          })
          Retry = [{
            ErrorEquals     = ["States.Timeout", "AmazonECS.Unknown", "States.TaskFailed"]
            IntervalSeconds = 30
            MaxAttempts     = 2
            BackoffRate     = 2
          }]
          TimeoutSeconds = 14400
          End            = true
        }
      }
    }
  ]
}

resource "aws_sfn_state_machine" "etl" {
  name     = "${var.project_name}-${var.environment}"
  role_arn = aws_iam_role.states.arn
  type     = "STANDARD"

  logging_configuration {
    include_execution_data = true
    level                  = "ERROR"
    log_destination        = "${aws_cloudwatch_log_group.states.arn}:*"
  }

  definition = jsonencode({
    Comment = "SakunaGraPH CLI-bound ETL; commands and result-envelope URIs are execution input."
    StartAt = "AcquireLock"
    States = {
      AcquireLock = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:dynamodb:putItem"
        Parameters = {
          TableName = aws_dynamodb_table.workflow_lock.name
          Item = {
            LockName     = { S = local.workflow_lock_name }
            ExecutionArn = { "S.$" = "$$.Execution.Id" }
            AcquiredAt   = { "S.$" = "$$.State.EnteredTime" }
          }
          ConditionExpression = "attribute_not_exists(LockName)"
        }
        ResultPath = null
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.lockFailure"
          Next        = "LockUnavailable"
        }]
        Next = "Sources"
      }
      Sources = {
        Type       = "Parallel"
        Branches   = local.source_branches
        ResultPath = "$.sourceTaskResults"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.failure"
          Next        = "ReleaseFailedLock"
        }]
        Next = "Align"
      }
      Align = {
        Type     = "Task"
        Resource = "arn:aws:states:::ecs:runTask.sync"
        Parameters = merge(local.ecs_task_parameters, {
          Overrides = { ContainerOverrides = [{ Name = "etl", "Command.$" = "$.commands.align" }] }
        })
        Retry          = [{ ErrorEquals = ["AmazonECS.Unknown"], IntervalSeconds = 60, MaxAttempts = 2, BackoffRate = 2 }]
        TimeoutSeconds = 7200
        ResultPath     = "$.alignTaskResult"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.failure"
          Next        = "ReleaseFailedLock"
        }]
        Next = "Publish"
      }
      Publish = {
        Type     = "Task"
        Resource = "arn:aws:states:::ecs:runTask.sync"
        Parameters = merge(local.ecs_task_parameters, {
          Overrides = { ContainerOverrides = [{ Name = "etl", "Command.$" = "$.commands.publish" }] }
        })
        Retry          = [{ ErrorEquals = ["AmazonECS.Unknown"], IntervalSeconds = 60, MaxAttempts = 2, BackoffRate = 2 }]
        TimeoutSeconds = 1800
        ResultPath     = "$.publishTaskResult"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.failure"
          Next        = "ReleaseFailedLock"
        }]
        Next = "ReleaseLock"
      }
      ReleaseLock = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:dynamodb:deleteItem"
        Parameters = {
          TableName = aws_dynamodb_table.workflow_lock.name
          Key       = { LockName = { S = local.workflow_lock_name } }
        }
        ResultPath = null
        Next       = "Completed"
      }
      ReleaseFailedLock = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:dynamodb:deleteItem"
        Parameters = {
          TableName = aws_dynamodb_table.workflow_lock.name
          Key       = { LockName = { S = local.workflow_lock_name } }
        }
        ResultPath = null
        Next       = "WorkflowFailed"
      }
      Completed = {
        Type = "Succeed"
      }
      WorkflowFailed = {
        Type  = "Fail"
        Error = "SakunaGraphEtlFailed"
        Cause = "A CLI-bound ETL task failed; inspect execution input and CloudWatch logs."
      }
      LockUnavailable = {
        Type  = "Fail"
        Error = "WorkflowLockUnavailable"
        Cause = "Another execution is active or the managed workflow lock requires recovery."
      }
    }
  })
}

resource "aws_dynamodb_table" "workflow_lock" {
  name         = "${var.project_name}-${var.environment}-workflow-lock"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockName"

  attribute {
    name = "LockName"
    type = "S"
  }

  server_side_encryption {
    enabled = true
  }
}

resource "aws_cloudwatch_log_group" "states" {
  name              = "/aws/vendedlogs/states/${var.project_name}-${var.environment}"
  retention_in_days = 90
}

data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name_prefix        = "${var.project_name}-scheduler-"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}

resource "aws_iam_role_policy" "scheduler" {
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "states:StartExecution", Resource = aws_sfn_state_machine.etl.arn }]
  })
}

resource "aws_scheduler_schedule" "etl" {
  count                        = var.schedule_input == "" ? 0 : 1
  name                         = "${var.project_name}-${var.environment}"
  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = var.schedule_timezone
  flexible_time_window { mode = "OFF" }
  target {
    arn      = aws_sfn_state_machine.etl.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = var.schedule_input
    retry_policy {
      maximum_event_age_in_seconds = 3600
      maximum_retry_attempts       = 2
    }
  }
}
