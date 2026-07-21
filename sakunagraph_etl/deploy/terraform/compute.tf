data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "task_execution" {
  name_prefix        = "${var.project_name}-execution-"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "task_execution" {
  role       = aws_iam_role.task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "task" {
  name_prefix        = "${var.project_name}-task-"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

data "aws_iam_policy_document" "task" {
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.artifacts.arn]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.artifacts.arn}/*"]
  }
}

resource "aws_iam_role_policy" "task" {
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task.json
}

resource "aws_ecs_cluster" "etl" {
  name = "${var.project_name}-${var.environment}"
  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_cloudwatch_log_group" "etl" {
  name              = "/sakunagraph/etl/${var.environment}"
  retention_in_days = 90
}

resource "aws_ecs_task_definition" "core" {
  family                   = "${var.project_name}-${var.environment}-core"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 4096
  memory                   = 16384
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task.arn

  ephemeral_storage { size_in_gib = 100 }

  container_definitions = jsonencode([{
    name      = "etl"
    image     = var.core_image_uri
    essential = true
    command   = ["workflow", "list"]
    environment = [
      { name = "SAKUNA_ETL_PROFILE", value = "cloud" },
      { name = "SAKUNA_OBJECT_BUCKET", value = aws_s3_bucket.artifacts.id },
      { name = "SAKUNA_OBJECT_PREFIX", value = "sakunagraph" },
      { name = "SAKUNA_LOG_FORMAT", value = "json" },
      { name = "AWS_REGION", value = var.aws_region },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.etl.name
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "task"
      }
    }
  }])
}
