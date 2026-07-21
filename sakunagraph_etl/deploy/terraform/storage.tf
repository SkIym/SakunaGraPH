resource "aws_s3_bucket" "artifacts" {
  bucket_prefix = "${var.project_name}-${var.environment}-"
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket     = aws_s3_bucket.artifacts.id
  depends_on = [aws_s3_bucket_versioning.artifacts]

  rule {
    id     = "immutable-run-retention"
    status = "Enabled"
    filter { prefix = "sakunagraph/runs/" }
    expiration { days = var.artifact_retention_days }
    noncurrent_version_expiration { noncurrent_days = 30 }
  }

  rule {
    id     = "quarantine-retention"
    status = "Enabled"
    filter { prefix = "sakunagraph/quarantine/" }
    expiration { days = 90 }
    noncurrent_version_expiration { noncurrent_days = 30 }
  }

  rule {
    id     = "graphdb-backup-retention"
    status = "Enabled"
    filter { prefix = "graphdb-backups/" }
    transition {
      days          = 30
      storage_class = "GLACIER_IR"
    }
    expiration { days = var.backup_retention_days }
    noncurrent_version_expiration { noncurrent_days = 90 }
  }

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}

resource "aws_ecr_repository" "core" {
  name                 = "${var.project_name}/core"
  image_tag_mutability = "IMMUTABLE"
  image_scanning_configuration { scan_on_push = true }
  encryption_configuration { encryption_type = "AES256" }
}

resource "aws_ecr_repository" "documents" {
  name                 = "${var.project_name}/documents"
  image_tag_mutability = "IMMUTABLE"
  image_scanning_configuration { scan_on_push = true }
  encryption_configuration { encryption_type = "AES256" }
}

resource "aws_ecr_lifecycle_policy" "images" {
  for_each = {
    core      = aws_ecr_repository.core.name
    documents = aws_ecr_repository.documents.name
  }
  repository = each.value
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Retain the 20 most recent release images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 20
      }
      action = { type = "expire" }
    }]
  })
}
