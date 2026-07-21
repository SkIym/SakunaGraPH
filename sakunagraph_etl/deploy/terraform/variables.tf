variable "project_name" {
  type    = string
  default = "sakunagraph-etl"
}

variable "environment" {
  type    = string
  default = "production"
}

variable "aws_region" {
  type    = string
  default = "ap-southeast-1"
}

variable "subnet_ids" {
  description = "Private subnets used by Fargate tasks."
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security groups allowing required egress and GraphDB access."
  type        = list(string)
}

variable "core_image_uri" {
  description = "Immutable ECR image URI, preferably pinned by digest."
  type        = string
}

variable "schedule_expression" {
  type    = string
  default = "cron(0 4 * * ? *)"
}

variable "schedule_timezone" {
  type    = string
  default = "Asia/Manila"
}

variable "schedule_input" {
  description = "JSON execution input containing CLI command arrays and result-envelope URIs. Empty disables the schedule."
  type        = string
  default     = ""
  validation {
    condition     = var.schedule_input == "" || can(jsondecode(var.schedule_input))
    error_message = "schedule_input must be empty or valid JSON."
  }
}

variable "alert_email" {
  description = "Optional email subscription for failed workflow alerts."
  type        = string
  default     = ""
}

variable "artifact_retention_days" {
  type    = number
  default = 365
}

variable "backup_retention_days" {
  type    = number
  default = 2555
}
