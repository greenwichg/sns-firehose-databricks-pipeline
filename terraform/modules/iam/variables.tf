variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "sns_source_account_id" {
  type = string
}

variable "firehose_account_id" {
  type = string
}

variable "landing_bucket_arn" {
  type = string
}

variable "sns_topic_arns" {
  description = "Map of topic name to ARN"
  type        = map(string)
}
