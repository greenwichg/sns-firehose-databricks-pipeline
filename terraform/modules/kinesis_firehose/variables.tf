variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "topic_names" {
  type = list(string)
}

variable "landing_bucket_arn" {
  type = string
}

variable "landing_bucket_id" {
  type = string
}

variable "firehose_role_arn" {
  type = string
}

variable "sns_subscription_role_arn" {
  type = string
}

variable "sns_source_account_id" {
  type = string
}

variable "buffer_size_mb" {
  type    = number
  default = 5
}

variable "buffer_interval_seconds" {
  type    = number
  default = 300
}
