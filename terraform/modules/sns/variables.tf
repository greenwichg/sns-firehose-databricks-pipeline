variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "topic_names" {
  type = list(string)
}

variable "firehose_account_id" {
  type = string
}
