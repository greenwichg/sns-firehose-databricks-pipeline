##############################################
# General
##############################################
variable "project_name" {
  description = "Project name used as a prefix for all resources"
  type        = string
  default     = "sns-firehose-pipeline"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "aws_region" {
  description = "AWS region for the Firehose / S3 account"
  type        = string
  default     = "us-east-1"
}

##############################################
# Cross-account
##############################################
variable "sns_source_account_id" {
  description = "AWS account ID that owns the SNS topics"
  type        = string
}

variable "firehose_account_id" {
  description = "AWS account ID where Firehose and S3 reside"
  type        = string
}

##############################################
# SNS
##############################################
variable "sns_topic_names" {
  description = "List of SNS topic names to create in the source account"
  type        = list(string)
  default     = ["orders", "customers", "products"]
}

##############################################
# Firehose
##############################################
variable "firehose_buffer_size_mb" {
  description = "Firehose buffer size in MB before delivery"
  type        = number
  default     = 5
}

variable "firehose_buffer_interval_seconds" {
  description = "Firehose buffer interval in seconds before delivery"
  type        = number
  default     = 300
}

##############################################
# S3
##############################################
variable "landing_bucket_name" {
  description = "S3 bucket name for the raw data landing zone"
  type        = string
}

##############################################
# Databricks
##############################################
variable "databricks_workspace_url" {
  description = "Databricks workspace URL"
  type        = string
}

variable "databricks_account_id" {
  description = "Databricks account ID for Unity Catalog"
  type        = string
}

variable "unity_catalog_name" {
  description = "Unity Catalog catalog name"
  type        = string
  default     = "pipeline_catalog"
}

variable "databricks_storage_credential_name" {
  description = "Name of the existing storage credential for S3 access"
  type        = string
  default     = "s3_landing_credential"
}

variable "databricks_external_location_url" {
  description = "S3 URL for the external location (s3://bucket/path)"
  type        = string
}

variable "unity_catalog_roles" {
  description = "Map of role name to list of principals for Gold layer access"
  type        = map(list(string))
  default = {
    analysts = []
    engineers = []
    data_scientists = []
  }
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
