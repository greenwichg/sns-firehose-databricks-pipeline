output "sns_topic_arns" {
  description = "SNS topic ARNs"
  value       = module.sns.topic_arns
}

output "firehose_stream_arns" {
  description = "Firehose delivery stream ARNs"
  value       = module.kinesis_firehose.firehose_arns
}

output "landing_bucket_arn" {
  description = "S3 landing bucket ARN"
  value       = module.s3.bucket_arn
}

output "databricks_catalog" {
  description = "Databricks Unity Catalog name"
  value       = module.databricks.catalog_name
}

output "databricks_schemas" {
  description = "Databricks schema names"
  value = {
    bronze = module.databricks.bronze_schema
    silver = module.databricks.silver_schema
    gold   = module.databricks.gold_schema
  }
}
