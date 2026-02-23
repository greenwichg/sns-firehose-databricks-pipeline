environment           = "dev"
project_name          = "sns-firehose-pipeline"
aws_region            = "us-east-1"
sns_source_account_id = "111111111111"  # Replace with actual SNS account ID
firehose_account_id   = "222222222222"  # Replace with actual Firehose account ID
landing_bucket_name   = "sns-firehose-pipeline-dev-landing"

# SNS topics that map to data domains
sns_topic_names = ["orders", "customers", "products"]

# Firehose buffering
firehose_buffer_size_mb          = 5
firehose_buffer_interval_seconds = 300

# Databricks
databricks_workspace_url          = "https://your-workspace.cloud.databricks.com"
databricks_account_id             = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
unity_catalog_name                = "pipeline_catalog"
databricks_storage_credential_name = "s3_landing_credential"
databricks_external_location_url  = "s3://sns-firehose-pipeline-dev-landing/"

unity_catalog_roles = {
  analysts        = ["analysts_group"]
  engineers       = ["engineers_group"]
  data_scientists = ["data_scientists_group"]
}

tags = {
  Team    = "data-engineering"
  CostCenter = "12345"
}
