##############################################
# Providers
##############################################

# Provider for the account that owns SNS topics
provider "aws" {
  alias  = "sns_account"
  region = var.aws_region

  assume_role {
    role_arn = "arn:aws:iam::${var.sns_source_account_id}:role/${var.project_name}-terraform-role"
  }

  default_tags {
    tags = merge(var.tags, {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    })
  }
}

# Provider for the account that owns Firehose + S3
provider "aws" {
  alias  = "firehose_account"
  region = var.aws_region

  assume_role {
    role_arn = "arn:aws:iam::${var.firehose_account_id}:role/${var.project_name}-terraform-role"
  }

  default_tags {
    tags = merge(var.tags, {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    })
  }
}

provider "databricks" {
  host = var.databricks_workspace_url
}

##############################################
# Modules
##############################################

module "s3" {
  source = "./modules/s3"

  providers = {
    aws = aws.firehose_account
  }

  bucket_name  = var.landing_bucket_name
  project_name = var.project_name
  environment  = var.environment
  topic_names  = var.sns_topic_names
}

module "iam" {
  source = "./modules/iam"

  providers = {
    aws.sns_account      = aws.sns_account
    aws.firehose_account = aws.firehose_account
  }

  project_name          = var.project_name
  environment           = var.environment
  sns_source_account_id = var.sns_source_account_id
  firehose_account_id   = var.firehose_account_id
  landing_bucket_arn    = module.s3.bucket_arn
  sns_topic_arns        = module.sns.topic_arns
}

module "sns" {
  source = "./modules/sns"

  providers = {
    aws = aws.sns_account
  }

  project_name        = var.project_name
  environment         = var.environment
  topic_names         = var.sns_topic_names
  firehose_account_id = var.firehose_account_id
}

module "kinesis_firehose" {
  source = "./modules/kinesis_firehose"

  providers = {
    aws = aws.firehose_account
  }

  project_name              = var.project_name
  environment               = var.environment
  topic_names               = var.sns_topic_names
  landing_bucket_arn        = module.s3.bucket_arn
  landing_bucket_id         = module.s3.bucket_id
  firehose_role_arn         = module.iam.firehose_role_arn
  sns_subscription_role_arn = module.iam.sns_subscription_role_arn
  sns_source_account_id     = var.sns_source_account_id
  buffer_size_mb            = var.firehose_buffer_size_mb
  buffer_interval_seconds   = var.firehose_buffer_interval_seconds
}

module "databricks" {
  source = "./modules/databricks"

  catalog_name              = var.unity_catalog_name
  environment               = var.environment
  project_name              = var.project_name
  storage_credential_name   = var.databricks_storage_credential_name
  external_location_url     = var.databricks_external_location_url
  landing_bucket_arn        = module.s3.bucket_arn
  roles                     = var.unity_catalog_roles
}
