##############################################
# IAM Roles (Cross-Account)
##############################################

# ------------------------------------------
# Firehose Delivery Role (Firehose Account)
# ------------------------------------------
resource "aws_iam_role" "firehose_delivery" {
  provider = aws.firehose_account

  name = "${var.project_name}-${var.environment}-firehose-delivery"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "firehose.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "firehose_s3_access" {
  provider = aws.firehose_account

  name = "firehose-s3-delivery"
  role = aws_iam_role.firehose_delivery.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:AbortMultipartUpload",
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:PutObject"
        ]
        Resource = [
          var.landing_bucket_arn,
          "${var.landing_bucket_arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "kms:ViaService" = "s3.*.amazonaws.com"
          }
        }
      }
    ]
  })
}

# ------------------------------------------
# SNS Subscription Role (Firehose Account)
# Allows Firehose to be subscribed to cross-account SNS
# ------------------------------------------
resource "aws_iam_role" "sns_subscription" {
  provider = aws.firehose_account

  name = "${var.project_name}-${var.environment}-sns-subscription"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "sns.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "sns_to_firehose" {
  provider = aws.firehose_account

  name = "sns-firehose-delivery"
  role = aws_iam_role.sns_subscription.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "firehose:DescribeDeliveryStream",
          "firehose:ListDeliveryStreams",
          "firehose:ListTagsForDeliveryStream",
          "firehose:PutRecord",
          "firehose:PutRecordBatch"
        ]
        Resource = "arn:aws:firehose:*:${var.firehose_account_id}:deliverystream/${var.project_name}-${var.environment}-*"
      }
    ]
  })
}

# ------------------------------------------
# Databricks S3 Access Role (Firehose Account)
# ------------------------------------------
resource "aws_iam_role" "databricks_s3_access" {
  provider = aws.firehose_account

  name = "${var.project_name}-${var.environment}-databricks-s3-access"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::414351767826:root" # Databricks AWS account
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.project_name
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "databricks_s3_read" {
  provider = aws.firehose_account

  name = "databricks-s3-read"
  role = aws_iam_role.databricks_s3_access.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:GetBucketNotification",
          "s3:PutBucketNotification"
        ]
        Resource = [
          var.landing_bucket_arn,
          "${var.landing_bucket_arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "sqs:DeleteMessage",
          "sqs:ReceiveMessage",
          "sqs:GetQueueUrl",
          "sqs:GetQueueAttributes",
          "sqs:ChangeMessageVisibility"
        ]
        Resource = "arn:aws:sqs:*:${var.firehose_account_id}:${var.project_name}-*"
      }
    ]
  })
}
