output "firehose_role_arn" {
  value = aws_iam_role.firehose_delivery.arn
}

output "sns_subscription_role_arn" {
  value = aws_iam_role.sns_subscription.arn
}

output "databricks_s3_access_role_arn" {
  value = aws_iam_role.databricks_s3_access.arn
}
