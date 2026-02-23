##############################################
# Kinesis Firehose Delivery Streams
# (Firehose Account — receives from cross-account SNS)
##############################################

resource "aws_kinesis_firehose_delivery_stream" "streams" {
  for_each = toset(var.topic_names)

  name        = "${var.project_name}-${var.environment}-${each.value}"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn   = var.firehose_role_arn
    bucket_arn = var.landing_bucket_arn

    prefix              = "${each.value}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "${each.value}_errors/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/!{firehose:error-output-type}/"

    buffering_size     = var.buffer_size_mb
    buffering_interval = var.buffer_interval_seconds
    compression_format = "GZIP"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = "/aws/kinesisfirehose/${var.project_name}-${var.environment}-${each.value}"
      log_stream_name = "DestinationDelivery"
    }
  }

  tags = {
    Topic = each.value
  }
}

# CloudWatch log groups for Firehose
resource "aws_cloudwatch_log_group" "firehose" {
  for_each = toset(var.topic_names)

  name              = "/aws/kinesisfirehose/${var.project_name}-${var.environment}-${each.value}"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_stream" "firehose" {
  for_each = aws_cloudwatch_log_group.firehose

  name           = "DestinationDelivery"
  log_group_name = each.value.name
}

# SNS Subscriptions (cross-account SNS -> Firehose)
resource "aws_sns_topic_subscription" "sns_to_firehose" {
  for_each = toset(var.topic_names)

  topic_arn = "arn:aws:sns:${data.aws_region.current.name}:${var.sns_source_account_id}:${var.project_name}-${var.environment}-${each.value}"
  protocol  = "firehose"
  endpoint  = aws_kinesis_firehose_delivery_stream.streams[each.value].arn

  subscription_role_arn = var.sns_subscription_role_arn

  raw_message_delivery = true
}

data "aws_region" "current" {}
