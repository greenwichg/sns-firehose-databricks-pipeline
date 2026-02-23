output "firehose_arns" {
  description = "Map of topic name to Firehose delivery stream ARN"
  value       = { for k, v in aws_kinesis_firehose_delivery_stream.streams : k => v.arn }
}

output "firehose_names" {
  description = "Map of topic name to Firehose delivery stream name"
  value       = { for k, v in aws_kinesis_firehose_delivery_stream.streams : k => v.name }
}
