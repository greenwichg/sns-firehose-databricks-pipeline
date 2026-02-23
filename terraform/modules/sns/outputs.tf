output "topic_arns" {
  description = "Map of topic name to ARN"
  value       = { for k, v in aws_sns_topic.topics : k => v.arn }
}

output "topic_ids" {
  description = "Map of topic name to ID"
  value       = { for k, v in aws_sns_topic.topics : k => v.id }
}
