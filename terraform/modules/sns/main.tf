##############################################
# SNS Topics (Source Account)
##############################################

resource "aws_sns_topic" "topics" {
  for_each = toset(var.topic_names)

  name = "${var.project_name}-${var.environment}-${each.value}"
}

# Allow the Firehose account to subscribe to these topics
resource "aws_sns_topic_policy" "cross_account_subscribe" {
  for_each = aws_sns_topic.topics

  arn = each.value.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowFirehoseAccountSubscribe"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${var.firehose_account_id}:root" }
        Action = [
          "SNS:Subscribe",
          "SNS:Receive"
        ]
        Resource = each.value.arn
      },
      {
        Sid       = "AllowPublishFromSourceAccount"
        Effect    = "Allow"
        Principal = { AWS = "*" }
        Action    = "SNS:Publish"
        Resource  = each.value.arn
        Condition = {
          StringEquals = {
            "AWS:SourceOwner" = data.aws_caller_identity.current.account_id
          }
        }
      }
    ]
  })
}

data "aws_caller_identity" "current" {}
