##############################################
# S3 Landing Zone (Firehose Account)
##############################################

resource "aws_s3_bucket" "landing" {
  bucket = var.bucket_name

  tags = {
    Name = "${var.project_name}-${var.environment}-landing"
  }
}

resource "aws_s3_bucket_versioning" "landing" {
  bucket = aws_s3_bucket.landing.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id

  rule {
    id     = "archive-old-data"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = 365
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "landing" {
  bucket = aws_s3_bucket.landing.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Create prefix structure for each topic
resource "aws_s3_object" "topic_prefixes" {
  for_each = toset(var.topic_names)

  bucket  = aws_s3_bucket.landing.id
  key     = "${each.value}/"
  content = ""
}

# Checkpoint location for Autoloader
resource "aws_s3_object" "checkpoint_prefix" {
  bucket  = aws_s3_bucket.landing.id
  key     = "_checkpoints/"
  content = ""
}
