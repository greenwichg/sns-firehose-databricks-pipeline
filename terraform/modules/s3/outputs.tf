output "bucket_arn" {
  value = aws_s3_bucket.landing.arn
}

output "bucket_id" {
  value = aws_s3_bucket.landing.id
}

output "bucket_domain_name" {
  value = aws_s3_bucket.landing.bucket_regional_domain_name
}
