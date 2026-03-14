output "s3_bucket_name" {
  value = aws_s3_bucket.edi_pipeline.bucket
}

output "s3_folders" {
  value = [for k, v in aws_s3_object.folders : v.key]
}

output "edi_jobs_table" {
  value = aws_dynamodb_table.edi_jobs.name
}