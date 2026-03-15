terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.3.0"
}

provider "aws" {
  region = var.aws_region
}

# ─────────────────────────────────────────────────────────
# S3 Bucket
# ─────────────────────────────────────────────────────────
resource "aws_s3_bucket" "edi_pipeline" {
  bucket = "${var.project_name}-${var.environment}"
  tags   = local.tags
}

# ─────────────────────────────────────────────────────────
# S3 Folder Structure
# ─────────────────────────────────────────────────────────
resource "aws_s3_object" "folders" {
  for_each = toset([
    "flatfiles/inbox/",
    "flatfiles/processed/",
    "edi/outbound/",
    "edi/cancelled/",
    "edi/archive/",
    "edi/inbound_archive/",       
    "edi/inbound_flatfiles/"   
  ])

  bucket  = aws_s3_bucket.edi_pipeline.bucket
  key     = each.value
  content = ""
}

locals {
  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}