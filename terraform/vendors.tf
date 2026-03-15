# ─────────────────────────────────────────────────────────
# S3 — Vendor Bucket
#
# Structure per vendor:
#   inbox/        ← 850 PO lands here
#   invoices/     ← 810 Invoice generated here
#   850_archive/  ← 850 moved here after invoice generated
#   810_archive/  ← 810 moved here after routed to pipeline
# ─────────────────────────────────────────────────────────
resource "aws_s3_bucket" "edi_vendors" {
  bucket = "${var.project_name}-vendors-${var.environment}"
  tags   = local.tags
}

resource "aws_s3_object" "vendor_folders" {
  for_each = toset([
    "tesla/inbox/",
    "tesla/invoices/",
    "tesla/850_archive/",
    "tesla/810_archive/",
    "apple/inbox/",
    "apple/invoices/",
    "apple/850_archive/",
    "apple/810_archive/",
    "samsung/inbox/",
    "samsung/invoices/",
    "samsung/850_archive/",
    "samsung/810_archive/",
    "dell/inbox/",
    "dell/invoices/",
    "dell/850_archive/",
    "dell/810_archive/",
    "hcl/inbox/",
    "hcl/invoices/",
    "hcl/850_archive/",
    "hcl/810_archive/",
  ])

  bucket  = aws_s3_bucket.edi_vendors.bucket
  key     = each.value
  content = ""
}

# ─────────────────────────────────────────────────────────
# S3 — Inbound partner folders in pipeline bucket
# ─────────────────────────────────────────────────────────
resource "aws_s3_object" "inbound_partner_folders" {
  for_each = toset([
    "edi/inbound/TESLA001/",
    "edi/inbound/APPLE001/",
    "edi/inbound/SAMSUNG001/",
    "edi/inbound/DELL001/",
    "edi/inbound/HCL001"
  ])

  bucket  = aws_s3_bucket.edi_pipeline.bucket
  key     = each.value
  content = ""
}

# ─────────────────────────────────────────────────────────
# DynamoDB — Invoices Table
# ─────────────────────────────────────────────────────────
resource "aws_dynamodb_table" "invoices" {
  name         = "${var.project_name}-invoices"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "invoice_number"

  attribute {
    name = "invoice_number"
    type = "S"
  }

  global_secondary_index {
    name            = "PONumberIndex"
    hash_key        = "po_number"
    projection_type = "ALL"
  }

  attribute {
    name = "po_number"
    type = "S"
  }

  global_secondary_index {
    name            = "PaymentStatusIndex"
    hash_key        = "payment_status"
    projection_type = "ALL"
  }

  attribute {
    name = "payment_status"
    type = "S"
  }

  tags = local.tags
}

# ─────────────────────────────────────────────────────────
# SNS — Invoice Notifications
# ─────────────────────────────────────────────────────────
resource "aws_sns_topic" "invoice_notifications" {
  name = "${var.project_name}-invoice-notifications"
  tags = local.tags
}

resource "aws_sns_topic_subscription" "invoice_email" {
  topic_arn = aws_sns_topic.invoice_notifications.arn
  protocol  = "email"
  endpoint  = var.alert_email
}
