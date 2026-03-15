# ─────────────────────────────────────────────────────────
# S3 — Vendor Bucket
#
# Structure:
#   tesla/inbox/       ← 850 PO copied here
#   tesla/invoices/    ← Tesla drops 810 invoice here
#   apple/inbox/
#   apple/invoices/
#   samsung/inbox/
#   samsung/invoices/
#   dell/inbox/
#   dell/invoices/
# ─────────────────────────────────────────────────────────
resource "aws_s3_bucket" "edi_vendors" {
  bucket = "${var.project_name}-vendors-${var.environment}"
  tags   = local.tags
}

resource "aws_s3_object" "vendor_folders" {
  for_each = toset([
    "tesla/inbox/",
    "tesla/invoices/",
    "apple/inbox/",
    "apple/invoices/",
    "samsung/inbox/",
    "samsung/invoices/",
    "dell/inbox/",
    "dell/invoices/",
    "hcl/inbox/",
    "hcl/invoices/"
  ])

  bucket  = aws_s3_bucket.edi_vendors.bucket
  key     = each.value
  content = ""
}

# ─────────────────────────────────────────────────────────
# S3 — Inbound partner folders in pipeline bucket
#
# Invoices routed here by partner ID:
#   edi/inbound/TESLA001/
#   edi/inbound/APPLE001/
#   edi/inbound/SAMSUNG001/
#   edi/inbound/DELL001/
#   edi/inbound/HCL001/
# ─────────────────────────────────────────────────────────
resource "aws_s3_object" "inbound_partner_folders" {
  for_each = toset([
    "edi/inbound/TESLA001/",
    "edi/inbound/APPLE001/",
    "edi/inbound/SAMSUNG001/",
    "edi/inbound/DELL001/",
    "edi/inbound/HCL001/"
  ])

  bucket  = aws_s3_bucket.edi_pipeline.bucket
  key     = each.value
  content = ""
}

# ─────────────────────────────────────────────────────────
# DynamoDB — Invoices Table
#
# Columns:
#   invoice_number  (PK)
#   po_number       (GSI)
#   partner_id
#   partner_name
#   invoice_date
#   due_date
#   line_items
#   total_amount
#   tds_amount
#   currency
#   payment_status  UNPAID → PAID
#   s3_edi_key
#   s3_flat_key
#   received_date
#   paid_date
#   error_message
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
#
# Triggers:
#   ✅ Invoice loaded successfully → payment reminder
#   ❌ Partner ID not found
#   ❌ Item number mismatch
#   ❌ TDS amount mismatch
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
