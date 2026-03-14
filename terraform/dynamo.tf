# ─────────────────────────────────────────────────────────
# DynamoDB — EDI Jobs Table
#
# Columns:
#   file_name       (PK)  - PO_583921_20240315.txt
#   po_number             - 583921
#   supplier_id           - TESLA001
#   supplier_name         - Tesla Inc
#   po_date               - 20240315
#   line_count            - 5
#   created_date          - 2024-03-15T09:00:00Z
#   edi_sent_status       - N → Y
# ─────────────────────────────────────────────────────────
resource "aws_dynamodb_table" "edi_jobs" {
  name             = "${var.project_name}-edi-jobs"
  billing_mode     = "PAY_PER_REQUEST"
  hash_key         = "file_name"
  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"

  attribute {
    name = "file_name"
    type = "S"
  }

  tags = local.tags
}