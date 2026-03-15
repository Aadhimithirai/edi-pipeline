"""
Invoice Processor
Reads 810 EDI from edi/inbound/PARTNER_ID/
Validates:
  1. Partner ID exists in partners.json
  2. Item numbers match original 850 PO in DynamoDB
  3. TDS total matches sum of line items
Converts EDI to flat file → uploads to S3 inbound_flatfiles/PARTNER_ID/
Archives .edi to edi/inbound_archive/PARTNER_ID/
Loads invoice record to DynamoDB invoices table
Sends SNS email notification

Flow:
  edi/inbound/SAMSUNG001/INV_847291.edi
        ↓  validate (partner, items, TDS)
        ↓  convert to flat file
        ↓  upload flat file → edi/inbound_flatfiles/SAMSUNG001/
        ↓  archive .edi    → edi/inbound_archive/SAMSUNG001/
        ↓  load DynamoDB invoices table
        ↓  send SNS email
"""

import boto3
import json
import sys
import os
from datetime import datetime, timezone
from decimal import Decimal
from botocore.exceptions import NoCredentialsError, ClientError

# ─────────────────────────────────────────────────────────
# CONFIG — update to match your resources
# ─────────────────────────────────────────────────────────
AWS_REGION       = "ap-south-1"
PIPELINE_BUCKET  = "edi-pipeline-dev"
INBOUND_PREFIX   = "edi/inbound/"
ARCHIVE_PREFIX   = "edi/inbound_archive/"
FLATFILES_PREFIX = "edi/inbound_flatfiles/"
DYNAMODB_JOBS    = "edi-pipeline-edi-jobs"
DYNAMODB_INV     = "edi-pipeline-invoices"
PARTNERS_FILE    = "config/partners.json"
SNS_TOPIC_ARN    = "arn:aws:sns:ap-south-1:730335456971:edi-pipeline-invoice-notifications"   # ← paste your SNS topic ARN here

PARTNERS_IDS = ["TESLA001", "APPLE001", "SAMSUNG001", "DELL001", "HCL001"]

# ─────────────────────────────────────────────────────────
# AWS clients
# ─────────────────────────────────────────────────────────
s3         = boto3.client("s3",         region_name=AWS_REGION)
sns        = boto3.client("sns",        region_name=AWS_REGION)
dynamodb   = boto3.resource("dynamodb", region_name=AWS_REGION)
jobs_table = dynamodb.Table(DYNAMODB_JOBS)
inv_table  = dynamodb.Table(DYNAMODB_INV)


# ─────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────
def divider(title=""):
    print("\n" + "─" * 52)
    if title:
        print(f"  {title}")
        print("─" * 52)


def log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"  [{ts}] {msg}")


def send_sns(subject: str, message: str):
    """Send SNS email notification."""
    if not SNS_TOPIC_ARN:
        log(f"⚠️  SNS_TOPIC_ARN not set — skipping email")
        log(f"   Subject: {subject}")
        return
    try:
        sns.publish(
            TopicArn = SNS_TOPIC_ARN,
            Subject  = subject[:100],
            Message  = message
        )
        log(f"📧 Email sent: {subject}")
    except ClientError as e:
        log(f"❌ SNS error: {e.response['Error']['Message']}")


# ─────────────────────────────────────────────────────────
# Load trading partners
# ─────────────────────────────────────────────────────────
def load_partners() -> dict:
    if not os.path.exists(PARTNERS_FILE):
        raise FileNotFoundError(f"Partners file not found: {PARTNERS_FILE}")
    with open(PARTNERS_FILE, "r") as f:
        data = json.load(f)
    return {
        p["partner_id"]: p
        for p in data["trading_partners"]
        if p.get("active", True)
    }


# ─────────────────────────────────────────────────────────
# List 810 EDI files in inbound/PARTNER_ID/
# ─────────────────────────────────────────────────────────
def list_inbound_invoices(partner_id: str) -> list:
    response = s3.list_objects_v2(
        Bucket = PIPELINE_BUCKET,
        Prefix = f"{INBOUND_PREFIX}{partner_id}/"
    )
    return [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".edi")
    ]


# ─────────────────────────────────────────────────────────
# Parse X12 810 Invoice EDI
# ─────────────────────────────────────────────────────────
def parse_810_invoice(edi_content: str) -> dict:
    """
    Parse X12 810 EDI and extract all invoice details.
    Returns invoice_number, po_number, partner_id,
            invoice_date, due_date, items, tds_amount
    """
    invoice_number = None
    po_number      = None
    partner_id     = None
    invoice_date   = None
    due_date       = None
    tds_amount     = None
    items          = []

    for line in edi_content.splitlines():
        line  = line.strip().rstrip("~")
        parts = line.split("*")
        tag   = parts[0]

        # ISA — sender ID (partner) at ISA06
        if tag == "ISA" and len(parts) >= 7:
            partner_id = parts[6].strip()

        # BIG — invoice header
        # BIG*InvoiceDate*InvoiceNumber**PONumber*DueDate
        if tag == "BIG" and len(parts) >= 5:
            invoice_date   = parts[1].strip()
            invoice_number = parts[2].strip()
            po_number      = parts[4].strip()
            due_date       = parts[5].strip() if len(parts) > 5 else ""

        # REF — PO reference backup
        if tag == "REF" and len(parts) >= 3:
            if parts[1].strip() == "PO":
                po_number = po_number or parts[2].strip()

        # IT1 — line item
        # IT1*LineNo*Qty*UOM*UnitPrice*PE*BP*ItemNumber
        if tag == "IT1" and len(parts) >= 5:
            qty         = parts[2].strip()
            uom         = parts[3].strip()
            unit_price  = parts[4].strip()
            item_number = ""
            for i in range(len(parts) - 1):
                if parts[i].strip() == "BP":
                    item_number = parts[i + 1].strip()
                    break
            if item_number:
                qty_f   = float(qty)        if qty        else 1.0
                price_f = float(unit_price) if unit_price else 0.0
                items.append({
                    "item_number": item_number,
                    "quantity":    qty_f,
                    "uom":         uom,
                    "unit_price":  price_f,
                    "line_total":  qty_f * price_f,
                })

        # TDS — total amount in cents
        if tag == "TDS" and len(parts) >= 2:
            tds_amount = float(parts[1].strip()) / 100

    if not invoice_number:
        raise ValueError("Could not parse invoice number from BIG segment")
    if not po_number:
        raise ValueError("Could not parse PO number from BIG/REF segment")

    return {
        "invoice_number": invoice_number,
        "po_number":      po_number,
        "partner_id":     partner_id,
        "invoice_date":   invoice_date,
        "due_date":       due_date,
        "items":          items,
        "tds_amount":     tds_amount,
    }


# ─────────────────────────────────────────────────────────
# VALIDATION 1 — Partner ID in partners.json
# ─────────────────────────────────────────────────────────
def validate_partner(partner_id: str, partners: dict) -> tuple:
    if partner_id not in partners:
        return False, f"Partner ID '{partner_id}' not found in partners.json"
    return True, None


# ─────────────────────────────────────────────────────────
# VALIDATION 2 — Item numbers match original 850 PO
# ─────────────────────────────────────────────────────────
def validate_items(invoice: dict) -> tuple:
    po_number = invoice["po_number"]

    response = jobs_table.scan(
        FilterExpression=boto3.dynamodb.conditions.Attr("po_number").eq(po_number)
    )
    records = response.get("Items", [])

    if not records:
        return False, f"Original PO {po_number} not found in DynamoDB"

    po_record       = records[0]
    po_items        = po_record.get("items", [])
    po_item_numbers = {item["item_number"] for item in po_items}

    for inv_item in invoice["items"]:
        item_number = inv_item["item_number"]
        if item_number not in po_item_numbers:
            return False, (
                f"Item '{item_number}' in invoice not found in "
                f"original PO {po_number}. "
                f"PO items: {', '.join(po_item_numbers)}"
            )

    return True, None


# ─────────────────────────────────────────────────────────
# VALIDATION 3 — TDS total matches sum of line items
# ─────────────────────────────────────────────────────────
def validate_tds(invoice: dict) -> tuple:
    calculated = sum(item["line_total"] for item in invoice["items"])
    tds        = invoice.get("tds_amount", 0)

    if abs(calculated - tds) > 0.01:
        return False, (
            f"TDS mismatch — "
            f"Line items sum: ${calculated:,.2f} | "
            f"TDS segment: ${tds:,.2f}"
        )
    return True, None


# ─────────────────────────────────────────────────────────
# Convert 810 EDI to flat file
# ─────────────────────────────────────────────────────────
def generate_flat_file(invoice: dict, partner_name: str) -> str:
    """
    Convert parsed 810 to pipe-delimited flat file.

    HEADER|INVOICE_NO|PO_NO|PARTNER_ID|PARTNER_NAME|INV_DATE|DUE_DATE|TOTAL
    ITEM|INVOICE_NO|ITEM_NUMBER|QTY|UOM|UNIT_PRICE|LINE_TOTAL
    TRAILER|INVOICE_NO|LINE_COUNT|TOTAL
    """
    lines = []
    total = invoice["tds_amount"]

    lines.append(
        f"HEADER|{invoice['invoice_number']}|{invoice['po_number']}|"
        f"{invoice['partner_id']}|{partner_name}|"
        f"{invoice['invoice_date']}|{invoice['due_date']}|{total:.2f}"
    )

    for item in invoice["items"]:
        lines.append(
            f"ITEM|{invoice['invoice_number']}|{item['item_number']}|"
            f"{int(item['quantity'])}|{item['uom']}|"
            f"{item['unit_price']:.2f}|{item['line_total']:.2f}"
        )

    lines.append(
        f"TRAILER|{invoice['invoice_number']}|"
        f"{len(invoice['items'])}|{total:.2f}"
    )

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────
# Upload flat file to S3 inbound_flatfiles/PARTNER_ID/
# ─────────────────────────────────────────────────────────
def upload_flat_file(flat_content: str, partner_id: str,
                     invoice_number: str, po_number: str) -> str:
    """Upload converted flat file to S3."""
    flat_filename = f"INV_{invoice_number}_{po_number}.txt"
    s3_key        = f"{FLATFILES_PREFIX}{partner_id}/{flat_filename}"

    s3.put_object(
        Bucket      = PIPELINE_BUCKET,
        Key         = s3_key,
        Body        = flat_content.encode("utf-8"),
        ContentType = "text/plain"
    )

    log(f"Flat file uploaded → s3://{PIPELINE_BUCKET}/{s3_key}")
    return s3_key


# ─────────────────────────────────────────────────────────
# Archive .edi to inbound_archive/PARTNER_ID/
# ─────────────────────────────────────────────────────────
def archive_edi(s3_key: str, partner_id: str, file_name: str) -> str:
    archive_key = f"{ARCHIVE_PREFIX}{partner_id}/{file_name}"

    s3.copy_object(
        Bucket     = PIPELINE_BUCKET,
        CopySource = {"Bucket": PIPELINE_BUCKET, "Key": s3_key},
        Key        = archive_key
    )
    s3.delete_object(Bucket=PIPELINE_BUCKET, Key=s3_key)

    log(f"EDI archived      → s3://{PIPELINE_BUCKET}/{archive_key}")
    return archive_key


# ─────────────────────────────────────────────────────────
# Save invoice to DynamoDB invoices table
# ─────────────────────────────────────────────────────────
def save_to_dynamodb(invoice: dict, partner_name: str,
                     s3_edi_key: str, s3_flat_key: str) -> None:
    """Save invoice record to DynamoDB."""

    def to_decimal(val):
        return Decimal(str(val))

    items_for_db = [
        {
            "item_number": item["item_number"],
            "quantity":    to_decimal(item["quantity"]),
            "uom":         item["uom"],
            "unit_price":  to_decimal(item["unit_price"]),
            "line_total":  to_decimal(item["line_total"]),
        }
        for item in invoice["items"]
    ]

    record = {
        "invoice_number": invoice["invoice_number"],
        "po_number":      invoice["po_number"],
        "partner_id":     invoice["partner_id"],
        "partner_name":   partner_name,
        "invoice_date":   invoice["invoice_date"],
        "due_date":       invoice["due_date"],
        "line_items":     items_for_db,
        "total_amount":   to_decimal(invoice["tds_amount"]),
        "currency":       "USD",
        "payment_status": "UNPAID",
        "s3_edi_key":     s3_edi_key,
        "s3_flat_key":    s3_flat_key,
        "received_date":  datetime.now(timezone.utc).isoformat(),
        "paid_date":      "",
        "error_message":  "",
    }

    inv_table.put_item(Item=record)
    log(f"DynamoDB saved    → invoice {invoice['invoice_number']} status=UNPAID")


# ─────────────────────────────────────────────────────────
# Process single invoice file
# ─────────────────────────────────────────────────────────
def process_invoice(s3_key: str, partner_id: str, partners: dict) -> bool:
    file_name    = s3_key.split("/")[-1]
    partner_name = partners.get(partner_id, {}).get("partner_name", partner_id)

    print(f"\n  ┌─ Invoice: {file_name} {'─'*18}┐")

    try:
        # Read 810 EDI
        response    = s3.get_object(Bucket=PIPELINE_BUCKET, Key=s3_key)
        edi_content = response["Body"].read().decode("utf-8")
        log(f"Read: {file_name}")

        # Parse 810
        invoice = parse_810_invoice(edi_content)
        log(f"Invoice No : {invoice['invoice_number']}")
        log(f"PO Number  : {invoice['po_number']}")
        log(f"Items      : {len(invoice['items'])}")
        log(f"Total      : ${invoice['tds_amount']:,.2f}")

        # ── VALIDATION 1 — Partner ─────────────────────
        log("Validating partner ID...")
        valid, error = validate_partner(partner_id, partners)
        if not valid:
            raise ValueError(f"PARTNER_ERROR: {error}")
        log(f"✅ Partner valid: {partner_name}")

        # ── VALIDATION 2 — Items ───────────────────────
        log("Validating item numbers against PO...")
        valid, error = validate_items(invoice)
        if not valid:
            raise ValueError(f"ITEM_ERROR: {error}")
        log(f"✅ All items match original PO")

        # ── VALIDATION 3 — TDS ────────────────────────
        log("Validating TDS total...")
        valid, error = validate_tds(invoice)
        if not valid:
            raise ValueError(f"TDS_ERROR: {error}")
        log(f"✅ TDS total matches: ${invoice['tds_amount']:,.2f}")

        # ── Generate flat file ─────────────────────────
        log("Converting EDI to flat file...")
        flat_content = generate_flat_file(invoice, partner_name)
        log("✅ Flat file generated")

        # ── Upload flat file to S3 ─────────────────────
        s3_flat_key = upload_flat_file(
            flat_content,
            partner_id,
            invoice["invoice_number"],
            invoice["po_number"]
        )

        # ── Archive .edi ───────────────────────────────
        s3_edi_key = archive_edi(s3_key, partner_id, file_name)

        # ── Save to DynamoDB ───────────────────────────
        save_to_dynamodb(invoice, partner_name, s3_edi_key, s3_flat_key)

        # ── Send success email ─────────────────────────
        send_sns(
            subject = f"Invoice Received — {invoice['invoice_number']} from {partner_name}",
            message = (
                f"Invoice Details\n"
                f"{'─' * 40}\n"
                f"Invoice Number : {invoice['invoice_number']}\n"
                f"PO Number      : {invoice['po_number']}\n"
                f"Vendor         : {partner_name} ({partner_id})\n"
                f"Invoice Date   : {invoice['invoice_date']}\n"
                f"Due Date       : {invoice['due_date']}\n"
                f"Total Amount   : ${invoice['tds_amount']:,.2f}\n"
                f"Payment Status : UNPAID\n"
                f"{'─' * 40}\n"
                f"S3 Flat File   : {s3_flat_key}\n"
                f"S3 EDI Archive : {s3_edi_key}\n"
                f"{'─' * 40}\n"
                f"Please make payment before the due date.\n"
                f"\nFlat File Preview:\n{flat_content}"
            )
        )

        print(f"  └─ {invoice['invoice_number']} ✅ PROCESSED {'─'*16}┘")
        return True

    except ValueError as e:
        error_msg = str(e)
        log(f"❌ Validation failed: {error_msg}")

        if "PARTNER_ERROR" in error_msg:
            subject = f"Invoice Error — Unknown Partner | {file_name}"
        elif "ITEM_ERROR" in error_msg:
            subject = f"Invoice Error — Item Mismatch | {file_name}"
        elif "TDS_ERROR" in error_msg:
            subject = f"Invoice Error — TDS Mismatch | {file_name}"
        else:
            subject = f"Invoice Error — Processing Failed | {file_name}"

        send_sns(
            subject = subject,
            message = (
                f"Invoice Processing Failed\n"
                f"{'─' * 40}\n"
                f"File        : {file_name}\n"
                f"Partner     : {partner_id}\n"
                f"Error       : {error_msg}\n"
                f"S3 Key      : {s3_key}\n"
                f"Time        : {datetime.now(timezone.utc).isoformat()}\n"
                f"{'─' * 40}\n"
                f"Action Required: Please investigate and reprocess."
            )
        )

        print(f"  └─ {file_name} ❌ FAILED {'─'*22}┘")
        return False

    except ClientError as e:
        log(f"❌ AWS error: {e.response['Error']['Message']}")
        print(f"  └─ {file_name} ❌ FAILED {'─'*22}┘")
        return False


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────
def main():
    print("\n" + "═" * 52)
    print("   INVOICE PROCESSOR — 810 EDI → DynamoDB")
    print("   Validate → Flat File → S3 → Archive → Email")
    print("═" * 52)
    print(f"\n  Bucket    : s3://{PIPELINE_BUCKET}/")
    print(f"  Inbound   : {INBOUND_PREFIX}")
    print(f"  Flat Files: {FLATFILES_PREFIX}")
    print(f"  Archive   : {ARCHIVE_PREFIX}")

    try:
        partners = load_partners()
        log(f"Loaded {len(partners)} trading partners")

        total_processed = 0
        total_failed    = 0

        for partner_id in PARTNERS_IDS:
            divider(f"Partner: {partner_id}")

            invoices = list_inbound_invoices(partner_id)

            if not invoices:
                log(f"No invoices in {INBOUND_PREFIX}{partner_id}/ — skipping")
                continue

            log(f"Found {len(invoices)} invoice(s)")

            for s3_key in invoices:
                if process_invoice(s3_key, partner_id, partners):
                    total_processed += 1
                else:
                    total_failed += 1

        # Summary
        divider("Summary")
        print(f"\n  ✅ Processed : {total_processed}")
        print(f"  ❌ Failed    : {total_failed}")

        if total_processed > 0:
            print(f"\n  DynamoDB     : {DYNAMODB_INV}")
            print(f"  Flat Files   : s3://{PIPELINE_BUCKET}/{FLATFILES_PREFIX}")
            print(f"  EDI Archive  : s3://{PIPELINE_BUCKET}/{ARCHIVE_PREFIX}")
            print(f"\n  📧 Payment reminder emails sent via SNS")

        print("\n" + "═" * 52 + "\n")

        if total_failed > 0:
            sys.exit(1)

    except FileNotFoundError as e:
        print(f"\n  ❌ {e}")
        sys.exit(1)
    except NoCredentialsError:
        print("\n  ❌ AWS credentials not found. Run: aws configure")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n  ❌ Cancelled by user.\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
