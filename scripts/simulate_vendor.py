"""
Simulate Vendor — 810 Invoice Generator
Scans all vendor inboxes for 850 PO files
Asks you to enter unit price for each item interactively
Generates X12 810 Invoice EDI
Places invoice in vendor invoices/ folder
Moves 850 PO to 850_archive/ folder

Flow:
  vendor/inbox/PO_583921.edi
        ↓
  Parse 850 — extract items + quantities
        ↓
  You enter unit price for each item
        ↓
  Generate 810 Invoice EDI
        ↓
  vendor/invoices/INV_847291_583921.edi
        ↓
  Move 850 → vendor/850_archive/PO_583921.edi
"""

import boto3
import secrets
import sys
from datetime import datetime, timezone, timedelta
from botocore.exceptions import NoCredentialsError, ClientError

# ─────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────
AWS_REGION    = "ap-south-1"
VENDOR_BUCKET = "edi-pipeline-vendors-dev"

VENDORS = ["tesla", "apple", "samsung", "dell", "hcl"]

FOLDER_TO_PARTNER = {
    "tesla":   "TESLA001",
    "apple":   "APPLE001",
    "samsung": "SAMSUNG001",
    "dell":    "DELL001",
    "hcl":     "HCL001"
}

# ─────────────────────────────────────────────────────────
# AWS clients
# ─────────────────────────────────────────────────────────
s3 = boto3.client("s3", region_name=AWS_REGION)


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


def generate_invoice_number() -> str:
    return str(secrets.randbelow(900000) + 100000)


def prompt_float(label: str) -> float:
    """Prompt user for a float value."""
    while True:
        try:
            value = float(input(f"  {label}: ").strip())
            if value >= 0:
                return value
            print("  ⚠️  Please enter a value >= 0")
        except ValueError:
            print("  ⚠️  Please enter a valid number e.g. 899.00")


def confirm(label: str) -> bool:
    while True:
        value = input(f"  {label} (y/n): ").strip().lower()
        if value in ("y", "yes"):
            return True
        if value in ("n", "no"):
            return False
        print("  ⚠️  Please enter y or n.")


# ─────────────────────────────────────────────────────────
# Step 1 — List PO files in vendor inbox
# ─────────────────────────────────────────────────────────
def list_vendor_pos(vendor_folder: str) -> list:
    response = s3.list_objects_v2(
        Bucket = VENDOR_BUCKET,
        Prefix = f"{vendor_folder}/inbox/"
    )
    return [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".edi")
    ]


# ─────────────────────────────────────────────────────────
# Step 2 — Parse 850 PO EDI
# ─────────────────────────────────────────────────────────
def parse_850_po(edi_content: str) -> dict:
    """Parse X12 850 — extract po_number, supplier_id, items."""
    po_number   = None
    supplier_id = None
    items       = []

    for line in edi_content.splitlines():
        line  = line.strip().rstrip("~")
        parts = line.split("*")
        tag   = parts[0]

        if tag == "BEG" and len(parts) >= 4:
            po_number = parts[3].strip()

        if tag == "ISA" and len(parts) >= 9:
            supplier_id = parts[8].strip()

        if tag == "PO1" and len(parts) >= 4:
            qty         = parts[2].strip()
            uom         = parts[3].strip()
            item_number = ""
            for i in range(len(parts) - 1):
                if parts[i].strip() == "BP":
                    item_number = parts[i + 1].strip()
                    break
            if item_number:
                items.append({
                    "item_number": item_number,
                    "quantity":    float(qty) if qty else 1.0,
                    "uom":         uom,
                })

    if not po_number:
        raise ValueError("Could not parse PO number from BEG segment")

    return {
        "po_number":   po_number,
        "supplier_id": supplier_id,
        "items":       items,
    }


# ─────────────────────────────────────────────────────────
# Step 3 — Ask prices interactively
# ─────────────────────────────────────────────────────────
def collect_prices(po: dict, vendor_folder: str) -> list:
    """
    Ask user to enter unit price for each item in the PO.
    Returns items list with unit_price and line_total added.
    """
    print(f"\n  Enter unit prices for PO {po['po_number']} ({vendor_folder.upper()}):")
    print(f"  {'─' * 48}")

    priced_items = []
    for item in po["items"]:
        print(f"\n  Item    : {item['item_number']}")
        print(f"  Qty     : {int(item['quantity'])} {item['uom']}")

        unit_price = prompt_float(f"  Unit Price (USD)")
        line_total = item["quantity"] * unit_price

        print(f"  Total   : ${line_total:,.2f}")

        priced_items.append({
            "item_number": item["item_number"],
            "quantity":    item["quantity"],
            "uom":         item["uom"],
            "unit_price":  unit_price,
            "line_total":  line_total,
        })

    total = sum(i["line_total"] for i in priced_items)
    print(f"\n  {'─' * 48}")
    print(f"  Invoice Total : ${total:,.2f}")
    print(f"  {'─' * 48}")

    return priced_items


# ─────────────────────────────────────────────────────────
# Step 4 — Generate X12 810 Invoice EDI
# ─────────────────────────────────────────────────────────
def generate_810_invoice(po: dict, priced_items: list,
                         invoice_number: str, partner_id: str) -> tuple:
    """
    Generate X12 810 Invoice EDI.
    Returns (edi_content, total_amount)
    """
    now        = datetime.now(timezone.utc)
    date_isa   = now.strftime("%y%m%d")
    date_gs    = now.strftime("%Y%m%d")
    time_str   = now.strftime("%H%M")
    due_date   = (now + timedelta(days=30)).strftime("%Y%m%d")
    po_number  = po["po_number"]
    isa_ctrl   = invoice_number.zfill(9)[:9]

    SEP  = "*"
    TERM = "~\n"

    def seg(*parts):
        return SEP.join(str(p) for p in parts) + TERM

    def pad(val, length):
        return str(val).ljust(length)[:length]

    segments = []

    # ISA
    segments.append(seg(
        "ISA", "00", pad("", 10), "00", pad("", 10),
        "ZZ", pad(partner_id, 15),
        "ZZ", pad("BUYER001", 15),
        date_isa, time_str, "^", "00501",
        isa_ctrl, "0", "P", ":"
    ))

    # GS — IN = Invoice
    segments.append(seg(
        "GS", "IN", partner_id, "BUYER001",
        date_gs, time_str, "1", "X", "005010"
    ))

    # ST
    segments.append(seg("ST", "810", "0001"))

    # BIG — Invoice header
    # BIG*InvoiceDate*InvoiceNumber**PONumber*DueDate
    segments.append(seg("BIG", date_gs, invoice_number, "", po_number, due_date))

    # REF — PO reference
    segments.append(seg("REF", "PO", po_number))

    # N1 — Remit To (vendor)
    segments.append(seg("N1", "RE", partner_id, "92", partner_id))

    # N1 — Pay To (buyer)
    segments.append(seg("N1", "PE", "BUYER001", "92", "BUYER001"))

    # IT1 — Invoice line items
    total_amount = 0.0
    for idx, item in enumerate(priced_items, start=1):
        total_amount += item["line_total"]

        segments.append(seg(
            "IT1", str(idx),
            str(int(item["quantity"])),
            item["uom"],
            f"{item['unit_price']:.2f}",
            "PE",
            "BP", item["item_number"],
        ))

        # PID — item description
        segments.append(seg("PID", "F", "", "", "", item["item_number"]))

        # SAC — line total
        segments.append(seg("SAC", "A", "", "", "", f"{item['line_total']:.2f}"))

    # TDS — total invoice amount in cents
    tds_cents = int(total_amount * 100)
    segments.append(seg("TDS", str(tds_cents)))

    # SE
    segment_count = len(segments) - 2 + 1
    segments.append(seg("SE", str(segment_count), "0001"))

    # GE
    segments.append(seg("GE", "1", "1"))

    # IEA
    segments.append(seg("IEA", "1", isa_ctrl))

    return "".join(segments), total_amount


# ─────────────────────────────────────────────────────────
# Step 5 — Upload invoice to vendor invoices/ folder
# ─────────────────────────────────────────────────────────
def upload_invoice(edi_content: str, vendor_folder: str,
                   invoice_filename: str) -> str:
    s3_key = f"{vendor_folder}/invoices/{invoice_filename}"
    s3.put_object(
        Bucket      = VENDOR_BUCKET,
        Key         = s3_key,
        Body        = edi_content.encode("utf-8"),
        ContentType = "text/plain"
    )
    log(f"Invoice uploaded  → s3://{VENDOR_BUCKET}/{s3_key}")
    return s3_key


# ─────────────────────────────────────────────────────────
# Step 6 — Move 850 PO to 850_archive/
# ─────────────────────────────────────────────────────────
def archive_850(s3_key: str, vendor_folder: str, file_name: str) -> str:
    archive_key = f"{vendor_folder}/850_archive/{file_name}"

    # Copy to archive
    s3.copy_object(
        Bucket     = VENDOR_BUCKET,
        CopySource = {"Bucket": VENDOR_BUCKET, "Key": s3_key},
        Key        = archive_key
    )

    # Delete from inbox
    s3.delete_object(Bucket=VENDOR_BUCKET, Key=s3_key)

    log(f"850 archived      → s3://{VENDOR_BUCKET}/{archive_key}")
    return archive_key


# ─────────────────────────────────────────────────────────
# Process single PO file
# ─────────────────────────────────────────────────────────
def process_po(s3_key: str, vendor_folder: str, partner_id: str) -> bool:
    file_name = s3_key.split("/")[-1]

    print(f"\n  ┌─ PO: {file_name} {'─'*25}┐")

    try:
        # Read 850 PO
        response    = s3.get_object(Bucket=VENDOR_BUCKET, Key=s3_key)
        edi_content = response["Body"].read().decode("utf-8")
        log(f"Read PO: {file_name}")

        # Parse 850
        po = parse_850_po(edi_content)
        log(f"PO Number : {po['po_number']}")
        log(f"Items     : {len(po['items'])}")

        # Ask prices interactively
        priced_items = collect_prices(po, vendor_folder)

        # Confirm before generating
        if not confirm("\n  Generate 810 Invoice?"):
            log("Skipped by user")
            print(f"  └─ {file_name} ⏭ SKIPPED {'─'*22}┘")
            return False

        # Generate 810 invoice
        invoice_number   = generate_invoice_number()
        invoice_content, total = generate_810_invoice(
            po, priced_items, invoice_number, partner_id
        )
        invoice_filename = f"INV_{invoice_number}_{po['po_number']}.edi"

        log(f"Invoice No : {invoice_number}")
        log(f"Total      : ${total:,.2f}")

        # Upload invoice to invoices/ folder
        upload_invoice(invoice_content, vendor_folder, invoice_filename)

        # Move 850 to 850_archive/
        archive_850(s3_key, vendor_folder, file_name)

        print(f"  └─ INV_{invoice_number} ✅ DONE {'─'*19}┘")
        return True

    except ValueError as e:
        log(f"❌ Parse error: {e}")
        print(f"  └─ {file_name} ❌ FAILED {'─'*21}┘")
        return False
    except ClientError as e:
        log(f"❌ AWS error: {e.response['Error']['Message']}")
        print(f"  └─ {file_name} ❌ FAILED {'─'*21}┘")
        return False


# ─────────────────────────────────────────────────────────
# Process single vendor
# ─────────────────────────────────────────────────────────
def process_vendor(vendor_folder: str) -> int:
    partner_id = FOLDER_TO_PARTNER.get(vendor_folder, "UNKNOWN")
    divider(f"Vendor: {vendor_folder.upper()} ({partner_id})")

    po_files = list_vendor_pos(vendor_folder)

    if not po_files:
        log(f"No PO files in {vendor_folder}/inbox/ — skipping")
        return 0

    log(f"Found {len(po_files)} PO file(s)")
    count = 0

    for s3_key in po_files:
        if process_po(s3_key, vendor_folder, partner_id):
            count += 1

    return count


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────
def main():
    print("\n" + "═" * 52)
    print("   SIMULATE VENDOR — 810 Invoice Generator")
    print("   Enter prices interactively per item")
    print("═" * 52)
    print(f"\n  Vendor Bucket : s3://{VENDOR_BUCKET}/")
    print(f"  Vendors       : {', '.join(VENDORS)}")

    try:
        total_invoices = 0

        for vendor_folder in VENDORS:
            count = process_vendor(vendor_folder)
            total_invoices += count

        divider("Summary")
        print(f"\n  Total Invoices Generated : {total_invoices}")

        if total_invoices > 0:
            print(f"\n  Invoice locations:")
            for v in VENDORS:
                print(f"    s3://{VENDOR_BUCKET}/{v}/invoices/")
            print(f"\n  850 POs archived to:")
            for v in VENDORS:
                print(f"    s3://{VENDOR_BUCKET}/{v}/850_archive/")
            print(f"\n  🚀 Next Step:")
            print(f"     Run invoice_router.py to route invoices")
            print(f"     to edi/inbound/PARTNER_ID/ folders")
        else:
            print(f"\n  ℹ️  No POs found in any vendor inbox.")
            print(f"     Run send_to_vendor.py first.")

        print("\n" + "═" * 52 + "\n")

    except NoCredentialsError:
        print("\n  ❌ AWS credentials not found. Run: aws configure")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n  ❌ Cancelled by user.\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
