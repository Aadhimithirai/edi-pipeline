"""
Invoice Router
Scans all vendor invoices/ folders for 810 EDI files
Reads partner ID from EDI ISA segment
Routes invoice to edi/inbound/PARTNER_ID/ in pipeline bucket
Moves 810 → vendor 810_archive/ folder

Flow:
  vendor/invoices/INV_847291_583921.edi
        ↓
  Read partner ID from ISA segment
        ↓
  Copy to edi-pipeline-dev/edi/inbound/TESLA001/
        ↓
  Move 810 → vendor/810_archive/
"""

import boto3
import json
import sys
import os
from datetime import datetime, timezone
from botocore.exceptions import NoCredentialsError, ClientError

# ─────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────
AWS_REGION      = "ap-south-1"
VENDOR_BUCKET   = "edi-pipeline-vendors-dev"
PIPELINE_BUCKET = "edi-pipeline-dev"
INBOUND_PREFIX  = "edi/inbound/"
PARTNERS_FILE   = "config/partners.json"

VENDORS = ["tesla", "apple", "samsung", "dell", "hcl"]

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


# ─────────────────────────────────────────────────────────
# Load trading partners
# ─────────────────────────────────────────────────────────
def load_partners() -> dict:
    if not os.path.exists(PARTNERS_FILE):
        raise FileNotFoundError(
            f"Partners file not found: {PARTNERS_FILE}\n"
            f"Run from project root: python3 scripts/invoice_router.py"
        )
    with open(PARTNERS_FILE, "r") as f:
        data = json.load(f)
    return {
        p["partner_id"]: p
        for p in data["trading_partners"]
        if p.get("active", True)
    }


# ─────────────────────────────────────────────────────────
# Extract partner ID from 810 EDI ISA segment
# ISA06 = sender ID = vendor/partner ID
# ─────────────────────────────────────────────────────────
def extract_partner_id(edi_content: str) -> str:
    for line in edi_content.splitlines():
        if line.startswith("ISA"):
            elements = line.split("*")
            if len(elements) >= 7:
                # ISA06 = sender ID
                return elements[6].strip()
    raise ValueError("ISA segment not found — invalid X12 format")


# ─────────────────────────────────────────────────────────
# List invoice files in vendor invoices/ folder
# ─────────────────────────────────────────────────────────
def list_vendor_invoices(vendor_folder: str) -> list:
    response = s3.list_objects_v2(
        Bucket = VENDOR_BUCKET,
        Prefix = f"{vendor_folder}/invoices/"
    )
    return [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".edi")
    ]


# ─────────────────────────────────────────────────────────
# Copy invoice to pipeline inbound/PARTNER_ID/ folder
# ─────────────────────────────────────────────────────────
def route_to_inbound(s3_key: str, partner_id: str, file_name: str) -> str:
    dest_key = f"{INBOUND_PREFIX}{partner_id}/{file_name}"

    s3.copy_object(
        Bucket     = PIPELINE_BUCKET,
        CopySource = {"Bucket": VENDOR_BUCKET, "Key": s3_key},
        Key        = dest_key
    )

    log(f"Routed → s3://{PIPELINE_BUCKET}/{dest_key}")
    return dest_key


# ─────────────────────────────────────────────────────────
# Move 810 invoice to vendor 810_archive/ folder
# ─────────────────────────────────────────────────────────
def archive_810(s3_key: str, vendor_folder: str, file_name: str) -> str:
    archive_key = f"{vendor_folder}/810_archive/{file_name}"

    # Copy to archive
    s3.copy_object(
        Bucket     = VENDOR_BUCKET,
        CopySource = {"Bucket": VENDOR_BUCKET, "Key": s3_key},
        Key        = archive_key
    )

    # Delete from invoices/
    s3.delete_object(Bucket=VENDOR_BUCKET, Key=s3_key)

    log(f"810 archived → s3://{VENDOR_BUCKET}/{archive_key}")
    return archive_key


# ─────────────────────────────────────────────────────────
# Process single invoice file
# ─────────────────────────────────────────────────────────
def process_invoice(s3_key: str, vendor_folder: str, partners: dict) -> bool:
    file_name = s3_key.split("/")[-1]

    print(f"\n  ┌─ Invoice: {file_name} {'─'*18}┐")

    try:
        # Read 810 EDI
        response    = s3.get_object(Bucket=VENDOR_BUCKET, Key=s3_key)
        edi_content = response["Body"].read().decode("utf-8")

        # Extract partner ID from ISA06
        partner_id = extract_partner_id(edi_content)
        log(f"Partner ID : {partner_id}")

        # Validate partner exists
        partner = partners.get(partner_id)
        if not partner:
            log(f"❌ Unknown partner ID: {partner_id}")
            log(f"   Add {partner_id} to config/partners.json")
            print(f"  └─ {file_name} ❌ UNKNOWN PARTNER {'─'*14}┘")
            return False

        log(f"Partner    : {partner['partner_name']}")

        # Route to inbound/PARTNER_ID/
        inbound_key = route_to_inbound(s3_key, partner_id, file_name)

        # Move 810 to 810_archive/
        archive_810(s3_key, vendor_folder, file_name)

        print(f"  └─ {file_name} ✅ ROUTED {'─'*22}┘")
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
def process_vendor(vendor_folder: str, partners: dict) -> int:
    divider(f"Vendor: {vendor_folder.upper()}")

    invoices = list_vendor_invoices(vendor_folder)

    if not invoices:
        log(f"No invoices in {vendor_folder}/invoices/ — skipping")
        return 0

    log(f"Found {len(invoices)} invoice(s)")
    count = 0

    for s3_key in invoices:
        if process_invoice(s3_key, vendor_folder, partners):
            count += 1

    return count


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────
def main():
    print("\n" + "═" * 52)
    print("   INVOICE ROUTER — Route 810 to Inbound")
    print("   Reads partner ID → routes to inbound folder")
    print("═" * 52)
    print(f"\n  Source  : s3://{VENDOR_BUCKET}/<vendor>/invoices/")
    print(f"  Dest    : s3://{PIPELINE_BUCKET}/edi/inbound/PARTNER_ID/")

    try:
        # Load partners
        partners = load_partners()
        log(f"Loaded {len(partners)} trading partners")

        total_routed = 0

        for vendor_folder in VENDORS:
            count = process_vendor(vendor_folder, partners)
            total_routed += count

        # Summary
        divider("Summary")
        print(f"\n  Total Routed : {total_routed}")

        if total_routed > 0:
            print(f"\n  Inbound folders:")
            for pid in partners.keys():
                print(f"    s3://{PIPELINE_BUCKET}/edi/inbound/{pid}/")
            print(f"\n  810 archived to:")
            for v in VENDORS:
                print(f"    s3://{VENDOR_BUCKET}/{v}/810_archive/")
            print(f"\n  🚀 Next Step:")
            print(f"     Run invoice_processor.py to validate")
            print(f"     and load invoices to DynamoDB")
        else:
            print(f"\n  ℹ️  No invoices found in any vendor folder.")
            print(f"     Run simulate_vendor.py first.")

        print("\n" + "═" * 52 + "\n")

        if total_routed == 0:
            sys.exit(0)

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
