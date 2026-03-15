"""
Send to Vendor
Reads 850 EDI files from S3 edi/outbound/
Looks up partner ID in partners.json
Copies to vendor bucket under correct vendor/inbox/ folder

Flow:
  edi/outbound/PO_583921_20240315.edi
        ↓
  Read partner ID from EDI (ISA08 segment)
        ↓
  Lookup partners.json → TESLA001 → vendor_folder = tesla
        ↓
  Copy to vendors-dev/tesla/inbox/PO_583921_20240315.edi
        ↓
  Update DynamoDB edi_jobs → vendor_sent = Y
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
AWS_REGION     = "ap-south-1"
S3_BUCKET      = "edi-pipeline-dev"
VENDOR_BUCKET  = "edi-pipeline-vendors-dev"
DYNAMODB_TABLE = "edi-pipeline-edi-jobs"
OUTBOUND_PREFIX = "edi/outbound/"
PARTNERS_FILE  = "config/partners.json"

# ─────────────────────────────────────────────────────────
# AWS clients
# ─────────────────────────────────────────────────────────
s3       = boto3.client("s3",         region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table    = dynamodb.Table(DYNAMODB_TABLE)


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
# Load trading partners lookup
# ─────────────────────────────────────────────────────────
def load_partners() -> dict:
    """
    Load partners.json
    Returns dict keyed by partner_id
    { "TESLA001": { "vendor_folder": "tesla", ... } }
    """
    if not os.path.exists(PARTNERS_FILE):
        raise FileNotFoundError(
            f"Partners file not found: {PARTNERS_FILE}\n"
            f"Run from project root: python3 scripts/send_to_vendor.py"
        )

    with open(PARTNERS_FILE, "r") as f:
        data = json.load(f)

    return {
        p["partner_id"]: p
        for p in data["trading_partners"]
        if p.get("active", True)
    }


# ─────────────────────────────────────────────────────────
# Parse partner ID from X12 EDI ISA segment
# ISA*00*...*ZZ*BUYER001*ZZ*TESLA001*...
# ISA08 = receiver ID = partner/supplier ID
# ─────────────────────────────────────────────────────────
def extract_partner_id(edi_content: str) -> str:
    """
    Extract partner ID from ISA segment (ISA08 — receiver ID).
    ISA segments are pipe-delimited with * separator.
    """
    for line in edi_content.splitlines():
        if line.startswith("ISA"):
            elements = line.split("*")
            if len(elements) >= 9:
                # ISA08 is receiver ID — strip whitespace padding
                return elements[8].strip()
    raise ValueError("ISA segment not found in EDI file — invalid X12 format")


# ─────────────────────────────────────────────────────────
# List EDI files in outbound folder
# ─────────────────────────────────────────────────────────
def list_outbound_files() -> list:
    """List all .edi files in S3 edi/outbound/ folder."""
    response = s3.list_objects_v2(
        Bucket = S3_BUCKET,
        Prefix = OUTBOUND_PREFIX
    )

    files = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".edi")
    ]
    return files


# ─────────────────────────────────────────────────────────
# Copy EDI file to vendor inbox
# ─────────────────────────────────────────────────────────
def copy_to_vendor(s3_key: str, vendor_folder: str, file_name: str) -> str:
    """Copy EDI file from outbound to vendor bucket inbox."""
    dest_key = f"{vendor_folder}/inbox/{file_name}"

    s3.copy_object(
        Bucket     = VENDOR_BUCKET,
        CopySource = {"Bucket": S3_BUCKET, "Key": s3_key},
        Key        = dest_key
    )

    log(f"Copied → s3://{VENDOR_BUCKET}/{dest_key}")
    return dest_key


# ─────────────────────────────────────────────────────────
# Update DynamoDB — mark vendor_sent = Y
# ─────────────────────────────────────────────────────────
def mark_vendor_sent(file_name: str, vendor_key: str):
    """Update EDI jobs table — vendor_sent = Y."""
    txt_name = file_name.replace(".edi", ".txt")

    table.update_item(
        Key = {"file_name": txt_name},
        UpdateExpression = (
            "SET vendor_sent = :vs, "
            "vendor_sent_date = :vd, "
            "s3_vendor_key = :vk" 
        ),
        ExpressionAttributeValues = {
            ":vs": "Y",
            ":vd": datetime.now(timezone.utc).isoformat(),
            ":vk": vendor_key
        }
    )
    log(f"DynamoDB updated → vendor_sent = Y")


# ─────────────────────────────────────────────────────────
# Process single EDI file
# ─────────────────────────────────────────────────────────
def process_file(s3_key: str, partners: dict) -> bool:
    file_name = s3_key.split("/")[-1]

    print(f"\n  ┌─ Processing {file_name} {'─'*20}┐")

    try:
        # Read EDI content from S3
        response    = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        edi_content = response["Body"].read().decode("utf-8")

        # Extract partner ID from ISA segment
        partner_id = extract_partner_id(edi_content)
        log(f"Partner ID  : {partner_id}")

        # Lookup partner in partners.json
        partner = partners.get(partner_id)
        if not partner:
            log(f"❌ Unknown partner ID: {partner_id}")
            log(f"   Add {partner_id} to config/partners.json")
            print(f"  └─ {file_name} ❌ SKIPPED {'─'*20}┘")
            return False

        vendor_folder = partner["vendor_folder"]
        log(f"Vendor      : {partner['partner_name']} → {vendor_folder}/inbox/")

        # Copy to vendor inbox
        vendor_key = copy_to_vendor(s3_key, vendor_folder, file_name)

        # Archive EDI file — move outbound/ → archive/YYYY/MM/
        po_date     = file_name.split("_")[2].replace(".edi", "")  # extract date from filename
        archive_key = archive_edi_file(s3_key, file_name, po_date)

        # Update DynamoDB
        mark_vendor_sent(file_name, vendor_key, archive_key)

        print(f"  └─ {file_name} ✅ SENT {'─'*23}┘")
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
# MAIN
# ─────────────────────────────────────────────────────────
def main():
    print("\n" + "═" * 52)
    print("   SEND TO VENDOR — Route 850 EDI to Vendors")
    print("═" * 52)
    print(f"\n  Source : s3://{S3_BUCKET}/{OUTBOUND_PREFIX}")
    print(f"  Dest   : s3://{VENDOR_BUCKET}/<vendor>/inbox/")

    try:
        # Load partners lookup
        partners = load_partners()
        log(f"Loaded {len(partners)} trading partners from {PARTNERS_FILE}")

        # List outbound EDI files
        divider("Scanning outbound folder...")
        files = list_outbound_files()

        if not files:
            print("\n  ✅ No EDI files found in outbound folder.")
            print("\n" + "═" * 52 + "\n")
            sys.exit(0)

        print(f"\n  Found {len(files)} EDI file(s)")

        # Process each file
        divider("Sending to Vendors")
        success_count = 0
        failed_count  = 0

        for s3_key in files:
            if process_file(s3_key, partners):
                success_count += 1
            else:
                failed_count += 1

        # Summary
        divider("Summary")
        print(f"\n  Total  : {len(files)}")
        print(f"  ✅ Sent   : {success_count}")
        print(f"  ❌ Failed : {failed_count}")

        if success_count > 0:
            print(f"\n  Vendor bucket: s3://{VENDOR_BUCKET}/")
            for partner_id, partner in partners.items():
                print(f"    {partner_id} → {partner['vendor_folder']}/inbox/")

        print("\n" + "═" * 52 + "\n")

        if failed_count > 0:
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
