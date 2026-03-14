"""
EDI Processor
Reads DynamoDB records with edi_sent_status=N
Generates flat file → S3 flatfiles/inbox/
Moves flat file     → S3 flatfiles/processed/
Converts to X12 850 → S3 edi/outbound/
Updates DynamoDB      edi_sent_status=Y
"""

import boto3
import sys
from datetime import datetime, timezone
from botocore.exceptions import NoCredentialsError, ClientError

# ─────────────────────────────────────────────────────────
# CONFIG — must match your terraform outputs
# ─────────────────────────────────────────────────────────
AWS_REGION     = "ap-south-1"
DYNAMODB_TABLE = "edi-pipeline-edi-jobs"
S3_BUCKET      = "edi-pipeline-dev"      # ← change to your bucket name

# S3 folder paths
INBOX_PREFIX     = "flatfiles/inbox/"
PROCESSED_PREFIX = "flatfiles/processed/"
OUTBOUND_PREFIX  = "edi/outbound/"

# ─────────────────────────────────────────────────────────
# AWS clients
# ─────────────────────────────────────────────────────────
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
s3       = boto3.client("s3",         region_name=AWS_REGION)
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
# Step 1 — Scan DynamoDB for pending records (status=N)
# ─────────────────────────────────────────────────────────
def get_pending_jobs() -> list:
    response = table.scan(
        FilterExpression=boto3.dynamodb.conditions.Attr("edi_sent_status").eq("N")
    )
    return response.get("Items", [])


# ─────────────────────────────────────────────────────────
# Step 2 — Generate flat file content from DynamoDB record
# ─────────────────────────────────────────────────────────
def generate_flat_file(job: dict) -> str:
    """
    Build pipe-delimited flat file from DynamoDB record.

    HEADER|PO_NUMBER|SUPPLIER_ID|SUPPLIER_NAME|PO_DATE
    ITEM|PO_NUMBER|ITEM_NUMBER|DESCRIPTION|QUANTITY|UOM
    TRAILER|PO_NUMBER|LINE_COUNT
    """
    lines   = []
    po      = job["po_number"]
    items   = job.get("items", [])

    # HEADER
    lines.append(
        f"HEADER|{po}|{job['supplier_id']}|"
        f"{job['supplier_name']}|{job['po_date']}"
    )

    # ITEMS
    for item in items:
        lines.append(
            f"ITEM|{po}|{item['item_number']}|"
            f"{item['description']}|{item['quantity']}|{item['uom']}"
        )

    # TRAILER
    lines.append(f"TRAILER|{po}|{len(items)}")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────
# Step 3 — Generate X12 850 EDI from DynamoDB record
# ─────────────────────────────────────────────────────────
def generate_edi_850(job: dict) -> str:
    """
    Generate ANSI X12 850 Purchase Order EDI document.
    Segments: ISA→GS→ST→BEG→REF→N1(BY)→N1(SE)→PO1+PID→CTT→SE→GE→IEA
    """
    now         = datetime.now(timezone.utc)
    date_isa    = now.strftime("%y%m%d")
    date_gs     = now.strftime("%Y%m%d")
    time_str    = now.strftime("%H%M")
    po          = job["po_number"]
    supplier_id = job["supplier_id"]
    items       = job.get("items", [])
    isa_ctrl    = po.zfill(9)[:9]

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
        "ZZ", pad("BUYER001", 15),
        "ZZ", pad(supplier_id, 15),
        date_isa, time_str, "^", "00501",
        isa_ctrl, "0", "P", ":"
    ))

    # GS
    segments.append(seg(
        "GS", "PO", "BUYER001", supplier_id,
        date_gs, time_str, "1", "X", "005010"
    ))

    # ST
    segments.append(seg("ST", "850", "0001"))

    # BEG
    segments.append(seg("BEG", "00", "SA", po, "", date_gs))

    # REF
    segments.append(seg("REF", "IA", supplier_id))

    # N1 Buyer
    segments.append(seg("N1", "BY", job["supplier_name"], "92", "BUYER001"))

    # N1 Supplier
    segments.append(seg("N1", "SE", job["supplier_name"], "92", supplier_id))

    # DTM delivery date
    if job.get("delivery_date"):
        segments.append(seg("DTM", "002", job["delivery_date"]))

    # PO1 + PID per item
    total_qty = 0
    for idx, item in enumerate(items, start=1):
        qty = int(item.get("quantity", 1))
        total_qty += qty
        segments.append(seg(
            "PO1", str(idx), str(qty),
            item.get("uom", "EA"), "", "",
            "BP", item["item_number"],
            "VP", item["item_number"]
        ))
        desc = str(item.get("description", ""))[:35]
        segments.append(seg("PID", "F", "", "", "", desc))

    # CTT
    segments.append(seg("CTT", str(len(items)), str(total_qty)))

    # SE — count all segments between ST and SE inclusive
    segment_count = len(segments) - 2 + 1  # exclude ISA, GS; +1 for SE itself
    segments.append(seg("SE", str(segment_count), "0001"))

    # GE
    segments.append(seg("GE", "1", "1"))

    # IEA
    segments.append(seg("IEA", "1", isa_ctrl))

    return "".join(segments)


# ─────────────────────────────────────────────────────────
# Step 4 — Upload flat file to S3 inbox
# ─────────────────────────────────────────────────────────
def upload_to_inbox(content: str, file_name: str) -> str:
    s3_key = f"{INBOX_PREFIX}{file_name}"
    s3.put_object(
        Bucket      = S3_BUCKET,
        Key         = s3_key,
        Body        = content.encode("utf-8"),
        ContentType = "text/plain"
    )
    log(f"Flat file uploaded → s3://{S3_BUCKET}/{s3_key}")
    return s3_key


# ─────────────────────────────────────────────────────────
# Step 5 — Move flat file from inbox → processed
# ─────────────────────────────────────────────────────────
def move_to_processed(file_name: str) -> str:
    source_key = f"{INBOX_PREFIX}{file_name}"
    dest_key   = f"{PROCESSED_PREFIX}{file_name}"

    # Copy to processed
    s3.copy_object(
        Bucket     = S3_BUCKET,
        CopySource = {"Bucket": S3_BUCKET, "Key": source_key},
        Key        = dest_key
    )

    # Delete from inbox
    s3.delete_object(Bucket=S3_BUCKET, Key=source_key)

    log(f"Flat file moved   → s3://{S3_BUCKET}/{dest_key}")
    return dest_key


# ─────────────────────────────────────────────────────────
# Step 6 — Upload EDI file to S3 outbound
# ─────────────────────────────────────────────────────────
def upload_edi_to_outbound(content: str, file_name: str) -> str:
    edi_name = file_name.replace(".txt", ".edi")
    s3_key   = f"{OUTBOUND_PREFIX}{edi_name}"

    s3.put_object(
        Bucket      = S3_BUCKET,
        Key         = s3_key,
        Body        = content.encode("utf-8"),
        ContentType = "text/plain"
    )
    log(f"EDI file uploaded → s3://{S3_BUCKET}/{s3_key}")
    return s3_key


# ─────────────────────────────────────────────────────────
# Step 7 — Update DynamoDB status N → Y
# ─────────────────────────────────────────────────────────
def mark_as_sent(file_name: str, s3_flat_key: str, s3_edi_key: str):
    table.update_item(
        Key = {"file_name": file_name},
        UpdateExpression = (
            "SET edi_sent_status = :s, "
            "processed_date = :pd, "
            "s3_flat_file_key = :fk, "
            "s3_edi_key = :ek"
        ),
        ExpressionAttributeValues = {
            ":s":  "Y",
            ":pd": datetime.now(timezone.utc).isoformat(),
            ":fk": s3_flat_key,
            ":ek": s3_edi_key,
        }
    )
    log(f"DynamoDB updated  → edi_sent_status = Y")


# ─────────────────────────────────────────────────────────
# Process a single job
# ─────────────────────────────────────────────────────────
def process_job(job: dict) -> bool:
    file_name = job["file_name"]
    po_number = job["po_number"]

    print(f"\n  ┌─ Processing PO {po_number} ───────────────────┐")
    log(f"File      : {file_name}")
    log(f"Supplier  : {job['supplier_name']} ({job['supplier_id']})")
    log(f"Items     : {job.get('line_count', 0)}")

    try:
        # Step 2 — Generate flat file
        log("Generating flat file...")
        flat_content = generate_flat_file(job)

        # Step 3 — Generate EDI
        log("Generating X12 850 EDI...")
        edi_content = generate_edi_850(job)

        # Step 4 — Upload flat file to inbox
        s3_inbox_key = upload_to_inbox(flat_content, file_name)

        # Step 5 — Move flat file to processed
        s3_flat_key = move_to_processed(file_name)

        # Step 6 — Upload EDI to outbound
        s3_edi_key = upload_edi_to_outbound(edi_content, file_name)

        # Step 7 — Update DynamoDB
        mark_as_sent(file_name, s3_flat_key, s3_edi_key)

        print(f"  └─ PO {po_number} ✅ DONE ────────────────────────┘")
        return True

    except ClientError as e:
        log(f"❌ AWS error: {e.response['Error']['Message']}")
        print(f"  └─ PO {po_number} ❌ FAILED ──────────────────────┘")
        return False
    except Exception as e:
        log(f"❌ Error: {str(e)}")
        print(f"  └─ PO {po_number} ❌ FAILED ──────────────────────┘")
        return False


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────
def main():
    print("\n" + "═" * 52)
    print("   EDI PROCESSOR — Generate 850 EDI Files")
    print("═" * 52)
    print(f"\n  Table  : {DYNAMODB_TABLE}")
    print(f"  Bucket : {S3_BUCKET}")
    print(f"  Region : {AWS_REGION}")

    try:
        # Fetch pending jobs
        divider("Scanning DynamoDB for pending jobs...")
        jobs = get_pending_jobs()

        if not jobs:
            print("\n  ✅ No pending jobs found. All POs already processed.")
            print("\n" + "═" * 52 + "\n")
            sys.exit(0)

        print(f"\n  Found {len(jobs)} pending job(s)")

        # Process each job
        divider("Processing Jobs")
        success_count = 0
        failed_count  = 0

        for job in jobs:
            if process_job(job):
                success_count += 1
            else:
                failed_count += 1

        # Summary
        divider("Summary")
        print(f"\n  Total Jobs  : {len(jobs)}")
        print(f"  ✅ Success  : {success_count}")
        print(f"  ❌ Failed   : {failed_count}")

        if success_count > 0:
            print(f"\n  S3 Paths:")
            print(f"  Flat files  → s3://{S3_BUCKET}/{PROCESSED_PREFIX}")
            print(f"  EDI files   → s3://{S3_BUCKET}/{OUTBOUND_PREFIX}")

        print("\n" + "═" * 52 + "\n")

        # Exit with error code if any failed (useful for GitHub Actions)
        if failed_count > 0:
            sys.exit(1)

    except NoCredentialsError:
        print("\n  ❌ AWS credentials not found. Run: aws configure")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n  ❌ Cancelled by user.\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
