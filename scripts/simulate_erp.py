"""
EDI Simulator — Interactive Purchase Order Generator
Simulates an ERP system raising a Purchase Order
Writes directly to DynamoDB with edi_sent_status = N
"""

import boto3
import secrets
import sys
from datetime import datetime, timezone
from botocore.exceptions import NoCredentialsError, ClientError

# ─────────────────────────────────────────────────────────
# CONFIG — update these to match your terraform outputs
# ─────────────────────────────────────────────────────────
AWS_REGION     = "ap-south-1"
DYNAMODB_TABLE = "edi-pipeline-edi-jobs"


# ─────────────────────────────────────────────────────────
# Known Suppliers
# ─────────────────────────────────────────────────────────
SUPPLIERS = {
    "1": {"id": "TESLA001",   "name": "Tesla Inc"},
    "2": {"id": "APPLE001",   "name": "Apple Inc"},
    "3": {"id": "SAMSUNG001", "name": "Samsung Electronics"},
    "4": {"id": "DELL001",    "name": "Dell Technologies"},
    "5": {"id": "OTHER",      "name": None},
}

# ─────────────────────────────────────────────────────────
# Units of Measure
# ─────────────────────────────────────────────────────────
UOMS = {
    "1": ("EA", "Each"),
    "2": ("CS", "Case"),
    "3": ("PK", "Pack"),
    "4": ("BX", "Box"),
    "5": ("KG", "Kilogram"),
}


# ─────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────
def divider(title=""):
    print("\n" + "─" * 52)
    if title:
        print(f"  {title}")
        print("─" * 52)


def prompt(label: str, required: bool = True) -> str:
    while True:
        value = input(f"  {label}: ").strip()
        if value:
            return value
        if not required:
            return ""
        print("  ⚠️  Required field. Please enter a value.")


def prompt_int(label: str, min_val: int = 1) -> int:
    while True:
        try:
            value = int(input(f"  {label}: ").strip())
            if value >= min_val:
                return value
            print(f"  ⚠️  Please enter a number >= {min_val}")
        except ValueError:
            print("  ⚠️  Please enter a valid number.")


def confirm(label: str) -> bool:
    while True:
        value = input(f"  {label} (y/n): ").strip().lower()
        if value in ("y", "yes"):
            return True
        if value in ("n", "no"):
            return False
        print("  ⚠️  Please enter y or n.")


def generate_po_number() -> str:
    """Generate a unique 6 digit PO number."""
    return str(secrets.randbelow(900000) + 100000)


# ─────────────────────────────────────────────────────────
# Step 1 — Welcome Banner
# ─────────────────────────────────────────────────────────
def welcome():
    print("\n" + "═" * 52)
    print("   EDI SIMULATOR — Purchase Order Generator")
    print("   Writes directly to DynamoDB  status = N")
    print("═" * 52)
    print(f"\n  Date   : {datetime.now(timezone.utc).strftime('%d %b %Y  %H:%M UTC')}")
    print(f"  Table  : {DYNAMODB_TABLE}")
    print(f"  Region : {AWS_REGION}")


# ─────────────────────────────────────────────────────────
# Step 2 — Select Supplier
# ─────────────────────────────────────────────────────────
def select_supplier() -> dict:
    divider("STEP 1 — Select Supplier")
    print()

    for key, s in SUPPLIERS.items():
        label = s["name"] if s["name"] else "Enter manually"
        if s["name"]:
            print(f"  [{key}] {label}  ({s['id']})")
        else:
            print(f"  [{key}] {label}")

    while True:
        choice = input("\n  Select supplier (1-5): ").strip()
        if choice not in SUPPLIERS:
            print("  ⚠️  Invalid choice. Please select 1-5.")
            continue

        supplier = SUPPLIERS[choice]

        if supplier["name"] is None:
            name = prompt("Supplier Name")
            sid  = prompt("Supplier ID (e.g. VENDOR001)")
            return {"id": sid.upper(), "name": name}

        print(f"\n  ✅ Selected: {supplier['name']} ({supplier['id']})")
        return supplier


# ─────────────────────────────────────────────────────────
# Step 3 — Add Items
# ─────────────────────────────────────────────────────────
def enter_items() -> list:
    divider("STEP 2 — Add Items to Order")

    items      = []
    item_count = 1

    while True:
        print(f"\n  ── Item {item_count} ──")

        item_number = prompt(f"  Item Number (e.g. ITEM00{item_count})")
        description = prompt("  Description")
        quantity    = prompt_int("  Quantity", min_val=1)
        uom         = select_uom()

        items.append({
            "item_number": item_number.upper(),
            "description": description,
            "quantity":    str(quantity),
            "uom":         uom,
        })

        print(f"\n  ✅ Added: [{item_number.upper()}] "
              f"{description} × {quantity} {uom}")

        item_count += 1

        if not confirm("\n  Add another item?"):
            break

    return items


def select_uom() -> str:
    print("\n  Unit of Measure:")
    for key, (code, label) in UOMS.items():
        print(f"    [{key}] {code} — {label}")

    while True:
        choice = input("  Select UOM (1-5) [default 1 = EA]: ").strip() or "1"
        if choice in UOMS:
            code, label = UOMS[choice]
            print(f"  ✅ UOM: {code} ({label})")
            return code
        print("  ⚠️  Invalid choice.")


# ─────────────────────────────────────────────────────────
# Step 4 — Delivery Date
# ─────────────────────────────────────────────────────────
def enter_delivery_date() -> str:
    divider("STEP 3 — Delivery Date (Optional)")
    print("\n  Format : YYYYMMDD  e.g. 20240415")
    print("  Press Enter to skip")

    while True:
        value = input("\n  Required Delivery Date: ").strip()
        if not value:
            return ""
        if len(value) == 8 and value.isdigit():
            print(f"  ✅ Delivery Date: {value}")
            return value
        print("  ⚠️  Invalid format. Use YYYYMMDD or press Enter.")


# ─────────────────────────────────────────────────────────
# Step 5 — Review Order
# ─────────────────────────────────────────────────────────
def review_order(po: dict) -> bool:
    divider("STEP 4 — Review Purchase Order")

    print(f"\n  PO Number     : {po['po_number']}")
    print(f"  Supplier      : {po['supplier_name']} ({po['supplier_id']})")
    print(f"  PO Date       : {po['po_date']}")

    if po.get("delivery_date"):
        print(f"  Delivery Date : {po['delivery_date']}")

    print(f"\n  Items ({len(po['items'])}):")
    for i, item in enumerate(po["items"], 1):
        print(f"    {i}. [{item['item_number']}] {item['description']}"
              f" — Qty: {item['quantity']} {item['uom']}")

    print()
    return confirm("  Confirm and save to DynamoDB?")


# ─────────────────────────────────────────────────────────
# Step 6 — Write to DynamoDB
# ─────────────────────────────────────────────────────────
def write_to_dynamodb(po: dict) -> bool:
    try:
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        table    = dynamodb.Table(DYNAMODB_TABLE)

        file_name = f"PO_{po['po_number']}_{po['po_date']}.txt"

        item = {
            # ── Partition Key ──────────────────────────────
            "file_name":        file_name,

            # ── PO Details ────────────────────────────────
            "po_number":        po["po_number"],
            "supplier_id":      po["supplier_id"],
            "supplier_name":    po["supplier_name"],
            "po_date":          po["po_date"],
            "delivery_date":    po.get("delivery_date", ""),
            "line_count":       len(po["items"]),
            "items":            po["items"],

            # ── Status ────────────────────────────────────
            "edi_sent_status":  "N",
            "created_date":     datetime.now(timezone.utc).isoformat(),

            # ── S3 paths filled by processor later ────────
            "s3_flat_file_key": "",
            "s3_edi_key":       "",
            "processed_date":   "",
            
            # ── Vendor fields filled by send_to_vendor ────  ← ADD THESE
            "vendor_sent":      "N",
            "vendor_sent_date": "",
            "s3_vendor_key":    "",
        }

        table.put_item(Item=item)
        return True

    except NoCredentialsError:
        print("\n  ❌ AWS credentials not found.")
        print("     Run: aws configure")
        return False
    except ClientError as e:
        print(f"\n  ❌ DynamoDB error: {e.response['Error']['Message']}")
        return False
    except Exception as e:
        print(f"\n  ❌ Unexpected error: {str(e)}")
        return False


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────
def main():
    welcome()

    try:
        supplier      = select_supplier()
        items         = enter_items()
        delivery_date = enter_delivery_date()

        po = {
            "po_number":     generate_po_number(),
            "supplier_id":   supplier["id"],
            "supplier_name": supplier["name"],
            "po_date":       datetime.now(timezone.utc).strftime("%Y%m%d"),
            "delivery_date": delivery_date,
            "items":         items,
        }

        if not review_order(po):
            print("\n  ❌ Order cancelled. Nothing saved.\n")
            sys.exit(0)

        divider("SAVING TO DYNAMODB")
        print(f"\n  Writing PO {po['po_number']} to DynamoDB...")

        success = write_to_dynamodb(po)

        if success:
            print(f"\n  ✅ DynamoDB entry created!")
            print(f"\n  ┌──────────────────────────────────────────┐")
            print(f"  │  PO Number   : {po['po_number']:<27}│")
            print(f"  │  Supplier    : {po['supplier_name']:<27}│")
            print(f"  │  Items       : {str(len(po['items'])):<27}│")
            print(f"  │  Status      : N  (pending processing)    │")
            print(f"  └──────────────────────────────────────────┘")
            print(f"\n  🚀 Next — run processor.py:")
            print(f"     1. Reads DynamoDB  status=N")
            print(f"     2. Generates flat file → S3 flatfiles/inbox/")
            print(f"     3. Moves flat file  → S3 flatfiles/processed/")
            print(f"     4. Converts to .edi → S3 edi/outbound/")
            print(f"     5. Updates DynamoDB  status=Y")
        else:
            print("\n  ❌ Failed to save. Check errors above.")

        print("\n" + "═" * 52 + "\n")

    except KeyboardInterrupt:
        print("\n\n  ❌ Cancelled by user.\n")
        sys.exit(0)


if __name__ == "__main__":
    main()