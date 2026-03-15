"""
One-time patch script
Adds vendor_sent=N to all existing DynamoDB records
that don't have the vendor_sent column yet
"""

import boto3

AWS_REGION     = "ap-south-1"
DYNAMODB_TABLE = "edi-pipeline-edi-jobs"

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table    = dynamodb.Table(DYNAMODB_TABLE)

# Scan all records
response = table.scan()
items    = response.get("Items", [])

print(f"Found {len(items)} records")

updated = 0
skipped = 0

for item in items:
    file_name = item["file_name"]

    # Skip if already has vendor_sent column
    if "vendor_sent" in item:
        print(f"  Skipping {file_name} — already has vendor_sent")
        skipped += 1
        continue

    # Add vendor_sent fields
    table.update_item(
        Key = {"file_name": file_name},
        UpdateExpression = (
            "SET vendor_sent = :vs, "
            "vendor_sent_date = :vd, "
            "s3_vendor_key = :vk"
        ),
        ExpressionAttributeValues = {
            ":vs": "N",
            ":vd": "",
            ":vk": "",
        }
    )
    print(f"  ✅ Updated {file_name} → vendor_sent = N")
    updated += 1

print(f"\nDone — Updated: {updated}, Skipped: {skipped}")
