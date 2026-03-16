# edi-pipeline
AWS EDI 850 Pipeline — DynamoDB, S3, Python
Complete pipeline architecture! It covers all three phases:
Phase 1 — 850 PO flow starting from simulate_erp.py → DynamoDB → EDI generation → vendor bucket, all chained via GitHub Actions.
Phase 2 — 810 Invoice flow starting from simulate_vendor.py → invoice routing → inbound folder, with 850 and 810 archiving along the way.
Phase 3 — Outcomes showing the three validations, DynamoDB invoice table, S3 flat file storage, success email and error email paths.
