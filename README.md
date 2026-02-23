# SNS → Firehose → S3 → Databricks Medallion Pipeline

## Architecture

```
┌─────────────────┐     ┌───────────────────┐    ┌───────────┐     ┌──────────────────────────┐
│  SNS Topics     │───▶ │ Kinesis Firehose  │───▶│ S3        │───▶ │  Databricks              │
│  (Account A)    │     │ (Account B)       │    │ Landing   │     │  Bronze → Silver → Gold  │
│                 │     │                   │    │           │     │                          │
│ • orders        │     │ • orders stream   │    │ /orders/  │     │ Autoloader ──▶ Delta     │
│ • customers     │     │ • customers stream│    │ /customers│     │ Valid/Invalid split      │
│ • products      │     │ • products stream │    │ /products │     │ Facts + Dimensions       │
└─────────────────┘     └───────────────────┘    └───────────┘     │ Views + RBAC             │
                                                                   └──────────────────────────┘
```

## Project Structure

```
├── terraform/                    # Infrastructure as Code
│   ├── modules/
│   │   ├── sns/                  # SNS topics (source account)
│   │   ├── kinesis_firehose/     # Firehose streams (target account)
│   │   ├── s3/                   # Landing zone bucket
│   │   ├── iam/                  # Cross-account IAM roles
│   │   └── databricks/           # Unity Catalog, schemas, grants
│   ├── environments/
│   │   ├── dev/
│   │   └── prod/
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
│
├── databricks/                   # Databricks Asset Bundle
│   ├── databricks.yml            # Bundle configuration
│   ├── resources/                # Job definitions
│   │   ├── bronze_jobs.yml
│   │   ├── silver_jobs.yml
│   │   └── gold_jobs.yml
│   ├── src/
│   │   ├── bronze/
│   │   │   └── autoloader_ingestion.py
│   │   ├── silver/
│   │   │   ├── fact_orders.py
│   │   │   ├── dim_customers.py
│   │   │   └── dim_products.py
│   │   ├── gold/
│   │   │   └── create_views.py
│   │   ├── schemas/              # PySpark schema definitions
│   │   └── utils/                # Shared utilities
│   │       ├── config_loader.py
│   │       ├── validation.py
│   │       └── salt_key.py
│   └── tests/
│
└── configs/
    └── pipeline_config.yaml
```

## Setup

### 1. Terraform — Infrastructure

```bash
cd terraform
terraform init -backend-config=environments/dev/backend.hcl
terraform plan -var-file=environments/dev/terraform.tfvars
terraform apply -var-file=environments/dev/terraform.tfvars
```

### 2. Databricks Asset Bundle — Deploy

```bash
cd databricks
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run full_pipeline -t dev
```

## Medallion Layers

### Bronze
- **Autoloader** reads JSON from S3 (`cloudFiles` format)
- Records are validated against required fields
- Split into `{topic}_raw` (valid) and `{topic}_invalid` (invalid) Delta tables
- Both tables share the same schema + audit columns (`_record_status`, `_validation_errors`, `_ingestion_timestamp`, `_source_file`)

### Silver
- **Fact tables** (`fact_orders`) — selected business columns, deduped, upserted via Delta merge
- **Dimension tables** (`dim_customers`, `dim_products`) — SCD Type 1, with SHA-256 salt keys on PII columns

### Gold
- **Views** — `vw_order_summary`, `vw_customer_lifetime_value`, `vw_product_sales`
- **RBAC via Unity Catalog** — role-based grants (analysts: SELECT, engineers: SELECT+MODIFY, data_scientists: SELECT on subset)
