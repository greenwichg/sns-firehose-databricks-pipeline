# Databricks Data Engineering — Interview Q&A (Part 2)

*Based on: SNS → Firehose → S3 → Databricks Medallion Pipeline*

---

## 1. Day-to-Day Responsibilities & Project Architecture

### My Day-to-Day Responsibilities

As a Data Engineer on this project, my typical day involves:

**Morning — Monitoring & Triage:**
- Check Databricks job run history for overnight failures (Gold views job runs at 1 AM UTC, Silver runs hourly).
- Review Bronze `_invalid` tables for spikes in rejected records — this often signals upstream schema changes or data quality issues.
- Monitor Firehose CloudWatch logs for delivery failures or throttling.
- Check S3 landing zone for expected file arrival patterns.

**Core Development Work:**
- Build and maintain PySpark notebooks for Bronze ingestion, Silver transformations, and Gold view creation.
- Write Delta MERGE logic for SCD Type 1 upserts in Silver dimension tables (`dim_customers`, `dim_products`).
- Develop and maintain reusable utilities — validation framework (`validation.py`), salt key generator (`salt_key.py`), config loader (`config_loader.py`).
- Define and update PySpark schemas for each data domain (orders, customers, products) in the schema registry.

**Infrastructure & DevOps:**
- Author and maintain Terraform modules for AWS infrastructure — S3 buckets, IAM roles, Kinesis Firehose streams, SNS topics, cross-account policies.
- Configure Databricks Asset Bundles (DABs) — job definitions, cluster configurations, scheduling, and environment-specific variables.
- Manage CI/CD pipelines in GitHub Actions — lint, test, Terraform validate, bundle validate, deploy, and pipeline trigger.

**Testing & Quality:**
- Write and run unit tests using pytest + local SparkSession with Delta extensions.
- Generate realistic test data using the `generate_test_data.py` script for validation testing.
- Perform data reconciliation — compare SNS message counts → Firehose delivery counts → Bronze row counts.

**Collaboration:**
- Work with upstream teams on schema changes and new data domains (e.g., adding an "inventory" topic).
- Configure Unity Catalog RBAC grants for analysts, engineers, and data scientists based on data governance requirements.
- Document pipeline design decisions and known trade-offs (e.g., the INTERVIEW_QUESTIONS.md in our repo).

---

### Project Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                           ACCOUNT A (Source)                                     │
│                                                                                  │
│   Upstream Services ──▶  SNS Topics                                              │
│                          ├── orders                                               │
│                          ├── customers        Cross-Account                      │
│                          └── products      ──────Subscribe──────┐                │
│                                                                  │                │
└──────────────────────────────────────────────────────────────────│────────────────┘
                                                                   │
                                                                   ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                           ACCOUNT B (Processing)                                 │
│                                                                                  │
│   ┌─────────────────────┐    ┌──────────────────────────────────┐                │
│   │  Kinesis Firehose    │    │  S3 Landing Zone                 │                │
│   │  ├── orders stream   │──▶│  s3://pipeline-landing/           │                │
│   │  ├── customers stream│    │  ├── orders/year=.../month=.../  │                │
│   │  └── products stream │    │  ├── customers/year=.../         │                │
│   │                      │    │  ├── products/year=.../          │                │
│   │  Buffer: 5MB/300s    │    │  ├── _checkpoints/               │                │
│   │  (dev)               │    │  └── {topic}_errors/             │                │
│   │  Compression: GZIP   │    │                                  │                │
│   └─────────────────────┘    │  Encryption: KMS                  │                │
│                               │  Versioning: Enabled              │                │
│                               │  Lifecycle: 90d→Glacier, 365d→Del│                │
│                               └──────────────┬───────────────────┘                │
│                                               │                                   │
│                                               ▼                                   │
│   ┌───────────────────────────────────────────────────────────────────┐           │
│   │                    DATABRICKS WORKSPACE                           │           │
│   │                                                                   │           │
│   │   ┌─────────────────────────────────────────────────────────┐     │           │
│   │   │  BRONZE LAYER (Every 15 min)                            │     │           │
│   │   │                                                         │     │           │
│   │   │  Autoloader (cloudFiles, notification mode)             │     │           │
│   │   │  ├── Read GZIP JSON from S3                             │     │           │
│   │   │  ├── Add audit columns (_ingestion_timestamp,           │     │           │
│   │   │  │   _source_file)                                      │     │           │
│   │   │  ├── Validate required fields                           │     │           │
│   │   │  ├── Split: valid → {topic}_raw table                   │     │           │
│   │   │  └── Split: invalid → {topic}_invalid table             │     │           │
│   │   │  OPTIMIZE after each batch                              │     │           │
│   │   └────────────────────┬────────────────────────────────────┘     │           │
│   │                        │                                          │           │
│   │                        ▼                                          │           │
│   │   ┌─────────────────────────────────────────────────────────┐     │           │
│   │   │  SILVER LAYER (Hourly at :30)                           │     │           │
│   │   │                                                         │     │           │
│   │   │  ┌── fact_orders ──────────────────────────────┐        │     │           │
│   │   │  │   Read orders_raw (valid only)              │        │     │           │
│   │   │  │   Select business columns                   │        │     │           │
│   │   │  │   Dedup on order_id                         │        │     │           │
│   │   │  │   Delta MERGE (upsert)                      │        │     │           │
│   │   │  │   OPTIMIZE + ZORDER (customer_id, order_date)│       │     │           │
│   │   │  └─────────────────────────────────────────────┘        │     │           │
│   │   │                                                         │     │           │
│   │   │  ┌── dim_customers ────────────────────────────┐        │     │           │
│   │   │  │   Read customers_raw (valid only)           │        │     │           │
│   │   │  │   SCD Type 1 MERGE on customer_id           │        │     │           │
│   │   │  │   Generate salt keys (email, phone, name)   │        │     │           │
│   │   │  │   OPTIMIZE + ZORDER (customer_id)           │        │     │           │
│   │   │  └─────────────────────────────────────────────┘        │     │           │
│   │   │                                                         │     │           │
│   │   │  ┌── dim_products ─────────────────────────────┐        │     │           │
│   │   │  │   Read products_raw (valid only)            │        │     │           │
│   │   │  │   SCD Type 1 MERGE on product_id            │        │     │           │
│   │   │  │   Generate salt key (name)                  │        │     │           │
│   │   │  │   OPTIMIZE + ZORDER (product_id, category)  │        │     │           │
│   │   │  └─────────────────────────────────────────────┘        │     │           │
│   │   └────────────────────┬────────────────────────────────────┘     │           │
│   │                        │                                          │           │
│   │                        ▼                                          │           │
│   │   ┌─────────────────────────────────────────────────────────┐     │           │
│   │   │  GOLD LAYER (Daily at 1 AM)                             │     │           │
│   │   │                                                         │     │           │
│   │   │  Views (no data copy — virtual):                        │     │           │
│   │   │  ├── vw_order_summary (orders + customers)              │     │           │
│   │   │  ├── vw_customer_lifetime_value (aggregated CLV)        │     │           │
│   │   │  └── vw_product_sales (product performance)             │     │           │
│   │   │                                                         │     │           │
│   │   │  Unity Catalog RBAC:                                    │     │           │
│   │   │  ├── analysts → SELECT on all 3 views                  │     │           │
│   │   │  ├── engineers → SELECT + MODIFY on all 3 views         │     │           │
│   │   │  └── data_scientists → SELECT on 2 views (no PII view) │     │           │
│   │   └─────────────────────────────────────────────────────────┘     │           │
│   │                                                                   │           │
│   │   SQL Warehouse (2X-Small, auto-stop 10 min)                     │           │
│   │   └── Serves Gold views to BI tools / analysts                   │           │
│   │                                                                   │           │
│   │   Unity Catalog: pipeline_catalog_{env}                          │           │
│   │   ├── bronze schema                                               │           │
│   │   ├── silver schema                                               │           │
│   │   └── gold schema                                                 │           │
│   └───────────────────────────────────────────────────────────────────┘           │
└──────────────────────────────────────────────────────────────────────────────────┘
```

**Key numbers to remember for the interview:**
- 3 data domains: orders, customers, products
- 2 AWS accounts: Account A (SNS source), Account B (Firehose + S3 + Databricks)
- 3 IAM roles: Firehose delivery, SNS subscription, Databricks S3 access
- 3 medallion layers: Bronze (raw), Silver (curated), Gold (business views)
- 3 Gold views with role-based access for 3 user groups

---

## 2. Medallion Architecture — How Bronze and Silver Layers Are Connected

### Overview

The medallion architecture organizes data into three progressive layers of quality:

| Layer | Purpose | Tables | Schedule |
|-------|---------|--------|----------|
| **Bronze** | Raw ingestion with validation | `orders_raw`, `orders_invalid`, `customers_raw`, `customers_invalid`, `products_raw`, `products_invalid` | Every 15 min |
| **Silver** | Cleaned, deduplicated, business-modeled | `fact_orders`, `dim_customers`, `dim_products` | Hourly at :30 |
| **Gold** | Business views for consumption | `vw_order_summary`, `vw_customer_lifetime_value`, `vw_product_sales` | Daily at 1 AM |

### Bronze → Silver Connection

**Bronze writes to Delta tables.** Autoloader reads GZIP JSON from S3, validates records, and writes valid records to `{topic}_raw` Delta tables in the `bronze` schema. Each record gets audit columns: `_ingestion_timestamp`, `_source_file`, `_record_status`, and `_validation_errors`.

**Silver reads from Bronze Delta tables.** Silver notebooks use `spark.table(source_table)` to read from Bronze `_raw` tables, filtering only valid records:

```python
bronze_df = (
    spark.table(f"{catalog}.bronze.orders_raw")
    .filter(F.col("_record_status") == "valid")
)
```

**The connection is through Unity Catalog table references.** Bronze and Silver are separate schemas within the same catalog (`pipeline_catalog_{env}`). Silver reads from `catalog.bronze.{topic}_raw` and writes to `catalog.silver.{table_name}`. There is no direct file dependency — Delta Lake provides snapshot isolation, so Silver always reads a consistent view of Bronze regardless of concurrent writes.

### Bronze Implementation Details

1. **Autoloader** reads files using `cloudFiles` format with notification mode (SQS-based file discovery).
2. **Audit columns** added via `add_audit_columns()` — `_ingestion_timestamp` (current timestamp) and `_source_file` (input file name).
3. **Validation** via `validate_required_fields()` — checks each required field for NULL or empty string, produces `_record_status` and `_validation_errors`.
4. **Split** — valid records go to `{topic}_raw`, invalid to `{topic}_invalid`. Both use append mode with `mergeSchema = true`.
5. **Checkpointing** — Autoloader tracks processed files at `s3://{bucket}/_checkpoints/bronze/{topic}`.
6. **`trigger(availableNow=True)`** — processes all available files then stops (batch mode, not continuous).

### Silver Implementation Details

1. **Read Bronze** — `spark.table()` reads the entire `_raw` table (filtered to valid records).
2. **Select columns** — only business-relevant columns are carried forward (dropping `_source_file`, `_validation_errors`, etc.).
3. **Deduplicate** — `dropDuplicates(["order_id"])` removes duplicate records from SNS at-least-once delivery.
4. **Add processing timestamp** — `_silver_processed_at = current_timestamp()`.
5. **Delta MERGE** — upsert into the Silver table on primary key, updating only if source has a newer `_ingestion_timestamp`.
6. **OPTIMIZE + ZORDER** — compact files and cluster data for downstream query performance.

---

## 3. Responsibilities and Processing Steps in the Silver Layer

### Silver Layer Responsibilities

The Silver layer is responsible for:

1. **Data cleansing** — Only valid Bronze records are processed (`_record_status == "valid"`).
2. **Column selection** — Removing ingestion metadata and keeping only business-relevant columns.
3. **Deduplication** — Removing duplicates caused by SNS at-least-once delivery.
4. **Type casting** — Converting string timestamps to proper `TIMESTAMP` type (e.g., `F.col("order_date").cast("timestamp")`).
5. **Business modeling** — Organizing data into facts (events/transactions) and dimensions (reference/lookup entities).
6. **PII protection** — Generating salted SHA-256 hash keys for sensitive columns (email, phone, name).
7. **Incremental upsert** — Using Delta MERGE for idempotent insert/update operations.
8. **Physical optimization** — Running OPTIMIZE and ZORDER for downstream query performance.

### Processing Steps — Fact Table (fact_orders)

```
Bronze orders_raw
    │
    ▼
Filter: _record_status == "valid"
    │
    ▼
Select: order_id, customer_id, order_date, total_amount, status,
        payment_method, shipping_address_id, _ingestion_timestamp
    │
    ▼
Cast: order_date → TIMESTAMP
    │
    ▼
Add: _silver_processed_at = current_timestamp()
    │
    ▼
Dedup: dropDuplicates(["order_id"])
    │
    ▼
Delta MERGE into silver.fact_orders:
  - Match on: order_id
  - If matched AND source._ingestion_timestamp > target._ingestion_timestamp → UPDATE ALL
  - If not matched → INSERT ALL
    │
    ▼
OPTIMIZE fact_orders ZORDER BY (customer_id, order_date)
```

### Processing Steps — Dimension Table (dim_customers)

```
Bronze customers_raw
    │
    ▼
Filter: _record_status == "valid"
    │
    ▼
Select: customer_id, name, email, phone, address, city, state,
        country, postal_code, created_at, updated_at, _ingestion_timestamp
    │
    ▼
Cast: created_at, updated_at → TIMESTAMP
    │
    ▼
Add: _silver_processed_at = current_timestamp()
    │
    ▼
Dedup: dropDuplicates(["customer_id"])
    │
    ▼
Generate Salt Keys:
  - email → email_salt_key (SHA-256 with secret salt)
  - phone → phone_salt_key
  - name  → name_salt_key
    │
    ▼
Delta MERGE into silver.dim_customers (SCD Type 1):
  - Match on: customer_id
  - If matched AND source._ingestion_timestamp > target._ingestion_timestamp → UPDATE ALL
  - If not matched → INSERT ALL
    │
    ▼
OPTIMIZE dim_customers ZORDER BY (customer_id)
```

### Key Differences Between Fact and Dimension Processing

| Aspect | Fact (fact_orders) | Dimension (dim_customers, dim_products) |
|--------|--------------------|-----------------------------------------|
| **Pattern** | Event/transaction data | Reference/lookup data |
| **SCD Type** | N/A (append with update) | SCD Type 1 (overwrite old values) |
| **Salt keys** | None | Yes — PII columns get salted SHA-256 hashes |
| **Salt source** | N/A | Databricks Secrets (`pipeline-secrets` scope) |
| **Partition** | `order_date` | None |
| **ZORDER** | `customer_id, order_date` | `customer_id` / `product_id, category` |
| **Extra params** | None | `salt_secret_scope`, `salt_secret_key` |

---

## 4. Data Volume and Size Questions

### Q: What is the volume of data your pipeline handles?

**Firehose delivery rates:**
- **Dev:** Buffer is 5 MB / 300 seconds — Firehose flushes every 5 minutes or when 5 MB accumulates, whichever comes first.
- **Prod:** Buffer is 10 MB / 60 seconds — flushes every minute or at 10 MB.

**Per-topic estimates (prod):**
- If Firehose flushes every 60 seconds with ~10 MB compressed GZIP JSON, that's roughly 10 MB × 60 flushes/hour × 24 hours = **~14.4 GB/day compressed per topic**.
- With 3 topics, that's **~43 GB/day compressed** landing in S3.
- GZIP typically compresses JSON ~80%, so raw uncompressed volume is roughly **~200 GB/day**.

**Autoloader throughput:**
- `maxFilesPerTrigger = 1000` — processes up to 1000 files per micro-batch.
- Bronze runs every 15 minutes — each run processes all files that arrived since the last run.

**Cluster sizing:**
- Bronze: 2 workers (`i3.xlarge` — 4 vCPUs, 30.5 GB RAM, NVMe SSD) per topic.
- Silver: 3 workers (`i3.xlarge`) for all three tasks running in parallel.
- Gold: 1 worker (`i3.xlarge`) — lightweight since views are virtual.

### Q: How do you handle data growth over time?

- **S3 lifecycle:** Data transitions to Glacier after 90 days and is deleted after 365 days. This keeps landing zone costs low.
- **Delta OPTIMIZE:** Compacts small files into larger ones, improving read performance as tables grow.
- **ZORDER:** Clusters data for efficient data skipping on commonly filtered columns.
- **Identified gap:** Silver currently reads the full Bronze table every run. As Bronze grows to hundreds of millions of records, this becomes a bottleneck. The planned fix is incremental processing using a high-water mark or Delta Change Data Feed.

### Q: What happens during a volume spike (e.g., 100x normal traffic)?

- **Firehose** auto-scales to handle the burst (up to account limits).
- **S3** handles unlimited writes — no bottleneck.
- **Autoloader** processes at most 1000 files per trigger (`maxFilesPerTrigger`), creating natural backpressure. Remaining files are processed in the next trigger.
- **Risk:** The Bronze cluster (2 workers) may be undersized for 100x volume. Autoscaling clusters (`min_workers` / `max_workers`) would help.

---

## 5. SQL Warehouse Questions

### Q: What is a SQL Warehouse in Databricks?

A SQL Warehouse is a Databricks-managed compute resource optimized for SQL queries and BI workloads. Unlike general-purpose clusters (which run notebooks and Spark jobs), SQL Warehouses are specifically tuned for low-latency analytical queries against Delta tables.

### Q: How is the SQL Warehouse configured in your project?

From the Terraform module (`databricks/main.tf`):

```hcl
resource "databricks_sql_endpoint" "gold_warehouse" {
  name             = "${var.project_name}-${var.environment}-gold-warehouse"
  cluster_size     = "2X-Small"
  max_num_clusters = 1
  auto_stop_mins   = 10
}
```

- **Name:** `sns-firehose-pipeline-{env}-gold-warehouse`
- **Size:** `2X-Small` — the smallest available size, suitable for light BI queries.
- **Max clusters:** 1 — no auto-scaling to additional clusters (cost control).
- **Auto-stop:** 10 minutes — warehouse shuts down after 10 minutes of inactivity to save costs.

### Q: Who uses the SQL Warehouse and for what?

- **Analysts** query Gold views (`vw_order_summary`, `vw_customer_lifetime_value`, `vw_product_sales`) for business reporting.
- **BI tools** (Tableau, Power BI, Looker) connect via JDBC/ODBC to the warehouse endpoint.
- **Ad-hoc queries** from the Databricks SQL editor.

### Q: What is the difference between SQL Warehouse and a regular cluster?

| Aspect | SQL Warehouse | General-Purpose Cluster |
|--------|---------------|------------------------|
| **Optimized for** | SQL queries, BI | Notebooks, Spark jobs, ML |
| **Languages** | SQL only | Python, Scala, R, SQL |
| **Auto-stop** | Yes (configurable) | Yes (configurable) |
| **Scaling** | Multi-cluster scaling | Worker autoscaling |
| **Use case** | Gold layer queries, dashboards | Bronze/Silver processing |
| **Cost model** | DBU/hour while running | DBU/hour while running |

### Q: Why use a 2X-Small warehouse?

- Gold views are relatively simple SQL queries (joins and aggregations on Silver tables).
- The `2X-Small` size is the most cost-effective for low-concurrency, moderate-complexity queries.
- `auto_stop_mins = 10` ensures we don't pay for idle time.
- If query performance degrades with data growth, we can scale up the size or increase `max_num_clusters` for concurrency.

---

## 6. Databricks Optimization and Performance Tuning

### Q: What optimization techniques do you use?

**1. Delta OPTIMIZE + ZORDER:**
```sql
OPTIMIZE fact_orders ZORDER BY (customer_id, order_date)
OPTIMIZE dim_customers ZORDER BY (customer_id)
OPTIMIZE dim_products ZORDER BY (product_id, category)
```
- OPTIMIZE compacts small files into larger ones (solves the small files problem from frequent Firehose flushes).
- ZORDER clusters data on the specified columns, enabling data skipping — queries filtering by `customer_id` or `order_date` skip irrelevant files entirely.

**2. Optimized Write + Auto-Merge:**
```yaml
spark_conf:
  "spark.databricks.delta.schema.autoMerge.enabled": "true"
  "spark.databricks.delta.optimizeWrite.enabled": "true"
```
- `optimizeWrite` automatically right-sizes output files during write operations, reducing the need for post-write OPTIMIZE.
- `autoMerge` allows schema evolution without manual DDL changes.

**3. Autoloader Notification Mode:**
- `cloudFiles.useNotifications = true` — uses S3 event notifications + SQS instead of directory listing.
- For buckets with millions of files, this avoids expensive S3 LIST calls that would slow down file discovery.

**4. Trigger Mode (`availableNow=True`):**
- Processes all available files then stops the cluster.
- Cost savings: cluster only runs during active processing, not idle between triggers.

**5. GZIP Compression:**
- Firehose compresses files before writing to S3, reducing storage by ~80% and improving read throughput.

**6. Partitioning:**
- `fact_orders` is partitioned by `order_date` — enables partition pruning for time-range queries.
- Note: using a TIMESTAMP column directly as partition key can create too many small partitions. A coarser granularity like `date(order_date)` would be better.

**7. Parallel Task Execution:**
- Bronze: 3 topics ingest in parallel (separate jobs).
- Silver: fact_orders, dim_customers, dim_products run in parallel within the same job (no dependencies between them).

### Q: How do you decide ZORDER columns?

Choose columns that are:
- Frequently used in `WHERE` clauses or `JOIN` conditions.
- High cardinality (many distinct values) — ZORDER is most effective on high-cardinality columns.

In our project:
- `fact_orders` → `customer_id` (JOIN key) + `order_date` (filter key).
- `dim_customers` → `customer_id` (primary/JOIN key).
- `dim_products` → `product_id` (primary key) + `category` (common filter).

---

## 7. Scenario-Based Questions — Slow Jobs and Performance Issues

### Scenario 1: Bronze ingestion is taking longer than 15 minutes, causing runs to pile up.

**Diagnosis:**
- Check if Firehose delivered an unusually large number of files (volume spike).
- Check Spark UI for skewed partitions, OOM errors, or executor failures.
- Check if `OPTIMIZE` after each batch is the bottleneck (it rewrites all files).

**Solutions:**
- Increase `num_workers` from 2 to 4, or enable autoscaling (`min_workers: 2, max_workers: 6`).
- Move `OPTIMIZE` to a separate scheduled job (e.g., nightly) instead of running it after every batch.
- Increase `maxFilesPerTrigger` if the cluster can handle more files per batch, or decrease it to process smaller batches faster.
- Set `max_concurrent_runs = 1` on the job to prevent overlapping runs (already the default).

### Scenario 2: Silver `fact_orders` MERGE is getting slower as the table grows.

**Diagnosis:**
- Silver reads the entire Bronze table every run — as Bronze grows, this becomes the bottleneck.
- The MERGE also scans the entire target table to find matches.

**Solutions:**
- **Implement incremental reads:** Track a high-water mark (`MAX(_ingestion_timestamp)` from last successful run) and filter Bronze to only newer records.
- **Use Delta Change Data Feed (CDF):** Enable CDF on Bronze tables and read only changes since the last Silver run.
- **ZORDER the target table** on the MERGE key (`order_id`) to speed up the match phase.
- **Run OPTIMIZE less frequently** — nightly instead of after every Silver run.

### Scenario 3: Gold view queries are slow for analysts.

**Diagnosis:**
- Views are virtual (no materialized data) — every query re-executes the SQL against Silver tables.
- Silver tables may have many small files or poor clustering.
- SQL Warehouse may be undersized.

**Solutions:**
- Ensure Silver tables are well-optimized (regular OPTIMIZE + ZORDER).
- Scale up the SQL Warehouse from `2X-Small` to `Small` or `Medium`.
- Increase `max_num_clusters` if multiple analysts query concurrently.
- Consider materializing frequently-queried views as Delta tables instead of virtual views.
- Add caching: `spark.conf.set("spark.databricks.io.cache.enabled", "true")`.

### Scenario 4: A job fails with "ConcurrentModificationException" on a Delta table.

**Diagnosis:**
- Two operations tried to modify the same Delta table simultaneously with conflicting changes.

**Solutions:**
- In our pipeline this shouldn't happen — each Silver task writes to a different table.
- If it does occur (e.g., manual ad-hoc writes during a scheduled run), retry the failed job. Delta's optimistic concurrency control resolves after the first operation commits.
- Set isolation level: `ALTER TABLE SET TBLPROPERTIES ('delta.isolationLevel' = 'WriteSerializable')`.

### Scenario 5: Autoloader stops processing new files even though new files exist in S3.

**Diagnosis:**
- **Checkpoint corruption:** Autoloader's checkpoint might be inconsistent.
- **SQS queue issues (notification mode):** Messages expired (4-day retention) or queue was deleted.
- **Schema mismatch:** New files have a schema that conflicts with the explicit schema.

**Solutions:**
- Check the checkpoint directory for corruption. If needed, reset the checkpoint (this will reprocess all files — duplicates handled by Silver MERGE).
- Verify the SQS queue exists and has messages. Monitor queue depth with CloudWatch.
- Check Autoloader's `_rescued_data` column (if configured) or Spark driver logs for parse errors.
- Set `cloudFiles.badRecordsPath` to capture unparseable files instead of failing the entire batch.

### Scenario 6: Suddenly 30% of records are going to the `_invalid` table.

**Diagnosis:**
- Upstream schema change — a required field was renamed or its type changed.
- Upstream bug — a service is sending incomplete records.

**Solutions:**
- Query the `_invalid` table: `SELECT _validation_errors, COUNT(*) FROM orders_invalid GROUP BY 1` to identify which fields are failing.
- Contact the upstream team with specific error patterns.
- If the field was renamed, update the schema definition in `schemas/orders.py` and the required fields list.
- **Production recommendation:** Set up an alert when invalid percentage exceeds a threshold (e.g., >5%).

---

## 8. Schema Evolution Scenarios

### Q: If a table currently has 10 columns and 10 new columns are introduced upstream, how would you handle this?

**Step-by-step approach in our pipeline:**

**At the Autoloader (Bronze) level:**

Our Autoloader has two relevant configurations:
```python
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
.option("cloudFiles.inferColumnTypes", "true")
.option("mergeSchema", "true")
.schema(raw_schema)  # ← This is the problem
```

The explicit `.schema(raw_schema)` takes precedence and drops any columns not defined in the PySpark schema. So the 10 new columns would be **silently dropped** in the current setup.

**To handle the 10 new columns:**

**Option A — Update the explicit schema (controlled evolution):**
1. Update the PySpark schema in `schemas/orders.py` to include the 10 new `StructField` entries.
2. Update `pipeline_config.yaml` if any new fields are required or need validation.
3. Deploy via `databricks bundle deploy`.
4. The `mergeSchema = true` on the Delta write automatically adds new columns to the existing table without breaking it.

**Option B — Remove explicit schema (automatic evolution):**
1. Remove the `.schema(raw_schema)` line from the Autoloader readStream.
2. Let Autoloader infer the schema from the data and evolve it automatically with `schemaEvolutionMode = "addNewColumns"`.
3. The schema location at `{checkpoint_path}/_schema` tracks the inferred schema.
4. New columns appear automatically in Bronze tables.

**Trade-offs:**

| Approach | Pros | Cons |
|----------|------|------|
| **Explicit schema (Option A)** | Full control, predictable types, catches unexpected changes | Manual updates needed for every schema change |
| **Schema inference (Option B)** | Zero-touch for new columns | Types may be inferred incorrectly, unexpected columns can bloat tables |

**At the Silver level:**

Silver notebooks explicitly select columns:
```python
fact_df = bronze_df.select(
    "order_id", "customer_id", "order_date", ...
)
```

New Bronze columns are **not automatically propagated** to Silver. This is intentional — Silver is a curated layer where only business-relevant columns should exist.

To add new columns to Silver:
1. Update the Silver notebook's `select()` to include the new columns.
2. Update the Silver table DDL (`CREATE TABLE IF NOT EXISTS`) to include new column definitions.
3. Use `mergeSchema = true` on the MERGE write so existing data isn't affected.
4. Backfill: Re-run Silver with the full Bronze table to populate new columns for historical records.

**At the Gold level:**

Gold views are `CREATE OR REPLACE VIEW` — they are recreated daily. Simply update the view SQL to include new columns from Silver. No data migration needed since views are virtual.

### Q: What if a column's data type changes (e.g., `total_amount` from DOUBLE to STRING)?

- With the explicit schema, Spark attempts to cast the string to double. Invalid values become `NULL`.
- `NULL` in a required field fails validation → record goes to `_invalid` table.
- This is actually good — data quality issues are caught at Bronze.
- If the type change is permanent, update the PySpark schema and adjust Silver transformations accordingly.

### Q: What if a column is removed from upstream?

- With explicit schema, the column becomes `NULL` for all new records.
- If it's a required field, all new records fail validation → 100% invalid.
- Detection: Monitor invalid record counts. A sudden spike to 100% invalid clearly indicates a schema break.
- Resolution: Update required fields list if the column is no longer relevant, or coordinate with upstream to restore it.

### Q: How does `mergeSchema` work on Delta writes?

When `mergeSchema = true`:
- **New columns** in the DataFrame are added to the Delta table schema automatically.
- **Missing columns** in the DataFrame are filled with `NULL` in the written rows.
- **Type changes** are NOT automatically handled — they cause errors. You need to explicitly cast or use `overwriteSchema = true` (which replaces the entire schema).

This is why we have `"spark.databricks.delta.schema.autoMerge.enabled": "true"` in our cluster Spark conf — it enables merge schema globally for all Delta writes on that cluster.
