# Databricks Data Engineering — Interview Q&A (Part 3)

*Based on: SNS → Firehose → S3 → Databricks Medallion Pipeline + General Databricks Knowledge*

---

## 1. Unity Catalog (UC)

### Q: What is Unity Catalog?

Unity Catalog is Databricks' centralized governance layer for all data and AI assets. It provides a single place to manage permissions, audit access, track lineage, and discover data across all workspaces in an account.

In our project, Unity Catalog manages:
- **Catalog:** `pipeline_catalog_{env}` (one per environment)
- **Schemas:** `bronze`, `silver`, `gold`
- **Tables:** Delta tables for each medallion layer
- **Views:** Gold layer business views
- **External Locations:** S3 landing bucket mapped to a storage credential
- **Grants:** Role-based access for analysts, engineers, data scientists

### Q: What is the UC hierarchy/namespace?

```
Metastore (one per region)
  └── Catalog: pipeline_catalog_dev
        ├── Schema: bronze
        │     ├── Table: orders_raw
        │     ├── Table: orders_invalid
        │     ├── Table: customers_raw
        │     ├── Table: customers_invalid
        │     ├── Table: products_raw
        │     └── Table: products_invalid
        ├── Schema: silver
        │     ├── Table: fact_orders
        │     ├── Table: dim_customers
        │     └── Table: dim_products
        └── Schema: gold
              ├── View: vw_order_summary
              ├── View: vw_customer_lifetime_value
              └── View: vw_product_sales
```

A fully qualified table reference is: `pipeline_catalog_dev.silver.fact_orders`

### Q: Where are logs stored in Unity Catalog?

**Audit Logs:**
- Unity Catalog generates audit logs for every data access event — who accessed which table, when, and what operation was performed.
- These logs are delivered to a customer-configured storage location (S3 bucket) via the account-level audit log delivery configuration.
- Typical path: `s3://{audit-log-bucket}/unityCatalog/{date}/audit-logs-*.json`
- Logs capture: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `GRANT`, `REVOKE`, schema changes, credential access, and more.

**System Tables (Databricks-managed):**
- `system.access.audit` — query-level audit logs accessible via SQL.
- `system.billing.usage` — billing and DBU consumption data.
- `system.compute.clusters` — cluster lifecycle events.
- `system.storage.predictive_optimization` — optimization recommendations.

**In our project, relevant log locations:**
- **Firehose delivery logs:** CloudWatch at `/aws/kinesisfirehose/{stream-name}` (30-day retention).
- **Autoloader checkpoints:** `s3://{landing-bucket}/_checkpoints/bronze/{topic}` — tracks processed files.
- **Delta transaction logs:** `_delta_log/` directory within each Delta table's storage location — records every commit, MERGE, OPTIMIZE operation.
- **Job run logs:** Available in the Databricks Jobs UI under each run's output.

### Q: Explain UC architecture and access control with table-level restrictions.

**Architecture:**

```
┌───────────────────────────────────────────────────┐
│                 ACCOUNT LEVEL                      │
│                                                    │
│  ┌──────────────────────────────────┐              │
│  │  Unity Catalog Metastore         │              │
│  │  (one per region)                │              │
│  │                                  │              │
│  │  ┌── Storage Credentials ──┐    │              │
│  │  │   IAM roles for S3      │    │              │
│  │  └─────────────────────────┘    │              │
│  │                                  │              │
│  │  ┌── External Locations ───┐    │              │
│  │  │   S3 URLs mapped to     │    │              │
│  │  │   storage credentials   │    │              │
│  │  └─────────────────────────┘    │              │
│  └──────────────────────────────────┘              │
│                    │                                │
│          Attached to workspaces                     │
│                    │                                │
│  ┌─────────────┐  ┌─────────────┐                  │
│  │ Workspace A │  │ Workspace B │  ...             │
│  │ (dev)       │  │ (prod)      │                  │
│  └─────────────┘  └─────────────┘                  │
└───────────────────────────────────────────────────┘
```

**Access Control Model:**

Unity Catalog uses a **grant-based** model with inheritance:

```
GRANT USE CATALOG ON CATALOG pipeline_catalog_dev TO `analysts`
  └── Required before any schema/table access

GRANT USE SCHEMA ON SCHEMA pipeline_catalog_dev.gold TO `analysts`
  └── Required before any table/view access in that schema

GRANT SELECT ON VIEW pipeline_catalog_dev.gold.vw_order_summary TO `analysts`
  └── Actual data access permission
```

**Table-Level Restrictions in Our Project:**

```python
# From create_views.py — role-based grants
role_grants = {
    "analysts": {
        "views": ["vw_order_summary", "vw_customer_lifetime_value", "vw_product_sales"],
        "privileges": ["SELECT"],
    },
    "engineers": {
        "views": ["vw_order_summary", "vw_customer_lifetime_value", "vw_product_sales"],
        "privileges": ["SELECT", "MODIFY"],
    },
    "data_scientists": {
        "views": ["vw_customer_lifetime_value", "vw_product_sales"],
        "privileges": ["SELECT"],
        # NOTE: No access to vw_order_summary (contains plain-text customer_name)
    },
}
```

**Beyond table-level, UC also supports:**

- **Column Masking:** Apply a function that returns masked data for specific users.
  ```sql
  ALTER TABLE silver.dim_customers
  ALTER COLUMN email SET MASK mask_pii_email USING COLUMNS (email);
  ```
  Non-privileged users see `****@****.com` instead of the real email.

- **Row Filters:** Restrict which rows a user can see.
  ```sql
  ALTER TABLE silver.fact_orders
  SET ROW FILTER region_filter ON (customer_state);
  ```
  Analysts in the "US-West" team only see orders from western states.

- **Data Lineage:** UC automatically tracks which tables feed into which views/tables. Visible in the Catalog Explorer → Lineage tab.

---

## 2. API & Architecture — FastAPI vs REST API

### Q: What is the difference between FastAPI and REST API?

**REST API** is an architectural pattern — a set of principles for designing web APIs using HTTP methods (GET, POST, PUT, DELETE) with resource-oriented URLs. REST is not a framework; it's a design philosophy.

**FastAPI** is a Python web framework for building REST APIs. It's one specific implementation of the REST pattern, comparable to Flask or Django REST Framework.

| Aspect | REST API (Concept) | FastAPI (Framework) |
|--------|--------------------|--------------------|
| **What it is** | Architectural style/pattern | Python framework that implements REST |
| **Language** | Language-agnostic | Python |
| **Key features** | Stateless, resource-oriented, HTTP methods | Auto-generated OpenAPI docs, async support, Pydantic validation, type hints |
| **Comparison targets** | GraphQL, gRPC, SOAP | Flask, Django REST, Express.js |

### Q: How is FastAPI relevant to a data engineering project?

In a data pipeline context, FastAPI could serve as:

1. **Pipeline trigger API:** An endpoint that upstream services call to publish messages (instead of or alongside SNS).
2. **Metadata API:** Expose pipeline status, record counts, data quality metrics via REST endpoints.
3. **Configuration API:** Allow teams to update pipeline parameters (e.g., validation rules, scheduling) without redeploying.
4. **Data serving layer:** Serve Gold layer query results to applications that can't connect to the SQL Warehouse directly.

**Example architecture with FastAPI:**

```
Upstream App → FastAPI (validation + routing) → SNS Topics → Firehose → S3 → Databricks
                  │
                  └── /api/v1/status → Query Databricks SQL Warehouse → Return pipeline health
```

### Q: Basic REST API design principles?

- **Resources as nouns:** `/api/v1/orders`, `/api/v1/customers/{id}` — not `/api/v1/getOrders`.
- **HTTP methods as verbs:** GET (read), POST (create), PUT (update), DELETE (remove).
- **Stateless:** Each request contains all information needed — no server-side session.
- **Status codes:** 200 (OK), 201 (Created), 400 (Bad Request), 404 (Not Found), 500 (Server Error).
- **Pagination:** For large datasets, use `?limit=100&offset=0` or cursor-based pagination.
- **Versioning:** `/api/v1/...` to allow backward-compatible evolution.

---

## 3. Handling Large Data — Millions of Records with Wide Tables

### Q: How do you handle tables with 1000+ columns?

Wide tables (1000+ columns) present specific challenges:

**Storage & Read Optimization:**
- **Columnar format (Parquet/Delta):** Delta Lake stores data in Parquet, which is columnar. Reading 10 columns from a 1000-column table only reads those 10 columns from disk — no penalty for table width.
- **Column pruning:** Always `SELECT` only the columns you need — never `SELECT *` on a wide table.
- **ZORDER on frequently queried columns:** Cluster data on the 2-3 most commonly filtered columns.

**Schema Management:**
- **Group related columns:** Instead of 1000 flat columns, use nested structs: `address.city`, `address.state`, `address.zip`. Delta supports nested types.
- **Vertical partitioning:** Split into multiple narrower tables joined by a key. For example, separate `customer_core` (10 key columns) from `customer_extended` (990 attribute columns).

**Performance:**
- **Predicate pushdown:** Filters on partitioned or ZORDERed columns skip irrelevant files entirely.
- **Caching:** Enable Delta caching (`spark.databricks.io.cache.enabled = true`) for repeated queries on the same columns.
- **Broadcast joins:** If one side of a join is small, broadcast it to avoid shuffles.

**In our project context:**
Our tables are relatively narrow (10-15 columns), but the same principles apply at scale. The `items` array column in `orders_raw` is an example of using nested structures instead of flattening into many columns.

### Q: How would you add new columns and backfill 10–20 TB of historical data?

**Step 1 — Add new columns to the schema:**
```sql
ALTER TABLE silver.fact_orders ADD COLUMNS (
    discount_amount DOUBLE COMMENT 'Order discount',
    loyalty_points INT COMMENT 'Points earned'
);
```
Or use `mergeSchema = true` on the next write — Delta automatically adds new columns.

**Step 2 — Backfill strategy for 10-20 TB:**

**Option A — Partitioned backfill (recommended):**
```python
# Process one partition at a time to avoid OOM and enable restartability
partitions = spark.sql("""
    SELECT DISTINCT date(order_date) as partition_date
    FROM silver.fact_orders
    ORDER BY partition_date
""").collect()

for row in partitions:
    partition_date = row.partition_date
    
    # Read one partition
    df = spark.table("silver.fact_orders").filter(
        F.col("order_date").cast("date") == partition_date
    )
    
    # Compute new columns (e.g., join with source data)
    df_enriched = df.join(discount_source, on="order_id", how="left")
    
    # Overwrite just this partition
    (df_enriched.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"CAST(order_date AS DATE) = '{partition_date}'")
        .saveAsTable("silver.fact_orders")
    )
```

**Option B — Delta MERGE backfill:**
```python
# For smaller backfills, MERGE is simpler
backfill_df = compute_new_columns(source_df)

(DeltaTable.forName(spark, "silver.fact_orders")
    .alias("target")
    .merge(backfill_df.alias("source"), "target.order_id = source.order_id")
    .whenMatchedUpdate(set={
        "discount_amount": "source.discount_amount",
        "loyalty_points": "source.loyalty_points"
    })
    .execute()
)
```

**Option C — Full rewrite (last resort for massive schema changes):**
```python
# Read all data, transform, write to new table, swap
df = spark.table("silver.fact_orders")
df_new = df.withColumn("discount_amount", compute_discount_udf(...))

df_new.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.fact_orders_v2")

# Then swap tables via ALTER TABLE RENAME
```

**Key considerations for 10-20 TB backfill:**
- **Use an autoscaling cluster** — `min_workers: 4, max_workers: 32` to handle the volume.
- **Process in chunks** (by partition or date range) for restartability — if the job fails at partition 50/100, resume from 50.
- **Schedule during off-peak hours** to avoid impacting production queries.
- **Monitor with `DESCRIBE HISTORY`** to track backfill progress via Delta transaction log.
- **Run OPTIMIZE after backfill** to compact files created during the update.
- **Estimated time:** 10-20 TB with a well-sized cluster (32 workers, `i3.2xlarge`) could take 2-8 hours depending on transformation complexity.

---

## 4. Storage & Metadata — Monitoring Size of Logs and Tables

### Q: How do you monitor the size of Delta tables?

**Method 1 — DESCRIBE DETAIL:**
```sql
DESCRIBE DETAIL silver.fact_orders;
```
Returns: `sizeInBytes`, `numFiles`, `numPartitions`, `createdAt`, `lastModified`, `minReaderVersion`, `minWriterVersion`.

**Method 2 — DESCRIBE HISTORY (transaction log):**
```sql
DESCRIBE HISTORY silver.fact_orders LIMIT 20;
```
Returns: every commit (WRITE, MERGE, OPTIMIZE, DELETE) with timestamp, operation metrics (rows added/removed, bytes written), and the user who performed it.

**Method 3 — Information Schema (UC system tables):**
```sql
SELECT table_name, 
       data_size_in_bytes / (1024*1024*1024) AS size_gb,
       row_count
FROM system.information_schema.tables
WHERE table_schema = 'silver'
  AND table_catalog = 'pipeline_catalog_dev';
```

**Method 4 — File-level inspection:**
```python
# List Delta log files
dbutils.fs.ls("dbfs:/user/hive/warehouse/silver.db/fact_orders/_delta_log/")

# Check total size
total = sum(f.size for f in dbutils.fs.ls("dbfs:/path/to/table/"))
print(f"Table size: {total / (1024**3):.2f} GB")
```

### Q: How do you monitor log storage and growth?

**Delta Transaction Log (`_delta_log/`):**
- Every commit creates a new JSON file (`000000.json`, `000001.json`, ...).
- Every 10 commits, a checkpoint Parquet file is created for faster log replay.
- Log retention default: 30 days (`delta.logRetentionDuration`).
- Run `VACUUM` to clean up old data files (not log files).

**Monitoring table growth over time:**
```sql
-- Track table size over time using DESCRIBE HISTORY
SELECT timestamp, 
       operationMetrics.numOutputRows,
       operationMetrics.numOutputBytes
FROM (DESCRIBE HISTORY silver.fact_orders)
WHERE operation IN ('WRITE', 'MERGE')
ORDER BY timestamp;
```

**S3 landing zone monitoring:**
- S3 storage metrics via CloudWatch: `BucketSizeBytes`, `NumberOfObjects`.
- Our lifecycle policy: 90 days → Glacier, 365 days → delete.
- Monitor with: `aws s3 ls s3://{bucket}/ --recursive --summarize`.

---

## 5. Data Migration — Teradata to Snowflake

### Q: What connectors and approach would you use for Teradata → Snowflake migration?

**Architecture Overview:**

```
Teradata ──(JDBC)──▶ Databricks (Spark) ──(Snowflake Connector)──▶ Snowflake
    │                     │
    │                     ├── Data validation
    │                     ├── Schema mapping
    │                     ├── Transformation
    │                     └── Reconciliation
    │
    └──(Export to S3/Azure)──▶ Snowflake COPY INTO (for bulk loads)
```

**Approach 1 — Databricks as ETL middleware (recommended for complex transformations):**

1. **Extract from Teradata:**
   ```python
   teradata_df = (spark.read
       .format("jdbc")
       .option("url", "jdbc:teradata://host/DATABASE=mydb")
       .option("driver", "com.teradata.jdbc.TeraDriver")
       .option("dbtable", "source_table")
       .option("user", dbutils.secrets.get("td-scope", "user"))
       .option("password", dbutils.secrets.get("td-scope", "pass"))
       .option("fetchsize", "10000")
       .option("numPartitions", "16")
       .option("partitionColumn", "id")
       .option("lowerBound", "1")
       .option("upperBound", "10000000")
       .load()
   )
   ```

2. **Transform in Databricks:**
   - Schema mapping (Teradata types → Snowflake types).
   - Data cleansing, deduplication, business logic.
   - Write to Delta as a staging layer for validation.

3. **Load into Snowflake:**
   ```python
   (transformed_df.write
       .format("snowflake")
       .option("sfUrl", "account.snowflakecomputing.com")
       .option("sfDatabase", "target_db")
       .option("sfSchema", "public")
       .option("dbtable", "target_table")
       .option("sfWarehouse", "LOAD_WH")
       .mode("overwrite")
       .save()
   )
   ```

**Approach 2 — Direct bulk export (for simple table copies):**

1. Export Teradata tables to S3 as Parquet/CSV using Teradata Parallel Transporter (TPT).
2. Use Snowflake `COPY INTO` to load from S3.
3. Fastest for large tables with minimal transformation.

**Approach 3 — Tools-based migration:**
- **AWS DMS (Database Migration Service):** Supports Teradata as source and S3/Snowflake as target. Handles CDC for ongoing replication.
- **Fivetran / Airbyte:** Managed connectors for Teradata → Snowflake with scheduling and monitoring.

**Key considerations:**
- **Data type mapping:** Teradata `DECIMAL(38,0)` → Snowflake `NUMBER(38,0)`. Teradata `BYTEINT` → Snowflake `SMALLINT`. Test edge cases.
- **Partitioned reads:** Use `numPartitions` + `partitionColumn` on JDBC reads to parallelize extraction.
- **Reconciliation:** Compare row counts, checksums, and sample records between source and target.
- **Incremental migration:** For ongoing sync, use a CDC column (`updated_at`) or Teradata's change tracking.

---

## 6. Deployment — DAB via S3

### Q: How does Databricks Asset Bundle (DAB) deployment work?

**DAB** is Databricks' infrastructure-as-code tool for deploying jobs, notebooks, schemas, and configurations to a workspace.

**Our project's DAB structure:**

```
databricks/
├── databricks.yml          # Bundle config: variables, targets, includes
├── resources/
│   ├── bronze_jobs.yml     # Bronze Autoloader job definitions
│   ├── silver_jobs.yml     # Silver processing job definitions
│   └── gold_jobs.yml       # Gold views + full pipeline job definitions
└── src/                    # Notebook source code
    ├── bronze/
    ├── silver/
    ├── gold/
    ├── schemas/
    └── utils/
```

**Deployment flow:**

```
Developer → git push → GitHub Actions → databricks bundle deploy -t {env}
                                              │
                                              ▼
                                        Databricks Workspace
                                        ├── Jobs created/updated
                                        ├── Notebooks uploaded
                                        └── Schedules activated
```

**Environment-specific deployment:**

From `databricks.yml`:
```yaml
targets:
  dev:
    mode: development
    variables:
      environment: "dev"
      landing_bucket: "sns-firehose-pipeline-dev-landing"
    workspace:
      root_path: /Users/${workspace.current_user.userName}/.bundle/${bundle.name}/dev

  prod:
    mode: production
    variables:
      environment: "prod"
      landing_bucket: "sns-firehose-pipeline-prod-landing"
    workspace:
      root_path: /Shared/.bundle/${bundle.name}/prod
    permissions:
      - group_name: "engineers"
        level: CAN_MANAGE
      - group_name: "analysts"
        level: CAN_VIEW
```

**DAB deployment via S3 — how it works under the hood:**

1. `databricks bundle deploy` packages notebooks and configuration into an artifact.
2. The artifact is uploaded to the Databricks workspace's artifact storage (which can be backed by S3 in AWS deployments).
3. Job definitions are created/updated via the Databricks REST API.
4. Notebook source files are synced to the workspace file system at the configured `root_path`.

**Our CI/CD pipeline (from `.github/workflows/deploy.yml`):**
```yaml
- name: Deploy bundle
  working-directory: databricks
  run: databricks bundle deploy -t ${{ env.ENV }}
  env:
    DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
    DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
```

**Key commands:**
```bash
databricks bundle validate -t dev    # Check config validity
databricks bundle deploy -t dev      # Deploy to workspace
databricks bundle run full_pipeline -t dev  # Trigger a job
databricks bundle destroy -t dev     # Remove all deployed resources
```

---

## 7. Workspace Features & Governance

### Q: What are the key workspace features in Databricks?

- **Notebooks:** Interactive development in Python, SQL, Scala, R. Supports `%md` markdown cells, `%sql` inline SQL, and parameterization via `dbutils.widgets`.
- **Repos / Git integration:** Connect to GitHub/GitLab for version control. Our project uses GitHub Actions for CI/CD.
- **Jobs:** Scheduled and triggered pipeline orchestration with task dependencies.
- **SQL Editor:** Write and run SQL queries against the SQL Warehouse.
- **SQL Dashboards:** Build visualizations on top of SQL queries.
- **Clusters:** Interactive and job clusters with configurable instance types, autoscaling, and Spark configurations.
- **Secrets:** Secure credential storage (e.g., our PII salt key in `pipeline-secrets` scope).
- **Unity Catalog Explorer:** Browse catalogs, schemas, tables, views. View lineage, grants, and column-level metadata.
- **Workflows:** Visual job orchestration with DAG view of task dependencies.

### Q: How do you structure the workspace and Unity Catalog for teams?

**Workspace Structure:**

```
/Shared/
  └── .bundle/
        └── sns-firehose-pipeline/
              ├── dev/      (dev bundle deployment)
              ├── staging/  (staging bundle deployment)
              └── prod/     (prod bundle deployment — engineers: CAN_MANAGE, analysts: CAN_VIEW)

/Users/
  └── {user@email}/
        └── .bundle/
              └── sns-firehose-pipeline/
                    └── dev/  (personal dev deployment)
```

**Unity Catalog Structure for Teams:**

```
pipeline_catalog_dev         ← Dev environment (full access for engineers)
pipeline_catalog_staging     ← Staging (read-only for most, write for CI/CD)
pipeline_catalog_prod        ← Production (strict RBAC)
  ├── bronze                 ← Pipeline service account: READ + WRITE
  │                            Engineers: READ (for debugging)
  │                            Analysts: NO ACCESS
  ├── silver                 ← Pipeline service account: READ + WRITE
  │                            Engineers: READ
  │                            Data scientists: READ
  ├── gold                   ← Analysts: SELECT on views
  │                            Engineers: SELECT + MODIFY
  │                            Data scientists: SELECT on subset
  └── (external location)    ← S3 landing bucket, accessible only via storage credential
```

**Governance principles:**
- **Least privilege:** Each role gets only the access needed. Analysts never see Bronze or Silver directly.
- **Environment isolation:** Separate catalogs per environment prevent dev changes from affecting prod.
- **Service accounts:** Pipeline jobs run under a service principal with specific grants, not a personal user.
- **Audit trail:** Unity Catalog logs every data access event for compliance.

---

## 8. Framework Internals — Control Tables, Data Quality, SLA, Audit

### Q: How do you build a pipeline framework with control tables and quality checks?

**Control Table Pattern:**

A control table tracks pipeline execution metadata — what ran, when, how many records, and whether it succeeded.

```sql
CREATE TABLE IF NOT EXISTS pipeline_catalog_dev.bronze.pipeline_control (
    run_id STRING,
    topic_name STRING,
    layer STRING,                    -- bronze / silver / gold
    job_name STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING,                   -- running / success / failed
    records_read BIGINT,
    records_written BIGINT,
    records_invalid BIGINT,
    error_message STRING,
    _updated_at TIMESTAMP
)
USING DELTA;
```

**Usage in a notebook:**
```python
import uuid
from datetime import datetime

run_id = str(uuid.uuid4())
start_time = datetime.utcnow()

try:
    # ... pipeline logic ...
    records_written = spark.table(target_table).count()
    
    spark.sql(f"""
        INSERT INTO pipeline_control VALUES (
            '{run_id}', '{topic_name}', 'silver', 'fact_orders',
            '{start_time}', current_timestamp(), 'success',
            {records_read}, {records_written}, 0, NULL, current_timestamp()
        )
    """)
except Exception as e:
    spark.sql(f"""
        INSERT INTO pipeline_control VALUES (
            '{run_id}', '{topic_name}', 'silver', 'fact_orders',
            '{start_time}', current_timestamp(), 'failed',
            0, 0, 0, '{str(e)[:500]}', current_timestamp()
        )
    """)
    raise
```

**Data Quality Checks:**

Beyond our current required-field validation, a production framework would include:

```sql
-- Quality check table
CREATE TABLE pipeline_catalog_dev.bronze.data_quality_checks (
    check_id STRING,
    run_id STRING,
    table_name STRING,
    check_type STRING,          -- completeness / uniqueness / range / freshness / referential
    check_description STRING,
    check_result STRING,        -- pass / fail / warn
    expected_value STRING,
    actual_value STRING,
    threshold DOUBLE,
    checked_at TIMESTAMP
);
```

**Example checks:**

| Check Type | Example | Implementation |
|------------|---------|----------------|
| **Completeness** | NULL% for `customer_id` < 1% | `COUNT(NULLIF(customer_id, '')) / COUNT(*) > 0.99` |
| **Uniqueness** | `order_id` is unique in `fact_orders` | `COUNT(DISTINCT order_id) == COUNT(*)` |
| **Range** | `total_amount > 0` | `COUNT(CASE WHEN total_amount <= 0 THEN 1 END) == 0` |
| **Freshness** | Latest `_ingestion_timestamp` < 30 min old | `MAX(_ingestion_timestamp) > current_timestamp() - INTERVAL 30 MINUTES` |
| **Referential** | Every `customer_id` in `fact_orders` exists in `dim_customers` | `LEFT ANTI JOIN` returns 0 rows |
| **Volume** | Record count within ±20% of previous run | Compare with `pipeline_control` table |

**SLA Tracking:**

```sql
-- SLA table
CREATE TABLE pipeline_catalog_dev.bronze.sla_tracking (
    table_name STRING,
    sla_type STRING,            -- freshness / completeness / availability
    sla_threshold_minutes INT,  -- e.g., data must be < 30 min old
    actual_latency_minutes DOUBLE,
    sla_met BOOLEAN,
    checked_at TIMESTAMP
);

-- Example: Check if fact_orders data is fresh
INSERT INTO sla_tracking
SELECT
    'silver.fact_orders' AS table_name,
    'freshness' AS sla_type,
    30 AS sla_threshold_minutes,
    TIMESTAMPDIFF(MINUTE, MAX(_ingestion_timestamp), current_timestamp()) AS actual_latency_minutes,
    TIMESTAMPDIFF(MINUTE, MAX(_ingestion_timestamp), current_timestamp()) <= 30 AS sla_met,
    current_timestamp() AS checked_at
FROM silver.fact_orders;
```

**Audit Trend Analysis:**

```sql
-- Daily pipeline health summary
SELECT
    DATE(start_time) AS run_date,
    layer,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successful,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
    AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) AS avg_duration_seconds,
    SUM(records_written) AS total_records_processed
FROM pipeline_control
GROUP BY DATE(start_time), layer
ORDER BY run_date DESC, layer;
```

---

## 9. Performance Optimization — Large Gold Layer Tables

### Q: How do you optimize large Gold layer tables for faster SQL Warehouse queries?

**Current setup:** Our Gold layer uses **virtual views** (`CREATE OR REPLACE VIEW`) — no data is materialized. Every query re-executes the SQL against Silver tables.

**When views become slow, optimization options include:**

**1. Materialize as Delta tables instead of views:**
```sql
CREATE OR REPLACE TABLE gold.vw_order_summary_materialized
USING DELTA
AS
SELECT
    o.order_id, o.order_date, o.total_amount, ...
FROM silver.fact_orders o
LEFT JOIN silver.dim_customers c ON o.customer_id = c.customer_id;
```
Trade-off: Requires a refresh schedule but queries are much faster since data is pre-computed.

**2. OPTIMIZE + ZORDER the underlying Silver tables:**
```sql
OPTIMIZE silver.fact_orders ZORDER BY (customer_id, order_date);
OPTIMIZE silver.dim_customers ZORDER BY (customer_id);
```
Ensures the join key (`customer_id`) is co-located in the same files for both tables.

**3. Enable Delta caching on the SQL Warehouse:**
```sql
SET spark.databricks.io.cache.enabled = true;
SET spark.databricks.io.cache.maxDiskUsage = 50g;
```
Caches frequently accessed data on local SSDs of warehouse nodes.

**4. Partition pruning:**
If Gold queries frequently filter by date:
```sql
-- This query benefits from partition pruning on order_date
SELECT * FROM gold.vw_order_summary
WHERE order_date BETWEEN '2024-01-01' AND '2024-03-31';
```
Ensure `fact_orders` is partitioned by a date column (not raw timestamp — use `DATE(order_date)`).

**5. Scale the SQL Warehouse:**
- Increase from `2X-Small` to `Small` or `Medium` for more compute.
- Increase `max_num_clusters` from 1 to 2-4 for concurrent query handling.
- Use Serverless SQL Warehouse for automatic scaling.

**6. Aggregate tables for common queries:**
```sql
-- Pre-aggregated daily summary for dashboards
CREATE TABLE gold.daily_order_summary
USING DELTA
AS
SELECT
    DATE(order_date) AS order_date,
    COUNT(*) AS order_count,
    SUM(total_amount) AS daily_revenue,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM silver.fact_orders
GROUP BY DATE(order_date);
```
Dashboard queries hit the small aggregate table instead of scanning millions of rows.

**7. Liquid clustering (Databricks-specific, replacing ZORDER):**
```sql
ALTER TABLE silver.fact_orders
CLUSTER BY (customer_id, order_date);
```
Liquid clustering is incremental (unlike OPTIMIZE + ZORDER which rewrites all files) and automatically maintains clustering on writes.

---

## 10. Visualization & Cost Tracking

### Q: How do you create dashboards in Databricks?

**Databricks SQL Dashboards:**

1. **Create SQL queries** in the SQL Editor connected to the Gold SQL Warehouse.
   ```sql
   -- Daily revenue trend
   SELECT DATE(order_date) AS date,
          SUM(total_amount) AS revenue,
          COUNT(DISTINCT order_id) AS orders
   FROM gold.vw_order_summary
   GROUP BY DATE(order_date)
   ORDER BY date;
   ```

2. **Add visualizations** — choose from bar, line, pie, scatter, table, counter, map, heatmap, etc.

3. **Compose a dashboard** — arrange multiple visualizations on a single page with filters.

4. **Schedule refresh** — set the dashboard to auto-refresh every hour/day.

5. **Share** — grant VIEW access to specific users or groups. Publish for read-only access.

**Example dashboard for our pipeline:**

| Widget | Query | Visualization |
|--------|-------|---------------|
| Daily Revenue | `SUM(total_amount) GROUP BY date` | Line chart |
| Order Volume | `COUNT(*) GROUP BY date` | Bar chart |
| Top Customers (CLV) | `SELECT * FROM vw_customer_lifetime_value ORDER BY total_spend DESC LIMIT 10` | Table |
| Product Performance | `SELECT * FROM vw_product_sales ORDER BY total_revenue DESC LIMIT 10` | Horizontal bar |
| Data Quality | `SELECT _record_status, COUNT(*) FROM bronze.orders_raw GROUP BY 1` | Pie chart (valid vs invalid) |
| Pipeline Health | `SELECT * FROM pipeline_control ORDER BY start_time DESC LIMIT 20` | Table with status colors |

### Q: How do you track billing and cost metrics in dashboards?

**Using Databricks System Tables:**

```sql
-- Daily DBU consumption by workspace
SELECT
    usage_date,
    workspace_id,
    sku_name,
    usage_unit,
    SUM(usage_quantity) AS total_dbus
FROM system.billing.usage
WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY usage_date, workspace_id, sku_name, usage_unit
ORDER BY usage_date DESC;
```

```sql
-- Cost by job (approximate — DBU * price per DBU)
SELECT
    usage_metadata.job_id,
    usage_metadata.job_name,
    SUM(usage_quantity) AS total_dbus,
    SUM(usage_quantity * 0.07) AS estimated_cost_usd  -- adjust price per SKU
FROM system.billing.usage
WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
  AND usage_metadata.job_id IS NOT NULL
GROUP BY usage_metadata.job_id, usage_metadata.job_name
ORDER BY total_dbus DESC;
```

```sql
-- Cost breakdown by layer (using job tags)
SELECT
    CASE
        WHEN usage_metadata.job_name LIKE '%Bronze%' THEN 'Bronze'
        WHEN usage_metadata.job_name LIKE '%Silver%' THEN 'Silver'
        WHEN usage_metadata.job_name LIKE '%Gold%' THEN 'Gold'
        ELSE 'Other'
    END AS layer,
    SUM(usage_quantity) AS total_dbus,
    SUM(usage_quantity * 0.07) AS estimated_cost_usd
FROM system.billing.usage
WHERE usage_date >= DATEADD(DAY, -7, CURRENT_DATE())
GROUP BY 1
ORDER BY total_dbus DESC;
```

**Cost tracking dashboard widgets:**

| Widget | Description |
|--------|-------------|
| **Daily DBU trend** | Line chart of DBU consumption over last 30 days |
| **Cost by layer** | Pie chart — Bronze vs Silver vs Gold costs |
| **Top 5 expensive jobs** | Table sorted by total DBUs |
| **SQL Warehouse utilization** | Line chart of warehouse uptime vs idle time |
| **Cluster hours by job** | Bar chart showing compute hours per job |
| **Cost anomaly alert** | Counter — flag if today's cost > 2x 7-day average |
| **Month-to-date spend** | Counter — total estimated cost this month |
| **Storage growth** | Line chart — Delta table sizes over time |

**Main cost drivers in our architecture (for interview context):**

| Component | Cost Driver | Optimization |
|-----------|------------|-------------|
| **Firehose** | ~$0.029/GB ingested | GZIP compression reduces billable volume |
| **S3** | Storage + requests | Glacier lifecycle after 90 days |
| **Databricks (biggest)** | DBUs per cluster-hour | Job clusters (auto-terminate), `auto_stop_mins = 10` on SQL Warehouse, `trigger(availableNow=True)` instead of continuous streaming |
| **Delta storage** | Managed storage volume | `VACUUM` to remove old versions, `OPTIMIZE` to reduce file count |
