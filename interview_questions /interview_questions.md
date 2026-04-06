# Databricks Data Engineering — Interview Q&A

*Based on: SNS → Firehose → S3 → Databricks Medallion Pipeline*

---

## 1. What challenges have you faced in your data engineering projects? How did you handle them?

**Cross-Account Data Delivery & IAM Complexity:**
Our pipeline spans two AWS accounts — SNS topics live in Account A while Firehose, S3, and Databricks operate in Account B. Setting up the three IAM trust relationships (Firehose delivery role, SNS subscription role, Databricks S3 access role) with proper cross-account permissions was challenging. I solved this by codifying everything in Terraform modules with clearly separated providers (`aws.sns_account`, `aws.firehose_account`) so each resource is created under the correct account context.

**Schema Evolution vs. Explicit Schema Conflict:**
We configured Autoloader with both an explicit `.schema(raw_schema)` and `cloudFiles.inferColumnTypes = true` / `schemaEvolutionMode = addNewColumns`. The explicit schema overrides inference, so new upstream fields were silently dropped. I identified this through data reconciliation and resolved it by choosing one approach — using the explicit schema for stability and adding new fields manually when upstream changes are confirmed.

**Small Files Problem:**
Firehose delivers many small GZIP files (especially in dev with 5 MB / 300s buffers). This created performance issues for Autoloader and downstream reads. I addressed this by running `OPTIMIZE` on Delta tables after ingestion and using Autoloader's notification mode instead of directory listing to reduce S3 LIST API costs.

**Non-Incremental Silver Processing:**
The Silver layer originally read the entire Bronze table on every run (`spark.table(source_table)`). As Bronze grew, Silver processing time increased linearly. The solution path involves implementing a high-water mark on `_ingestion_timestamp` or using Delta Change Data Feed (CDF) to process only new records.

**PII Handling with Salt Keys:**
We needed deterministic, irreversible surrogates for PII columns (email, phone, name) to enable cross-table joins without exposing raw data. I implemented salted SHA-256 hashing via a reusable `generate_salt_key` utility. A challenge was salt rotation — if the salt changes, all historical hashes become inconsistent. We mitigated this by storing the salt in Databricks Secrets and documenting a re-hash procedure.

**Duplicate Records from SNS At-Least-Once Delivery:**
SNS delivers messages at least once, so duplicates land in Bronze. We intentionally keep Bronze append-only and handle deduplication at the Silver layer using `dropDuplicates` followed by Delta MERGE on primary key. I later identified that `dropDuplicates` is non-deterministic and proposed replacing it with a window function (`ROW_NUMBER` ordered by `_ingestion_timestamp DESC`) to always keep the latest record.

---

## 2. Explain a real-time issue you solved in Databricks.

**Issue:** The `process_bronze_batch` function called `valid_df.count()` and `invalid_df.count()` before writing. Since Spark DataFrames are lazily evaluated and not cached, this caused the data to be computed twice — once for `count()` and once for `write`. On large batches this doubled processing time and occasionally caused OOM errors on the Bronze cluster (2 workers, `i3.xlarge`).

**Root Cause:** `count()` triggers full DataFrame materialization. The subsequent `.write` re-evaluates the entire lineage again.

**Solution:** I replaced the `if valid_df.count() > 0` guard with a simpler approach — always call `.write` and let Delta handle empty appends as a no-op. For metrics, I collected counts after writing by querying the Delta table's version metadata. Alternatively, caching the filtered DataFrames before counting and writing would also work, but the no-op write approach was simpler and avoided memory pressure.

---

## 3. What is Databricks?

Databricks is a unified analytics platform built on Apache Spark. It provides a collaborative environment for data engineering, data science, and machine learning. It offers managed Spark clusters, Delta Lake for reliable data storage with ACID transactions, Unity Catalog for governance, and features like Autoloader for incremental file ingestion. In our project, Databricks serves as the compute and processing layer that reads raw data from S3 and processes it through a Bronze → Silver → Gold medallion architecture.

---

## 4. What are the main components of Databricks?

- **Workspace:** Collaborative environment with notebooks, repos, and folder organization.
- **Clusters:** Managed Spark clusters (all-purpose for interactive work, job clusters for scheduled pipelines).
- **Delta Lake:** ACID-compliant storage layer on top of Parquet with time travel, schema enforcement, and MERGE support.
- **Databricks Jobs:** Orchestration layer to schedule and chain notebook tasks with dependencies.
- **Unity Catalog:** Centralized governance for data access, permissions, and lineage.
- **Databricks SQL (SQL Warehouses):** Serverless SQL compute for BI and analytics queries.
- **Autoloader:** Incremental file ingestion using `cloudFiles` structured streaming format.
- **Databricks Asset Bundles (DABs):** Infrastructure-as-code for deploying jobs, notebooks, and configurations.
- **Secrets Management:** Secure storage for credentials (e.g., our PII salt key stored in a secret scope).

---

## 5. Explain the architecture of Databricks.

Databricks uses a **control plane / data plane** architecture:

- **Control Plane (Databricks-managed):** Hosts the workspace UI, job scheduler, notebook server, Unity Catalog metastore, cluster manager, and REST APIs. This runs in Databricks' own cloud account.
- **Data Plane (Customer-managed):** The actual Spark clusters run in the customer's cloud account (AWS in our case). Data stays in the customer's S3 buckets and VPC. Clusters read/write data directly without routing through the control plane.

In our project specifically:
- S3 landing bucket (customer's Account B) holds raw data from Firehose.
- Databricks clusters in the customer's AWS account assume an IAM role to read from S3.
- Unity Catalog metastore tracks table metadata, schemas, and permissions.
- External Locations map S3 paths to storage credentials for secure access.

---

## 6. How do you handle large volumes of data in Databricks?

- **Autoloader with notification mode:** Uses S3 event notifications + SQS instead of expensive directory listings, efficiently handling buckets with millions of files.
- **Delta Lake:** Columnar Parquet storage with data skipping, Z-ordering, and file compaction via `OPTIMIZE`.
- **Partitioning:** Tables like `fact_orders` are partitioned by `order_date` to enable partition pruning on time-range queries.
- **`maxFilesPerTrigger`:** Set to 1000 to create backpressure during volume spikes — Autoloader processes files in manageable batches rather than overwhelming the cluster.
- **Job clusters with appropriate sizing:** Silver processing uses 3 workers (`i3.xlarge`) with optimized write and auto-merge enabled via Spark conf.
- **GZIP compression:** Firehose compresses files before landing in S3, reducing storage by ~80% and improving read throughput.

---

## 7. What techniques do you use for optimization?

- **`OPTIMIZE` with `ZORDER`:** We run `OPTIMIZE {table} ZORDER BY (customer_id, order_date)` to compact small files and co-locate related data for faster data skipping.
- **`mergeSchema` enabled:** Allows schema evolution without manual DDL changes.
- **`spark.databricks.delta.optimizeWrite.enabled = true`:** Automatically right-sizes output files during writes.
- **Notification-based Autoloader:** Avoids expensive S3 LIST calls.
- **`trigger(availableNow=True)`:** Processes all available files then stops the cluster, saving costs compared to continuous streaming.
- **Parallel task execution:** Bronze ingestion for orders, customers, and products runs in parallel. Silver fact and dimension tasks also run in parallel.

---

## 8. How do you handle incremental data loading?

In the **Bronze layer**, Autoloader inherently handles incremental loading — it tracks processed files via a checkpoint at `s3://{bucket}/_checkpoints/bronze/{topic}`. Only new files are processed on each run.

In the **Silver layer**, we use **Delta MERGE (upsert)** for incremental updates:

```python
delta_target.alias("target").merge(
    fact_df.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll(
    condition="source._ingestion_timestamp > target._ingestion_timestamp"
).whenNotMatchedInsertAll().execute()
```

New records are inserted; existing records are updated only if the source has a newer ingestion timestamp.

**Current gap:** The Silver notebooks currently read the entire Bronze table rather than only new records. To make this truly incremental, we could implement a high-water mark approach (filter Bronze by `_ingestion_timestamp > last_processed_timestamp`) or use Delta's Change Data Feed.

---

## 9. What is the difference between full load and incremental load?

| Aspect | Full Load | Incremental Load |
|---|---|---|
| **Scope** | Reads and writes the entire dataset every time | Processes only new or changed records |
| **Performance** | Expensive for large datasets | Efficient — processes a small delta |
| **Use case** | Initial loads, small reference tables, recovery scenarios | Regular pipeline runs on growing tables |
| **In our project** | Silver currently reads full Bronze table (a known issue) | Bronze Autoloader is truly incremental via checkpoints; Silver MERGE is idempotent but source read is not yet incremental |

---

## 10. Have you used Delta Lake MERGE for incremental loads?

Yes. All three Silver tables use Delta MERGE:

- **`fact_orders`:** MERGE on `order_id` — insert new orders, update existing ones if `_ingestion_timestamp` is newer.
- **`dim_customers`:** SCD Type 1 MERGE on `customer_id` — overwrites customer attributes with the latest values.
- **`dim_products`:** SCD Type 1 MERGE on `product_id` — same pattern.

Key properties of our MERGE:
- **Atomic:** If a cluster fails mid-MERGE, Delta rolls back — no partial writes.
- **Idempotent:** Re-running produces the same result, which simplifies failure recovery.
- **Conditional update:** `whenMatchedUpdateAll(condition="source._ingestion_timestamp > target._ingestion_timestamp")` prevents overwriting with stale data.

---

## 11. How do you schedule pipelines in Databricks?

We use **Databricks Asset Bundles (DABs)** with YAML job definitions:

- **Bronze:** Three separate jobs (orders, customers, products), each scheduled every 15 minutes via `quartz_cron_expression: "0 0/15 * * * ?"`.
- **Silver:** Single job with three parallel tasks (fact_orders, dim_customers, dim_products), scheduled hourly at `:30`.
- **Gold:** Single job for view creation and RBAC grants, scheduled daily at 1:00 AM UTC.
- **Full pipeline:** An end-to-end orchestration job with explicit `depends_on` chains — Silver tasks depend on their corresponding Bronze tasks, Gold depends on all Silver tasks.

Jobs are deployed via `databricks bundle deploy -t <env>` and can be triggered manually with `databricks bundle run`.

---

## 12. What is Databricks Jobs?

Databricks Jobs is the orchestration service for scheduling and running tasks. Each job consists of one or more tasks that can run notebooks, Python scripts, JARs, or SQL queries on job clusters or existing clusters.

Key features we use:
- **Task dependencies (`depends_on`):** In the full pipeline job, Silver tasks depend on Bronze tasks, and Gold depends on all Silver tasks.
- **Job clusters:** Ephemeral clusters created for the job and terminated after completion — more cost-effective than all-purpose clusters.
- **Parameterization:** Notebooks receive parameters via `base_parameters` (e.g., `topic_name`, `environment`, `catalog_name`).
- **Tags:** Jobs are tagged by `layer` and `environment` for filtering and cost attribution.

---

## 13. How do you monitor pipeline execution?

**Current monitoring:**
- Post-ingestion print statements in notebooks showing valid/invalid record counts per batch.
- CloudWatch logging for Firehose delivery streams (30-day retention).
- `SHOW GRANTS` verification after Gold view creation.

**Gaps identified for production:**
- No automated alerting when invalid record counts spike.
- No pipeline-level monitoring for job success/failure rates or execution times.
- No S3 file lag monitoring (alert if no new files arrive).
- No data freshness tracking (e.g., `MAX(_ingestion_timestamp)` per table vs. expected SLA).
- No CloudWatch alarms or PagerDuty/Slack integration.

**Planned improvements:**
- Track `MAX(_ingestion_timestamp)` per table and alert if it falls behind.
- Monitor Autoloader's `numFilesOutstanding` metric.
- Implement reconciliation: compare SNS message count → Firehose records → Bronze row count.

---

## 14. What is Unity Catalog?

Unity Catalog is Databricks' centralized governance solution for data and AI assets. It provides a three-level namespace (`catalog.schema.table`), fine-grained access control, data lineage, and auditing across all Databricks workspaces.

In our project:
- **Catalog:** `pipeline_catalog_{environment}` (e.g., `pipeline_catalog_dev`).
- **Schemas:** `bronze`, `silver`, `gold` — one per medallion layer.
- **Tables:** Delta tables registered under their respective schemas.
- **Views:** Gold layer views (`vw_order_summary`, etc.) with role-based grants.
- **External Locations:** Map S3 landing bucket to a storage credential for Autoloader access.

---

## 15. What are the components of Unity Catalog?

- **Metastore:** Top-level container, one per region, holds all catalogs.
- **Catalog:** Logical grouping (we use `pipeline_catalog_{env}`).
- **Schema:** Namespace within a catalog (bronze, silver, gold).
- **Tables / Views:** Delta tables and SQL views.
- **Storage Credentials:** IAM roles or service principals for accessing external storage (our `s3_landing_credential`).
- **External Locations:** Map cloud storage URLs to storage credentials (our S3 landing bucket).
- **Grants:** `GRANT SELECT ON VIEW ... TO group` for RBAC.
- **Data Lineage:** Tracks table-to-table dependencies automatically.

---

## 16. How do you provide security in Databricks?

- **Unity Catalog RBAC:** Role-based grants on Gold views — analysts get SELECT only, engineers get SELECT + MODIFY, data scientists get SELECT on a subset (excluding PII-containing views).
- **Prerequisite grants:** `USE CATALOG` and `USE SCHEMA` granted before view-level privileges.
- **PII protection via salt keys:** Sensitive columns (email, phone, name) are hashed with salted SHA-256. The salt is stored in Databricks Secrets, not in source code.
- **IAM roles with least privilege:** Firehose delivery role only has S3 and KMS permissions; Databricks role only has read access to the landing bucket.
- **S3 encryption:** Server-side encryption with KMS (`aws:kms`) and bucket key enabled.
- **S3 public access block:** All four public access settings blocked.
- **Cross-account trust with ExternalId:** Databricks assumes an IAM role with an ExternalId condition to prevent confused deputy attacks.

---

## 17. What are access control mechanisms?

- **Unity Catalog Grants:**
  - `GRANT USE CATALOG ON CATALOG {catalog} TO {group}`
  - `GRANT USE SCHEMA ON SCHEMA {catalog}.{gold_schema} TO {group}`
  - `GRANT SELECT ON VIEW {view} TO {group}`
- **Role-based view restriction:** Data scientists cannot access `vw_order_summary` (contains plain-text `customer_name`).
- **Column-level masking (future):** Unity Catalog supports column masks — e.g., masking `dim_customers.email` to return NULL or the salt key for non-privileged users.
- **Row filters (future):** Could restrict rows visible to specific groups.
- **Databricks Asset Bundle permissions:** In production, engineers get `CAN_MANAGE` and analysts get `CAN_VIEW` on the bundle workspace.

---

## 18. Where can you see lineage in Databricks?

Unity Catalog provides **automatic data lineage** tracking. You can view lineage in:

- **Unity Catalog Explorer:** Navigate to any table or view → Lineage tab. It shows upstream tables (what data feeds into this object) and downstream tables (what depends on this object).
- For example, `vw_order_summary` would show lineage from `silver.fact_orders` and `silver.dim_customers`, which in turn trace back to `bronze.orders_raw` and `bronze.customers_raw`.
- **Column-level lineage:** Tracks which source columns map to which target columns through transformations.

Lineage is captured automatically when Spark SQL or DataFrame operations run within Unity Catalog-enabled workspaces.

---

## 19. Where do you check logs and errors?

- **Databricks Job Runs UI:** Each job run shows task-level status, duration, start/end time, and links to notebook output with full stdout/stderr.
- **Notebook output:** Our notebooks print record counts, table names, and batch metrics. Errors are visible in the cell output.
- **Driver logs / Spark UI:** Accessible from the cluster page — shows Spark stages, executor logs, and GC metrics.
- **CloudWatch:** Firehose delivery logs at `/aws/kinesisfirehose/{stream-name}` with 30-day retention.
- **Delta invalid table:** Records failing validation land in `{topic}_invalid` tables with `_validation_errors` listing exactly which fields failed.
- **Firehose error prefix:** Failed Firehose deliveries go to `{topic}_errors/` in S3 with the error type in the path.

---

## 20. How do you debug failed jobs?

1. **Check the Databricks Jobs UI** for the failed task — see error message and stack trace in notebook output.
2. **Inspect driver logs** for OOM errors, network timeouts, or permission issues.
3. **Check the Delta table state:** Since Delta MERGE is atomic, a failed MERGE leaves the table in its pre-MERGE state. No data corruption to worry about.
4. **For Autoloader failures:** Check the checkpoint directory. If corrupted, Autoloader reprocesses all files (duplicates handled by Silver MERGE).
5. **For permission errors:** Verify IAM role trust relationships and Unity Catalog grants.
6. **For data quality issues:** Query the `_invalid` table to see which records failed and why (the `_validation_errors` column lists the specific failed fields).
7. **Re-run the task:** All Silver tasks are idempotent — simply re-running produces the correct result.

---

## 21. How do you create dashboards in Databricks?

- **Databricks SQL Dashboards:** Connect to the Gold layer SQL Warehouse (`2X-Small`, auto-stop after 10 minutes) and build dashboards querying Gold views like `vw_order_summary`, `vw_customer_lifetime_value`, and `vw_product_sales`.
- **Notebook visualizations:** Databricks notebooks support built-in charts — after running a query cell, click the chart icon to create bar charts, line charts, scatter plots, etc.
- **Third-party BI tools:** The SQL Warehouse provides a JDBC/ODBC endpoint that tools like Tableau, Power BI, or Looker can connect to for dashboard creation.

---

## 22. What visualization tools are available in notebooks?

- **Built-in display():** `display(df)` renders an interactive table with chart options (bar, line, pie, scatter, map, pivot).
- **Matplotlib / Seaborn:** Standard Python plotting libraries work in notebooks.
- **Plotly:** For interactive visualizations.
- **Bamboolib:** No-code data exploration (available as a Databricks add-on).
- **SQL cell results:** SQL cells automatically render results with chart options.

---

## 23. How do you share dashboards with users?

- **Databricks SQL Dashboards:** Share via workspace permissions — grant VIEW or EDIT access to specific users or groups. Dashboards can also be published for read-only access.
- **Unity Catalog RBAC:** Dashboard queries respect Unity Catalog grants — analysts see only the views they have SELECT access to. Data scientists querying `vw_customer_lifetime_value` see results, but cannot access `vw_order_summary`.
- **Scheduled email reports:** Databricks SQL supports scheduling dashboard refreshes and emailing PDF/PNG snapshots to stakeholders.
- **Embedding:** Dashboard results can be embedded in external applications via the Databricks SQL API.
