# Interview Questions — SNS -> Firehose -> S3 -> Databricks Medallion Pipeline

---

## 1. Architecture & End-to-End Data Flow

**Q1.1: Walk me through the end-to-end data flow of this pipeline. What happens from the moment a message is published to SNS until it appears in a Gold layer view?**

Expected answer:
1. Upstream services publish JSON messages to SNS topics (orders, customers, products) in **Account A**.
2. SNS subscriptions (protocol: `firehose`) deliver messages cross-account to Kinesis Firehose delivery streams in **Account B** using `raw_message_delivery = true`.
3. Firehose buffers records (5 MB / 300s for dev, 10 MB / 60s for prod) and writes GZIP-compressed JSON files to S3 with a Hive-style partition prefix: `{topic}/year=YYYY/month=MM/day=DD/hour=HH/`.
4. Databricks **Autoloader** (cloudFiles) with notification mode detects new files in S3, reads GZIP JSON, validates required fields, and splits into `{topic}_raw` (valid) and `{topic}_invalid` (invalid) Delta tables in the **Bronze** schema.
5. **Silver** jobs read from Bronze `_raw` tables, select business columns, deduplicate on primary key, and upsert (Delta MERGE) into fact tables (`fact_orders`) and SCD Type 1 dimension tables (`dim_customers`, `dim_products`). PII columns get salted SHA-256 hash keys.
6. **Gold** layer creates SQL views (`vw_order_summary`, `vw_customer_lifetime_value`, `vw_product_sales`) joining Silver tables, and applies Unity Catalog RBAC grants per role.

**Q1.2: Why did you choose SNS -> Firehose instead of SNS -> SQS -> Lambda -> S3? What are the trade-offs?**

- Firehose is a managed service that handles batching, compression, buffering, error handling, and S3 delivery without custom code.
- SQS + Lambda would give more control over transformation before landing but adds operational complexity (concurrency, DLQs, Lambda timeouts, custom batching logic).
- Firehose natively supports GZIP compression and Hive-style partitioning, which aligns perfectly with Autoloader's expectations.
- Trade-off: Firehose has limited transformation capability (no Lambda transform configured here), so all transformation happens in Databricks.

**Q1.3: Why is `raw_message_delivery = true` set on the SNS subscription? What happens if you set it to `false`?**

- With `raw_message_delivery = true`, Firehose receives the raw JSON message body directly.
- With `false`, SNS wraps the message in an SNS envelope containing `Type`, `MessageId`, `TopicArn`, `Message` (as escaped JSON string), `Timestamp`, etc. This means Firehose would write the envelope to S3, and Autoloader would need to parse nested JSON and extract the `Message` field — adding complexity and potential schema issues.

**Q1.4: This is a cross-account architecture. What are the IAM trust relationships involved?**

Three IAM roles:
1. **Firehose Delivery Role** — assumed by `firehose.amazonaws.com`, grants `s3:PutObject`, `s3:GetObject`, `s3:ListBucket`, `kms:Decrypt`, `kms:GenerateDataKey` on the landing bucket.
2. **SNS Subscription Role** — assumed by `sns.amazonaws.com`, grants `firehose:PutRecord`, `firehose:PutRecordBatch` on Firehose streams. Enables cross-account SNS-to-Firehose delivery.
3. **Databricks S3 Access Role** — assumed by Databricks AWS account (`414351767826`), grants `s3:GetObject`, `s3:ListBucket`, plus SQS permissions for Autoloader notification mode.

SNS Topic Policy in Account A allows Account B to `SNS:Subscribe` and `SNS:Receive`.

---

## 2. Idempotency & Exactly-Once Processing

**Q2.1: How does this pipeline handle duplicate messages from SNS? Is the Bronze layer idempotent?**

- SNS has **at-least-once** delivery semantics. The same message can be delivered multiple times.
- At the **Bronze layer**, Autoloader uses `foreachBatch` with `append` mode. There is **no dedup at Bronze** — duplicate messages from SNS will result in duplicate rows in the `_raw` table. This is by design: Bronze is append-only.
- Deduplication happens at the **Silver layer** via `dropDuplicates(["order_id"])` before the Delta MERGE operation. The MERGE key (e.g., `target.order_id = source.order_id`) ensures idempotent upserts — inserting a duplicate record matches the existing row and updates it only if `_ingestion_timestamp` is newer.

**Q2.2: What happens if Autoloader processes the same S3 file twice? Is it protected against that?**

- Autoloader maintains a **checkpoint** (at `s3://{bucket}/_checkpoints/bronze/{topic}`) that tracks which files have been processed.
- If the streaming job restarts, it reads from the checkpoint and skips already-processed files — providing exactly-once file processing.
- However, if the checkpoint is lost/corrupted, all files would be re-processed, leading to duplicates in Bronze. Silver's MERGE would still handle dedup.

**Q2.3: Your Silver MERGE condition uses `source._ingestion_timestamp > target._ingestion_timestamp` for `whenMatchedUpdateAll`. What issue could arise with this approach?**

- If two records with the same primary key arrive in the same micro-batch and have the same `_ingestion_timestamp` (since `current_timestamp()` is set per-batch, not per-record), the condition would not trigger the update, potentially losing the second update.
- A better approach might be to use the source system's business timestamp (e.g., `updated_at`) instead of the ingestion timestamp.
- Also, `dropDuplicates(["order_id"])` before the MERGE is non-deterministic — it doesn't guarantee which duplicate is kept. Should use a window function with ordering by `_ingestion_timestamp` DESC to keep the latest.

**Q2.4: If the Silver job fails mid-execution (e.g., after writing `fact_orders` but before `dim_customers`), what is the state of the system? How do you recover?**

- Since Silver tasks in the `silver_pipeline` job don't have `depends_on` between them (they run in parallel), each task is independent. A failure in `dim_customers` doesn't affect `fact_orders`.
- Delta MERGE is **atomic** — if it fails mid-way, it rolls back thanks to Delta's ACID transactions. No partial writes.
- Recovery: Simply re-run the failed task. The MERGE is idempotent, so re-running produces the same result.

**Q2.5: What about Firehose delivery failures? How are they handled?**

- Firehose has an `error_output_prefix` configured: `{topic}_errors/year=.../!{firehose:error-output-type}/`. Failed records go to this error path.
- CloudWatch logging is enabled for each stream to diagnose delivery failures.
- However, there's no alerting or dead-letter mechanism configured in the current Terraform — this is a gap for production.

---

## 3. Firehose Buffering & Data Freshness

**Q3.1: The Firehose buffer is 5 MB / 300 seconds in dev and 10 MB / 60 seconds in prod. What's the trade-off here?**

- **Buffer interval** controls maximum latency — in dev, data can be up to 5 minutes old; in prod, up to 1 minute.
- **Buffer size** controls file size — larger buffers = fewer, bigger S3 files = better Autoloader performance (fewer file listings, better compression).
- Trade-off: Smaller buffers = lower latency but more small files (the "small files problem"). Larger buffers = higher latency but more efficient downstream processing.

**Q3.2: Given the Autoloader runs every 15 minutes (Bronze cron), but Firehose delivers every 1-5 minutes, what's the actual end-to-end latency?**

- Worst case: Message published just after a Firehose flush + just after an Autoloader trigger = ~buffer_interval + 15 minutes.
- In dev: up to ~20 minutes. In prod: up to ~16 minutes.
- This is a **micro-batch** architecture, not real-time. If real-time is needed, you'd switch to continuous streaming (remove `trigger(availableNow=True)` and use a continuous trigger) or use a different architecture like Firehose -> Spark Structured Streaming with continuous processing.

**Q3.3: The Autoloader uses `trigger(availableNow=True)`. What does this mean and why was it chosen over continuous streaming?**

- `availableNow=True` processes all currently available files, then stops the stream. This turns Autoloader into a triggered batch job rather than a long-running stream.
- Advantages: Cluster shuts down between runs = cost savings; simpler failure semantics; no long-running cluster management.
- Disadvantage: Higher latency (depends on cron schedule). Not suitable if sub-minute latency is required.

---

## 4. Schema Evolution & Data Quality

**Q4.1: How does this pipeline handle schema evolution? What happens when a new field is added to the JSON payload?**

- Autoloader is configured with `cloudFiles.schemaEvolutionMode = "addNewColumns"` and `cloudFiles.inferColumnTypes = true`.
- Schema location is stored at `{checkpoint_path}/_schema`.
- When a new field appears, Autoloader adds it as a new column. The `.option("mergeSchema", "true")` on the Delta write ensures the table schema is updated.
- However, the explicit `raw_schema` is also set via `.schema(raw_schema)` on the readStream, which **overrides** schema inference. This means new columns not in the PySpark schema definition would be **dropped**. This is a conflict in the current code — you can't use both `.schema()` and `inferColumnTypes` effectively. Need to choose one approach.

**Q4.2: What if a required field's data type changes (e.g., `total_amount` changes from double to string in upstream)?**

- If the schema is explicitly set (which it is), Spark would attempt to cast. A string that's not a valid double would become `null`, which would then fail the required field validation, and the record would go to the `_invalid` table.
- This is actually good behavior — data quality issues are caught at Bronze.
- However, if the upstream permanently changes the type, you'd need to update the PySpark schema definition and potentially the Silver DDL.

**Q4.3: How are invalid records handled? What's the monitoring story around data quality?**

- Invalid records (missing required fields or empty strings) go to `{topic}_invalid` Delta tables.
- Post-ingestion, the notebook prints counts of valid vs invalid records.
- **Gaps**: No automated alerting when invalid record counts spike. No data quality metrics tracked over time. In production, you'd want: (1) a threshold-based alert if invalid% exceeds a threshold, (2) Great Expectations or similar for richer validation, (3) a monitoring dashboard.

**Q4.4: The validation checks for `null` and empty strings. What about other data quality issues like negative `total_amount`, future dates, or email format violations?**

- Current validation only checks presence (not null, not empty). It doesn't validate business rules.
- In production, you'd want richer validation rules: range checks, format regex, referential integrity, etc.
- The YAML config has `field_types` defined but they're not used by the `validate_required_fields` function — only `required_fields` is used.

---

## 5. Delta Lake & Merge Operations

**Q5.1: Explain the Delta MERGE pattern used in the Silver layer. Why MERGE instead of simple overwrite?**

- MERGE (upsert) allows incremental processing: new records are inserted, existing records are updated.
- Simple overwrite would re-process the entire dataset every time — expensive and slow.
- Delta MERGE is ACID — either the entire operation succeeds or it rolls back.

**Q5.2: In `fact_orders.py`, the code does `dropDuplicates(["order_id"])` before MERGE. Is this safe? What could go wrong?**

- `dropDuplicates` is non-deterministic when there are multiple rows with the same key. It doesn't guarantee which row is kept.
- If order ORD-001 has two versions (status=pending, status=shipped), `dropDuplicates` might keep the older one.
- Better approach: Use a window function `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingestion_timestamp DESC)` and filter to keep only the latest version before MERGE.

**Q5.3: The Silver code reads the entire Bronze table each time (`spark.table(source_table)`). How would you make this incremental?**

- Currently, the Silver jobs read ALL records from Bronze `_raw` tables, not just new ones. This means every run re-processes the entire history.
- For incremental processing, you could:
  1. Track a high-water mark (e.g., max `_ingestion_timestamp` from the last run) and filter Bronze to only newer records.
  2. Use Delta's Change Data Feed (CDF) to read only changes since the last version.
  3. Use Structured Streaming with a MERGE sink (Delta `foreachBatch` with MERGE).
- This is a significant scalability concern — as Bronze tables grow, Silver processing time increases linearly.

**Q5.4: What is ZORDER and why is it used in `OPTIMIZE` statements?**

- ZORDER is Delta Lake's multi-dimensional data clustering. It co-locates related data in the same files.
- `OPTIMIZE {table} ZORDER BY (customer_id, order_date)` clusters data so queries filtering by `customer_id` or `order_date` skip more files (data skipping).
- Trade-off: OPTIMIZE + ZORDER is expensive (rewrites all data files). In production, you'd run it less frequently or on specific partitions.

**Q5.5: Fact_orders is partitioned by `order_date`. Is this a good choice? What problems could arise?**

- `order_date` as a timestamp partition could create too many partitions (one per distinct timestamp value), leading to the small files problem.
- Should partition by a coarser granularity like `date(order_date)` or `year/month`.
- The DDL says `PARTITIONED BY (order_date)` where `order_date` is a TIMESTAMP — this creates one partition per unique timestamp. This is almost certainly wrong for production.

---

## 6. PII Handling & Salt Keys

**Q6.1: Explain the salt key approach for PII columns. Why use salted SHA-256 hashing?**

- Salt keys provide a deterministic, irreversible surrogate for PII (email, phone, name).
- Deterministic: Same input always produces the same hash, enabling joins across tables without exposing raw PII.
- Salted: The salt prevents rainbow table attacks — without the salt, an attacker could pre-compute hashes.
- SHA-256 is one-way — you can't reverse it to get the original PII.

**Q6.2: The salt is stored in Databricks Secrets. What happens if the salt changes? What's the impact on existing data?**

- If the salt changes, all new records will have different hash values for the same PII input.
- Existing records in Silver tables retain the OLD salt key values.
- This **breaks joins** between old and new records — a customer's `email_salt_key` from last month won't match today's.
- Mitigation: Salt rotation requires a full re-hash of all historical data. There's no mechanism for this in the current pipeline.

**Q6.3: The code falls back to a hardcoded default salt (`pipeline_default_salt_2024`) if the secret is unavailable. Is this a security concern?**

- Yes. If the secret scope is misconfigured or the secret is missing, the pipeline silently falls back to a well-known default salt embedded in source code.
- An attacker with access to the code could pre-compute hashes using this default salt.
- In production: The fallback should raise an error rather than silently degrading security. The default salt should be removed from source code.

**Q6.4: The PII columns (email, phone, name) are still stored in plain text in Silver. The salt key is an ADDITIONAL column. Is this correct?**

- Yes, looking at the `dim_customers` table DDL, both `email` (plain text) and `email_salt_key` (hash) columns exist.
- The salt key enables joins without PII, but the raw PII is still in the Silver table — it's only useful if downstream Gold views use the salt key instead of the plain text.
- For true PII protection, you'd either: (1) drop the raw PII columns in Silver and only keep salt keys, (2) use column-level masking in Unity Catalog, or (3) encrypt PII columns with column-level encryption.

---

## 7. Cross-Account Networking & Security

**Q7.1: What IAM permissions does the Firehose delivery role need? Why does it need `kms:Decrypt` and `kms:GenerateDataKey`?**

- Firehose writes to an S3 bucket encrypted with KMS (`sse_algorithm = "aws:kms"`).
- `kms:GenerateDataKey` is needed to encrypt objects being written.
- `kms:Decrypt` is needed if Firehose needs to read existing data (e.g., for error re-delivery).
- The condition `kms:ViaService = "s3.*.amazonaws.com"` scopes KMS usage to only S3 contexts.

**Q7.2: The SNS topic policy allows `SNS:Publish` from `AWS:SourceOwner = account_id`. Why not restrict by specific IAM role instead?**

- `AWS:SourceOwner` restricts publishing to entities within the same AWS account.
- This is a broad permission — any role/user in the account can publish. In production, you'd want to restrict by specific IAM role ARNs of the upstream services.

**Q7.3: How does Databricks access the S3 bucket? Explain the storage credential and external location chain.**

- Databricks IAM Role (in Firehose account) trusts the Databricks AWS account with an ExternalId condition.
- A **Storage Credential** in Unity Catalog references this IAM role.
- An **External Location** maps the S3 URL to the storage credential.
- Autoloader uses this external location to read files from S3.
- Additional SQS permissions on the role enable Autoloader's notification-based file discovery.

**Q7.4: The S3 bucket has versioning enabled and a lifecycle policy (90 days -> Glacier, 365 days -> delete). Why?**

- **Versioning**: Protects against accidental overwrites/deletes of landing zone data. Also required for certain compliance standards.
- **Glacier transition**: After 90 days, data moves to cheaper storage since it's already in Delta tables.
- **Expiration at 365 days**: Removes data entirely after a year, assuming it's fully in Delta and backed up.
- Risk: If Autoloader checkpoints are reset after 90 days, it might try to re-read files that are now in Glacier (which requires a restore request and would fail).

---

## 8. Autoloader Deep Dive

**Q8.1: What is the difference between Autoloader's directory listing mode and notification mode? Which is used here?**

- **Directory listing**: Autoloader periodically lists the S3 prefix to discover new files. Simple but expensive for large directories (many LIST API calls).
- **Notification mode** (`cloudFiles.useNotifications = true`): Autoloader sets up S3 event notifications -> SQS queue -> reads new file events from SQS. More efficient for high-volume buckets.
- This pipeline uses notification mode. The IAM role includes SQS permissions for this reason.

**Q8.2: What happens if Autoloader's SQS queue falls behind or messages expire?**

- SQS messages have a default retention of 4 days. If Autoloader doesn't process them in time, messages are lost and those files are missed.
- Autoloader can fall back to directory listing when it detects missed files (backfill mode).
- In production, you'd monitor the SQS queue depth and set up alarms.

**Q8.3: The Autoloader reads JSON with an explicit schema (`raw_schema`) but also has `inferColumnTypes = true`. Which takes precedence?**

- When `.schema()` is explicitly set on the readStream, it takes precedence. Schema inference and evolution are effectively disabled for columns already in the schema.
- The `inferColumnTypes` and `schemaEvolutionMode` settings become relevant only for columns NOT in the explicit schema — but since a schema is provided, these settings are mostly inoperative.
- This is a configuration inconsistency that should be resolved.

---

## 9. Databricks Jobs & Orchestration

**Q9.1: Describe the job orchestration. How are dependencies between Bronze, Silver, and Gold managed?**

- **Individual layer jobs**: Bronze runs every 15 minutes (3 parallel jobs, one per topic). Silver runs every hour at :30. Gold runs daily at 1 AM.
- **Full pipeline job**: Has explicit `depends_on` chains — Silver tasks depend on their corresponding Bronze tasks, Gold depends on all Silver tasks.
- This means the scheduled approach (individual jobs) has a temporal dependency: Silver at :30 relies on Bronze at :00/:15 having completed. If Bronze is delayed, Silver processes stale data (not failures, but stale).

**Q9.2: What happens if the Bronze job takes longer than 15 minutes? Could concurrent runs overlap?**

- The Databricks job definition doesn't set `max_concurrent_runs`. Default is 1 in most configurations, meaning new runs queue until the current one completes.
- If it consistently takes >15 min, runs pile up. You'd need to either: increase cluster size, increase the trigger interval, or configure `max_concurrent_runs` and handle concurrent Autoloader instances (which share the same checkpoint and could conflict).

**Q9.3: Silver tasks (`fact_orders`, `dim_customers`, `dim_products`) run in parallel in the silver_pipeline job. Could this cause issues?**

- They share the same `silver_cluster` job cluster, so they compete for resources.
- They read from different Bronze tables and write to different Silver tables, so there are no data conflicts.
- If the cluster is undersized, all three tasks running simultaneously could cause OOM or slowness.

**Q9.4: Why does the Gold views job run daily while Bronze/Silver run more frequently?**

- Gold views are `CREATE OR REPLACE VIEW` — they are virtual (no data copy). They just redefine the SQL.
- The views always reflect the latest Silver data at query time.
- The daily run is for refreshing RBAC grants and ensuring views exist — not for data freshness.
- The Gold job could actually run less frequently (only when view SQL changes or roles change).

---

## 10. Unity Catalog & RBAC

**Q10.1: Explain the RBAC model. How are permissions structured?**

Three-tier access:
- **Analysts**: SELECT on all 3 gold views
- **Engineers**: SELECT + MODIFY on all 3 gold views
- **Data Scientists**: SELECT on 2 views (no `vw_order_summary` — which has customer PII like `customer_name`)

Prerequisite grants: `USE CATALOG` on the catalog, `USE SCHEMA` on the gold schema.

**Q10.2: What does `MODIFY` privilege on a VIEW mean? Can engineers actually write to the underlying tables through the view?**

- In Unity Catalog, `MODIFY` on a view doesn't allow data modification through the view.
- `MODIFY` allows operations like `INSERT`, `UPDATE`, `DELETE` on that object — but views are read-only.
- This grant is likely a mistake or future-proofing. In practice, engineers might need `MODIFY` on Silver/Bronze tables, not Gold views.

**Q10.3: Data scientists don't have access to `vw_order_summary`. Why? How does this relate to PII?**

- `vw_order_summary` includes `customer_name` and `email_salt_key`. While the salt key is anonymized, `customer_name` is plain text PII.
- Restricting data scientists from this view is a data governance decision — they work with aggregated/anonymized data only.
- However, `vw_customer_lifetime_value` also exposes `customer_name` — so the restriction is inconsistent.

**Q10.4: How would you implement column-level security instead of view-level?**

- Unity Catalog supports **row filters** and **column masks**.
- You could create a column mask on `dim_customers.email` that returns `NULL` or the salt key for non-privileged users.
- This is more flexible than view-based access control as it works on the base tables.

---

## 11. Production Failure Scenarios

**Q11.1: The SNS topic in Account A goes down. What happens to the pipeline?**

- Firehose receives no new messages. S3 gets no new files. Autoloader finds nothing to process.
- Bronze tables remain stale. Silver and Gold continue to serve existing data.
- No data loss (assuming messages are durably queued upstream before SNS).
- Need monitoring: alert if no new files appear in S3 for >X minutes.

**Q11.2: Firehose delivery fails (e.g., S3 permissions revoked). What happens?**

- Firehose retries internally for up to 24 hours.
- Failed records go to the error prefix (`{topic}_errors/...`).
- CloudWatch logs capture the error.
- If permissions are restored within 24 hours, buffered records are delivered. After 24 hours, data is lost.

**Q11.3: The S3 landing bucket is accidentally deleted. How do you recover?**

- S3 versioning is enabled, but if the bucket itself is deleted, all versions are gone.
- Recovery: Recreate the bucket via Terraform. Data loss for unprocessed files.
- Delta tables in Databricks still have all previously processed data (they're separate from the S3 landing zone).
- Prevention: Enable S3 bucket deletion protection (not currently in Terraform), and MFA delete.

**Q11.4: A Databricks cluster fails mid-MERGE in the Silver layer. Is there data corruption?**

- No. Delta MERGE is ACID. If the cluster dies mid-operation, the MERGE is not committed.
- The Delta log (transaction log) ensures consistency — the table is in the state before the MERGE started.
- Re-running the Silver job will re-attempt the MERGE from scratch.

**Q11.5: Autoloader checkpoint gets corrupted. What happens?**

- Autoloader doesn't know which files have been processed. On the next run, it reprocesses all files in the S3 prefix.
- This causes duplicate records in Bronze.
- Silver's MERGE handles dedup on primary key, so Silver and Gold are not affected (assuming timestamps are handled correctly).
- To prevent: Back up checkpoints regularly, or use a separate metastore-based tracking approach.

**Q11.6: An upstream service starts sending malformed JSON. What's the impact?**

- Firehose delivers whatever it receives to S3 (no validation at this layer).
- Autoloader tries to parse JSON against the schema. Malformed JSON fails parsing and is handled by `cloudFiles.schemaHints` / `_rescued_data` column (if configured) or dropped.
- Records that parse but have missing required fields go to `_invalid` tables.
- Completely unparseable files may cause the entire micro-batch to fail depending on `badRecordsPath` configuration (not currently set).
- Recommendation: Set `cloudFiles.badRecordsPath` to capture corrupt files.

**Q11.7: What if a single massive SNS spike sends 100x normal volume in 5 minutes?**

- Firehose auto-scales to handle the burst (up to account limits).
- S3 handles unlimited writes.
- Autoloader with `maxFilesPerTrigger = 1000` processes at most 1000 files per trigger, creating backpressure. Remaining files are processed in subsequent triggers.
- Cluster might be undersized — `num_workers: 2` (Bronze) may not handle 100x volume. Auto-scaling clusters would help.

---

## 12. Cost & Performance Optimization

**Q12.1: What are the main cost drivers in this architecture?**

1. **Firehose**: Pay per GB ingested (~$0.029/GB).
2. **S3**: Storage + request costs. GZIP compression reduces storage by ~80%.
3. **Databricks clusters**: Biggest cost — DBUs per hour. Three Bronze clusters, one Silver cluster, one Gold cluster.
4. **Delta storage**: Managed Delta tables storage costs.
5. **Autoloader SQS**: Minimal cost.

**Q12.2: How would you reduce Databricks cluster costs?**

- Use **job clusters** (already done) instead of all-purpose clusters — they auto-terminate after the job.
- Use `auto_stop_mins = 10` on SQL warehouses (already configured).
- Use spot instances for non-critical (dev/staging) workloads.
- Reduce `num_workers` or use autoscaling (`min_workers` / `max_workers`).
- Optimize Silver processing to be incremental (currently reads entire Bronze table — wasteful).

**Q12.3: The Silver layer reads the full Bronze table every run. For a table with 100 million records, how long would this take?**

- Reading 100M records from a Delta table and performing a MERGE would take significant time and compute.
- This is the biggest scalability issue. Incremental processing (high-water mark or CDF) would reduce this to only new records (maybe a few thousand per run).
- The MERGE also needs to scan the entire target table to find matches, though Delta's data skipping and ZORDER can help.

**Q12.4: How does `OPTIMIZE` and `ZORDER` affect costs?**

- `OPTIMIZE` rewrites all data files to compact small files into larger ones.
- `ZORDER` adds multi-dimensional sorting, which improves query performance but makes `OPTIMIZE` more expensive (it has to sort).
- Running `OPTIMIZE` after every Bronze batch (as the current code does) is expensive — in production, run it on a schedule (e.g., nightly) or use `OPTIMIZE WHERE` on recent partitions.

---

## 13. Testing & CI/CD

**Q13.1: How is this pipeline tested? What's missing?**

Current testing:
- Unit tests for `validation.py` (required field checks, audit columns)
- Unit tests for `salt_key.py` (deterministic hashing, null handling, multi-column)
- Configuration tests for Gold views (role grants structure)

Missing:
- **Integration tests**: No tests that run the actual Autoloader or MERGE on sample data.
- **End-to-end tests**: No tests that verify data flows from S3 files through Bronze/Silver/Gold.
- **Data quality tests**: No tests that validate row counts, primary key uniqueness, or referential integrity after processing.
- **Terraform tests**: No `terratest` or plan validation beyond `terraform validate`.

**Q13.2: The CI pipeline runs `terraform validate` but not `terraform plan`. Why? What's the risk?**

- `terraform validate` only checks syntax and internal consistency. It doesn't connect to AWS or check real resource state.
- `terraform plan` would show actual changes but requires AWS credentials, which CI doesn't have (or shouldn't have in a PR check).
- The Deploy workflow runs `plan` then `apply` with proper credentials. This is a common pattern.

**Q13.3: How would you test the Autoloader ingestion notebook locally?**

- The test `conftest.py` creates a local SparkSession with Delta extensions.
- You could write an integration test that: (1) creates test JSON files locally, (2) runs the `process_bronze_batch` function, (3) validates the output Delta table.
- The `generate_test_data.py` script generates realistic test data matching the expected schema — this is useful for local testing.

**Q13.4: The deploy pipeline deploys Terraform first, then Databricks bundle. What if the bundle deploy fails?**

- Infrastructure (Terraform) is already applied. The S3 bucket, IAM roles, and Firehose exist.
- Databricks jobs/views don't exist yet. Data will accumulate in S3 but won't be processed.
- Recovery: Fix the bundle issue and re-deploy. No data loss since S3 data is retained.
- The deploy pipeline doesn't have a rollback mechanism — this is a gap.

---

## 14. Monitoring & Observability

**Q14.1: What monitoring exists today? What's missing for production?**

Existing:
- CloudWatch logging for Firehose delivery streams (30-day retention).
- Post-ingestion print statements for record counts.

Missing:
- **Pipeline-level monitoring**: Job success/failure rates, execution times, record counts over time.
- **Data quality dashboards**: Valid vs. invalid record ratios, schema drift detection.
- **Alerting**: No CloudWatch alarms, no PagerDuty/Slack integration.
- **S3 file lag monitoring**: Alert if no new files arrive.
- **Autoloader lag monitoring**: Time between file arrival and processing.
- **Delta table metrics**: Table sizes, number of files, time travel history.

**Q14.2: How would you detect data freshness issues?**

- Track `MAX(_ingestion_timestamp)` per table and alert if it falls behind expected SLA.
- Monitor Autoloader's `numFilesOutstanding` and `numBytesOutstanding` metrics.
- CloudWatch alarm on Firehose's `DeliveryToS3.Success` metric.
- A "heartbeat" record published to SNS periodically to verify end-to-end flow.

**Q14.3: How would you detect a silent data loss scenario?**

- **Reconciliation**: Compare SNS message count (CloudWatch `NumberOfMessagesPublished`) with Firehose records delivered (`IncomingRecords`) with Bronze table row count.
- **Watermark tracking**: If a gap appears in expected `order_date` ranges, investigate.
- **Checksum validation**: Compare hash of source records with hash of landed data.

---

## 15. Scalability & Future Concerns

**Q15.1: What are the scaling limits of this architecture?**

- **SNS**: 12,500 FIFO / unlimited Standard throughput. No issue.
- **Firehose**: 1 MB/s or 1000 records/s per shard. Can scale by requesting higher limits.
- **S3**: Unlimited. No issue.
- **Autoloader**: Scales with cluster size. Notification mode handles high-volume efficiently.
- **Delta MERGE**: Scales with source and target size. Can become slow as tables grow (millions+ of records) without incremental processing.

**Q15.2: How would you add a new data domain (e.g., "inventory")?**

1. Add `"inventory"` to `sns_topic_names` in Terraform variables. Run `terraform apply` — creates SNS topic, Firehose stream, S3 prefix, and IAM permissions.
2. Add a schema definition in `databricks/src/schemas/inventory.py` and register it in `SCHEMA_REGISTRY`.
3. Add `inventory` to `pipeline_config.yaml` with primary key, required fields, etc.
4. Add a new Bronze job in `bronze_jobs.yml` (or the existing parameterized notebook handles it).
5. Create Silver fact/dimension notebooks and Gold views as needed.
6. Update RBAC grants.

**Q15.3: How would you convert this to a real-time (sub-second latency) pipeline?**

- Replace `trigger(availableNow=True)` with `trigger(processingTime="10 seconds")` or continuous processing.
- Run Autoloader as a long-running stream on a persistent cluster.
- Reduce Firehose buffer interval to minimum (60 seconds — Firehose minimum).
- For true sub-second: Replace Firehose with direct SNS -> Lambda -> Delta Lake writes, or use Kafka/Kinesis Data Streams with Spark Structured Streaming.

**Q15.4: What about backfilling historical data?**

- Current design: Drop files into S3 with the correct prefix structure. Autoloader processes them.
- Issue: If backfilling millions of historical files, `maxFilesPerTrigger = 1000` throttles processing. May need to increase or run a separate batch job.
- Alternative: Use a separate bulk load job that reads from a historical S3 prefix and writes directly to Bronze Delta tables.

---

## 16. Terraform & Infrastructure Specifics

**Q16.1: Why use separate providers for `sns_account` and `firehose_account`?**

- Cross-account architecture: SNS topics live in Account A, Firehose/S3 in Account B.
- Each provider assumes a different IAM role in a different account.
- Terraform manages resources across both accounts in a single state file.

**Q16.2: The Terraform state is stored in S3 (backend config). What are the risks?**

- State file contains sensitive information (resource IDs, some secrets). Must be encrypted and access-controlled.
- State locking (via DynamoDB, not shown in config) prevents concurrent modifications.
- If state is lost, Terraform loses track of all managed resources. Regular state backups are essential.

**Q16.3: Why `for_each = toset(var.topic_names)` instead of `count`?**

- `for_each` creates resources indexed by name (e.g., `aws_sns_topic.topics["orders"]`), which is stable across additions/removals.
- `count` creates resources indexed by position (e.g., `aws_sns_topic.topics[0]`). Adding/removing a topic shifts indices, potentially destroying and recreating resources.
- `for_each` is the recommended pattern for this use case.

**Q16.4: There are double colons in the IAM ARNs (e.g., `arn:aws:iam::${var.sns_source_account_id}:role/...`). Is this correct?**

- The format is `arn:aws:iam::{account_id}:role/{role_name}`. The empty field between the two colons is the region — IAM is a global service, so region is always empty.
- This is correct AWS ARN syntax for IAM resources.

---

## 17. SCD (Slowly Changing Dimensions)

**Q17.1: The dimension tables use SCD Type 1. What does that mean and what are the alternatives?**

- **SCD Type 1**: Overwrite the old value. No history preserved. Current state only.
- **SCD Type 2**: Add a new row with effective dates (`valid_from`, `valid_to`, `is_current`). Full history preserved.
- **SCD Type 3**: Add columns for previous value (`previous_email`, `current_email`). Limited history.
- This pipeline uses Type 1 via Delta MERGE `whenMatchedUpdateAll` — when a customer's address changes, the old address is overwritten.

**Q17.2: If a customer changes their email, what's the impact on the salt key and downstream views?**

- The `email_salt_key` changes (new email -> new hash).
- `vw_customer_lifetime_value` shows the current salt key. Historical orders still reference the same `customer_id`.
- No issue for joins (they're on `customer_id`, not salt key).
- But if anyone used `email_salt_key` for matching across systems, the change breaks that linkage.

**Q17.3: When would you use SCD Type 2 here? How would the MERGE change?**

- Use SCD Type 2 if you need to know a customer's address at the time of an order (e.g., for shipping verification).
- MERGE would: (1) mark existing matching rows as expired (`is_current = false`, `valid_to = current_timestamp`), (2) insert new rows with `is_current = true`, `valid_from = current_timestamp`, `valid_to = null`.
- This significantly increases table size and query complexity.

---

## 18. Concurrency & Race Conditions

**Q18.1: Can two Bronze jobs for the same topic run concurrently? What happens?**

- If two Autoloader instances use the same checkpoint path, they conflict. One will fail or both may process the same files, causing duplicates.
- Databricks job default `max_concurrent_runs = 1` prevents this for scheduled runs.
- For manual triggers, you need to be careful.

**Q18.2: Can Bronze and Silver run at the same time on the same table?**

- Bronze writes (appends) to `orders_raw`. Silver reads from `orders_raw` and writes to `fact_orders`.
- Delta Lake supports concurrent reads and writes (MVCC — Multi-Version Concurrency Control).
- Silver reads a snapshot of Bronze at a point in time. Concurrent Bronze writes don't affect the Silver read (snapshot isolation).
- No issues.

**Q18.3: What if two Silver jobs try to MERGE into the same table simultaneously?**

- Delta Lake uses optimistic concurrency control. Two concurrent MERGE operations on the same table may conflict.
- If they modify different rows, both succeed. If they modify the same rows, one fails with a `ConcurrentModificationException`.
- In this pipeline, each Silver task writes to a different table, so this isn't an issue.

---

## 19. Disaster Recovery & Data Retention

**Q19.1: What's the disaster recovery strategy?**

- **S3**: Versioning enabled. Cross-region replication not configured (gap).
- **Delta tables**: Stored in Databricks-managed storage. Delta time travel allows recovery to previous versions (default 30 days).
- **Terraform state**: S3 backend. Should have versioning and cross-region replication.
- **Databricks**: Asset bundles can redeploy all jobs/views from code. Infrastructure-as-code enables full rebuild.

**Q19.2: How far back can you recover data using Delta time travel?**

- Default Delta retention is 30 days (`delta.logRetentionDuration = 30 days`, `delta.deletedFileRetentionDuration = 7 days`).
- `VACUUM` removes files older than the retention period. After vacuum, time travel beyond retention is impossible.
- The `OPTIMIZE` commands in the pipeline may trigger automatic vacuum (depending on settings).

**Q19.3: If you need to replay data from 3 months ago, how would you do it?**

- S3 landing zone still has files (lifecycle moves to Glacier after 90 days, deletes after 365).
- Restore files from Glacier (takes hours), reset Autoloader checkpoint, re-run Bronze.
- Or: If Delta time travel is within range, `SELECT * FROM table VERSION AS OF timestamp`.
- For a full replay, you'd need to: (1) restore S3 files, (2) clear checkpoints, (3) re-run Bronze/Silver/Gold.

---

## 20. Specific Code Deep-Dive Questions

**Q20.1: In `validation.py`, the `validate_required_fields` function uses `F.filter(error_array, lambda x: x.isNotNull())`. What does this do?**

- It creates an array of error messages (one per required field), where valid fields have `null` entries.
- `F.filter` with a lambda removes `null` entries, leaving only actual error messages.
- The remaining errors are joined with commas into `_validation_errors`.
- If the filtered array is empty, `concat_ws` returns an empty string, which is then checked to determine `_record_status`.

**Q20.2: In `salt_key.py`, why use `F.concat_ws("|", F.lit(salt), ...)` instead of simple concatenation?**

- `concat_ws` uses a separator (`|`) between values, preventing ambiguity. Without a separator, `("abc", "def")` and `("ab", "cdef")` would produce the same hash.
- The salt is prepended to ensure different salts produce different hashes even for identical data.
- `F.coalesce(F.col(c).cast("string"), F.lit(""))` ensures NULLs are handled consistently.

**Q20.3: The `config_loader.py` uses `string.Template` for variable substitution. Why not Jinja2 or direct env var replacement?**

- `string.Template` is stdlib — no external dependency.
- It uses `${var_name}` syntax with `safe_substitute` (doesn't error on missing vars, leaves them as-is).
- Jinja2 would be more powerful (conditionals, loops) but adds a dependency for a simple use case.

**Q20.4: In `autoloader_ingestion.py`, `process_bronze_batch` checks `if valid_df.count() > 0`. What's the performance concern?**

- `count()` triggers a full evaluation of the DataFrame — it materializes the data.
- After calling `count()`, the write operation re-evaluates the DataFrame again (Spark doesn't cache by default).
- This means the data is processed twice — once for `count()`, once for `write`.
- Better approach: Use `df.isEmpty()` (already used at the top) or cache the filtered DataFrames before counting and writing. Or simply always write and let Delta handle empty appends gracefully (no-op).

---

## 21. Bonus Questions — Architecture Alternatives

**Q21.1: How would this architecture change if you used Delta Live Tables (DLT) instead of custom notebooks?**

- DLT would handle orchestration, schema enforcement, data quality expectations, and incremental processing automatically.
- Bronze: `@dlt.table` with `dlt.read_stream` from cloudFiles.
- Silver: `@dlt.table` with `dlt.read_stream` from Bronze tables + expectations for quality.
- Gold: `@dlt.view` for the views.
- Benefits: Built-in lineage, quality monitoring, automatic retry, managed infrastructure.
- Trade-off: Less control, DLT-specific API, vendor lock-in.

**Q21.2: How would the architecture change if you replaced SNS + Firehose with Kafka?**

- Kafka provides consumer groups, exactly-once semantics (with transactions), replay capability, and much lower latency.
- Could use Spark Structured Streaming with Kafka connector directly — no S3 landing zone needed.
- Trade-off: More infrastructure to manage (Kafka cluster), higher operational complexity.
- Kafka would be better for: High throughput, low latency, complex event processing.

**Q21.3: Why Databricks instead of AWS Glue or EMR?**

- **Databricks**: Managed Spark with Delta Lake, Unity Catalog for governance, collaborative notebooks, Autoloader for incremental ingestion.
- **Glue**: Cheaper for simple ETL, serverless, but less control, no interactive development, limited monitoring.
- **EMR**: Full Spark control, cheaper for large sustained workloads, but more operational overhead.
- Decision factors: Team familiarity, governance requirements (Unity Catalog), development velocity, cost profile.
