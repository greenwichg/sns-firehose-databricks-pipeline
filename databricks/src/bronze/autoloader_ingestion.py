# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Autoloader Ingestion
# MAGIC
# MAGIC Reads raw JSON files from S3 (delivered by Kinesis Firehose), validates records,
# MAGIC and writes to two Delta tables:
# MAGIC - **raw table**: valid records
# MAGIC - **invalid table**: records that failed validation
# MAGIC
# MAGIC Both tables share the same schema with additional audit columns:
# MAGIC `_ingestion_timestamp`, `_source_file`, `_record_status`, `_validation_errors`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets / Parameters

# COMMAND ----------

dbutils.widgets.text("topic_name", "orders", "Topic / Data Domain")
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("landing_bucket", "", "S3 Landing Bucket")
dbutils.widgets.text("catalog_name", "pipeline_catalog", "Unity Catalog Name")

# COMMAND ----------

topic_name = dbutils.widgets.get("topic_name")
environment = dbutils.widgets.get("environment")
landing_bucket = dbutils.widgets.get("landing_bucket")
catalog_name = dbutils.widgets.get("catalog_name")

catalog = f"{catalog_name}_{environment}"
bronze_schema = "bronze"

s3_source_path = f"s3://{landing_bucket}/{topic_name}/"
checkpoint_path = f"s3://{landing_bucket}/_checkpoints/bronze/{topic_name}"

raw_table = f"{catalog}.{bronze_schema}.{topic_name}_raw"
invalid_table = f"{catalog}.{bronze_schema}.{topic_name}_invalid"

print(f"Topic:       {topic_name}")
print(f"Source:      {s3_source_path}")
print(f"Checkpoint:  {checkpoint_path}")
print(f"Raw table:   {raw_table}")
print(f"Invalid tbl: {invalid_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema & Validation Setup

# COMMAND ----------

from pyspark.sql import functions as F

# Import schemas and validation utilities
from databricks.src.schemas import SCHEMA_REGISTRY
from databricks.src.utils.validation import add_audit_columns, validate_required_fields

topic_config = SCHEMA_REGISTRY[topic_name]
raw_schema = topic_config["raw_schema"]
required_fields = topic_config["required_fields"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze Tables (if not exist)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")

# Tables are created on first write with mergeSchema enabled.
# No placeholder tables needed — Autoloader foreachBatch handles creation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Autoloader — Structured Streaming Ingestion

# COMMAND ----------

def process_bronze_batch(batch_df, batch_id):
    """Process each micro-batch: validate, split, and write to Delta tables.

    Args:
        batch_df: The micro-batch DataFrame from Autoloader.
        batch_id: The batch identifier.
    """
    if batch_df.isEmpty():
        return

    # Add audit columns
    df = add_audit_columns(batch_df)

    # Validate required fields
    df = validate_required_fields(df, required_fields)

    # Split into valid and invalid
    valid_df = df.filter(F.col("_record_status") == "valid")
    invalid_df = df.filter(F.col("_record_status") == "invalid")

    # Write valid records to raw table
    if valid_df.count() > 0:
        (
            valid_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(raw_table)
        )

    # Write invalid records to invalid table
    if invalid_df.count() > 0:
        (
            invalid_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(invalid_table)
        )

    print(f"Batch {batch_id}: {valid_df.count()} valid, {invalid_df.count()} invalid")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Start Autoloader Stream

# COMMAND ----------

autoloader_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.maxFilesPerTrigger", 1000)
    # Firehose delivers GZIP-compressed files
    .option("compression", "gzip")
    # Use notification mode for efficient S3 file discovery
    .option("cloudFiles.useNotifications", "true")
    .schema(raw_schema)
    .load(s3_source_path)
)

# COMMAND ----------

query = (
    autoloader_stream
    .writeStream
    .foreachBatch(process_bronze_batch)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)  # Process all available files then stop
    .start()
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Ingestion Metrics

# COMMAND ----------

raw_count = spark.table(raw_table).count()
invalid_count = spark.table(invalid_table).count()

print(f"Total records in {raw_table}: {raw_count}")
print(f"Total records in {invalid_table}: {invalid_count}")

# COMMAND ----------

# Optimize tables after ingestion
spark.sql(f"OPTIMIZE {raw_table}")
spark.sql(f"OPTIMIZE {invalid_table}")
