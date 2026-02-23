# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Fact Orders
# MAGIC
# MAGIC Creates the `fact_orders` table from bronze `orders_raw` with selected columns.
# MAGIC Implements incremental processing using Delta merge (upsert).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("catalog_name", "pipeline_catalog", "Unity Catalog Name")

# COMMAND ----------

environment = dbutils.widgets.get("environment")
catalog_name = dbutils.widgets.get("catalog_name")

catalog = f"{catalog_name}_{environment}"
bronze_schema = "bronze"
silver_schema = "silver"

source_table = f"{catalog}.{bronze_schema}.orders_raw"
target_table = f"{catalog}.{silver_schema}.fact_orders"

print(f"Source: {source_table}")
print(f"Target: {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Fact Table

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        order_id STRING NOT NULL,
        customer_id STRING NOT NULL,
        order_date TIMESTAMP NOT NULL,
        total_amount DOUBLE NOT NULL,
        status STRING,
        payment_method STRING,
        shipping_address_id STRING,
        _ingestion_timestamp TIMESTAMP,
        _silver_processed_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Silver fact table for orders'
    PARTITIONED BY (order_date)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform and Load

# COMMAND ----------

from pyspark.sql import functions as F

# Read from bronze — only valid records
bronze_df = (
    spark.table(source_table)
    .filter(F.col("_record_status") == "valid")
)

# Select fact columns and add processing timestamp
fact_df = (
    bronze_df
    .select(
        "order_id",
        "customer_id",
        F.col("order_date").cast("timestamp").alias("order_date"),
        "total_amount",
        "status",
        "payment_method",
        "shipping_address_id",
        "_ingestion_timestamp",
    )
    .withColumn("_silver_processed_at", F.current_timestamp())
    .dropDuplicates(["order_id"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert into Silver Fact Table

# COMMAND ----------

from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(spark, target_table):
    delta_target = DeltaTable.forName(spark, target_table)

    (
        delta_target.alias("target")
        .merge(
            fact_df.alias("source"),
            "target.order_id = source.order_id"
        )
        .whenMatchedUpdateAll(
            condition="source._ingestion_timestamp > target._ingestion_timestamp"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    fact_df.write.format("delta").mode("overwrite").saveAsTable(target_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Processing

# COMMAND ----------

count = spark.table(target_table).count()
print(f"Total records in {target_table}: {count}")

spark.sql(f"OPTIMIZE {target_table} ZORDER BY (customer_id, order_date)")
