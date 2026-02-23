# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Dimension Products
# MAGIC
# MAGIC Creates the `dim_products` SCD Type 1 dimension table from bronze `products_raw`.
# MAGIC Generates salt keys for the product name column.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("catalog_name", "pipeline_catalog", "Unity Catalog Name")
dbutils.widgets.text("salt_secret_scope", "pipeline-secrets", "Databricks Secret Scope")
dbutils.widgets.text("salt_secret_key", "pii-salt-key", "Secret Key for Salt")

# COMMAND ----------

environment = dbutils.widgets.get("environment")
catalog_name = dbutils.widgets.get("catalog_name")
salt_secret_scope = dbutils.widgets.get("salt_secret_scope")
salt_secret_key = dbutils.widgets.get("salt_secret_key")

catalog = f"{catalog_name}_{environment}"
bronze_schema = "bronze"
silver_schema = "silver"

source_table = f"{catalog}.{bronze_schema}.products_raw"
target_table = f"{catalog}.{silver_schema}.dim_products"

try:
    salt = dbutils.secrets.get(scope=salt_secret_scope, key=salt_secret_key)
except Exception:
    salt = None

print(f"Source: {source_table}")
print(f"Target: {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Dimension Table

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        product_id STRING NOT NULL,
        name STRING,
        category STRING,
        price DOUBLE,
        description STRING,
        brand STRING,
        sku STRING,
        weight DOUBLE,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        name_salt_key STRING,
        _ingestion_timestamp TIMESTAMP,
        _silver_processed_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Silver dimension table for products with salt keys'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform and Load

# COMMAND ----------

from pyspark.sql import functions as F
from databricks.src.utils.salt_key import generate_salt_key

# Read from bronze — only valid records
bronze_df = (
    spark.table(source_table)
    .filter(F.col("_record_status") == "valid")
)

# Select dimension columns
dim_df = (
    bronze_df
    .select(
        "product_id",
        "name",
        "category",
        "price",
        "description",
        "brand",
        "sku",
        "weight",
        F.col("created_at").cast("timestamp").alias("created_at"),
        F.col("updated_at").cast("timestamp").alias("updated_at"),
        "_ingestion_timestamp",
    )
    .withColumn("_silver_processed_at", F.current_timestamp())
    .dropDuplicates(["product_id"])
)

# Generate salt key for product name
dim_df = generate_salt_key(dim_df, ["name"], output_column="name_salt_key", salt=salt)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert (SCD Type 1)

# COMMAND ----------

from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(spark, target_table):
    delta_target = DeltaTable.forName(spark, target_table)

    (
        delta_target.alias("target")
        .merge(
            dim_df.alias("source"),
            "target.product_id = source.product_id"
        )
        .whenMatchedUpdateAll(
            condition="source._ingestion_timestamp > target._ingestion_timestamp"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    dim_df.write.format("delta").mode("overwrite").saveAsTable(target_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Processing

# COMMAND ----------

count = spark.table(target_table).count()
print(f"Total records in {target_table}: {count}")

spark.sql(f"OPTIMIZE {target_table} ZORDER BY (product_id, category)")
