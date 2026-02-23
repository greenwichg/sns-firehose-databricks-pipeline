# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Dimension Customers
# MAGIC
# MAGIC Creates the `dim_customers` SCD Type 1 dimension table from bronze `customers_raw`.
# MAGIC Generates salt keys for PII columns (email, phone, name).

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

source_table = f"{catalog}.{bronze_schema}.customers_raw"
target_table = f"{catalog}.{silver_schema}.dim_customers"

# Retrieve salt from Databricks secrets
try:
    salt = dbutils.secrets.get(scope=salt_secret_scope, key=salt_secret_key)
except Exception:
    salt = None  # Falls back to default in salt_key module

print(f"Source: {source_table}")
print(f"Target: {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Dimension Table

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        customer_id STRING NOT NULL,
        name STRING,
        email STRING,
        phone STRING,
        address STRING,
        city STRING,
        state STRING,
        country STRING,
        postal_code STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        email_salt_key STRING,
        phone_salt_key STRING,
        name_salt_key STRING,
        _ingestion_timestamp TIMESTAMP,
        _silver_processed_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Silver dimension table for customers with salt keys for PII'
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
        "customer_id",
        "name",
        "email",
        "phone",
        "address",
        "city",
        "state",
        "country",
        "postal_code",
        F.col("created_at").cast("timestamp").alias("created_at"),
        F.col("updated_at").cast("timestamp").alias("updated_at"),
        "_ingestion_timestamp",
    )
    .withColumn("_silver_processed_at", F.current_timestamp())
    .dropDuplicates(["customer_id"])
)

# Generate salt keys for PII columns
dim_df = generate_salt_key(dim_df, ["email"], output_column="email_salt_key", salt=salt)
dim_df = generate_salt_key(dim_df, ["phone"], output_column="phone_salt_key", salt=salt)
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
            "target.customer_id = source.customer_id"
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

spark.sql(f"OPTIMIZE {target_table} ZORDER BY (customer_id)")
