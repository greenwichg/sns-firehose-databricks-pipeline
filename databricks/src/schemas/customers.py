"""Schema definitions for the customers data domain."""

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Raw JSON schema for customers
CUSTOMERS_RAW_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), nullable=False),
        StructField("email", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("phone", StringType(), nullable=True),
        StructField("address", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField("created_at", StringType(), nullable=True),
        StructField("updated_at", StringType(), nullable=True),
        StructField("lifetime_value", DoubleType(), nullable=True),
    ]
)

# Bronze structured schema
CUSTOMERS_BRONZE_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), nullable=False),
        StructField("email", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("phone", StringType(), nullable=True),
        StructField("address", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=True),
        StructField("lifetime_value", DoubleType(), nullable=True),
        StructField("_ingestion_timestamp", TimestampType(), nullable=False),
        StructField("_source_file", StringType(), nullable=False),
        StructField("_record_status", StringType(), nullable=False),
        StructField("_validation_errors", StringType(), nullable=True),
    ]
)

CUSTOMERS_REQUIRED_FIELDS = ["customer_id", "email", "name"]
