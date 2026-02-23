"""Schema definitions for the products data domain."""

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Raw JSON schema for products
PRODUCTS_RAW_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=False),
        StructField("category", StringType(), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("brand", StringType(), nullable=True),
        StructField("sku", StringType(), nullable=True),
        StructField("weight", DoubleType(), nullable=True),
        StructField("created_at", StringType(), nullable=True),
        StructField("updated_at", StringType(), nullable=True),
    ]
)

# Bronze structured schema
PRODUCTS_BRONZE_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=False),
        StructField("category", StringType(), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("brand", StringType(), nullable=True),
        StructField("sku", StringType(), nullable=True),
        StructField("weight", DoubleType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=True),
        StructField("_ingestion_timestamp", TimestampType(), nullable=False),
        StructField("_source_file", StringType(), nullable=False),
        StructField("_record_status", StringType(), nullable=False),
        StructField("_validation_errors", StringType(), nullable=True),
    ]
)

PRODUCTS_REQUIRED_FIELDS = ["product_id", "name", "price"]
