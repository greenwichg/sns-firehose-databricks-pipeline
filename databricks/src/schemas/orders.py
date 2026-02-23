"""Schema definitions for the orders data domain."""

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Raw JSON schema for orders arriving from SNS -> Firehose -> S3
ORDERS_RAW_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("order_date", StringType(), nullable=False),
        StructField("total_amount", DoubleType(), nullable=False),
        StructField("status", StringType(), nullable=True),
        StructField("payment_method", StringType(), nullable=True),
        StructField("shipping_address_id", StringType(), nullable=True),
        StructField(
            "items",
            ArrayType(
                StructType(
                    [
                        StructField("product_id", StringType(), nullable=True),
                        StructField("quantity", IntegerType(), nullable=True),
                        StructField("unit_price", DoubleType(), nullable=True),
                    ]
                )
            ),
            nullable=True,
        ),
    ]
)

# Bronze structured schema (after JSON parsing and type casting)
ORDERS_BRONZE_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("order_date", TimestampType(), nullable=False),
        StructField("total_amount", DoubleType(), nullable=False),
        StructField("status", StringType(), nullable=True),
        StructField("payment_method", StringType(), nullable=True),
        StructField("shipping_address_id", StringType(), nullable=True),
        StructField(
            "items",
            ArrayType(
                StructType(
                    [
                        StructField("product_id", StringType(), nullable=True),
                        StructField("quantity", IntegerType(), nullable=True),
                        StructField("unit_price", DoubleType(), nullable=True),
                    ]
                )
            ),
            nullable=True,
        ),
        StructField("_ingestion_timestamp", TimestampType(), nullable=False),
        StructField("_source_file", StringType(), nullable=False),
        StructField("_record_status", StringType(), nullable=False),  # valid / invalid
        StructField("_validation_errors", StringType(), nullable=True),
    ]
)

# Required fields for validation
ORDERS_REQUIRED_FIELDS = ["order_id", "customer_id", "order_date", "total_amount"]
