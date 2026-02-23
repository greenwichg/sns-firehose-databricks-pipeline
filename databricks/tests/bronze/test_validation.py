"""Tests for Bronze layer validation utilities."""

import pytest
from pyspark.sql import functions as F

from databricks.src.utils.validation import add_audit_columns, validate_required_fields


class TestValidateRequiredFields:
    """Test record validation logic."""

    def test_all_fields_present_marks_valid(self, spark):
        """Records with all required fields should be marked valid."""
        data = [
            ("ORD-001", "CUST-001", "2024-01-01", 100.0),
            ("ORD-002", "CUST-002", "2024-01-02", 200.0),
        ]
        df = spark.createDataFrame(
            data, ["order_id", "customer_id", "order_date", "total_amount"]
        )

        result = validate_required_fields(
            df, ["order_id", "customer_id", "order_date"]
        )

        valid_count = result.filter(F.col("_record_status") == "valid").count()
        assert valid_count == 2

    def test_missing_required_field_marks_invalid(self, spark):
        """Records with null required fields should be marked invalid."""
        data = [
            ("ORD-001", None, "2024-01-01", 100.0),
            ("ORD-002", "CUST-002", "2024-01-02", 200.0),
        ]
        df = spark.createDataFrame(
            data, ["order_id", "customer_id", "order_date", "total_amount"]
        )

        result = validate_required_fields(
            df, ["order_id", "customer_id", "order_date"]
        )

        invalid_count = result.filter(F.col("_record_status") == "invalid").count()
        valid_count = result.filter(F.col("_record_status") == "valid").count()
        assert invalid_count == 1
        assert valid_count == 1

    def test_empty_string_marks_invalid(self, spark):
        """Records with empty-string required fields should be marked invalid."""
        data = [
            ("ORD-001", "", "2024-01-01", 100.0),
        ]
        df = spark.createDataFrame(
            data, ["order_id", "customer_id", "order_date", "total_amount"]
        )

        result = validate_required_fields(
            df, ["order_id", "customer_id"]
        )

        invalid_count = result.filter(F.col("_record_status") == "invalid").count()
        assert invalid_count == 1

    def test_validation_errors_column_populated(self, spark):
        """Validation errors column should list the failed fields."""
        data = [
            (None, None, "2024-01-01", 100.0),
        ]
        df = spark.createDataFrame(
            data, ["order_id", "customer_id", "order_date", "total_amount"]
        )

        result = validate_required_fields(
            df, ["order_id", "customer_id"]
        )

        errors = result.select("_validation_errors").collect()[0][0]
        assert "missing_order_id" in errors
        assert "missing_customer_id" in errors


class TestAddAuditColumns:
    """Test audit column generation."""

    def test_adds_ingestion_timestamp(self, spark):
        """Should add _ingestion_timestamp column."""
        df = spark.createDataFrame([("a",)], ["col1"])
        result = add_audit_columns(df)
        assert "_ingestion_timestamp" in result.columns

    def test_adds_source_file(self, spark):
        """Should add _source_file column."""
        df = spark.createDataFrame([("a",)], ["col1"])
        result = add_audit_columns(df)
        assert "_source_file" in result.columns
