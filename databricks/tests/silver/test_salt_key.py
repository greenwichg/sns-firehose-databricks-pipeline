"""Tests for Silver layer salt key generation."""

import pytest
from pyspark.sql import functions as F

from databricks.src.utils.salt_key import generate_salt_key


class TestGenerateSaltKey:
    """Test salt key generation logic."""

    def test_generates_deterministic_hash(self, spark):
        """Same input should produce the same salt key."""
        df = spark.createDataFrame(
            [("alice@example.com",), ("alice@example.com",)], ["email"]
        )

        result = generate_salt_key(df, ["email"], salt="test_salt")
        keys = result.select("salt_key").distinct().collect()
        assert len(keys) == 1  # Both rows should have the same hash

    def test_different_inputs_produce_different_hashes(self, spark):
        """Different inputs should produce different salt keys."""
        df = spark.createDataFrame(
            [("alice@example.com",), ("bob@example.com",)], ["email"]
        )

        result = generate_salt_key(df, ["email"], salt="test_salt")
        keys = result.select("salt_key").distinct().collect()
        assert len(keys) == 2

    def test_different_salts_produce_different_hashes(self, spark):
        """Same data with different salts should produce different hashes."""
        df = spark.createDataFrame([("alice@example.com",)], ["email"])

        result1 = generate_salt_key(df, ["email"], output_column="key1", salt="salt_a")
        result2 = generate_salt_key(
            result1, ["email"], output_column="key2", salt="salt_b"
        )

        row = result2.collect()[0]
        assert row["key1"] != row["key2"]

    def test_null_values_handled(self, spark):
        """Null values should be replaced with empty string before hashing."""
        df = spark.createDataFrame([(None,)], ["email"])
        result = generate_salt_key(df, ["email"], salt="test_salt")
        key = result.select("salt_key").collect()[0][0]
        assert key is not None
        assert len(key) == 64  # SHA-256 hex digest

    def test_multi_column_salt_key(self, spark):
        """Salt key from multiple columns should combine all values."""
        df = spark.createDataFrame(
            [("alice@example.com", "555-0100")], ["email", "phone"]
        )

        result = generate_salt_key(df, ["email", "phone"], salt="test_salt")
        key = result.select("salt_key").collect()[0][0]
        assert key is not None
        assert len(key) == 64

    def test_custom_output_column_name(self, spark):
        """Should use the provided output column name."""
        df = spark.createDataFrame([("test",)], ["data"])
        result = generate_salt_key(
            df, ["data"], output_column="my_hash", salt="test_salt"
        )
        assert "my_hash" in result.columns
