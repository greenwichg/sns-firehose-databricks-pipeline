"""Record validation utilities for Bronze layer processing."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def validate_required_fields(df: DataFrame, required_fields: list[str]) -> DataFrame:
    """Add validation columns to identify valid and invalid records.

    Checks that all required fields are present and non-null.

    Args:
        df: Input DataFrame with raw data.
        required_fields: List of column names that must be non-null.

    Returns:
        DataFrame with _record_status and _validation_errors columns.
    """
    # Build a list of validation error expressions
    error_conditions: list[Column] = []
    for field in required_fields:
        if field in df.columns:
            error_conditions.append(
                F.when(
                    F.col(field).isNull() | (F.trim(F.col(field).cast("string")) == ""),
                    F.lit(f"missing_{field}"),
                )
            )
        else:
            error_conditions.append(F.lit(f"missing_{field}"))

    # Concatenate all errors into a single string
    if error_conditions:
        validation_errors = F.concat_ws(
            ",",
            F.array([expr for expr in error_conditions]),
        )
        # Remove nulls from the error array
        validation_errors = F.concat_ws(
            ",",
            F.filter(
                F.split(validation_errors, ","),
                lambda x: x != "",
            ),
        )
    else:
        validation_errors = F.lit(None).cast("string")

    df = df.withColumn("_validation_errors", validation_errors)

    # Mark record as valid or invalid
    df = df.withColumn(
        "_record_status",
        F.when(
            (F.col("_validation_errors").isNull())
            | (F.col("_validation_errors") == ""),
            F.lit("valid"),
        ).otherwise(F.lit("invalid")),
    )

    return df


def add_audit_columns(df: DataFrame) -> DataFrame:
    """Add ingestion audit metadata columns.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with _ingestion_timestamp and _source_file columns.
    """
    return df.withColumn(
        "_ingestion_timestamp", F.current_timestamp()
    ).withColumn(
        "_source_file", F.input_file_name()
    )
