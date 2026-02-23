"""Salt key generation for dimension tables in the Silver layer.

Salt keys provide a deterministic, irreversible surrogate key derived
from sensitive PII columns. This enables joins across tables without
exposing raw PII values in the silver/gold layers.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# Default salt — override via Databricks secrets in production
_DEFAULT_SALT = "pipeline_default_salt_2024"


def generate_salt_key(
    df: DataFrame,
    columns: list[str],
    output_column: str = "salt_key",
    salt: str | None = None,
) -> DataFrame:
    """Generate a salted SHA-256 hash key from one or more columns.

    The function concatenates the specified columns with a salt value and
    produces a deterministic SHA-256 digest. NULL values are replaced with
    the empty string before hashing to ensure consistency.

    Args:
        df: Input DataFrame.
        columns: Column names to include in the hash.
        output_column: Name of the output hash column.
        salt: Salt string. If None, uses the default salt.

    Returns:
        DataFrame with the new salt key column.
    """
    if salt is None:
        salt = _DEFAULT_SALT

    # Coalesce NULLs and concatenate with salt
    concat_expr = F.concat_ws(
        "|",
        F.lit(salt),
        *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns],
    )

    return df.withColumn(output_column, F.sha2(concat_expr, 256))
