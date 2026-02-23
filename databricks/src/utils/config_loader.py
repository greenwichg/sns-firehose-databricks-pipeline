"""Load and resolve pipeline configuration."""

import os
import string

import yaml


def load_config(config_path: str = None) -> dict:
    """Load pipeline configuration from YAML.

    Resolves environment variable placeholders like ${VAR_NAME}.

    Args:
        config_path: Path to the YAML config file. If None, uses the default.

    Returns:
        Resolved configuration dictionary.
    """
    if config_path is None:
        config_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "..", "configs", "pipeline_config.yaml"
        )

    with open(config_path, "r") as f:
        raw = f.read()

    # Resolve ${VAR_NAME} placeholders from environment / Databricks widgets
    template = string.Template(raw)
    env_vars = {
        "landing_bucket_name": os.getenv("LANDING_BUCKET_NAME", "landing-bucket"),
        "catalog_name": os.getenv("CATALOG_NAME", "pipeline_catalog"),
        "environment": os.getenv("ENVIRONMENT", "dev"),
    }
    resolved = template.safe_substitute(env_vars)

    return yaml.safe_load(resolved)


def get_topic_config(config: dict, topic_name: str) -> dict:
    """Get configuration for a specific topic.

    Args:
        config: Full pipeline configuration.
        topic_name: Name of the topic (e.g. 'orders').

    Returns:
        Topic-specific configuration.
    """
    return config["topics"][topic_name]


def get_table_path(config: dict, layer: str, table_name: str) -> str:
    """Build a three-level Unity Catalog table path.

    Args:
        config: Full pipeline configuration.
        layer: Medallion layer (bronze, silver, gold).
        table_name: Table name.

    Returns:
        Fully qualified table name: catalog.schema.table
    """
    catalog = config["catalog"]
    schema = config["schemas"][layer]
    return f"{catalog}.{schema}.{table_name}"
