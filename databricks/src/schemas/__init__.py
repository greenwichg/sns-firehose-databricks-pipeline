"""Schema registry for all data domains."""

from databricks.src.schemas.customers import (
    CUSTOMERS_BRONZE_SCHEMA,
    CUSTOMERS_RAW_SCHEMA,
    CUSTOMERS_REQUIRED_FIELDS,
)
from databricks.src.schemas.orders import (
    ORDERS_BRONZE_SCHEMA,
    ORDERS_RAW_SCHEMA,
    ORDERS_REQUIRED_FIELDS,
)
from databricks.src.schemas.products import (
    PRODUCTS_BRONZE_SCHEMA,
    PRODUCTS_RAW_SCHEMA,
    PRODUCTS_REQUIRED_FIELDS,
)

SCHEMA_REGISTRY = {
    "orders": {
        "raw_schema": ORDERS_RAW_SCHEMA,
        "bronze_schema": ORDERS_BRONZE_SCHEMA,
        "required_fields": ORDERS_REQUIRED_FIELDS,
    },
    "customers": {
        "raw_schema": CUSTOMERS_RAW_SCHEMA,
        "bronze_schema": CUSTOMERS_BRONZE_SCHEMA,
        "required_fields": CUSTOMERS_REQUIRED_FIELDS,
    },
    "products": {
        "raw_schema": PRODUCTS_RAW_SCHEMA,
        "bronze_schema": PRODUCTS_BRONZE_SCHEMA,
        "required_fields": PRODUCTS_REQUIRED_FIELDS,
    },
}
