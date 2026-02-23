# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Views and Permissions
# MAGIC
# MAGIC Creates business-level views in the gold schema and assigns Unity Catalog
# MAGIC grants based on roles (analysts, engineers, data_scientists).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("catalog_name", "pipeline_catalog", "Unity Catalog Name")

# COMMAND ----------

environment = dbutils.widgets.get("environment")
catalog_name = dbutils.widgets.get("catalog_name")

catalog = f"{catalog_name}_{environment}"
silver_schema = "silver"
gold_schema = "gold"

print(f"Catalog: {catalog}")
print(f"Silver:  {catalog}.{silver_schema}")
print(f"Gold:    {catalog}.{gold_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Gold Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 1 — Order Summary
# MAGIC
# MAGIC Joins fact_orders with dim_customers and enriches with customer context.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{gold_schema}.vw_order_summary AS
    SELECT
        o.order_id,
        o.order_date,
        o.total_amount,
        o.status AS order_status,
        o.payment_method,
        c.customer_id,
        c.name AS customer_name,
        c.email_salt_key,
        c.city AS customer_city,
        c.state AS customer_state,
        c.country AS customer_country,
        o._silver_processed_at
    FROM {catalog}.{silver_schema}.fact_orders o
    LEFT JOIN {catalog}.{silver_schema}.dim_customers c
        ON o.customer_id = c.customer_id
""")

print(f"Created view: {catalog}.{gold_schema}.vw_order_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 2 — Customer Lifetime Value
# MAGIC
# MAGIC Aggregates order data per customer to compute lifetime metrics.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{gold_schema}.vw_customer_lifetime_value AS
    SELECT
        c.customer_id,
        c.name AS customer_name,
        c.email_salt_key,
        c.city,
        c.state,
        c.country,
        c.created_at AS customer_since,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(o.total_amount) AS total_spend,
        AVG(o.total_amount) AS avg_order_value,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS last_order_date,
        DATEDIFF(MAX(o.order_date), MIN(o.order_date)) AS customer_tenure_days
    FROM {catalog}.{silver_schema}.dim_customers c
    LEFT JOIN {catalog}.{silver_schema}.fact_orders o
        ON c.customer_id = o.customer_id
    GROUP BY
        c.customer_id, c.name, c.email_salt_key,
        c.city, c.state, c.country, c.created_at
""")

print(f"Created view: {catalog}.{gold_schema}.vw_customer_lifetime_value")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 3 — Product Sales Performance
# MAGIC
# MAGIC Aggregates sales data at the product level.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{gold_schema}.vw_product_sales AS
    SELECT
        p.product_id,
        p.name AS product_name,
        p.category,
        p.brand,
        p.price AS current_price,
        p.name_salt_key,
        COUNT(DISTINCT o.order_id) AS times_ordered,
        SUM(o.total_amount) AS total_revenue,
        AVG(o.total_amount) AS avg_order_value,
        MIN(o.order_date) AS first_sale_date,
        MAX(o.order_date) AS last_sale_date
    FROM {catalog}.{silver_schema}.dim_products p
    LEFT JOIN {catalog}.{silver_schema}.fact_orders o
        ON 1 = 1  -- Full join placeholder; in production, join via order items
    GROUP BY
        p.product_id, p.name, p.category, p.brand,
        p.price, p.name_salt_key
""")

print(f"Created view: {catalog}.{gold_schema}.vw_product_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog Permissions
# MAGIC
# MAGIC Grant role-based access to Gold views.

# COMMAND ----------

# Role definitions — map role group to allowed privileges
role_grants = {
    "analysts": {
        "views": [
            "vw_order_summary",
            "vw_customer_lifetime_value",
            "vw_product_sales",
        ],
        "privileges": ["SELECT"],
    },
    "engineers": {
        "views": [
            "vw_order_summary",
            "vw_customer_lifetime_value",
            "vw_product_sales",
        ],
        "privileges": ["SELECT", "MODIFY"],
    },
    "data_scientists": {
        "views": [
            "vw_customer_lifetime_value",
            "vw_product_sales",
        ],
        "privileges": ["SELECT"],
    },
}

# COMMAND ----------

# Grant USE CATALOG and USE SCHEMA first
for role_group in role_grants:
    spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{role_group}`")
    spark.sql(
        f"GRANT USE SCHEMA ON SCHEMA {catalog}.{gold_schema} TO `{role_group}`"
    )
    print(f"Granted USE CATALOG/SCHEMA to {role_group}")

# Grant view-level privileges
for role_group, config in role_grants.items():
    for view_name in config["views"]:
        for privilege in config["privileges"]:
            fq_view = f"{catalog}.{gold_schema}.{view_name}"
            spark.sql(f"GRANT {privilege} ON VIEW {fq_view} TO `{role_group}`")
            print(f"Granted {privilege} on {fq_view} to {role_group}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Grants

# COMMAND ----------

for view_name in ["vw_order_summary", "vw_customer_lifetime_value", "vw_product_sales"]:
    fq_view = f"{catalog}.{gold_schema}.{view_name}"
    grants = spark.sql(f"SHOW GRANTS ON VIEW {fq_view}")
    print(f"\nGrants on {fq_view}:")
    grants.show(truncate=False)
