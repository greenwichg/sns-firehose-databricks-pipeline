##############################################
# Unity Catalog – Catalog, Schemas, External Location
##############################################

resource "databricks_catalog" "pipeline" {
  name    = "${var.catalog_name}_${var.environment}"
  comment = "Catalog for ${var.project_name} ${var.environment} environment"
}

# ------------------------------------------
# Schemas for each medallion layer
# ------------------------------------------
resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.pipeline.name
  name         = "bronze"
  comment      = "Bronze layer – raw ingested data"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.pipeline.name
  name         = "silver"
  comment      = "Silver layer – cleansed facts and dimensions"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.pipeline.name
  name         = "gold"
  comment      = "Gold layer – business-level views"
}

# ------------------------------------------
# External Location for S3 landing zone
# ------------------------------------------
resource "databricks_external_location" "landing" {
  name            = "${var.project_name}-${var.environment}-landing"
  url             = var.external_location_url
  credential_name = var.storage_credential_name
  comment         = "External location for S3 landing bucket"
}

# ------------------------------------------
# Grants – Bronze (read/write for pipeline)
# ------------------------------------------
resource "databricks_grants" "bronze_schema" {
  schema = databricks_schema.bronze.id

  grant {
    principal  = "account users"
    privileges = ["USE_SCHEMA"]
  }
}

resource "databricks_grants" "silver_schema" {
  schema = databricks_schema.silver.id

  grant {
    principal  = "account users"
    privileges = ["USE_SCHEMA"]
  }
}

# ------------------------------------------
# Grants – Gold (role-based access)
# ------------------------------------------
resource "databricks_grants" "gold_schema" {
  schema = databricks_schema.gold.id

  dynamic "grant" {
    for_each = var.roles
    content {
      principal  = grant.key
      privileges = ["USE_SCHEMA", "SELECT"]
    }
  }
}

resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.pipeline.name

  grant {
    principal  = "account users"
    privileges = ["USE_CATALOG"]
  }
}

# ------------------------------------------
# SQL Warehouse for Gold layer queries
# ------------------------------------------
resource "databricks_sql_endpoint" "gold_warehouse" {
  name             = "${var.project_name}-${var.environment}-gold-warehouse"
  cluster_size     = "2X-Small"
  max_num_clusters = 1
  auto_stop_mins   = 10

  tags {
    custom_tags {
      key   = "Project"
      value = var.project_name
    }
    custom_tags {
      key   = "Environment"
      value = var.environment
    }
  }
}
