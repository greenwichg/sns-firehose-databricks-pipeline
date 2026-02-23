# ==============================================================================
# SNS -> Firehose -> S3 -> Databricks Medallion Pipeline
# ==============================================================================
#
# Usage:
#   make help                       Show all targets
#   make test                       Run unit tests
#   make infra-plan ENV=dev         Terraform plan for dev
#   make infra-apply ENV=dev        Terraform apply for dev
#   make bundle-deploy ENV=dev      Deploy Databricks asset bundle
#   make run-pipeline ENV=dev       Trigger the full pipeline
#   make all ENV=dev                End-to-end: infra + deploy + test
# ==============================================================================

SHELL := /bin/bash
.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ENV           ?= dev
RECORDS       ?= 50
INVALID_PCT   ?= 0.1
SEED          ?=
TEST_DATA_DIR ?= ./test_data

TF_DIR        := terraform
TF_VARS       := $(TF_DIR)/environments/$(ENV)/terraform.tfvars
TF_BACKEND    := $(TF_DIR)/environments/$(ENV)/backend.hcl
DB_DIR        := databricks
SCRIPTS_DIR   := scripts

# Build seed flag only if SEED is set
SEED_FLAG     := $(if $(SEED),--seed $(SEED),)

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------
.PHONY: help
help: ## Show this help message
	@echo "Usage: make <target> [ENV=dev|staging|prod]"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Testing
# ---------------------------------------------------------------------------
.PHONY: test test-bronze test-silver test-gold lint

test: ## Run all unit tests
	pytest $(DB_DIR)/tests/ -v

test-bronze: ## Run Bronze layer tests only
	pytest $(DB_DIR)/tests/bronze/ -v

test-silver: ## Run Silver layer tests only
	pytest $(DB_DIR)/tests/silver/ -v

test-gold: ## Run Gold layer tests only
	pytest $(DB_DIR)/tests/gold/ -v

lint: ## Lint Python code with ruff
	ruff check $(DB_DIR)/src/ $(DB_DIR)/tests/ $(SCRIPTS_DIR)/
	ruff format --check $(DB_DIR)/src/ $(DB_DIR)/tests/ $(SCRIPTS_DIR)/

lint-fix: ## Auto-fix lint issues
	ruff check --fix $(DB_DIR)/src/ $(DB_DIR)/tests/ $(SCRIPTS_DIR)/
	ruff format $(DB_DIR)/src/ $(DB_DIR)/tests/ $(SCRIPTS_DIR)/

# ---------------------------------------------------------------------------
# Terraform — Infrastructure
# ---------------------------------------------------------------------------
.PHONY: infra-init infra-plan infra-apply infra-destroy infra-fmt infra-validate

infra-init: ## Initialize Terraform for the target environment
	terraform -chdir=$(TF_DIR) init \
		-backend-config=environments/$(ENV)/backend.hcl \
		-reconfigure

infra-fmt: ## Format Terraform files
	terraform -chdir=$(TF_DIR) fmt -recursive

infra-validate: infra-init ## Validate Terraform configuration
	terraform -chdir=$(TF_DIR) validate

infra-plan: infra-init ## Plan infrastructure changes
	terraform -chdir=$(TF_DIR) plan \
		-var-file=environments/$(ENV)/terraform.tfvars \
		-out=tfplan-$(ENV)

infra-apply: ## Apply infrastructure changes (requires prior plan)
	terraform -chdir=$(TF_DIR) apply tfplan-$(ENV)

infra-apply-auto: infra-init ## Plan and apply infrastructure without interactive approval
	terraform -chdir=$(TF_DIR) apply \
		-var-file=environments/$(ENV)/terraform.tfvars \
		-auto-approve

infra-destroy: infra-init ## Destroy infrastructure (interactive)
	terraform -chdir=$(TF_DIR) destroy \
		-var-file=environments/$(ENV)/terraform.tfvars

# ---------------------------------------------------------------------------
# Databricks — Asset Bundle
# ---------------------------------------------------------------------------
.PHONY: bundle-validate bundle-deploy bundle-destroy

bundle-validate: ## Validate Databricks asset bundle
	cd $(DB_DIR) && databricks bundle validate -t $(ENV)

bundle-deploy: ## Deploy Databricks asset bundle
	cd $(DB_DIR) && databricks bundle deploy -t $(ENV)

bundle-destroy: ## Destroy Databricks asset bundle resources
	cd $(DB_DIR) && databricks bundle destroy -t $(ENV)

# ---------------------------------------------------------------------------
# Pipeline Execution
# ---------------------------------------------------------------------------
.PHONY: run-pipeline run-bronze run-silver run-gold

run-pipeline: ## Trigger the full end-to-end pipeline (Bronze -> Silver -> Gold)
	cd $(DB_DIR) && databricks bundle run full_pipeline -t $(ENV)

run-bronze: ## Trigger Bronze ingestion jobs
	cd $(DB_DIR) && databricks bundle run bronze_orders_ingestion -t $(ENV) &
	cd $(DB_DIR) && databricks bundle run bronze_customers_ingestion -t $(ENV) &
	cd $(DB_DIR) && databricks bundle run bronze_products_ingestion -t $(ENV) &
	wait

run-silver: ## Trigger Silver pipeline
	cd $(DB_DIR) && databricks bundle run silver_pipeline -t $(ENV)

run-gold: ## Trigger Gold views pipeline
	cd $(DB_DIR) && databricks bundle run gold_views_pipeline -t $(ENV)

# ---------------------------------------------------------------------------
# Test Data
# ---------------------------------------------------------------------------
.PHONY: generate-data generate-data-sns clean-data

generate-data: ## Generate test data locally (set RECORDS=N, SEED=N)
	python $(SCRIPTS_DIR)/generate_test_data.py \
		--mode local \
		--output-dir $(TEST_DATA_DIR) \
		--records $(RECORDS) \
		--invalid-pct $(INVALID_PCT) \
		$(SEED_FLAG)

generate-data-sns: ## Publish test data to SNS topics (set RECORDS=N)
	python $(SCRIPTS_DIR)/generate_test_data.py \
		--mode sns \
		--records $(RECORDS) \
		--invalid-pct $(INVALID_PCT) \
		--sns-prefix "sns-firehose-pipeline-$(ENV)" \
		$(SEED_FLAG)

clean-data: ## Remove locally generated test data
	rm -rf $(TEST_DATA_DIR)

# ---------------------------------------------------------------------------
# Composite Targets
# ---------------------------------------------------------------------------
.PHONY: all deploy e2e clean

all: test infra-plan ## Run tests, then plan infrastructure (safe default)
	@echo "Tests passed and infra plan generated. Run 'make infra-apply' to apply."

deploy: infra-apply-auto bundle-deploy ## Apply infra and deploy Databricks bundle

e2e: deploy generate-data-sns run-pipeline ## Full end-to-end: deploy + generate data + run pipeline
	@echo "End-to-end pipeline triggered for ENV=$(ENV)."

clean: clean-data ## Clean all generated artifacts
	rm -f $(TF_DIR)/tfplan-*
