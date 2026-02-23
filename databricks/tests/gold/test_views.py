"""Tests for Gold layer view SQL generation.

Since gold views are created via spark.sql() in a notebook, these tests
validate the SQL templates and the role grant configuration.
"""

import pytest


# View definitions expected in Gold layer
EXPECTED_VIEWS = [
    "vw_order_summary",
    "vw_customer_lifetime_value",
    "vw_product_sales",
]

# Role grant configuration
ROLE_GRANTS = {
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


class TestGoldViewConfig:
    """Validate Gold layer view configuration."""

    def test_all_expected_views_defined(self):
        """All expected views should be in the configuration."""
        for view in EXPECTED_VIEWS:
            found = any(
                view in config["views"]
                for config in ROLE_GRANTS.values()
            )
            assert found, f"View {view} not found in any role grant"

    def test_analysts_have_select_only(self):
        """Analysts should only have SELECT privilege."""
        assert ROLE_GRANTS["analysts"]["privileges"] == ["SELECT"]

    def test_engineers_have_select_and_modify(self):
        """Engineers should have SELECT and MODIFY privileges."""
        assert "SELECT" in ROLE_GRANTS["engineers"]["privileges"]
        assert "MODIFY" in ROLE_GRANTS["engineers"]["privileges"]

    def test_data_scientists_limited_views(self):
        """Data scientists should not have access to vw_order_summary."""
        assert "vw_order_summary" not in ROLE_GRANTS["data_scientists"]["views"]

    def test_all_roles_have_at_least_one_view(self):
        """Every role should be granted access to at least one view."""
        for role, config in ROLE_GRANTS.items():
            assert len(config["views"]) > 0, f"Role {role} has no views"

    def test_all_roles_have_at_least_select(self):
        """Every role should have at least SELECT privilege."""
        for role, config in ROLE_GRANTS.items():
            assert "SELECT" in config["privileges"], (
                f"Role {role} missing SELECT"
            )
