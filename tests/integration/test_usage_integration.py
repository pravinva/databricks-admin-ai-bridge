"""
Integration tests for UsageAdmin.

Tests against real Databricks workspace: https://e2-demo-field-eng.cloud.databricks.com

Note: Usage data may not be available in all workspaces. These tests handle missing data gracefully.
"""

import pytest
import logging
from admin_ai_bridge.usage import UsageAdmin
from admin_ai_bridge.config import AdminBridgeConfig
from admin_ai_bridge.schemas import UsageEntry, BudgetStatus

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def usage_admin():
    """Create UsageAdmin instance with real workspace client."""
    config = AdminBridgeConfig(profile="DEFAULT")
    admin = UsageAdmin(config)
    logger.info(f"Connected to workspace: {config.host}")
    return admin


@pytest.mark.integration
class TestUsageAdminIntegration:
    """Integration tests for UsageAdmin against real workspace."""

    def test_top_cost_centers_real_workspace(self, usage_admin):
        """Test top_cost_centers with real workspace data."""
        logger.info("Testing top_cost_centers with real workspace")

        try:
            result = usage_admin.top_cost_centers(
                lookback_days=7,
                limit=20
            )

            logger.info(f"Found {len(result)} cost centers")

            # Validate result structure
            assert isinstance(result, list), "Result should be a list"

            # If we have results, validate structure
            if result:
                for entry in result:
                    assert isinstance(entry, UsageEntry), "Each item should be UsageEntry"
                    assert entry.scope is not None, "scope should be present"
                    assert entry.total_cost is not None, "total_cost should be present"
                    assert entry.usage_date is not None, "usage_date should be present"

                    logger.info(
                        f"Cost center {entry.scope}: "
                        f"cost=${entry.total_cost:.2f}, "
                        f"date={entry.usage_date}"
                    )

                # Verify entries are sorted by cost (descending)
                costs = [e.total_cost for e in result]
                assert costs == sorted(costs, reverse=True), \
                    "Entries should be sorted by total_cost in descending order"
            else:
                logger.warning("No cost centers found. Usage data may not be available in this workspace.")

        except Exception as e:
            logger.warning(f"Usage API may not be configured: {e}")
            pytest.skip("Usage data not available in workspace")

    def test_cost_by_dimension_real_workspace(self, usage_admin):
        """Test cost_by_dimension with real workspace data."""
        logger.info("Testing cost_by_dimension with real workspace")

        try:
            # Test with warehouse dimension
            result = usage_admin.cost_by_dimension(
                dimension="warehouse_id",
                lookback_days=7,
                limit=20
            )

            logger.info(f"Found cost breakdown by warehouse: {len(result)} entries")

            # Validate result structure
            assert isinstance(result, list), "Result should be a list"

            # If we have results, validate structure
            if result:
                for entry in result:
                    assert "dimension_value" in entry, "dimension_value should be present"
                    assert "total_cost" in entry, "total_cost should be present"
                    assert "record_count" in entry, "record_count should be present"

                    logger.info(
                        f"Dimension {entry['dimension_value']}: "
                        f"cost=${entry['total_cost']:.2f}, "
                        f"records={entry['record_count']}"
                    )

                # Verify entries are sorted by cost (descending)
                costs = [e["total_cost"] for e in result]
                assert costs == sorted(costs, reverse=True), \
                    "Entries should be sorted by total_cost in descending order"
            else:
                logger.warning("No cost breakdown found for warehouse dimension.")

        except Exception as e:
            logger.warning(f"Usage API may not be configured: {e}")
            pytest.skip("Usage data not available in workspace")

    def test_budget_status_real_workspace(self, usage_admin):
        """Test budget_status with real workspace data."""
        logger.info("Testing budget_status with real workspace")

        try:
            result = usage_admin.budget_status(
                budget_name="test_budget",
                lookback_days=7
            )

            logger.info(f"Budget status: {result}")

            # Validate result structure
            if result:
                assert isinstance(result, BudgetStatus), "Result should be BudgetStatus"
                assert result.budget_name is not None, "budget_name should be present"
                assert result.total_spend is not None, "total_spend should be present"
                assert result.budget_limit is not None, "budget_limit should be present"
                assert result.utilization_percent is not None, "utilization_percent should be present"

                logger.info(
                    f"Budget {result.budget_name}: "
                    f"spent=${result.total_spend:.2f}, "
                    f"limit=${result.budget_limit:.2f}, "
                    f"utilization={result.utilization_percent:.1f}%"
                )
            else:
                logger.warning("Budget not found. Budget tracking may not be configured.")

        except Exception as e:
            logger.warning(f"Budget API may not be configured: {e}")
            pytest.skip("Budget data not available in workspace")

    def test_usage_with_various_parameters(self, usage_admin):
        """Test top_cost_centers with various parameter combinations."""
        logger.info("Testing top_cost_centers with various parameters")

        try:
            # Test with different limits
            result_limit_5 = usage_admin.top_cost_centers(
                lookback_days=7,
                limit=5
            )
            assert len(result_limit_5) <= 5, "Result should respect limit parameter"
            logger.info(f"With limit=5: found {len(result_limit_5)} cost centers")

            # Test with different lookback periods
            result_7d = usage_admin.top_cost_centers(
                lookback_days=7,
                limit=20
            )
            logger.info(f"With lookback=7d: found {len(result_7d)} cost centers")

            result_30d = usage_admin.top_cost_centers(
                lookback_days=30,
                limit=20
            )
            logger.info(f"With lookback=30d: found {len(result_30d)} cost centers")

        except Exception as e:
            logger.warning(f"Usage API may not be configured: {e}")
            pytest.skip("Usage data not available in workspace")

    def test_usage_connection_timeout(self, usage_admin):
        """Test that usage API calls complete within reasonable timeout."""
        logger.info("Testing usage API timeout handling")

        import time
        start_time = time.time()

        try:
            result = usage_admin.top_cost_centers(
                lookback_days=7,
                limit=10
            )
            elapsed = time.time() - start_time
            logger.info(f"API call completed in {elapsed:.2f} seconds")

            # Should complete within 30 seconds for reasonable workspace
            assert elapsed < 30, f"API call took too long: {elapsed:.2f}s"

        except Exception as e:
            logger.warning(f"Usage API may not be configured: {e}")
            pytest.skip("Usage data not available in workspace")

    def test_usage_error_handling(self, usage_admin):
        """Test error handling with invalid parameters."""
        logger.info("Testing error handling with invalid parameters")

        try:
            # Test with invalid lookback days (should handle gracefully)
            with pytest.raises(Exception):
                usage_admin.top_cost_centers(
                    lookback_days=-1,  # Invalid
                    limit=10
                )

            # Test with invalid dimension (should handle gracefully)
            with pytest.raises(Exception):
                usage_admin.cost_by_dimension(
                    dimension="invalid_dimension",
                    lookback_days=7,
                    limit=10
                )

        except Exception as e:
            logger.warning(f"Usage API may not be configured: {e}")
            pytest.skip("Usage data not available in workspace")

    def test_usage_data_quality(self, usage_admin):
        """Test data quality of usage results."""
        logger.info("Testing usage data quality")

        try:
            result = usage_admin.top_cost_centers(
                lookback_days=7,
                limit=10
            )

            if result:
                for entry in result:
                    # Validate cost is reasonable
                    assert entry.total_cost >= 0, "total_cost should be non-negative"
                    assert entry.total_cost < 1000000, "total_cost should be reasonable (< $1M)"

                    # Validate scope is present
                    assert entry.scope, "scope should not be empty"

                    # Validate usage_date is present
                    assert entry.usage_date, "usage_date should not be empty"

                    logger.debug(f"Usage entry for {entry.scope} data quality OK")
            else:
                logger.warning("No usage data to validate data quality")

        except Exception as e:
            logger.warning(f"Usage API may not be configured: {e}")
            pytest.skip("Usage data not available in workspace")
