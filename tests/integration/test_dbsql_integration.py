"""
Integration tests for DBSQLAdmin.

Tests against real Databricks workspace: https://e2-demo-field-eng.cloud.databricks.com
"""

import pytest
import logging
from datetime import datetime, timezone
from admin_ai_bridge.dbsql import DBSQLAdmin
from admin_ai_bridge.config import AdminBridgeConfig
from admin_ai_bridge.schemas import QueryHistoryEntry

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def dbsql_admin():
    """Create DBSQLAdmin instance with real workspace client."""
    config = AdminBridgeConfig(profile="DEFAULT")
    admin = DBSQLAdmin(config)
    logger.info(f"Connected to workspace: {config.host}")
    return admin


@pytest.mark.integration
class TestDBSQLAdminIntegration:
    """Integration tests for DBSQLAdmin against real workspace."""

    def test_top_slowest_queries_real_workspace(self, dbsql_admin):
        """Test top_slowest_queries with real workspace data."""
        logger.info("Testing top_slowest_queries with real workspace")

        # Use permissive parameters to capture queries
        result = dbsql_admin.top_slowest_queries(
            lookback_hours=72,  # Last 3 days
            limit=20,
            min_duration_seconds=1  # 1 second minimum
        )

        logger.info(f"Found {len(result)} slow queries")

        # Validate result structure
        assert isinstance(result, list), "Result should be a list"

        # If we have results, validate structure
        if result:
            for query in result:
                assert isinstance(query, QueryHistoryEntry), "Each item should be QueryHistoryEntry"
                assert query.query_id is not None, "query_id should be present"
                assert query.duration_ms is not None, "duration_ms should be present"
                assert query.status is not None, "status should be present"
                assert query.user_name is not None, "user_name should be present"

                logger.info(
                    f"Query {query.query_id}: "
                    f"duration={query.duration_ms}ms, "
                    f"user={query.user_name}, "
                    f"status={query.status}, "
                    f"warehouse={query.warehouse_id or 'N/A'}"
                )

            # Verify queries are sorted by duration (descending)
            durations = [q.duration_ms for q in result]
            assert durations == sorted(durations, reverse=True), \
                "Queries should be sorted by duration in descending order"
        else:
            logger.warning("No slow queries found. This is OK if workspace has no recent query activity.")

    def test_user_query_summary_real_workspace(self, dbsql_admin):
        """Test user_query_summary with real workspace data."""
        logger.info("Testing user_query_summary with real workspace")

        result = dbsql_admin.user_query_summary(
            lookback_hours=72,
            limit=20
        )

        logger.info(f"Found query summaries for {len(result)} users")

        # Validate result structure
        assert isinstance(result, list), "Result should be a list"

        # If we have results, validate structure
        if result:
            for summary in result:
                assert "user_name" in summary, "user_name should be present"
                assert "query_count" in summary, "query_count should be present"
                assert "total_duration_ms" in summary, "total_duration_ms should be present"
                assert "avg_duration_ms" in summary, "avg_duration_ms should be present"

                logger.info(
                    f"User {summary['user_name']}: "
                    f"queries={summary['query_count']}, "
                    f"total_duration={summary['total_duration_ms']}ms, "
                    f"avg_duration={summary['avg_duration_ms']:.2f}ms"
                )

            # Verify summaries are sorted by query count (descending)
            query_counts = [s["query_count"] for s in result]
            assert query_counts == sorted(query_counts, reverse=True), \
                "Summaries should be sorted by query_count in descending order"
        else:
            logger.warning("No query summaries found. This is OK if workspace has no recent query activity.")

    def test_queries_with_various_parameters(self, dbsql_admin):
        """Test top_slowest_queries with various parameter combinations."""
        logger.info("Testing top_slowest_queries with various parameters")

        # Test with different limits
        result_limit_5 = dbsql_admin.top_slowest_queries(
            lookback_hours=48,
            limit=5,
            min_duration_seconds=0.5
        )
        assert len(result_limit_5) <= 5, "Result should respect limit parameter"
        logger.info(f"With limit=5: found {len(result_limit_5)} queries")

        # Test with different duration thresholds
        result_1s = dbsql_admin.top_slowest_queries(
            lookback_hours=48,
            limit=20,
            min_duration_seconds=1
        )
        logger.info(f"With min_duration=1s: found {len(result_1s)} queries")

        result_10s = dbsql_admin.top_slowest_queries(
            lookback_hours=48,
            limit=20,
            min_duration_seconds=10
        )
        logger.info(f"With min_duration=10s: found {len(result_10s)} queries")

        # Higher threshold should return fewer or equal queries
        assert len(result_10s) <= len(result_1s), \
            "Higher duration threshold should return <= queries"

    def test_dbsql_connection_timeout(self, dbsql_admin):
        """Test that DBSQL API calls complete within reasonable timeout."""
        logger.info("Testing DBSQL API timeout handling")

        import time
        start_time = time.time()

        try:
            result = dbsql_admin.top_slowest_queries(
                lookback_hours=24,
                limit=10,
                min_duration_seconds=1
            )
            elapsed = time.time() - start_time
            logger.info(f"API call completed in {elapsed:.2f} seconds")

            # Should complete within 30 seconds for reasonable workspace
            assert elapsed < 30, f"API call took too long: {elapsed:.2f}s"

        except Exception as e:
            logger.error(f"API call failed: {e}")
            raise

    def test_dbsql_error_handling(self, dbsql_admin):
        """Test error handling with invalid parameters."""
        logger.info("Testing error handling with invalid parameters")

        # Test with invalid duration (should handle gracefully)
        with pytest.raises(Exception):
            dbsql_admin.top_slowest_queries(
                lookback_hours=24,
                limit=10,
                min_duration_seconds=-1  # Invalid
            )

        # Test with invalid lookback (should handle gracefully)
        with pytest.raises(Exception):
            dbsql_admin.top_slowest_queries(
                lookback_hours=-1,  # Invalid
                limit=10,
                min_duration_seconds=1
            )

    def test_query_history_data_quality(self, dbsql_admin):
        """Test data quality of query history results."""
        logger.info("Testing query history data quality")

        result = dbsql_admin.top_slowest_queries(
            lookback_hours=48,
            limit=10,
            min_duration_seconds=1
        )

        if result:
            for query in result:
                # Validate duration is reasonable
                assert query.duration_ms > 0, "Duration should be positive"
                assert query.duration_ms < 86400000, "Duration should be less than 1 day (in ms)"

                # Validate query_id format
                assert len(query.query_id) > 0, "query_id should not be empty"

                # Validate user_name is present
                assert query.user_name, "user_name should not be empty"

                logger.debug(f"Query {query.query_id} data quality OK")
        else:
            logger.warning("No queries to validate data quality")
