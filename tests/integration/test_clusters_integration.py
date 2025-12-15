"""
Integration tests for ClustersAdmin.

Tests against real Databricks workspace: https://e2-demo-field-eng.cloud.databricks.com
"""

import pytest
import logging
from datetime import datetime, timezone
from admin_ai_bridge.clusters import ClustersAdmin
from admin_ai_bridge.config import AdminBridgeConfig
from admin_ai_bridge.schemas import ClusterSummary

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def clusters_admin():
    """Create ClustersAdmin instance with real workspace client."""
    config = AdminBridgeConfig(profile="DEFAULT")
    admin = ClustersAdmin(config)
    logger.info(f"Connected to workspace: {config.host}")
    return admin


@pytest.mark.integration
class TestClustersAdminIntegration:
    """Integration tests for ClustersAdmin against real workspace."""

    def test_list_long_running_clusters_real_workspace(self, clusters_admin):
        """Test list_long_running_clusters with real workspace data."""
        logger.info("Testing list_long_running_clusters with real workspace")

        # Use permissive parameters to capture any long-running clusters
        result = clusters_admin.list_long_running_clusters(
            min_duration_hours=0.1,  # 6 minutes
            lookback_hours=48,
            limit=50
        )

        logger.info(f"Found {len(result)} long-running clusters")

        # Validate result structure
        assert isinstance(result, list), "Result should be a list"

        # If we have results, validate structure
        if result:
            for cluster in result:
                assert isinstance(cluster, ClusterSummary), "Each item should be ClusterSummary"
                assert cluster.cluster_id is not None, "cluster_id should be present"
                assert cluster.state is not None, "state should be present"
                assert cluster.uptime_hours is not None, "uptime_hours should be present"
                assert cluster.is_long_running is True, "is_long_running should be True"

                logger.info(
                    f"Cluster {cluster.cluster_name or cluster.cluster_id}: "
                    f"state={cluster.state}, "
                    f"uptime={cluster.uptime_hours:.2f}h, "
                    f"creator={cluster.creator_user_name or 'N/A'}"
                )
        else:
            logger.warning("No long-running clusters found. This is OK if workspace has no qualifying clusters.")

    def test_list_idle_clusters_real_workspace(self, clusters_admin):
        """Test list_idle_clusters with real workspace data."""
        logger.info("Testing list_idle_clusters with real workspace")

        result = clusters_admin.list_idle_clusters(
            idle_hours=1,  # Idle for at least 1 hour
            limit=50
        )

        logger.info(f"Found {len(result)} idle clusters")

        # Validate result structure
        assert isinstance(result, list), "Result should be a list"

        # If we have results, validate structure
        if result:
            for cluster in result:
                assert isinstance(cluster, ClusterSummary), "Each item should be ClusterSummary"
                assert cluster.cluster_id is not None, "cluster_id should be present"
                assert cluster.state is not None, "state should be present"
                assert cluster.idle_hours is not None, "idle_hours should be present"

                logger.info(
                    f"Idle cluster {cluster.cluster_name or cluster.cluster_id}: "
                    f"state={cluster.state}, "
                    f"idle_hours={cluster.idle_hours:.2f}h"
                )
        else:
            logger.warning("No idle clusters found. This is OK if workspace has no idle clusters.")

    def test_list_all_clusters_real_workspace(self, clusters_admin):
        """Test list_all_clusters with real workspace data."""
        logger.info("Testing list_all_clusters with real workspace")

        result = clusters_admin.list_all_clusters(limit=100)

        logger.info(f"Found {len(result)} total clusters")

        # Validate result structure
        assert isinstance(result, list), "Result should be a list"

        # If we have results, validate structure
        if result:
            for cluster in result:
                assert isinstance(cluster, ClusterSummary), "Each item should be ClusterSummary"
                assert cluster.cluster_id is not None, "cluster_id should be present"
                assert cluster.state is not None, "state should be present"

                logger.info(
                    f"Cluster {cluster.cluster_name or cluster.cluster_id}: "
                    f"id={cluster.cluster_id}, "
                    f"state={cluster.state}, "
                    f"creator={cluster.creator_user_name or 'N/A'}"
                )

            # Verify limit is respected
            assert len(result) <= 100, "Result should respect limit parameter"
        else:
            logger.warning("No clusters found in workspace.")

    def test_clusters_with_various_parameters(self, clusters_admin):
        """Test list_long_running_clusters with various parameter combinations."""
        logger.info("Testing list_long_running_clusters with various parameters")

        # Test with different limits
        result_limit_5 = clusters_admin.list_long_running_clusters(
            min_duration_hours=0.05,  # 3 minutes
            lookback_hours=24,
            limit=5
        )
        assert len(result_limit_5) <= 5, "Result should respect limit parameter"
        logger.info(f"With limit=5: found {len(result_limit_5)} clusters")

        # Test with different lookback periods
        result_12h = clusters_admin.list_long_running_clusters(
            min_duration_hours=0.1,
            lookback_hours=12,
            limit=20
        )
        logger.info(f"With lookback=12h: found {len(result_12h)} clusters")

        result_48h = clusters_admin.list_long_running_clusters(
            min_duration_hours=0.1,
            lookback_hours=48,
            limit=20
        )
        logger.info(f"With lookback=48h: found {len(result_48h)} clusters")

        # 48h should have >= 12h results (or both can be 0)
        assert len(result_48h) >= len(result_12h), \
            "Longer lookback should find >= clusters than shorter lookback"

    def test_clusters_connection_timeout(self, clusters_admin):
        """Test that clusters API calls complete within reasonable timeout."""
        logger.info("Testing clusters API timeout handling")

        import time
        start_time = time.time()

        try:
            result = clusters_admin.list_all_clusters(limit=50)
            elapsed = time.time() - start_time
            logger.info(f"API call completed in {elapsed:.2f} seconds")

            # Should complete within 30 seconds for reasonable workspace
            assert elapsed < 30, f"API call took too long: {elapsed:.2f}s"

        except Exception as e:
            logger.error(f"API call failed: {e}")
            raise

    def test_clusters_error_handling(self, clusters_admin):
        """Test error handling with invalid parameters."""
        logger.info("Testing error handling with invalid parameters")

        # Test with invalid duration (should handle gracefully)
        with pytest.raises(Exception):
            clusters_admin.list_long_running_clusters(
                min_duration_hours=-1,  # Invalid
                lookback_hours=24,
                limit=10
            )

        # Test with invalid idle hours (should handle gracefully)
        with pytest.raises(Exception):
            clusters_admin.list_idle_clusters(
                idle_hours=-1,  # Invalid
                limit=10
            )

    def test_cluster_data_quality(self, clusters_admin):
        """Test data quality of cluster results."""
        logger.info("Testing cluster data quality")

        result = clusters_admin.list_all_clusters(limit=20)

        if result:
            for cluster in result:
                # Validate cluster_id format
                assert cluster.cluster_id, "cluster_id should not be empty"

                # Validate state is valid
                valid_states = ["PENDING", "RUNNING", "RESTARTING", "RESIZING",
                               "TERMINATING", "TERMINATED", "ERROR", "UNKNOWN"]
                assert cluster.state in valid_states, \
                    f"Invalid cluster state: {cluster.state}"

                # If uptime is present, validate it's reasonable
                if cluster.uptime_hours is not None:
                    assert cluster.uptime_hours >= 0, "uptime_hours should be non-negative"
                    assert cluster.uptime_hours < 8760, "uptime_hours should be less than 1 year"

                logger.debug(f"Cluster {cluster.cluster_id} data quality OK")
        else:
            logger.warning("No clusters to validate data quality")
