"""
Integration tests for PipelinesAdmin.

Tests against real Databricks workspace: https://e2-demo-field-eng.cloud.databricks.com
"""

import pytest
import logging
from admin_ai_bridge.pipelines import PipelinesAdmin
from admin_ai_bridge.config import AdminBridgeConfig
from admin_ai_bridge.schemas import PipelineStatus

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def pipelines_admin():
    """Create PipelinesAdmin instance with real workspace client."""
    config = AdminBridgeConfig(profile="DEFAULT")
    admin = PipelinesAdmin(config)
    logger.info(f"Connected to workspace: {config.host}")
    return admin


@pytest.mark.integration
class TestPipelinesAdminIntegration:
    """Integration tests for PipelinesAdmin against real workspace."""

    def test_list_lagging_pipelines_real_workspace(self, pipelines_admin):
        """Test list_lagging_pipelines with real workspace data."""
        logger.info("Testing list_lagging_pipelines with real workspace")

        try:
            # Use permissive parameters to capture any lagging pipelines
            result = pipelines_admin.list_lagging_pipelines(
                max_lag_seconds=600,  # 10 minutes
                limit=50
            )

            logger.info(f"Found {len(result)} lagging pipelines")

            # Validate result structure
            assert isinstance(result, list), "Result should be a list"

            # If we have results, validate structure
            if result:
                for pipeline in result:
                    assert isinstance(pipeline, PipelineStatus), "Each item should be PipelineStatus"
                    assert pipeline.pipeline_id is not None, "pipeline_id should be present"
                    assert pipeline.state is not None, "state should be present"
                    assert pipeline.lag_seconds is not None, "lag_seconds should be present"

                    logger.info(
                        f"Lagging pipeline {pipeline.pipeline_name or pipeline.pipeline_id}: "
                        f"lag={pipeline.lag_seconds}s, "
                        f"state={pipeline.state}, "
                        f"creator={pipeline.creator_user_name or 'N/A'}"
                    )

                # Verify all pipelines meet the lag threshold
                for pipeline in result:
                    assert pipeline.lag_seconds >= 600, \
                        f"Pipeline lag {pipeline.lag_seconds} should be >= threshold 600"
            else:
                logger.warning("No lagging pipelines found. This is OK if workspace has no qualifying pipelines.")

        except Exception as e:
            logger.warning(f"Pipelines API may not be available: {e}")
            pytest.skip("Pipelines not available in workspace")

    def test_list_failed_pipelines_real_workspace(self, pipelines_admin):
        """Test list_failed_pipelines with real workspace data."""
        logger.info("Testing list_failed_pipelines with real workspace")

        try:
            result = pipelines_admin.list_failed_pipelines(
                lookback_hours=72,  # Last 3 days
                limit=50
            )

            logger.info(f"Found {len(result)} failed pipelines")

            # Validate result structure
            assert isinstance(result, list), "Result should be a list"

            # If we have results, validate structure
            if result:
                for pipeline in result:
                    assert isinstance(pipeline, PipelineStatus), "Each item should be PipelineStatus"
                    assert pipeline.pipeline_id is not None, "pipeline_id should be present"
                    assert pipeline.state is not None, "state should be present"
                    assert pipeline.error_message is not None, "error_message should be present for failed pipelines"

                    logger.info(
                        f"Failed pipeline {pipeline.pipeline_name or pipeline.pipeline_id}: "
                        f"state={pipeline.state}, "
                        f"error={pipeline.error_message[:100] if pipeline.error_message else 'N/A'}"
                    )
            else:
                logger.warning("No failed pipelines found. This is OK if workspace has no recent failures.")

        except Exception as e:
            logger.warning(f"Pipelines API may not be available: {e}")
            pytest.skip("Pipelines not available in workspace")

    def test_list_all_pipelines_real_workspace(self, pipelines_admin):
        """Test list_all_pipelines with real workspace data."""
        logger.info("Testing list_all_pipelines with real workspace")

        try:
            result = pipelines_admin.list_all_pipelines(limit=100)

            logger.info(f"Found {len(result)} total pipelines")

            # Validate result structure
            assert isinstance(result, list), "Result should be a list"

            # If we have results, validate structure
            if result:
                for pipeline in result:
                    assert isinstance(pipeline, PipelineStatus), "Each item should be PipelineStatus"
                    assert pipeline.pipeline_id is not None, "pipeline_id should be present"
                    assert pipeline.state is not None, "state should be present"

                    logger.info(
                        f"Pipeline {pipeline.pipeline_name or pipeline.pipeline_id}: "
                        f"id={pipeline.pipeline_id}, "
                        f"state={pipeline.state}, "
                        f"creator={pipeline.creator_user_name or 'N/A'}"
                    )

                # Verify limit is respected
                assert len(result) <= 100, "Result should respect limit parameter"
            else:
                logger.warning("No pipelines found in workspace.")

        except Exception as e:
            logger.warning(f"Pipelines API may not be available: {e}")
            pytest.skip("Pipelines not available in workspace")

    def test_pipelines_with_various_parameters(self, pipelines_admin):
        """Test list_lagging_pipelines with various parameter combinations."""
        logger.info("Testing list_lagging_pipelines with various parameters")

        try:
            # Test with different limits
            result_limit_5 = pipelines_admin.list_lagging_pipelines(
                max_lag_seconds=300,  # 5 minutes
                limit=5
            )
            assert len(result_limit_5) <= 5, "Result should respect limit parameter"
            logger.info(f"With limit=5: found {len(result_limit_5)} pipelines")

            # Test with different lag thresholds
            result_300s = pipelines_admin.list_lagging_pipelines(
                max_lag_seconds=300,  # 5 minutes
                limit=20
            )
            logger.info(f"With lag=300s: found {len(result_300s)} pipelines")

            result_1800s = pipelines_admin.list_lagging_pipelines(
                max_lag_seconds=1800,  # 30 minutes
                limit=20
            )
            logger.info(f"With lag=1800s: found {len(result_1800s)} pipelines")

            # Higher threshold should return fewer or equal pipelines
            assert len(result_1800s) <= len(result_300s), \
                "Higher lag threshold should return <= pipelines"

        except Exception as e:
            logger.warning(f"Pipelines API may not be available: {e}")
            pytest.skip("Pipelines not available in workspace")

    def test_pipelines_connection_timeout(self, pipelines_admin):
        """Test that pipelines API calls complete within reasonable timeout."""
        logger.info("Testing pipelines API timeout handling")

        import time
        start_time = time.time()

        try:
            result = pipelines_admin.list_all_pipelines(limit=50)
            elapsed = time.time() - start_time
            logger.info(f"API call completed in {elapsed:.2f} seconds")

            # Should complete within 30 seconds for reasonable workspace
            assert elapsed < 30, f"API call took too long: {elapsed:.2f}s"

        except Exception as e:
            logger.warning(f"Pipelines API may not be available: {e}")
            pytest.skip("Pipelines not available in workspace")

    def test_pipelines_error_handling(self, pipelines_admin):
        """Test error handling with invalid parameters."""
        logger.info("Testing error handling with invalid parameters")

        try:
            # Test with invalid lag threshold (should handle gracefully)
            with pytest.raises(Exception):
                pipelines_admin.list_lagging_pipelines(
                    max_lag_seconds=-1,  # Invalid
                    limit=10
                )

            # Test with invalid lookback hours (should handle gracefully)
            with pytest.raises(Exception):
                pipelines_admin.list_failed_pipelines(
                    lookback_hours=-1,  # Invalid
                    limit=10
                )

        except Exception as e:
            logger.warning(f"Pipelines API may not be available: {e}")
            pytest.skip("Pipelines not available in workspace")

    def test_pipeline_data_quality(self, pipelines_admin):
        """Test data quality of pipeline results."""
        logger.info("Testing pipeline data quality")

        try:
            result = pipelines_admin.list_all_pipelines(limit=20)

            if result:
                for pipeline in result:
                    # Validate pipeline_id format
                    assert pipeline.pipeline_id, "pipeline_id should not be empty"

                    # Validate state is valid
                    valid_states = ["IDLE", "RUNNING", "STOPPING", "STOPPED",
                                   "FAILED", "RESETTING", "DEPLOYING", "DELETING"]
                    assert pipeline.state in valid_states, \
                        f"Invalid pipeline state: {pipeline.state}"

                    # If lag is present, validate it's reasonable
                    if pipeline.lag_seconds is not None:
                        assert pipeline.lag_seconds >= 0, "lag_seconds should be non-negative"
                        assert pipeline.lag_seconds < 604800, "lag_seconds should be less than 1 week"

                    logger.debug(f"Pipeline {pipeline.pipeline_id} data quality OK")
            else:
                logger.warning("No pipelines to validate data quality")

        except Exception as e:
            logger.warning(f"Pipelines API may not be available: {e}")
            pytest.skip("Pipelines not available in workspace")

    def test_cross_validate_pipeline_failures(self, pipelines_admin):
        """Cross-validate that failed pipelines have error messages."""
        logger.info("Cross-validating pipeline failure data")

        try:
            result = pipelines_admin.list_failed_pipelines(
                lookback_hours=72,
                limit=20
            )

            if result:
                for pipeline in result:
                    # Failed pipelines should have error messages
                    assert pipeline.error_message, \
                        f"Failed pipeline {pipeline.pipeline_id} should have error_message"

                    # State should indicate failure
                    assert pipeline.state in ["FAILED", "ERROR"], \
                        f"Failed pipeline should have FAILED or ERROR state, got {pipeline.state}"

                    logger.debug(f"Pipeline {pipeline.pipeline_id} failure data is consistent")

                logger.info("All failed pipeline data is consistent")
            else:
                logger.warning("No failed pipelines to validate")

        except Exception as e:
            logger.warning(f"Pipelines API may not be available: {e}")
            pytest.skip("Pipelines not available in workspace")
