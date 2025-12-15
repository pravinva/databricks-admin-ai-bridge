"""
Integration tests for JobsAdmin.

Tests against real Databricks workspace: https://e2-demo-field-eng.cloud.databricks.com
"""

import pytest
import logging
from datetime import datetime, timezone, timedelta
from admin_ai_bridge.jobs import JobsAdmin
from admin_ai_bridge.config import AdminBridgeConfig
from admin_ai_bridge.schemas import JobRunSummary

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def jobs_admin():
    """Create JobsAdmin instance with real workspace client."""
    config = AdminBridgeConfig(profile="DEFAULT")
    admin = JobsAdmin(config)
    logger.info(f"Connected to workspace: {config.host}")
    return admin


@pytest.mark.integration
class TestJobsAdminIntegration:
    """Integration tests for JobsAdmin against real workspace."""

    def test_list_long_running_jobs_real_workspace(self, jobs_admin):
        """Test list_long_running_jobs with real workspace data."""
        logger.info("Testing list_long_running_jobs with real workspace")

        # Use permissive parameters to capture any long-running jobs
        result = jobs_admin.list_long_running_jobs(
            min_duration_hours=0.1,  # 6 minutes
            lookback_hours=48,
            limit=50
        )

        logger.info(f"Found {len(result)} long-running jobs")

        # Validate result structure
        assert isinstance(result, list), "Result should be a list"

        # If we have results, validate structure
        if result:
            for job_run in result:
                assert isinstance(job_run, JobRunSummary), "Each item should be JobRunSummary"
                assert job_run.run_id is not None, "run_id should be present"
                assert job_run.job_id is not None, "job_id should be present"
                assert job_run.state is not None, "state should be present"
                assert job_run.start_time is not None, "start_time should be present"
                assert job_run.duration_seconds is not None, "duration_seconds should be present"
                assert job_run.is_long_running is True, "is_long_running should be True"

                logger.info(
                    f"Job {job_run.job_name or job_run.job_id}: "
                    f"run_id={job_run.run_id}, "
                    f"duration={job_run.duration_seconds}s, "
                    f"state={job_run.state}"
                )
        else:
            logger.warning("No long-running jobs found. This is OK if workspace has no qualifying jobs.")

    def test_list_failed_jobs_real_workspace(self, jobs_admin):
        """Test list_failed_jobs with real workspace data."""
        logger.info("Testing list_failed_jobs with real workspace")

        result = jobs_admin.list_failed_jobs(
            lookback_hours=72,  # Last 3 days
            limit=50
        )

        logger.info(f"Found {len(result)} failed jobs")

        # Validate result structure
        assert isinstance(result, list), "Result should be a list"

        # If we have results, validate structure
        if result:
            for job_run in result:
                assert isinstance(job_run, JobRunSummary), "Each item should be JobRunSummary"
                assert job_run.run_id is not None, "run_id should be present"
                assert job_run.job_id is not None, "job_id should be present"
                assert job_run.state is not None, "state should be present"
                assert job_run.error_message is not None, "error_message should be present for failed jobs"

                logger.info(
                    f"Failed job {job_run.job_name or job_run.job_id}: "
                    f"run_id={job_run.run_id}, "
                    f"state={job_run.state}, "
                    f"error={job_run.error_message[:100] if job_run.error_message else 'N/A'}"
                )
        else:
            logger.warning("No failed jobs found. This is OK if workspace has no recent failures.")

    def test_list_jobs_with_various_parameters(self, jobs_admin):
        """Test list_long_running_jobs with various parameter combinations."""
        logger.info("Testing list_long_running_jobs with various parameters")

        # Test with different limits
        result_limit_5 = jobs_admin.list_long_running_jobs(
            min_duration_hours=0.05,  # 3 minutes
            lookback_hours=24,
            limit=5
        )
        assert len(result_limit_5) <= 5, "Result should respect limit parameter"
        logger.info(f"With limit=5: found {len(result_limit_5)} jobs")

        # Test with different lookback periods
        result_12h = jobs_admin.list_long_running_jobs(
            min_duration_hours=0.1,
            lookback_hours=12,
            limit=20
        )
        logger.info(f"With lookback=12h: found {len(result_12h)} jobs")

        result_48h = jobs_admin.list_long_running_jobs(
            min_duration_hours=0.1,
            lookback_hours=48,
            limit=20
        )
        logger.info(f"With lookback=48h: found {len(result_48h)} jobs")

        # 48h should have >= 12h results (or both can be 0)
        assert len(result_48h) >= len(result_12h), \
            "Longer lookback should find >= jobs than shorter lookback"

    def test_jobs_connection_timeout(self, jobs_admin):
        """Test that jobs API calls complete within reasonable timeout."""
        logger.info("Testing jobs API timeout handling")

        import time
        start_time = time.time()

        try:
            result = jobs_admin.list_long_running_jobs(
                min_duration_hours=1,
                lookback_hours=24,
                limit=10
            )
            elapsed = time.time() - start_time
            logger.info(f"API call completed in {elapsed:.2f} seconds")

            # Should complete within 30 seconds for reasonable workspace
            assert elapsed < 30, f"API call took too long: {elapsed:.2f}s"

        except Exception as e:
            logger.error(f"API call failed: {e}")
            raise

    def test_jobs_error_handling(self, jobs_admin):
        """Test error handling with invalid parameters."""
        logger.info("Testing error handling with invalid parameters")

        # Test with invalid duration (should handle gracefully)
        with pytest.raises(Exception):
            jobs_admin.list_long_running_jobs(
                min_duration_hours=-1,  # Invalid
                lookback_hours=24,
                limit=10
            )

        # Test with invalid lookback (should handle gracefully)
        with pytest.raises(Exception):
            jobs_admin.list_long_running_jobs(
                min_duration_hours=1,
                lookback_hours=-1,  # Invalid
                limit=10
            )
