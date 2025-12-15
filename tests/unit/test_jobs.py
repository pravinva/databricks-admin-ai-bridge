"""
Unit tests for jobs module.
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, MagicMock, patch
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

from admin_ai_bridge.jobs import JobsAdmin
from admin_ai_bridge.config import AdminBridgeConfig
from admin_ai_bridge.errors import ValidationError, APIError
from admin_ai_bridge.schemas import JobRunSummary


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient."""
    mock_client = Mock()
    mock_client.jobs = Mock()
    return mock_client


@pytest.fixture
def jobs_admin(mock_workspace_client):
    """Create JobsAdmin instance with mocked client."""
    with patch('admin_ai_bridge.jobs.get_workspace_client', return_value=mock_workspace_client):
        admin = JobsAdmin(AdminBridgeConfig(profile="TEST"))
    return admin


class TestJobsAdminInit:
    """Test JobsAdmin initialization."""

    def test_init_with_config(self):
        """Test initialization with config."""
        with patch('admin_ai_bridge.jobs.get_workspace_client') as mock_get_client:
            cfg = AdminBridgeConfig(profile="TEST")
            admin = JobsAdmin(cfg)
            mock_get_client.assert_called_once_with(cfg)
            assert admin.ws is not None

    def test_init_without_config(self):
        """Test initialization without config."""
        with patch('admin_ai_bridge.jobs.get_workspace_client') as mock_get_client:
            admin = JobsAdmin()
            mock_get_client.assert_called_once_with(None)
            assert admin.ws is not None


class TestListLongRunningJobs:
    """Test list_long_running_jobs method."""

    def test_validation_negative_min_duration(self, jobs_admin):
        """Test validation fails with negative min_duration_hours."""
        with pytest.raises(ValidationError, match="min_duration_hours must be positive"):
            jobs_admin.list_long_running_jobs(min_duration_hours=-1.0)

    def test_validation_negative_lookback(self, jobs_admin):
        """Test validation fails with negative lookback_hours."""
        with pytest.raises(ValidationError, match="lookback_hours must be positive"):
            jobs_admin.list_long_running_jobs(lookback_hours=-1.0)

    def test_validation_negative_limit(self, jobs_admin):
        """Test validation fails with negative limit."""
        with pytest.raises(ValidationError, match="limit must be positive"):
            jobs_admin.list_long_running_jobs(limit=-1)

    def test_no_jobs(self, jobs_admin):
        """Test with no jobs in workspace."""
        jobs_admin.ws.jobs.list.return_value = []

        result = jobs_admin.list_long_running_jobs()

        assert result == []
        jobs_admin.ws.jobs.list.assert_called_once()

    def test_long_running_job_found(self, jobs_admin):
        """Test finding a long-running job."""
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=6)

        # Create mock job
        mock_job = Mock()
        mock_job.job_id = 123
        mock_job.settings = Mock()
        mock_job.settings.name = "Long Running Job"

        # Create mock run
        mock_run = Mock()
        mock_run.run_id = 456
        mock_run.start_time = int(start_time.timestamp() * 1000)
        mock_run.end_time = int(now.timestamp() * 1000)
        mock_run.state = Mock()
        mock_run.state.result_state = RunResultState.SUCCESS
        mock_run.state.life_cycle_state = RunLifeCycleState.TERMINATED

        jobs_admin.ws.jobs.list.return_value = [mock_job]
        jobs_admin.ws.jobs.list_runs.return_value = [mock_run]

        result = jobs_admin.list_long_running_jobs(min_duration_hours=4.0, lookback_hours=24.0)

        assert len(result) == 1
        assert result[0].job_id == 123
        assert result[0].job_name == "Long Running Job"
        assert result[0].run_id == 456
        assert result[0].state == "SUCCESS"
        assert result[0].duration_seconds >= 4 * 3600

    def test_running_job_with_no_end_time(self, jobs_admin):
        """Test finding a currently running job."""
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=6)

        # Create mock job
        mock_job = Mock()
        mock_job.job_id = 123
        mock_job.settings = Mock()
        mock_job.settings.name = "Currently Running Job"

        # Create mock run (still running, no end time)
        mock_run = Mock()
        mock_run.run_id = 456
        mock_run.start_time = int(start_time.timestamp() * 1000)
        mock_run.end_time = None
        mock_run.state = Mock()
        mock_run.state.result_state = None
        mock_run.state.life_cycle_state = RunLifeCycleState.RUNNING

        jobs_admin.ws.jobs.list.return_value = [mock_job]
        jobs_admin.ws.jobs.list_runs.return_value = [mock_run]

        result = jobs_admin.list_long_running_jobs(min_duration_hours=4.0, lookback_hours=24.0)

        assert len(result) == 1
        assert result[0].job_id == 123
        assert result[0].state == "RUNNING"
        assert result[0].duration_seconds >= 4 * 3600

    def test_short_duration_job_filtered(self, jobs_admin):
        """Test that short jobs are filtered out."""
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=2)

        # Create mock job
        mock_job = Mock()
        mock_job.job_id = 123
        mock_job.settings = Mock()
        mock_job.settings.name = "Short Job"

        # Create mock run (only 2 hours, below 4 hour threshold)
        mock_run = Mock()
        mock_run.run_id = 456
        mock_run.start_time = int(start_time.timestamp() * 1000)
        mock_run.end_time = int(now.timestamp() * 1000)
        mock_run.state = Mock()
        mock_run.state.result_state = RunResultState.SUCCESS
        mock_run.state.life_cycle_state = RunLifeCycleState.TERMINATED

        jobs_admin.ws.jobs.list.return_value = [mock_job]
        jobs_admin.ws.jobs.list_runs.return_value = [mock_run]

        result = jobs_admin.list_long_running_jobs(min_duration_hours=4.0, lookback_hours=24.0)

        assert len(result) == 0

    def test_sorting_by_duration(self, jobs_admin):
        """Test that results are sorted by duration (longest first)."""
        now = datetime.now(timezone.utc)

        # Create mock jobs with different durations
        mock_job1 = Mock()
        mock_job1.job_id = 1
        mock_job1.settings = Mock()
        mock_job1.settings.name = "Job 1 (8h)"

        mock_job2 = Mock()
        mock_job2.job_id = 2
        mock_job2.settings = Mock()
        mock_job2.settings.name = "Job 2 (12h)"

        # Mock run 1: 8 hours
        mock_run1 = Mock()
        mock_run1.run_id = 101
        mock_run1.start_time = int((now - timedelta(hours=8)).timestamp() * 1000)
        mock_run1.end_time = int(now.timestamp() * 1000)
        mock_run1.state = Mock()
        mock_run1.state.result_state = RunResultState.SUCCESS
        mock_run1.state.life_cycle_state = RunLifeCycleState.TERMINATED

        # Mock run 2: 12 hours
        mock_run2 = Mock()
        mock_run2.run_id = 102
        mock_run2.start_time = int((now - timedelta(hours=12)).timestamp() * 1000)
        mock_run2.end_time = int(now.timestamp() * 1000)
        mock_run2.state = Mock()
        mock_run2.state.result_state = RunResultState.SUCCESS
        mock_run2.state.life_cycle_state = RunLifeCycleState.TERMINATED

        jobs_admin.ws.jobs.list.return_value = [mock_job1, mock_job2]

        def mock_list_runs(job_id, **kwargs):
            if job_id == 1:
                return [mock_run1]
            elif job_id == 2:
                return [mock_run2]
            return []

        jobs_admin.ws.jobs.list_runs.side_effect = mock_list_runs

        result = jobs_admin.list_long_running_jobs(min_duration_hours=4.0, lookback_hours=24.0)

        assert len(result) == 2
        # Should be sorted longest first
        assert result[0].job_name == "Job 2 (12h)"
        assert result[1].job_name == "Job 1 (8h)"

    def test_limit_enforced(self, jobs_admin):
        """Test that limit parameter is enforced."""
        now = datetime.now(timezone.utc)

        # Create 5 long-running jobs
        mock_jobs = []
        for i in range(5):
            mock_job = Mock()
            mock_job.job_id = i
            mock_job.settings = Mock()
            mock_job.settings.name = f"Job {i}"
            mock_jobs.append(mock_job)

        def mock_list_runs(job_id, **kwargs):
            mock_run = Mock()
            mock_run.run_id = job_id * 100
            mock_run.start_time = int((now - timedelta(hours=10)).timestamp() * 1000)
            mock_run.end_time = int(now.timestamp() * 1000)
            mock_run.state = Mock()
            mock_run.state.result_state = RunResultState.SUCCESS
            mock_run.state.life_cycle_state = RunLifeCycleState.TERMINATED
            return [mock_run]

        jobs_admin.ws.jobs.list.return_value = mock_jobs
        jobs_admin.ws.jobs.list_runs.side_effect = mock_list_runs

        result = jobs_admin.list_long_running_jobs(min_duration_hours=4.0, limit=3)

        assert len(result) <= 3

    def test_api_error_handling(self, jobs_admin):
        """Test API error handling."""
        jobs_admin.ws.jobs.list.side_effect = Exception("API error")

        with pytest.raises(APIError, match="Failed to list long-running jobs"):
            jobs_admin.list_long_running_jobs()


class TestListFailedJobs:
    """Test list_failed_jobs method."""

    def test_validation_negative_lookback(self, jobs_admin):
        """Test validation fails with negative lookback_hours."""
        with pytest.raises(ValidationError, match="lookback_hours must be positive"):
            jobs_admin.list_failed_jobs(lookback_hours=-1.0)

    def test_validation_negative_limit(self, jobs_admin):
        """Test validation fails with negative limit."""
        with pytest.raises(ValidationError, match="limit must be positive"):
            jobs_admin.list_failed_jobs(limit=-1)

    def test_no_failed_jobs(self, jobs_admin):
        """Test with no failed jobs."""
        now = datetime.now(timezone.utc)

        mock_job = Mock()
        mock_job.job_id = 123
        mock_job.settings = Mock()
        mock_job.settings.name = "Successful Job"

        mock_run = Mock()
        mock_run.run_id = 456
        mock_run.start_time = int((now - timedelta(hours=1)).timestamp() * 1000)
        mock_run.end_time = int(now.timestamp() * 1000)
        mock_run.state = Mock()
        mock_run.state.result_state = RunResultState.SUCCESS
        mock_run.state.life_cycle_state = RunLifeCycleState.TERMINATED

        jobs_admin.ws.jobs.list.return_value = [mock_job]
        jobs_admin.ws.jobs.list_runs.return_value = [mock_run]

        result = jobs_admin.list_failed_jobs()

        assert len(result) == 0

    def test_failed_job_found(self, jobs_admin):
        """Test finding a failed job."""
        now = datetime.now(timezone.utc)

        mock_job = Mock()
        mock_job.job_id = 123
        mock_job.settings = Mock()
        mock_job.settings.name = "Failed Job"

        mock_run = Mock()
        mock_run.run_id = 456
        mock_run.start_time = int((now - timedelta(hours=1)).timestamp() * 1000)
        mock_run.end_time = int(now.timestamp() * 1000)
        mock_run.state = Mock()
        mock_run.state.result_state = RunResultState.FAILED
        mock_run.state.life_cycle_state = RunLifeCycleState.TERMINATED

        jobs_admin.ws.jobs.list.return_value = [mock_job]
        jobs_admin.ws.jobs.list_runs.return_value = [mock_run]

        result = jobs_admin.list_failed_jobs()

        assert len(result) == 1
        assert result[0].job_id == 123
        assert result[0].job_name == "Failed Job"
        assert result[0].run_id == 456
        assert result[0].state == "FAILED"

    def test_timedout_job_found(self, jobs_admin):
        """Test finding a timed out job."""
        now = datetime.now(timezone.utc)

        mock_job = Mock()
        mock_job.job_id = 123
        mock_job.settings = Mock()
        mock_job.settings.name = "Timed Out Job"

        mock_run = Mock()
        mock_run.run_id = 456
        mock_run.start_time = int((now - timedelta(hours=1)).timestamp() * 1000)
        mock_run.end_time = int(now.timestamp() * 1000)
        mock_run.state = Mock()
        mock_run.state.result_state = RunResultState.TIMEDOUT
        mock_run.state.life_cycle_state = RunLifeCycleState.TERMINATED

        jobs_admin.ws.jobs.list.return_value = [mock_job]
        jobs_admin.ws.jobs.list_runs.return_value = [mock_run]

        result = jobs_admin.list_failed_jobs()

        assert len(result) == 1
        assert result[0].state == "TIMEDOUT"

    def test_internal_error_job_found(self, jobs_admin):
        """Test finding a job with internal error."""
        now = datetime.now(timezone.utc)

        mock_job = Mock()
        mock_job.job_id = 123
        mock_job.settings = Mock()
        mock_job.settings.name = "Error Job"

        mock_run = Mock()
        mock_run.run_id = 456
        mock_run.start_time = int((now - timedelta(hours=1)).timestamp() * 1000)
        mock_run.end_time = int(now.timestamp() * 1000)
        mock_run.state = Mock()
        mock_run.state.result_state = None
        mock_run.state.life_cycle_state = RunLifeCycleState.INTERNAL_ERROR

        jobs_admin.ws.jobs.list.return_value = [mock_job]
        jobs_admin.ws.jobs.list_runs.return_value = [mock_run]

        result = jobs_admin.list_failed_jobs()

        assert len(result) == 1
        assert result[0].state == "INTERNAL_ERROR"

    def test_sorting_by_start_time(self, jobs_admin):
        """Test that results are sorted by start time (newest first)."""
        now = datetime.now(timezone.utc)

        mock_job1 = Mock()
        mock_job1.job_id = 1
        mock_job1.settings = Mock()
        mock_job1.settings.name = "Older Failed Job"

        mock_job2 = Mock()
        mock_job2.job_id = 2
        mock_job2.settings = Mock()
        mock_job2.settings.name = "Newer Failed Job"

        # Older run
        mock_run1 = Mock()
        mock_run1.run_id = 101
        mock_run1.start_time = int((now - timedelta(hours=5)).timestamp() * 1000)
        mock_run1.end_time = int((now - timedelta(hours=4)).timestamp() * 1000)
        mock_run1.state = Mock()
        mock_run1.state.result_state = RunResultState.FAILED
        mock_run1.state.life_cycle_state = RunLifeCycleState.TERMINATED

        # Newer run
        mock_run2 = Mock()
        mock_run2.run_id = 102
        mock_run2.start_time = int((now - timedelta(hours=2)).timestamp() * 1000)
        mock_run2.end_time = int((now - timedelta(hours=1)).timestamp() * 1000)
        mock_run2.state = Mock()
        mock_run2.state.result_state = RunResultState.FAILED
        mock_run2.state.life_cycle_state = RunLifeCycleState.TERMINATED

        jobs_admin.ws.jobs.list.return_value = [mock_job1, mock_job2]

        def mock_list_runs(job_id, **kwargs):
            if job_id == 1:
                return [mock_run1]
            elif job_id == 2:
                return [mock_run2]
            return []

        jobs_admin.ws.jobs.list_runs.side_effect = mock_list_runs

        result = jobs_admin.list_failed_jobs()

        assert len(result) == 2
        # Should be sorted newest first
        assert result[0].job_name == "Newer Failed Job"
        assert result[1].job_name == "Older Failed Job"

    def test_api_error_handling(self, jobs_admin):
        """Test API error handling."""
        jobs_admin.ws.jobs.list.side_effect = Exception("API error")

        with pytest.raises(APIError, match="Failed to list failed jobs"):
            jobs_admin.list_failed_jobs()
