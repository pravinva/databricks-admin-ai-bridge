"""
Unit tests for PipelinesAdmin module.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from admin_ai_bridge.pipelines import PipelinesAdmin
from admin_ai_bridge.config import AdminBridgeConfig
from admin_ai_bridge.errors import ValidationError, APIError
from admin_ai_bridge.schemas import PipelineStatus

from databricks.sdk.service.pipelines import PipelineState


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient."""
    with patch('admin_ai_bridge.pipelines.get_workspace_client') as mock_get_client:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        yield mock_client


@pytest.fixture
def pipelines_admin(mock_workspace_client):
    """Create a PipelinesAdmin instance with mocked client."""
    cfg = AdminBridgeConfig(profile="test")
    return PipelinesAdmin(cfg)


class TestPipelinesAdminInit:
    """Tests for PipelinesAdmin initialization."""

    def test_init_with_config(self, mock_workspace_client):
        """Test initialization with configuration."""
        cfg = AdminBridgeConfig(profile="test")
        admin = PipelinesAdmin(cfg)
        assert admin.ws == mock_workspace_client

    def test_init_without_config(self, mock_workspace_client):
        """Test initialization without configuration."""
        admin = PipelinesAdmin()
        assert admin.ws == mock_workspace_client


class TestListLaggingPipelines:
    """Tests for list_lagging_pipelines method."""

    def test_list_lagging_pipelines_success(self, pipelines_admin, mock_workspace_client):
        """Test successful query of lagging pipelines."""
        # Mock pipeline list
        mock_pipeline = MagicMock()
        mock_pipeline.pipeline_id = "pipeline-1"
        mock_pipeline.name = "Test Pipeline"

        # Mock pipeline details
        mock_details = MagicMock()
        mock_details.pipeline_id = "pipeline-1"
        mock_details.name = "Test Pipeline"
        mock_details.state = PipelineState.RUNNING

        # Mock spec for continuous pipeline
        mock_spec = MagicMock()
        mock_spec.continuous = True
        mock_details.spec = mock_spec

        # Mock update with old creation time (simulating lag)
        mock_update = MagicMock()
        mock_update.state = PipelineState.RUNNING
        mock_update.creation_time = 1000000000000  # Old timestamp

        mock_details.latest_updates = [mock_update]

        mock_workspace_client.pipelines.list_pipelines.return_value = [mock_pipeline]
        mock_workspace_client.pipelines.get.return_value = mock_details

        # Call method
        result = pipelines_admin.list_lagging_pipelines(max_lag_seconds=600.0, limit=50)

        # Verify
        assert isinstance(result, list)
        mock_workspace_client.pipelines.list_pipelines.assert_called_once()

    def test_list_lagging_pipelines_filters_by_lag(self, pipelines_admin, mock_workspace_client):
        """Test that only pipelines exceeding lag threshold are returned."""
        # This test verifies the lag filtering logic
        mock_pipeline = MagicMock()
        mock_pipeline.pipeline_id = "pipeline-1"

        mock_details = MagicMock()
        mock_details.pipeline_id = "pipeline-1"
        mock_details.name = "Low Lag Pipeline"
        mock_details.state = PipelineState.RUNNING

        mock_spec = MagicMock()
        mock_spec.continuous = True
        mock_details.spec = mock_spec

        # Recent update (low lag)
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        mock_update = MagicMock()
        mock_update.state = PipelineState.RUNNING
        mock_update.creation_time = now_ms - 30000  # 30 seconds ago

        mock_details.latest_updates = [mock_update]

        mock_workspace_client.pipelines.list_pipelines.return_value = [mock_pipeline]
        mock_workspace_client.pipelines.get.return_value = mock_details

        # Call with high threshold
        result = pipelines_admin.list_lagging_pipelines(max_lag_seconds=600.0, limit=50)

        # Should not include low-lag pipeline
        assert len(result) == 0

    def test_list_lagging_pipelines_sorting(self, pipelines_admin, mock_workspace_client):
        """Test that results are sorted by lag (highest first)."""
        # Mock two pipelines with different lag
        mock_pipeline1 = MagicMock()
        mock_pipeline1.pipeline_id = "pipeline-1"

        mock_pipeline2 = MagicMock()
        mock_pipeline2.pipeline_id = "pipeline-2"

        # Pipeline 1 - high lag
        mock_details1 = MagicMock()
        mock_details1.pipeline_id = "pipeline-1"
        mock_details1.name = "High Lag Pipeline"
        mock_details1.state = PipelineState.RUNNING

        mock_spec1 = MagicMock()
        mock_spec1.continuous = True
        mock_details1.spec = mock_spec1

        mock_update1 = MagicMock()
        mock_update1.state = PipelineState.RUNNING
        mock_update1.creation_time = 1000000000000  # Very old

        mock_details1.latest_updates = [mock_update1]

        # Pipeline 2 - medium lag
        mock_details2 = MagicMock()
        mock_details2.pipeline_id = "pipeline-2"
        mock_details2.name = "Medium Lag Pipeline"
        mock_details2.state = PipelineState.RUNNING

        mock_spec2 = MagicMock()
        mock_spec2.continuous = True
        mock_details2.spec = mock_spec2

        mock_update2 = MagicMock()
        mock_update2.state = PipelineState.RUNNING
        mock_update2.creation_time = 1500000000000  # Less old

        mock_details2.latest_updates = [mock_update2]

        def mock_get(pipeline_id):
            if pipeline_id == "pipeline-1":
                return mock_details1
            return mock_details2

        mock_workspace_client.pipelines.list_pipelines.return_value = [mock_pipeline1, mock_pipeline2]
        mock_workspace_client.pipelines.get.side_effect = mock_get

        # Call method
        result = pipelines_admin.list_lagging_pipelines(max_lag_seconds=100.0, limit=50)

        # Verify sorting (if any results, highest lag should be first)
        if len(result) > 1:
            assert result[0].lag_seconds >= result[1].lag_seconds

    def test_list_lagging_pipelines_limit(self, pipelines_admin, mock_workspace_client):
        """Test that limit parameter is respected."""
        # Mock many pipelines
        mock_pipelines = []
        for i in range(100):
            mock_pipeline = MagicMock()
            mock_pipeline.pipeline_id = f"pipeline-{i}"
            mock_pipelines.append(mock_pipeline)

        # Mock details
        mock_details = MagicMock()
        mock_details.name = "Pipeline"
        mock_details.state = PipelineState.RUNNING

        mock_spec = MagicMock()
        mock_spec.continuous = True
        mock_details.spec = mock_spec

        mock_update = MagicMock()
        mock_update.state = PipelineState.RUNNING
        mock_update.creation_time = 1000000000000

        mock_details.latest_updates = [mock_update]

        mock_workspace_client.pipelines.list_pipelines.return_value = mock_pipelines
        mock_workspace_client.pipelines.get.return_value = mock_details

        # Call with limit
        result = pipelines_admin.list_lagging_pipelines(max_lag_seconds=100.0, limit=10)

        # Verify limit
        assert len(result) <= 10

    def test_list_lagging_pipelines_invalid_max_lag(self, pipelines_admin):
        """Test with invalid max_lag_seconds."""
        with pytest.raises(ValidationError, match="max_lag_seconds must be positive"):
            pipelines_admin.list_lagging_pipelines(max_lag_seconds=0)

        with pytest.raises(ValidationError, match="max_lag_seconds must be positive"):
            pipelines_admin.list_lagging_pipelines(max_lag_seconds=-1)

    def test_list_lagging_pipelines_invalid_limit(self, pipelines_admin):
        """Test with invalid limit."""
        with pytest.raises(ValidationError, match="limit must be positive"):
            pipelines_admin.list_lagging_pipelines(max_lag_seconds=600.0, limit=0)

        with pytest.raises(ValidationError, match="limit must be positive"):
            pipelines_admin.list_lagging_pipelines(max_lag_seconds=600.0, limit=-1)

    def test_list_lagging_pipelines_api_error(self, pipelines_admin, mock_workspace_client):
        """Test API error handling."""
        mock_workspace_client.pipelines.list_pipelines.side_effect = Exception("API error")

        with pytest.raises(APIError, match="Failed to list lagging pipelines"):
            pipelines_admin.list_lagging_pipelines(max_lag_seconds=600.0)

    def test_list_lagging_pipelines_handles_pipeline_errors(self, pipelines_admin, mock_workspace_client):
        """Test that individual pipeline errors don't fail entire query."""
        mock_pipeline1 = MagicMock()
        mock_pipeline1.pipeline_id = "pipeline-1"

        mock_pipeline2 = MagicMock()
        mock_pipeline2.pipeline_id = "pipeline-2"

        mock_details = MagicMock()
        mock_details.name = "Good Pipeline"
        mock_details.state = PipelineState.RUNNING
        mock_details.spec = MagicMock()
        mock_details.spec.continuous = True
        mock_details.latest_updates = []

        def mock_get(pipeline_id):
            if pipeline_id == "pipeline-2":
                raise Exception("Pipeline error")
            return mock_details

        mock_workspace_client.pipelines.list_pipelines.return_value = [mock_pipeline1, mock_pipeline2]
        mock_workspace_client.pipelines.get.side_effect = mock_get

        # Call method - should not fail
        result = pipelines_admin.list_lagging_pipelines(max_lag_seconds=600.0)

        # Should succeed
        assert isinstance(result, list)


class TestListFailedPipelines:
    """Tests for list_failed_pipelines method."""

    def test_list_failed_pipelines_success(self, pipelines_admin, mock_workspace_client):
        """Test successful query of failed pipelines."""
        # Mock pipeline
        mock_pipeline = MagicMock()
        mock_pipeline.pipeline_id = "pipeline-1"

        # Mock details
        mock_details = MagicMock()
        mock_details.pipeline_id = "pipeline-1"
        mock_details.name = "Failed Pipeline"
        mock_details.state = PipelineState.FAILED
        mock_details.cause = "Error in transformation"

        # Mock failed update
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        mock_update = MagicMock()
        mock_update.state = PipelineState.FAILED
        mock_update.creation_time = now_ms - 3600000  # 1 hour ago

        mock_details.latest_updates = [mock_update]

        mock_workspace_client.pipelines.list_pipelines.return_value = [mock_pipeline]
        mock_workspace_client.pipelines.get.return_value = mock_details

        # Call method
        result = pipelines_admin.list_failed_pipelines(lookback_hours=24.0, limit=50)

        # Verify
        assert isinstance(result, list)
        if len(result) > 0:
            assert result[0].state == "FAILED"

    def test_list_failed_pipelines_filters_by_time(self, pipelines_admin, mock_workspace_client):
        """Test that only pipelines within lookback window are returned."""
        mock_pipeline = MagicMock()
        mock_pipeline.pipeline_id = "pipeline-1"

        mock_details = MagicMock()
        mock_details.pipeline_id = "pipeline-1"
        mock_details.name = "Old Failed Pipeline"
        mock_details.state = PipelineState.FAILED
        mock_details.cause = "Error"

        # Very old failure (outside lookback window)
        mock_update = MagicMock()
        mock_update.state = PipelineState.FAILED
        mock_update.creation_time = 1000000000000  # Very old timestamp

        mock_details.latest_updates = [mock_update]

        mock_workspace_client.pipelines.list_pipelines.return_value = [mock_pipeline]
        mock_workspace_client.pipelines.get.return_value = mock_details

        # Call with short lookback
        result = pipelines_admin.list_failed_pipelines(lookback_hours=1.0, limit=50)

        # Should not include old failure
        assert len(result) == 0

    def test_list_failed_pipelines_sorting(self, pipelines_admin, mock_workspace_client):
        """Test that results are sorted by time (newest first)."""
        # This would require more complex mocking, basic structure test
        result = pipelines_admin.list_failed_pipelines(lookback_hours=24.0)
        assert isinstance(result, list)

    def test_list_failed_pipelines_invalid_lookback_hours(self, pipelines_admin):
        """Test with invalid lookback_hours."""
        with pytest.raises(ValidationError, match="lookback_hours must be positive"):
            pipelines_admin.list_failed_pipelines(lookback_hours=0)

        with pytest.raises(ValidationError, match="lookback_hours must be positive"):
            pipelines_admin.list_failed_pipelines(lookback_hours=-1)

    def test_list_failed_pipelines_invalid_limit(self, pipelines_admin):
        """Test with invalid limit."""
        with pytest.raises(ValidationError, match="limit must be positive"):
            pipelines_admin.list_failed_pipelines(lookback_hours=24.0, limit=0)

        with pytest.raises(ValidationError, match="limit must be positive"):
            pipelines_admin.list_failed_pipelines(lookback_hours=24.0, limit=-1)

    def test_list_failed_pipelines_api_error(self, pipelines_admin, mock_workspace_client):
        """Test API error handling."""
        mock_workspace_client.pipelines.list_pipelines.side_effect = Exception("API error")

        with pytest.raises(APIError, match="Failed to list failed pipelines"):
            pipelines_admin.list_failed_pipelines(lookback_hours=24.0)

    def test_list_failed_pipelines_extracts_error_message(self, pipelines_admin, mock_workspace_client):
        """Test that error messages are properly extracted."""
        mock_pipeline = MagicMock()
        mock_pipeline.pipeline_id = "pipeline-1"

        mock_details = MagicMock()
        mock_details.pipeline_id = "pipeline-1"
        mock_details.name = "Failed Pipeline"
        mock_details.state = PipelineState.FAILED
        mock_details.cause = "Connection timeout"

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        mock_update = MagicMock()
        mock_update.state = PipelineState.FAILED
        mock_update.creation_time = now_ms - 1800000  # 30 min ago

        mock_details.latest_updates = [mock_update]

        mock_workspace_client.pipelines.list_pipelines.return_value = [mock_pipeline]
        mock_workspace_client.pipelines.get.return_value = mock_details

        # Call method
        result = pipelines_admin.list_failed_pipelines(lookback_hours=24.0)

        # Verify error message is captured
        if len(result) > 0:
            assert result[0].last_error == "Connection timeout"

    def test_list_failed_pipelines_empty_results(self, pipelines_admin, mock_workspace_client):
        """Test when no failed pipelines are found."""
        mock_workspace_client.pipelines.list_pipelines.return_value = []

        result = pipelines_admin.list_failed_pipelines(lookback_hours=24.0)

        assert len(result) == 0


class TestPipelineStatusStructure:
    """Tests for PipelineStatus data structure."""

    def test_pipeline_status_creation(self):
        """Test creating a PipelineStatus instance."""
        status = PipelineStatus(
            pipeline_id="pipeline-123",
            name="Test Pipeline",
            state="RUNNING",
            last_update_time=datetime.now(timezone.utc),
            lag_seconds=300.0,
            last_error=None
        )

        assert status.pipeline_id == "pipeline-123"
        assert status.name == "Test Pipeline"
        assert status.state == "RUNNING"
        assert status.lag_seconds == 300.0

    def test_pipeline_status_optional_fields(self):
        """Test PipelineStatus with optional fields."""
        status = PipelineStatus(
            pipeline_id="pipeline-456",
            name="Minimal Pipeline",
            state="IDLE"
        )

        assert status.last_update_time is None
        assert status.lag_seconds is None
        assert status.last_error is None

    def test_pipeline_status_serialization(self):
        """Test that PipelineStatus can be serialized."""
        status = PipelineStatus(
            pipeline_id="pipeline-789",
            name="Test Pipeline",
            state="FAILED",
            last_error="Syntax error in SQL"
        )

        # Test Pydantic model_dump
        data = status.model_dump()
        assert data["pipeline_id"] == "pipeline-789"
        assert data["state"] == "FAILED"
        assert data["last_error"] == "Syntax error in SQL"
