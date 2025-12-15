"""
Unit tests for UsageAdmin module.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from admin_ai_bridge.usage import UsageAdmin
from admin_ai_bridge.config import AdminBridgeConfig
from admin_ai_bridge.errors import ValidationError, APIError
from admin_ai_bridge.schemas import UsageEntry

# Note: We mock the SDK responses, so we don't need to import specific types


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient."""
    with patch('admin_ai_bridge.usage.get_workspace_client') as mock_get_client:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        yield mock_client


@pytest.fixture
def usage_admin(mock_workspace_client):
    """Create a UsageAdmin instance with mocked client."""
    cfg = AdminBridgeConfig(profile="test")
    return UsageAdmin(cfg)


class TestUsageAdminInit:
    """Tests for UsageAdmin initialization."""

    def test_init_with_config(self, mock_workspace_client):
        """Test initialization with configuration."""
        cfg = AdminBridgeConfig(profile="test")
        admin = UsageAdmin(cfg)
        assert admin.ws == mock_workspace_client

    def test_init_without_config(self, mock_workspace_client):
        """Test initialization without configuration."""
        admin = UsageAdmin()
        assert admin.ws == mock_workspace_client


class TestTopCostCenters:
    """Tests for top_cost_centers method."""

    def test_top_cost_centers_success_with_clusters(self, usage_admin, mock_workspace_client):
        """Test successful query of top cost centers with cluster data."""
        # Mock cluster
        mock_cluster = MagicMock()
        mock_cluster.cluster_id = "cluster-1"
        mock_cluster.cluster_name = "Test Cluster"
        mock_cluster.num_workers = 4

        # Mock cluster events
        mock_start_event = MagicMock()
        mock_start_event.type = MagicMock()
        mock_start_event.type.value = "STARTING"
        mock_start_event.timestamp = 1000000000000  # milliseconds

        mock_stop_event = MagicMock()
        mock_stop_event.type = MagicMock()
        mock_stop_event.type.value = "TERMINATING"
        mock_stop_event.timestamp = 1000003600000  # 1 hour later

        mock_workspace_client.clusters.list.return_value = [mock_cluster]
        mock_workspace_client.clusters.events.return_value = [mock_start_event, mock_stop_event]
        mock_workspace_client.warehouses.list.return_value = []

        # Call method
        result = usage_admin.top_cost_centers(lookback_days=7, limit=20)

        # Verify
        assert len(result) > 0
        assert all(isinstance(entry, UsageEntry) for entry in result)
        assert result[0].scope == "cluster"
        assert result[0].name == "Test Cluster"
        assert result[0].dbus is not None
        assert result[0].dbus > 0

        mock_workspace_client.clusters.list.assert_called_once()

    def test_top_cost_centers_success_with_warehouses(self, usage_admin, mock_workspace_client):
        """Test with SQL warehouse data."""
        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-1"
        mock_warehouse.name = "Test Warehouse"

        mock_warehouse_info = MagicMock()
        mock_warehouse_info.id = "warehouse-1"
        mock_warehouse_info.name = "Test Warehouse"
        mock_warehouse_info.state = MagicMock()
        mock_warehouse_info.state.value = "RUNNING"
        mock_warehouse_info.cluster_size = "Medium"

        mock_workspace_client.clusters.list.return_value = []
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]
        mock_workspace_client.warehouses.get.return_value = mock_warehouse_info

        # Call method
        result = usage_admin.top_cost_centers(lookback_days=7, limit=20)

        # Verify
        assert len(result) > 0
        assert result[0].scope == "warehouse"
        assert result[0].name == "Test Warehouse"
        assert result[0].dbus is not None
        assert result[0].dbus > 0

    def test_top_cost_centers_sorting(self, usage_admin, mock_workspace_client):
        """Test that results are sorted by DBU usage."""
        # Mock multiple clusters with different usage
        mock_cluster1 = MagicMock()
        mock_cluster1.cluster_id = "cluster-1"
        mock_cluster1.cluster_name = "Small Cluster"
        mock_cluster1.num_workers = 1

        mock_cluster2 = MagicMock()
        mock_cluster2.cluster_id = "cluster-2"
        mock_cluster2.cluster_name = "Large Cluster"
        mock_cluster2.num_workers = 10

        # Mock events for small cluster (short runtime)
        mock_start1 = MagicMock()
        mock_start1.type = MagicMock()
        mock_start1.type.value = "STARTING"
        mock_start1.timestamp = 1000000000000

        mock_stop1 = MagicMock()
        mock_stop1.type = MagicMock()
        mock_stop1.type.value = "TERMINATING"
        mock_stop1.timestamp = 1000001800000  # 30 minutes

        # Mock events for large cluster (long runtime)
        mock_start2 = MagicMock()
        mock_start2.type = MagicMock()
        mock_start2.type.value = "STARTING"
        mock_start2.timestamp = 1000000000000

        mock_stop2 = MagicMock()
        mock_stop2.type = MagicMock()
        mock_stop2.type.value = "TERMINATING"
        mock_stop2.timestamp = 1000014400000  # 4 hours

        def mock_events(cluster_id, start_time, end_time, max_items):
            if cluster_id == "cluster-1":
                return [mock_start1, mock_stop1]
            else:
                return [mock_start2, mock_stop2]

        mock_workspace_client.clusters.list.return_value = [mock_cluster1, mock_cluster2]
        mock_workspace_client.clusters.events.side_effect = mock_events
        mock_workspace_client.warehouses.list.return_value = []

        # Call method
        result = usage_admin.top_cost_centers(lookback_days=7, limit=20)

        # Verify - Large cluster should be first
        assert len(result) == 2
        assert result[0].name == "Large Cluster"
        assert result[1].name == "Small Cluster"
        assert result[0].dbus > result[1].dbus

    def test_top_cost_centers_limit(self, usage_admin, mock_workspace_client):
        """Test that limit parameter is respected."""
        # Mock many clusters
        mock_clusters = []
        for i in range(50):
            mock_cluster = MagicMock()
            mock_cluster.cluster_id = f"cluster-{i}"
            mock_cluster.cluster_name = f"Cluster {i}"
            mock_cluster.num_workers = 2
            mock_clusters.append(mock_cluster)

        # Mock events
        mock_start = MagicMock()
        mock_start.type = MagicMock()
        mock_start.type.value = "STARTING"
        mock_start.timestamp = 1000000000000

        mock_stop = MagicMock()
        mock_stop.type = MagicMock()
        mock_stop.type.value = "TERMINATING"
        mock_stop.timestamp = 1000003600000

        mock_workspace_client.clusters.list.return_value = mock_clusters
        mock_workspace_client.clusters.events.return_value = [mock_start, mock_stop]
        mock_workspace_client.warehouses.list.return_value = []

        # Call method with limit
        result = usage_admin.top_cost_centers(lookback_days=7, limit=10)

        # Verify
        assert len(result) == 10

    def test_top_cost_centers_invalid_lookback_days(self, usage_admin):
        """Test with invalid lookback_days."""
        with pytest.raises(ValidationError, match="lookback_days must be positive"):
            usage_admin.top_cost_centers(lookback_days=0)

        with pytest.raises(ValidationError, match="lookback_days must be positive"):
            usage_admin.top_cost_centers(lookback_days=-1)

    def test_top_cost_centers_invalid_limit(self, usage_admin):
        """Test with invalid limit."""
        with pytest.raises(ValidationError, match="limit must be positive"):
            usage_admin.top_cost_centers(lookback_days=7, limit=0)

        with pytest.raises(ValidationError, match="limit must be positive"):
            usage_admin.top_cost_centers(lookback_days=7, limit=-1)

    def test_top_cost_centers_api_error(self, usage_admin, mock_workspace_client):
        """Test API error handling."""
        mock_workspace_client.clusters.list.side_effect = Exception("API error")

        with pytest.raises(APIError, match="Failed to query usage data"):
            usage_admin.top_cost_centers(lookback_days=7)

    def test_top_cost_centers_handles_cluster_errors(self, usage_admin, mock_workspace_client):
        """Test that individual cluster errors don't fail entire query."""
        # Mock clusters where one will error
        mock_cluster1 = MagicMock()
        mock_cluster1.cluster_id = "cluster-1"
        mock_cluster1.cluster_name = "Good Cluster"
        mock_cluster1.num_workers = 2

        mock_cluster2 = MagicMock()
        mock_cluster2.cluster_id = "cluster-2"
        mock_cluster2.cluster_name = "Bad Cluster"
        mock_cluster2.num_workers = 2

        # Mock events - cluster-2 will error
        def mock_events(cluster_id, start_time, end_time, max_items):
            if cluster_id == "cluster-2":
                raise Exception("Cluster error")

            mock_start = MagicMock()
            mock_start.type = MagicMock()
            mock_start.type.value = "STARTING"
            mock_start.timestamp = 1000000000000

            mock_stop = MagicMock()
            mock_stop.type = MagicMock()
            mock_stop.type.value = "TERMINATING"
            mock_stop.timestamp = 1000003600000

            return [mock_start, mock_stop]

        mock_workspace_client.clusters.list.return_value = [mock_cluster1, mock_cluster2]
        mock_workspace_client.clusters.events.side_effect = mock_events
        mock_workspace_client.warehouses.list.return_value = []

        # Call method - should succeed with cluster-1 data
        result = usage_admin.top_cost_centers(lookback_days=7)

        # Verify we got data for the good cluster
        assert len(result) == 1
        assert result[0].name == "Good Cluster"

    def test_top_cost_centers_empty_results(self, usage_admin, mock_workspace_client):
        """Test when no usage data is found."""
        mock_workspace_client.clusters.list.return_value = []
        mock_workspace_client.warehouses.list.return_value = []

        result = usage_admin.top_cost_centers(lookback_days=7)

        assert len(result) == 0

    def test_top_cost_centers_still_running_cluster(self, usage_admin, mock_workspace_client):
        """Test with a cluster that is still running (no stop event)."""
        # Mock cluster
        mock_cluster = MagicMock()
        mock_cluster.cluster_id = "cluster-1"
        mock_cluster.cluster_name = "Running Cluster"
        mock_cluster.num_workers = 4

        # Mock only start event (no stop)
        mock_start_event = MagicMock()
        mock_start_event.type = MagicMock()
        mock_start_event.type.value = "STARTING"
        mock_start_event.timestamp = 1000000000000

        mock_workspace_client.clusters.list.return_value = [mock_cluster]
        mock_workspace_client.clusters.events.return_value = [mock_start_event]
        mock_workspace_client.warehouses.list.return_value = []

        # Call method
        result = usage_admin.top_cost_centers(lookback_days=7)

        # Verify - should still include the running cluster
        assert len(result) > 0
        assert result[0].name == "Running Cluster"
        assert result[0].dbus is not None
