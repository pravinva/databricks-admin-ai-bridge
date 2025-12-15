"""
Unit tests for clusters module.
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, patch
from databricks.sdk.service.compute import State

from admin_ai_bridge.clusters import ClustersAdmin
from admin_ai_bridge.config import AdminBridgeConfig
from admin_ai_bridge.errors import ValidationError, APIError
from admin_ai_bridge.schemas import ClusterSummary


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient."""
    mock_client = Mock()
    mock_client.clusters = Mock()
    return mock_client


@pytest.fixture
def clusters_admin(mock_workspace_client):
    """Create ClustersAdmin instance with mocked client."""
    with patch('admin_ai_bridge.clusters.get_workspace_client', return_value=mock_workspace_client):
        admin = ClustersAdmin(AdminBridgeConfig(profile="TEST"))
    return admin


class TestClustersAdminInit:
    """Test ClustersAdmin initialization."""

    def test_init_with_config(self):
        """Test initialization with config."""
        with patch('admin_ai_bridge.clusters.get_workspace_client') as mock_get_client:
            cfg = AdminBridgeConfig(profile="TEST")
            admin = ClustersAdmin(cfg)
            mock_get_client.assert_called_once_with(cfg)
            assert admin.ws is not None

    def test_init_without_config(self):
        """Test initialization without config."""
        with patch('admin_ai_bridge.clusters.get_workspace_client') as mock_get_client:
            admin = ClustersAdmin()
            mock_get_client.assert_called_once_with(None)
            assert admin.ws is not None


class TestListLongRunningClusters:
    """Test list_long_running_clusters method."""

    def test_validation_negative_min_duration(self, clusters_admin):
        """Test validation fails with negative min_duration_hours."""
        with pytest.raises(ValidationError, match="min_duration_hours must be positive"):
            clusters_admin.list_long_running_clusters(min_duration_hours=-1.0)

    def test_validation_negative_lookback(self, clusters_admin):
        """Test validation fails with negative lookback_hours."""
        with pytest.raises(ValidationError, match="lookback_hours must be positive"):
            clusters_admin.list_long_running_clusters(lookback_hours=-1.0)

    def test_validation_negative_limit(self, clusters_admin):
        """Test validation fails with negative limit."""
        with pytest.raises(ValidationError, match="limit must be positive"):
            clusters_admin.list_long_running_clusters(limit=-1)

    def test_no_clusters(self, clusters_admin):
        """Test with no clusters in workspace."""
        clusters_admin.ws.clusters.list.return_value = []

        result = clusters_admin.list_long_running_clusters()

        assert result == []
        clusters_admin.ws.clusters.list.assert_called_once()

    def test_long_running_cluster_found(self, clusters_admin):
        """Test finding a long-running cluster."""
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=10)

        # Create mock cluster list item
        mock_cluster_item = Mock()
        mock_cluster_item.cluster_id = "cluster-123"

        # Create mock detailed cluster info
        mock_cluster_info = Mock()
        mock_cluster_info.cluster_id = "cluster-123"
        mock_cluster_info.cluster_name = "Long Running Cluster"
        mock_cluster_info.state = State.RUNNING
        mock_cluster_info.creator_user_name = "user@example.com"
        mock_cluster_info.start_time = int(start_time.timestamp() * 1000)
        mock_cluster_info.driver_node_type_id = "i3.xlarge"
        mock_cluster_info.node_type_id = "i3.xlarge"
        mock_cluster_info.policy_id = "policy-123"
        mock_cluster_info.last_activity_time = int((now - timedelta(hours=1)).timestamp() * 1000)

        clusters_admin.ws.clusters.list.return_value = [mock_cluster_item]
        clusters_admin.ws.clusters.get.return_value = mock_cluster_info

        result = clusters_admin.list_long_running_clusters(
            min_duration_hours=8.0,
            lookback_hours=24.0
        )

        assert len(result) == 1
        assert result[0].cluster_id == "cluster-123"
        assert result[0].cluster_name == "Long Running Cluster"
        assert result[0].state == "RUNNING"
        assert result[0].creator == "user@example.com"
        assert result[0].is_long_running is True

    def test_cluster_started_before_lookback_window(self, clusters_admin):
        """Test that clusters started before lookback window are excluded."""
        now = datetime.now(timezone.utc)
        # Started 30 hours ago, but lookback is only 24 hours
        start_time = now - timedelta(hours=30)

        mock_cluster_item = Mock()
        mock_cluster_item.cluster_id = "cluster-123"

        mock_cluster_info = Mock()
        mock_cluster_info.cluster_id = "cluster-123"
        mock_cluster_info.cluster_name = "Old Cluster"
        mock_cluster_info.state = State.RUNNING
        mock_cluster_info.creator_user_name = "user@example.com"
        mock_cluster_info.start_time = int(start_time.timestamp() * 1000)

        clusters_admin.ws.clusters.list.return_value = [mock_cluster_item]
        clusters_admin.ws.clusters.get.return_value = mock_cluster_info

        result = clusters_admin.list_long_running_clusters(
            min_duration_hours=8.0,
            lookback_hours=24.0
        )

        assert len(result) == 0

    def test_cluster_running_less_than_threshold(self, clusters_admin):
        """Test that clusters running less than threshold are filtered out."""
        now = datetime.now(timezone.utc)
        # Running for 6 hours, but threshold is 8 hours
        start_time = now - timedelta(hours=6)

        mock_cluster_item = Mock()
        mock_cluster_item.cluster_id = "cluster-123"

        mock_cluster_info = Mock()
        mock_cluster_info.cluster_id = "cluster-123"
        mock_cluster_info.cluster_name = "Short Running Cluster"
        mock_cluster_info.state = State.RUNNING
        mock_cluster_info.creator_user_name = "user@example.com"
        mock_cluster_info.start_time = int(start_time.timestamp() * 1000)

        clusters_admin.ws.clusters.list.return_value = [mock_cluster_item]
        clusters_admin.ws.clusters.get.return_value = mock_cluster_info

        result = clusters_admin.list_long_running_clusters(
            min_duration_hours=8.0,
            lookback_hours=24.0
        )

        assert len(result) == 0

    def test_terminated_cluster_excluded(self, clusters_admin):
        """Test that terminated clusters are excluded."""
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=10)

        mock_cluster_item = Mock()
        mock_cluster_item.cluster_id = "cluster-123"

        mock_cluster_info = Mock()
        mock_cluster_info.cluster_id = "cluster-123"
        mock_cluster_info.cluster_name = "Terminated Cluster"
        mock_cluster_info.state = State.TERMINATED
        mock_cluster_info.start_time = int(start_time.timestamp() * 1000)

        clusters_admin.ws.clusters.list.return_value = [mock_cluster_item]
        clusters_admin.ws.clusters.get.return_value = mock_cluster_info

        result = clusters_admin.list_long_running_clusters()

        assert len(result) == 0

    def test_resizing_cluster_included(self, clusters_admin):
        """Test that resizing clusters are included."""
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=10)

        mock_cluster_item = Mock()
        mock_cluster_item.cluster_id = "cluster-123"

        mock_cluster_info = Mock()
        mock_cluster_info.cluster_id = "cluster-123"
        mock_cluster_info.cluster_name = "Resizing Cluster"
        mock_cluster_info.state = State.RESIZING
        mock_cluster_info.creator_user_name = "user@example.com"
        mock_cluster_info.start_time = int(start_time.timestamp() * 1000)

        clusters_admin.ws.clusters.list.return_value = [mock_cluster_item]
        clusters_admin.ws.clusters.get.return_value = mock_cluster_info

        result = clusters_admin.list_long_running_clusters(
            min_duration_hours=8.0,
            lookback_hours=24.0
        )

        assert len(result) == 1
        assert result[0].state == "RESIZING"

    def test_sorting_by_start_time(self, clusters_admin):
        """Test that results are sorted by start time (oldest first)."""
        now = datetime.now(timezone.utc)

        # Cluster 1: started 12 hours ago
        mock_cluster1 = Mock()
        mock_cluster1.cluster_id = "cluster-1"

        mock_cluster_info1 = Mock()
        mock_cluster_info1.cluster_id = "cluster-1"
        mock_cluster_info1.cluster_name = "Newer Cluster"
        mock_cluster_info1.state = State.RUNNING
        mock_cluster_info1.creator_user_name = "user@example.com"
        mock_cluster_info1.start_time = int((now - timedelta(hours=12)).timestamp() * 1000)

        # Cluster 2: started 20 hours ago
        mock_cluster2 = Mock()
        mock_cluster2.cluster_id = "cluster-2"

        mock_cluster_info2 = Mock()
        mock_cluster_info2.cluster_id = "cluster-2"
        mock_cluster_info2.cluster_name = "Older Cluster"
        mock_cluster_info2.state = State.RUNNING
        mock_cluster_info2.creator_user_name = "user@example.com"
        mock_cluster_info2.start_time = int((now - timedelta(hours=20)).timestamp() * 1000)

        clusters_admin.ws.clusters.list.return_value = [mock_cluster1, mock_cluster2]

        def mock_get(cluster_id):
            if cluster_id == "cluster-1":
                return mock_cluster_info1
            elif cluster_id == "cluster-2":
                return mock_cluster_info2

        clusters_admin.ws.clusters.get.side_effect = mock_get

        result = clusters_admin.list_long_running_clusters(
            min_duration_hours=8.0,
            lookback_hours=24.0
        )

        assert len(result) == 2
        # Should be sorted oldest first
        assert result[0].cluster_name == "Older Cluster"
        assert result[1].cluster_name == "Newer Cluster"

    def test_limit_enforced(self, clusters_admin):
        """Test that limit parameter is enforced."""
        now = datetime.now(timezone.utc)

        # Create 5 long-running clusters
        mock_cluster_items = []
        for i in range(5):
            mock_cluster = Mock()
            mock_cluster.cluster_id = f"cluster-{i}"
            mock_cluster_items.append(mock_cluster)

        def mock_get(cluster_id):
            mock_info = Mock()
            mock_info.cluster_id = cluster_id
            mock_info.cluster_name = f"Cluster {cluster_id}"
            mock_info.state = State.RUNNING
            mock_info.creator_user_name = "user@example.com"
            mock_info.start_time = int((now - timedelta(hours=10)).timestamp() * 1000)
            return mock_info

        clusters_admin.ws.clusters.list.return_value = mock_cluster_items
        clusters_admin.ws.clusters.get.side_effect = mock_get

        result = clusters_admin.list_long_running_clusters(
            min_duration_hours=8.0,
            limit=3
        )

        assert len(result) <= 3

    def test_api_error_handling(self, clusters_admin):
        """Test API error handling."""
        clusters_admin.ws.clusters.list.side_effect = Exception("API error")

        with pytest.raises(APIError, match="Failed to list long-running clusters"):
            clusters_admin.list_long_running_clusters()


class TestListIdleClusters:
    """Test list_idle_clusters method."""

    def test_validation_negative_idle_hours(self, clusters_admin):
        """Test validation fails with negative idle_hours."""
        with pytest.raises(ValidationError, match="idle_hours must be positive"):
            clusters_admin.list_idle_clusters(idle_hours=-1.0)

    def test_validation_negative_limit(self, clusters_admin):
        """Test validation fails with negative limit."""
        with pytest.raises(ValidationError, match="limit must be positive"):
            clusters_admin.list_idle_clusters(limit=-1)

    def test_no_clusters(self, clusters_admin):
        """Test with no clusters in workspace."""
        clusters_admin.ws.clusters.list.return_value = []

        result = clusters_admin.list_idle_clusters()

        assert result == []
        clusters_admin.ws.clusters.list.assert_called_once()

    def test_idle_cluster_found(self, clusters_admin):
        """Test finding an idle cluster."""
        now = datetime.now(timezone.utc)
        # Last activity 3 hours ago
        last_activity = now - timedelta(hours=3)

        mock_cluster_item = Mock()
        mock_cluster_item.cluster_id = "cluster-123"

        mock_cluster_info = Mock()
        mock_cluster_info.cluster_id = "cluster-123"
        mock_cluster_info.cluster_name = "Idle Cluster"
        mock_cluster_info.state = State.RUNNING
        mock_cluster_info.creator_user_name = "user@example.com"
        mock_cluster_info.start_time = int((now - timedelta(hours=5)).timestamp() * 1000)
        mock_cluster_info.last_activity_time = int(last_activity.timestamp() * 1000)

        clusters_admin.ws.clusters.list.return_value = [mock_cluster_item]
        clusters_admin.ws.clusters.get.return_value = mock_cluster_info

        result = clusters_admin.list_idle_clusters(idle_hours=2.0)

        assert len(result) == 1
        assert result[0].cluster_id == "cluster-123"
        assert result[0].cluster_name == "Idle Cluster"
        assert result[0].state == "RUNNING"

    def test_active_cluster_excluded(self, clusters_admin):
        """Test that recently active clusters are excluded."""
        now = datetime.now(timezone.utc)
        # Last activity 1 hour ago, but threshold is 2 hours
        last_activity = now - timedelta(hours=1)

        mock_cluster_item = Mock()
        mock_cluster_item.cluster_id = "cluster-123"

        mock_cluster_info = Mock()
        mock_cluster_info.cluster_id = "cluster-123"
        mock_cluster_info.cluster_name = "Active Cluster"
        mock_cluster_info.state = State.RUNNING
        mock_cluster_info.last_activity_time = int(last_activity.timestamp() * 1000)

        clusters_admin.ws.clusters.list.return_value = [mock_cluster_item]
        clusters_admin.ws.clusters.get.return_value = mock_cluster_info

        result = clusters_admin.list_idle_clusters(idle_hours=2.0)

        assert len(result) == 0

    def test_terminated_cluster_excluded(self, clusters_admin):
        """Test that terminated clusters are excluded."""
        now = datetime.now(timezone.utc)

        mock_cluster_item = Mock()
        mock_cluster_item.cluster_id = "cluster-123"

        mock_cluster_info = Mock()
        mock_cluster_info.cluster_id = "cluster-123"
        mock_cluster_info.cluster_name = "Terminated Cluster"
        mock_cluster_info.state = State.TERMINATED
        mock_cluster_info.last_activity_time = int((now - timedelta(hours=5)).timestamp() * 1000)

        clusters_admin.ws.clusters.list.return_value = [mock_cluster_item]
        clusters_admin.ws.clusters.get.return_value = mock_cluster_info

        result = clusters_admin.list_idle_clusters()

        assert len(result) == 0

    def test_fallback_to_start_time_when_no_activity(self, clusters_admin):
        """Test fallback to start time when no activity time available."""
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=5)

        mock_cluster_item = Mock()
        mock_cluster_item.cluster_id = "cluster-123"

        mock_cluster_info = Mock(spec=['cluster_id', 'cluster_name', 'state', 'start_time'])
        mock_cluster_info.cluster_id = "cluster-123"
        mock_cluster_info.cluster_name = "No Activity Cluster"
        mock_cluster_info.state = State.RUNNING
        mock_cluster_info.start_time = int(start_time.timestamp() * 1000)
        # No last_activity_time attribute (not in spec)

        clusters_admin.ws.clusters.list.return_value = [mock_cluster_item]
        clusters_admin.ws.clusters.get.return_value = mock_cluster_info

        result = clusters_admin.list_idle_clusters(idle_hours=2.0)

        assert len(result) == 1
        # Should use start_time as last_activity_time (check within 1 second for timestamp precision)
        expected_time = datetime.fromtimestamp(start_time.timestamp(), tz=timezone.utc)
        assert abs((result[0].last_activity_time - expected_time).total_seconds()) < 1.0

    def test_sorting_by_last_activity(self, clusters_admin):
        """Test that results are sorted by last activity (least recent first)."""
        now = datetime.now(timezone.utc)

        # Cluster 1: last activity 3 hours ago
        mock_cluster1 = Mock()
        mock_cluster1.cluster_id = "cluster-1"

        mock_cluster_info1 = Mock()
        mock_cluster_info1.cluster_id = "cluster-1"
        mock_cluster_info1.cluster_name = "More Recently Active"
        mock_cluster_info1.state = State.RUNNING
        mock_cluster_info1.start_time = int((now - timedelta(hours=5)).timestamp() * 1000)
        mock_cluster_info1.last_activity_time = int((now - timedelta(hours=3)).timestamp() * 1000)

        # Cluster 2: last activity 6 hours ago
        mock_cluster2 = Mock()
        mock_cluster2.cluster_id = "cluster-2"

        mock_cluster_info2 = Mock()
        mock_cluster_info2.cluster_id = "cluster-2"
        mock_cluster_info2.cluster_name = "Less Recently Active"
        mock_cluster_info2.state = State.RUNNING
        mock_cluster_info2.start_time = int((now - timedelta(hours=8)).timestamp() * 1000)
        mock_cluster_info2.last_activity_time = int((now - timedelta(hours=6)).timestamp() * 1000)

        clusters_admin.ws.clusters.list.return_value = [mock_cluster1, mock_cluster2]

        def mock_get(cluster_id):
            if cluster_id == "cluster-1":
                return mock_cluster_info1
            elif cluster_id == "cluster-2":
                return mock_cluster_info2

        clusters_admin.ws.clusters.get.side_effect = mock_get

        result = clusters_admin.list_idle_clusters(idle_hours=2.0)

        assert len(result) == 2
        # Should be sorted least recent first
        assert result[0].cluster_name == "Less Recently Active"
        assert result[1].cluster_name == "More Recently Active"

    def test_limit_enforced(self, clusters_admin):
        """Test that limit parameter is enforced."""
        now = datetime.now(timezone.utc)

        # Create 5 idle clusters
        mock_cluster_items = []
        for i in range(5):
            mock_cluster = Mock()
            mock_cluster.cluster_id = f"cluster-{i}"
            mock_cluster_items.append(mock_cluster)

        def mock_get(cluster_id):
            mock_info = Mock()
            mock_info.cluster_id = cluster_id
            mock_info.cluster_name = f"Cluster {cluster_id}"
            mock_info.state = State.RUNNING
            mock_info.start_time = int((now - timedelta(hours=10)).timestamp() * 1000)
            mock_info.last_activity_time = int((now - timedelta(hours=5)).timestamp() * 1000)
            return mock_info

        clusters_admin.ws.clusters.list.return_value = mock_cluster_items
        clusters_admin.ws.clusters.get.side_effect = mock_get

        result = clusters_admin.list_idle_clusters(idle_hours=2.0, limit=3)

        assert len(result) <= 3

    def test_cluster_without_activity_or_start_time(self, clusters_admin):
        """Test handling clusters without activity or start time."""
        mock_cluster_item = Mock()
        mock_cluster_item.cluster_id = "cluster-123"

        mock_cluster_info = Mock(spec=['cluster_id', 'cluster_name', 'state', 'start_time'])
        mock_cluster_info.cluster_id = "cluster-123"
        mock_cluster_info.cluster_name = "No Time Info"
        mock_cluster_info.state = State.RUNNING
        mock_cluster_info.start_time = None
        # No last_activity_time attribute (not in spec)

        clusters_admin.ws.clusters.list.return_value = [mock_cluster_item]
        clusters_admin.ws.clusters.get.return_value = mock_cluster_info

        result = clusters_admin.list_idle_clusters()

        # Should be excluded since we can't determine activity
        assert len(result) == 0

    def test_api_error_handling(self, clusters_admin):
        """Test API error handling."""
        clusters_admin.ws.clusters.list.side_effect = Exception("API error")

        with pytest.raises(APIError, match="Failed to list idle clusters"):
            clusters_admin.list_idle_clusters()
