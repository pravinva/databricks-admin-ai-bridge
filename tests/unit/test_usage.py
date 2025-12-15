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
    admin = UsageAdmin(cfg)
    # Mock table exists to return True by default for tests
    admin._table_exists = lambda table: True
    return admin


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


class TestCostByDimension:
    """Tests for cost_by_dimension method."""

    def test_cost_by_dimension_workspace(self, usage_admin, mock_workspace_client):
        """Test cost aggregation by workspace dimension."""
        # Mock warehouse for SQL execution
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock SQL statement execution result
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = [
            ["workspace-1", 1500.50, 750.25, "2024-11-15 00:00:00", "2024-12-15 00:00:00"],
            ["workspace-2", 1200.75, 600.50, "2024-11-15 00:00:00", "2024-12-15 00:00:00"],
            ["workspace-3", 800.25, 400.10, "2024-11-15 00:00:00", "2024-12-15 00:00:00"],
        ]
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method
        result = usage_admin.cost_by_dimension(dimension="workspace", lookback_days=30)

        # Verify
        assert len(result) == 3
        assert all(isinstance(entry, UsageEntry) for entry in result)
        assert result[0].scope == "workspace"
        assert result[0].name == "workspace-1"
        assert result[0].cost == 1500.50
        assert result[0].dbus == 750.25

        # Verify SQL execution was called
        mock_workspace_client.statement_execution.execute_statement.assert_called_once()
        call_args = mock_workspace_client.statement_execution.execute_statement.call_args
        assert call_args[1]["warehouse_id"] == "warehouse-123"
        assert "workspace_id" in call_args[1]["statement"]
        assert "billing.usage_events" in call_args[1]["statement"]

    def test_cost_by_dimension_cluster(self, usage_admin, mock_workspace_client):
        """Test cost aggregation by cluster dimension."""
        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock SQL result
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = [
            ["cluster-abc", 2500.00, 1250.00, "2024-11-15 00:00:00", "2024-12-15 00:00:00"],
            ["cluster-xyz", 1800.00, 900.00, "2024-11-15 00:00:00", "2024-12-15 00:00:00"],
        ]
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method
        result = usage_admin.cost_by_dimension(dimension="cluster", lookback_days=30)

        # Verify
        assert len(result) == 2
        assert result[0].scope == "cluster"
        assert result[0].name == "cluster-abc"
        assert result[0].cost == 2500.00

        # Verify SQL includes cluster_id
        call_args = mock_workspace_client.statement_execution.execute_statement.call_args
        assert "cluster_id" in call_args[1]["statement"]

    def test_cost_by_dimension_tag_project(self, usage_admin, mock_workspace_client):
        """Test cost aggregation by tag dimension (tag:project)."""
        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock SQL result
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = [
            ["project-alpha", 5000.00, 2500.00, "2024-11-15 00:00:00", "2024-12-15 00:00:00"],
            ["project-beta", 3500.00, 1750.00, "2024-11-15 00:00:00", "2024-12-15 00:00:00"],
            ["project-gamma", 2000.00, 1000.00, "2024-11-15 00:00:00", "2024-12-15 00:00:00"],
        ]
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method
        result = usage_admin.cost_by_dimension(dimension="tag:project", lookback_days=7)

        # Verify
        assert len(result) == 3
        assert result[0].scope == "tag"
        assert result[0].name == "project-alpha"
        assert result[0].cost == 5000.00

        # Verify SQL includes tag reference
        call_args = mock_workspace_client.statement_execution.execute_statement.call_args
        assert "tags['project']" in call_args[1]["statement"]

    def test_cost_by_dimension_job(self, usage_admin, mock_workspace_client):
        """Test cost aggregation by job dimension."""
        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock SQL result
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = [
            ["job-123", 1000.00, 500.00, "2024-11-15 00:00:00", "2024-12-15 00:00:00"],
        ]
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method
        result = usage_admin.cost_by_dimension(dimension="job", lookback_days=30)

        # Verify
        assert len(result) == 1
        assert result[0].scope == "job"
        assert result[0].name == "job-123"

        # Verify SQL includes job_id
        call_args = mock_workspace_client.statement_execution.execute_statement.call_args
        assert "job_id" in call_args[1]["statement"]

    def test_cost_by_dimension_warehouse(self, usage_admin, mock_workspace_client):
        """Test cost aggregation by warehouse dimension."""
        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock SQL result
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = [
            ["warehouse-abc", 3000.00, 1500.00, "2024-11-15 00:00:00", "2024-12-15 00:00:00"],
            ["warehouse-xyz", 2000.00, 1000.00, "2024-11-15 00:00:00", "2024-12-15 00:00:00"],
        ]
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method
        result = usage_admin.cost_by_dimension(dimension="warehouse", lookback_days=30)

        # Verify
        assert len(result) == 2
        assert result[0].scope == "warehouse"

        # Verify SQL includes warehouse_id
        call_args = mock_workspace_client.statement_execution.execute_statement.call_args
        assert "warehouse_id" in call_args[1]["statement"]

    def test_cost_by_dimension_limit(self, usage_admin, mock_workspace_client):
        """Test that limit parameter is respected."""
        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock SQL result with many entries
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = [
            [f"workspace-{i}", 1000.00 - i, 500.00 - i, "2024-11-15 00:00:00", "2024-12-15 00:00:00"]
            for i in range(50)
        ]
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method with limit
        result = usage_admin.cost_by_dimension(dimension="workspace", limit=10)

        # Verify - note: limit is applied in SQL, not in Python
        # So we need to verify the SQL query contains LIMIT
        call_args = mock_workspace_client.statement_execution.execute_statement.call_args
        assert "LIMIT 10" in call_args[1]["statement"]

    def test_cost_by_dimension_custom_table_names(self, mock_workspace_client):
        """Test with custom table names."""
        # Create admin with custom table names
        usage_admin = UsageAdmin(
            usage_table="custom_schema.usage_data",
            budget_table="custom_schema.budgets"
        )
        # Mock table exists for this instance
        usage_admin._table_exists = lambda table: True

        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock SQL result
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = []
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method
        usage_admin.cost_by_dimension(dimension="workspace")

        # Verify custom table name is used
        call_args = mock_workspace_client.statement_execution.execute_statement.call_args
        assert "custom_schema.usage_data" in call_args[1]["statement"]

    def test_cost_by_dimension_invalid_lookback_days(self, usage_admin):
        """Test with invalid lookback_days."""
        with pytest.raises(ValidationError, match="lookback_days must be positive"):
            usage_admin.cost_by_dimension(dimension="workspace", lookback_days=0)

        with pytest.raises(ValidationError, match="lookback_days must be positive"):
            usage_admin.cost_by_dimension(dimension="workspace", lookback_days=-1)

    def test_cost_by_dimension_invalid_limit(self, usage_admin):
        """Test with invalid limit."""
        with pytest.raises(ValidationError, match="limit must be positive"):
            usage_admin.cost_by_dimension(dimension="workspace", limit=0)

        with pytest.raises(ValidationError, match="limit must be positive"):
            usage_admin.cost_by_dimension(dimension="workspace", limit=-1)

    def test_cost_by_dimension_unsupported_dimension(self, usage_admin):
        """Test with unsupported dimension."""
        with pytest.raises(ValidationError, match="Unsupported dimension"):
            usage_admin.cost_by_dimension(dimension="invalid_dimension")

    def test_cost_by_dimension_empty_tag_key(self, usage_admin):
        """Test with tag dimension but empty key."""
        with pytest.raises(ValidationError, match="Tag dimension must specify a key"):
            usage_admin.cost_by_dimension(dimension="tag:")

    def test_cost_by_dimension_empty_results(self, usage_admin, mock_workspace_client):
        """Test when no cost data is found."""
        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock empty SQL result
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = []
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method
        result = usage_admin.cost_by_dimension(dimension="workspace")

        # Verify
        assert len(result) == 0

    def test_cost_by_dimension_api_error(self, usage_admin, mock_workspace_client):
        """Test API error handling."""
        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock SQL execution error
        mock_workspace_client.statement_execution.execute_statement.side_effect = Exception("SQL error")

        with pytest.raises(APIError, match="Failed to query cost by dimension"):
            usage_admin.cost_by_dimension(dimension="workspace")

    def test_cost_by_dimension_no_warehouse_error(self, usage_admin, mock_workspace_client):
        """Test error when no warehouse is available."""
        # Mock no warehouses available
        mock_workspace_client.warehouses.list.return_value = []

        with pytest.raises(APIError, match="No SQL warehouses available"):
            usage_admin.cost_by_dimension(dimension="workspace")

    def test_cost_by_dimension_with_warehouse_id(self, mock_workspace_client):
        """Test with explicit warehouse_id."""
        # Create admin with specific warehouse
        usage_admin = UsageAdmin(warehouse_id="my-warehouse-123")
        # Mock table exists for this instance
        usage_admin._table_exists = lambda table: True

        # Mock SQL result
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = []
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method
        usage_admin.cost_by_dimension(dimension="workspace")

        # Verify correct warehouse ID is used
        call_args = mock_workspace_client.statement_execution.execute_statement.call_args
        assert call_args[1]["warehouse_id"] == "my-warehouse-123"


class TestBudgetStatus:
    """Tests for budget_status method."""

    def test_budget_status_within_budget(self, usage_admin, mock_workspace_client):
        """Test budget status when all entities are within budget."""
        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock SQL result - all within budget
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = [
            ["workspace-1", 500.00, 1000.00],  # 50% utilization
            ["workspace-2", 300.00, 1000.00],  # 30% utilization
            ["workspace-3", 700.00, 1000.00],  # 70% utilization
        ]
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method
        result = usage_admin.budget_status(dimension="workspace", period_days=30)

        # Verify
        assert len(result) == 3
        assert all(status["status"] == "within_budget" for status in result)
        assert result[0]["dimension_value"] == "workspace-1"
        assert result[0]["actual_cost"] == 500.00
        assert result[0]["budget_amount"] == 1000.00
        assert result[0]["utilization_pct"] == 50.0

        # Verify SQL execution
        mock_workspace_client.statement_execution.execute_statement.assert_called_once()
        call_args = mock_workspace_client.statement_execution.execute_statement.call_args
        assert "billing.usage_events" in call_args[1]["statement"]
        assert "billing.budgets" in call_args[1]["statement"]

    def test_budget_status_warning(self, usage_admin, mock_workspace_client):
        """Test budget status with warning threshold."""
        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock SQL result - some at warning level
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = [
            ["workspace-1", 850.00, 1000.00],  # 85% - warning
            ["workspace-2", 900.00, 1000.00],  # 90% - warning
            ["workspace-3", 500.00, 1000.00],  # 50% - within budget
        ]
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method with default warn_threshold (0.8)
        result = usage_admin.budget_status(dimension="workspace", period_days=30)

        # Verify
        assert len(result) == 3
        assert result[0]["status"] == "warning"
        assert result[0]["utilization_pct"] == 85.0
        assert result[1]["status"] == "warning"
        assert result[1]["utilization_pct"] == 90.0
        assert result[2]["status"] == "within_budget"

    def test_budget_status_breached(self, usage_admin, mock_workspace_client):
        """Test budget status when budgets are breached."""
        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock SQL result - some breached
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = [
            ["workspace-1", 1200.00, 1000.00],  # 120% - breached
            ["workspace-2", 1500.00, 1000.00],  # 150% - breached
            ["workspace-3", 950.00, 1000.00],   # 95% - warning
            ["workspace-4", 500.00, 1000.00],   # 50% - within budget
        ]
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method
        result = usage_admin.budget_status(dimension="workspace", period_days=30)

        # Verify
        assert len(result) == 4
        assert result[0]["status"] == "breached"
        assert result[0]["utilization_pct"] == 120.0
        assert result[1]["status"] == "breached"
        assert result[1]["utilization_pct"] == 150.0
        assert result[2]["status"] == "warning"
        assert result[3]["status"] == "within_budget"

    def test_budget_status_custom_warn_threshold(self, usage_admin, mock_workspace_client):
        """Test budget status with custom warning threshold."""
        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock SQL result
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = [
            ["workspace-1", 850.00, 1000.00],  # 85%
            ["workspace-2", 950.00, 1000.00],  # 95%
        ]
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method with warn_threshold=0.9 (90%)
        result = usage_admin.budget_status(dimension="workspace", warn_threshold=0.9)

        # Verify - 85% should be within budget, 95% should be warning
        assert result[0]["status"] == "within_budget"
        assert result[1]["status"] == "warning"

    def test_budget_status_project_dimension(self, usage_admin, mock_workspace_client):
        """Test budget status for project dimension (tag-based)."""
        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock SQL result
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = [
            ["project-alpha", 5000.00, 10000.00],  # 50%
            ["project-beta", 8500.00, 10000.00],   # 85% - warning
        ]
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method
        result = usage_admin.budget_status(dimension="project", period_days=30)

        # Verify
        assert len(result) == 2
        assert result[0]["dimension_value"] == "project-alpha"
        assert result[0]["status"] == "within_budget"
        assert result[1]["status"] == "warning"

        # Verify SQL uses tags for project dimension
        call_args = mock_workspace_client.statement_execution.execute_statement.call_args
        assert "tags['project']" in call_args[1]["statement"]

    def test_budget_status_team_dimension(self, usage_admin, mock_workspace_client):
        """Test budget status for team dimension (tag-based)."""
        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock SQL result
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = [
            ["team-data-science", 15000.00, 20000.00],  # 75%
            ["team-engineering", 18000.00, 20000.00],   # 90% - warning
        ]
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method
        result = usage_admin.budget_status(dimension="team", period_days=30)

        # Verify
        assert len(result) == 2
        assert result[0]["status"] == "within_budget"
        assert result[1]["status"] == "warning"

        # Verify SQL uses tags for team dimension
        call_args = mock_workspace_client.statement_execution.execute_statement.call_args
        assert "tags['team']" in call_args[1]["statement"]

    def test_budget_status_zero_budget(self, usage_admin, mock_workspace_client):
        """Test budget status when budget amount is zero."""
        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock SQL result with zero budget
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = [
            ["workspace-1", 100.00, 0.00],  # Zero budget with cost = breached
            ["workspace-2", 0.00, 0.00],    # Zero budget, zero cost = within budget
        ]
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method
        result = usage_admin.budget_status(dimension="workspace")

        # Verify - zero budget with cost should be breached
        assert result[0]["status"] == "breached"
        assert result[0]["utilization_pct"] == float('inf')
        # Zero budget with zero cost should be within budget
        assert result[1]["status"] == "within_budget"
        assert result[1]["utilization_pct"] == 0.0

    def test_budget_status_empty_results(self, usage_admin, mock_workspace_client):
        """Test when no budget data is found."""
        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock empty SQL result
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = []
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method
        result = usage_admin.budget_status(dimension="workspace")

        # Verify
        assert len(result) == 0

    def test_budget_status_custom_table_names(self, mock_workspace_client):
        """Test with custom table names."""
        # Create admin with custom table names
        usage_admin = UsageAdmin(
            usage_table="custom_schema.usage_data",
            budget_table="custom_schema.budgets"
        )
        # Mock table exists for this instance
        usage_admin._table_exists = lambda table: True

        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock SQL result
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = []
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method
        usage_admin.budget_status(dimension="workspace")

        # Verify custom table names are used
        call_args = mock_workspace_client.statement_execution.execute_statement.call_args
        assert "custom_schema.usage_data" in call_args[1]["statement"]
        assert "custom_schema.budgets" in call_args[1]["statement"]

    def test_budget_status_invalid_period_days(self, usage_admin):
        """Test with invalid period_days."""
        with pytest.raises(ValidationError, match="period_days must be positive"):
            usage_admin.budget_status(dimension="workspace", period_days=0)

        with pytest.raises(ValidationError, match="period_days must be positive"):
            usage_admin.budget_status(dimension="workspace", period_days=-1)

    def test_budget_status_invalid_warn_threshold(self, usage_admin):
        """Test with invalid warn_threshold."""
        with pytest.raises(ValidationError, match="warn_threshold must be between 0 and 1"):
            usage_admin.budget_status(dimension="workspace", warn_threshold=-0.1)

        with pytest.raises(ValidationError, match="warn_threshold must be between 0 and 1"):
            usage_admin.budget_status(dimension="workspace", warn_threshold=1.5)

    def test_budget_status_api_error(self, usage_admin, mock_workspace_client):
        """Test API error handling."""
        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock SQL execution error
        mock_workspace_client.statement_execution.execute_statement.side_effect = Exception("SQL error")

        with pytest.raises(APIError, match="Failed to query budget status"):
            usage_admin.budget_status(dimension="workspace")

    def test_budget_status_no_warehouse_error(self, usage_admin, mock_workspace_client):
        """Test error when no warehouse is available."""
        # Mock no warehouses available
        mock_workspace_client.warehouses.list.return_value = []

        with pytest.raises(APIError, match="No SQL warehouses available"):
            usage_admin.budget_status(dimension="workspace")

    def test_budget_status_with_warehouse_id(self, mock_workspace_client):
        """Test with explicit warehouse_id."""
        # Create admin with specific warehouse
        usage_admin = UsageAdmin(warehouse_id="my-warehouse-123")
        # Mock table exists for this instance
        usage_admin._table_exists = lambda table: True

        # Mock SQL result
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = []
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method
        usage_admin.budget_status(dimension="workspace")

        # Verify correct warehouse ID is used
        call_args = mock_workspace_client.statement_execution.execute_statement.call_args
        assert call_args[1]["warehouse_id"] == "my-warehouse-123"

    def test_budget_status_mixed_scenarios(self, usage_admin, mock_workspace_client):
        """Test budget status with mixed scenarios (within budget, warning, breached)."""
        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "warehouse-123"
        mock_workspace_client.warehouses.list.return_value = [mock_warehouse]

        # Mock SQL result with mixed scenarios
        mock_statement = MagicMock()
        mock_statement.result = MagicMock()
        mock_statement.result.data_array = [
            ["workspace-1", 500.00, 1000.00],   # 50% - within budget
            ["workspace-2", 850.00, 1000.00],   # 85% - warning
            ["workspace-3", 1200.00, 1000.00],  # 120% - breached
            ["workspace-4", 700.00, 1000.00],   # 70% - within budget
            ["workspace-5", 950.00, 1000.00],   # 95% - warning
        ]
        mock_workspace_client.statement_execution.execute_statement.return_value = mock_statement

        # Call method
        result = usage_admin.budget_status(dimension="workspace", period_days=30)

        # Verify counts by status
        within_budget = [r for r in result if r["status"] == "within_budget"]
        warning = [r for r in result if r["status"] == "warning"]
        breached = [r for r in result if r["status"] == "breached"]

        assert len(within_budget) == 2
        assert len(warning) == 2
        assert len(breached) == 1

        # Verify specific values
        assert breached[0]["dimension_value"] == "workspace-3"
        assert breached[0]["actual_cost"] == 1200.00
