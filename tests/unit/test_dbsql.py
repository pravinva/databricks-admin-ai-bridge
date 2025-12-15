"""
Unit tests for dbsql module.
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, patch
from databricks.sdk.service.sql import QueryStatus

from admin_ai_bridge.dbsql import DBSQLAdmin
from admin_ai_bridge.config import AdminBridgeConfig
from admin_ai_bridge.errors import ValidationError, APIError
from admin_ai_bridge.schemas import QueryHistoryEntry


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient."""
    mock_client = Mock()
    mock_client.query_history = Mock()
    return mock_client


@pytest.fixture
def dbsql_admin(mock_workspace_client):
    """Create DBSQLAdmin instance with mocked client."""
    with patch('admin_ai_bridge.dbsql.get_workspace_client', return_value=mock_workspace_client):
        admin = DBSQLAdmin(AdminBridgeConfig(profile="TEST"))
    return admin


class TestDBSQLAdminInit:
    """Test DBSQLAdmin initialization."""

    def test_init_with_config(self):
        """Test initialization with config."""
        with patch('admin_ai_bridge.dbsql.get_workspace_client') as mock_get_client:
            cfg = AdminBridgeConfig(profile="TEST")
            admin = DBSQLAdmin(cfg)
            mock_get_client.assert_called_once_with(cfg)
            assert admin.ws is not None

    def test_init_without_config(self):
        """Test initialization without config."""
        with patch('admin_ai_bridge.dbsql.get_workspace_client') as mock_get_client:
            admin = DBSQLAdmin()
            mock_get_client.assert_called_once_with(None)
            assert admin.ws is not None


class TestTopSlowestQueries:
    """Test top_slowest_queries method."""

    def test_validation_negative_lookback(self, dbsql_admin):
        """Test validation fails with negative lookback_hours."""
        with pytest.raises(ValidationError, match="lookback_hours must be positive"):
            dbsql_admin.top_slowest_queries(lookback_hours=-1.0)

    def test_validation_negative_limit(self, dbsql_admin):
        """Test validation fails with negative limit."""
        with pytest.raises(ValidationError, match="limit must be positive"):
            dbsql_admin.top_slowest_queries(limit=-1)

    def test_no_queries(self, dbsql_admin):
        """Test with no queries in history."""
        dbsql_admin.ws.query_history.list.return_value = []

        result = dbsql_admin.top_slowest_queries()

        assert result == []
        dbsql_admin.ws.query_history.list.assert_called_once()

    def test_single_slow_query(self, dbsql_admin):
        """Test finding a single slow query."""
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(minutes=10)

        mock_query = Mock()
        mock_query.query_id = "query-123"
        mock_query.warehouse_id = "warehouse-456"
        mock_query.user_name = "user@example.com"
        mock_query.status = QueryStatus.FINISHED
        mock_query.query_start_time_ms = int(start_time.timestamp() * 1000)
        mock_query.query_end_time_ms = int(now.timestamp() * 1000)
        mock_query.query_text = "SELECT * FROM large_table"

        dbsql_admin.ws.query_history.list.return_value = [mock_query]

        result = dbsql_admin.top_slowest_queries(lookback_hours=24.0, limit=20)

        assert len(result) == 1
        assert result[0].query_id == "query-123"
        assert result[0].warehouse_id == "warehouse-456"
        assert result[0].user_name == "user@example.com"
        assert result[0].status == "FINISHED"
        assert result[0].duration_seconds == pytest.approx(600.0, rel=1e-2)
        assert result[0].sql_text == "SELECT * FROM large_table"

    def test_sorting_by_duration(self, dbsql_admin):
        """Test that results are sorted by duration (slowest first)."""
        now = datetime.now(timezone.utc)

        # Create queries with different durations
        mock_queries = []
        for i, minutes in enumerate([5, 15, 10]):
            mock_query = Mock()
            mock_query.query_id = f"query-{i}"
            mock_query.warehouse_id = "warehouse-456"
            mock_query.user_name = "user@example.com"
            mock_query.status = QueryStatus.FINISHED
            start_time = now - timedelta(minutes=minutes)
            mock_query.query_start_time_ms = int(start_time.timestamp() * 1000)
            mock_query.query_end_time_ms = int(now.timestamp() * 1000)
            mock_queries.append(mock_query)

        dbsql_admin.ws.query_history.list.return_value = mock_queries

        result = dbsql_admin.top_slowest_queries()

        assert len(result) == 3
        # Should be sorted slowest first: 15min, 10min, 5min
        assert result[0].query_id == "query-1"
        assert result[1].query_id == "query-2"
        assert result[2].query_id == "query-0"

    def test_limit_enforced(self, dbsql_admin):
        """Test that limit parameter is enforced."""
        now = datetime.now(timezone.utc)

        # Create 10 queries
        mock_queries = []
        for i in range(10):
            mock_query = Mock()
            mock_query.query_id = f"query-{i}"
            mock_query.warehouse_id = "warehouse-456"
            mock_query.user_name = "user@example.com"
            mock_query.status = QueryStatus.FINISHED
            start_time = now - timedelta(minutes=i+1)
            mock_query.query_start_time_ms = int(start_time.timestamp() * 1000)
            mock_query.query_end_time_ms = int(now.timestamp() * 1000)
            mock_queries.append(mock_query)

        dbsql_admin.ws.query_history.list.return_value = mock_queries

        result = dbsql_admin.top_slowest_queries(limit=5)

        assert len(result) == 5

    def test_filter_queries_without_times(self, dbsql_admin):
        """Test that queries without start/end times are filtered out."""
        now = datetime.now(timezone.utc)

        # Query with no start time
        mock_query1 = Mock()
        mock_query1.query_id = "query-1"
        mock_query1.warehouse_id = "warehouse-456"
        mock_query1.user_name = "user@example.com"
        mock_query1.status = QueryStatus.FINISHED
        mock_query1.query_start_time_ms = None
        mock_query1.query_end_time_ms = int(now.timestamp() * 1000)

        # Query with no end time
        mock_query2 = Mock()
        mock_query2.query_id = "query-2"
        mock_query2.warehouse_id = "warehouse-456"
        mock_query2.user_name = "user@example.com"
        mock_query2.status = QueryStatus.RUNNING
        mock_query2.query_start_time_ms = int((now - timedelta(minutes=5)).timestamp() * 1000)
        mock_query2.query_end_time_ms = None

        # Valid query
        mock_query3 = Mock()
        mock_query3.query_id = "query-3"
        mock_query3.warehouse_id = "warehouse-456"
        mock_query3.user_name = "user@example.com"
        mock_query3.status = QueryStatus.FINISHED
        start_time = now - timedelta(minutes=5)
        mock_query3.query_start_time_ms = int(start_time.timestamp() * 1000)
        mock_query3.query_end_time_ms = int(now.timestamp() * 1000)

        dbsql_admin.ws.query_history.list.return_value = [mock_query1, mock_query2, mock_query3]

        result = dbsql_admin.top_slowest_queries()

        assert len(result) == 1
        assert result[0].query_id == "query-3"

    def test_api_error_handling(self, dbsql_admin):
        """Test API error handling."""
        dbsql_admin.ws.query_history.list.side_effect = Exception("API error")

        with pytest.raises(APIError, match="Failed to list query history"):
            dbsql_admin.top_slowest_queries()


class TestUserQuerySummary:
    """Test user_query_summary method."""

    def test_validation_empty_username(self, dbsql_admin):
        """Test validation fails with empty username."""
        with pytest.raises(ValidationError, match="user_name must not be empty"):
            dbsql_admin.user_query_summary(user_name="")

    def test_validation_whitespace_username(self, dbsql_admin):
        """Test validation fails with whitespace-only username."""
        with pytest.raises(ValidationError, match="user_name must not be empty"):
            dbsql_admin.user_query_summary(user_name="   ")

    def test_validation_negative_lookback(self, dbsql_admin):
        """Test validation fails with negative lookback_hours."""
        with pytest.raises(ValidationError, match="lookback_hours must be positive"):
            dbsql_admin.user_query_summary(user_name="user@example.com", lookback_hours=-1.0)

    def test_no_queries_for_user(self, dbsql_admin):
        """Test summary with no queries for user."""
        dbsql_admin.ws.query_history.list.return_value = []

        result = dbsql_admin.user_query_summary(user_name="user@example.com")

        assert result["user_name"] == "user@example.com"
        assert result["total_queries"] == 0
        assert result["successful_queries"] == 0
        assert result["failed_queries"] == 0
        assert result["avg_duration_seconds"] == 0.0
        assert result["max_duration_seconds"] == 0.0
        assert result["min_duration_seconds"] == 0.0
        assert result["total_duration_seconds"] == 0.0
        assert result["failure_rate"] == 0.0
        assert result["warehouses_used"] == []

    def test_summary_with_single_query(self, dbsql_admin):
        """Test summary with a single query."""
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(minutes=5)

        mock_query = Mock()
        mock_query.query_id = "query-123"
        mock_query.warehouse_id = "warehouse-456"
        mock_query.user_name = "user@example.com"
        mock_query.status = QueryStatus.FINISHED
        mock_query.query_start_time_ms = int(start_time.timestamp() * 1000)
        mock_query.query_end_time_ms = int(now.timestamp() * 1000)

        dbsql_admin.ws.query_history.list.return_value = [mock_query]

        result = dbsql_admin.user_query_summary(user_name="user@example.com")

        assert result["user_name"] == "user@example.com"
        assert result["total_queries"] == 1
        assert result["successful_queries"] == 1
        assert result["failed_queries"] == 0
        assert result["avg_duration_seconds"] == pytest.approx(300.0, rel=1e-2)
        assert result["max_duration_seconds"] == pytest.approx(300.0, rel=1e-2)
        assert result["min_duration_seconds"] == pytest.approx(300.0, rel=1e-2)
        assert result["total_duration_seconds"] == pytest.approx(300.0, rel=1e-2)
        assert result["failure_rate"] == 0.0
        assert result["warehouses_used"] == ["warehouse-456"]

    def test_summary_with_multiple_queries(self, dbsql_admin):
        """Test summary with multiple queries."""
        now = datetime.now(timezone.utc)

        mock_queries = []

        # Successful query 1 (5 minutes)
        mock_query1 = Mock()
        mock_query1.query_id = "query-1"
        mock_query1.warehouse_id = "warehouse-1"
        mock_query1.user_name = "user@example.com"
        mock_query1.status = QueryStatus.FINISHED
        mock_query1.query_start_time_ms = int((now - timedelta(minutes=5)).timestamp() * 1000)
        mock_query1.query_end_time_ms = int(now.timestamp() * 1000)
        mock_queries.append(mock_query1)

        # Successful query 2 (10 minutes)
        mock_query2 = Mock()
        mock_query2.query_id = "query-2"
        mock_query2.warehouse_id = "warehouse-2"
        mock_query2.user_name = "user@example.com"
        mock_query2.status = QueryStatus.FINISHED
        mock_query2.query_start_time_ms = int((now - timedelta(minutes=10)).timestamp() * 1000)
        mock_query2.query_end_time_ms = int(now.timestamp() * 1000)
        mock_queries.append(mock_query2)

        # Failed query (2 minutes)
        mock_query3 = Mock()
        mock_query3.query_id = "query-3"
        mock_query3.warehouse_id = "warehouse-1"
        mock_query3.user_name = "user@example.com"
        mock_query3.status = QueryStatus.FAILED
        mock_query3.query_start_time_ms = int((now - timedelta(minutes=2)).timestamp() * 1000)
        mock_query3.query_end_time_ms = int(now.timestamp() * 1000)
        mock_queries.append(mock_query3)

        dbsql_admin.ws.query_history.list.return_value = mock_queries

        result = dbsql_admin.user_query_summary(user_name="user@example.com")

        assert result["user_name"] == "user@example.com"
        assert result["total_queries"] == 3
        assert result["successful_queries"] == 2
        assert result["failed_queries"] == 1
        # Average: (300 + 600 + 120) / 3 = 340
        assert result["avg_duration_seconds"] == pytest.approx(340.0, rel=1e-2)
        assert result["max_duration_seconds"] == pytest.approx(600.0, rel=1e-2)
        assert result["min_duration_seconds"] == pytest.approx(120.0, rel=1e-2)
        assert result["total_duration_seconds"] == pytest.approx(1020.0, rel=1e-2)
        # Failure rate: 1/3 = 33.33%
        assert result["failure_rate"] == pytest.approx(33.33, rel=1e-2)
        assert set(result["warehouses_used"]) == {"warehouse-1", "warehouse-2"}

    def test_summary_with_canceled_queries(self, dbsql_admin):
        """Test summary counts canceled queries as failed."""
        now = datetime.now(timezone.utc)

        mock_query = Mock()
        mock_query.query_id = "query-1"
        mock_query.warehouse_id = "warehouse-1"
        mock_query.user_name = "user@example.com"
        mock_query.status = QueryStatus.CANCELED
        mock_query.query_start_time_ms = int((now - timedelta(minutes=1)).timestamp() * 1000)
        mock_query.query_end_time_ms = int(now.timestamp() * 1000)

        dbsql_admin.ws.query_history.list.return_value = [mock_query]

        result = dbsql_admin.user_query_summary(user_name="user@example.com")

        assert result["total_queries"] == 1
        assert result["successful_queries"] == 0
        assert result["failed_queries"] == 1
        assert result["failure_rate"] == 100.0

    def test_summary_ignores_queries_without_duration(self, dbsql_admin):
        """Test that queries without duration are counted but not in duration stats."""
        now = datetime.now(timezone.utc)

        # Query with duration
        mock_query1 = Mock()
        mock_query1.query_id = "query-1"
        mock_query1.warehouse_id = "warehouse-1"
        mock_query1.user_name = "user@example.com"
        mock_query1.status = QueryStatus.FINISHED
        mock_query1.query_start_time_ms = int((now - timedelta(minutes=5)).timestamp() * 1000)
        mock_query1.query_end_time_ms = int(now.timestamp() * 1000)

        # Query without end time (still running)
        mock_query2 = Mock()
        mock_query2.query_id = "query-2"
        mock_query2.warehouse_id = "warehouse-1"
        mock_query2.user_name = "user@example.com"
        mock_query2.status = QueryStatus.RUNNING
        mock_query2.query_start_time_ms = int((now - timedelta(minutes=1)).timestamp() * 1000)
        mock_query2.query_end_time_ms = None

        dbsql_admin.ws.query_history.list.return_value = [mock_query1, mock_query2]

        result = dbsql_admin.user_query_summary(user_name="user@example.com")

        assert result["total_queries"] == 2
        assert result["successful_queries"] == 1
        # Duration stats based only on query-1
        assert result["avg_duration_seconds"] == pytest.approx(300.0, rel=1e-2)

    def test_time_window_in_result(self, dbsql_admin):
        """Test that time window is included in result."""
        dbsql_admin.ws.query_history.list.return_value = []

        result = dbsql_admin.user_query_summary(
            user_name="user@example.com",
            lookback_hours=48.0
        )

        assert "time_window_start" in result
        assert "time_window_end" in result
        # Verify the window is approximately 48 hours
        start = datetime.fromisoformat(result["time_window_start"])
        end = datetime.fromisoformat(result["time_window_end"])
        window_hours = (end - start).total_seconds() / 3600
        assert window_hours == pytest.approx(48.0, rel=1e-2)

    def test_api_error_handling(self, dbsql_admin):
        """Test API error handling."""
        dbsql_admin.ws.query_history.list.side_effect = Exception("API error")

        with pytest.raises(APIError, match="Failed to get query summary"):
            dbsql_admin.user_query_summary(user_name="user@example.com")

    def test_multiple_warehouses(self, dbsql_admin):
        """Test tracking multiple warehouses."""
        now = datetime.now(timezone.utc)

        mock_queries = []
        for i, warehouse_id in enumerate(["wh-1", "wh-2", "wh-1", "wh-3"]):
            mock_query = Mock()
            mock_query.query_id = f"query-{i}"
            mock_query.warehouse_id = warehouse_id
            mock_query.user_name = "user@example.com"
            mock_query.status = QueryStatus.FINISHED
            mock_query.query_start_time_ms = int((now - timedelta(minutes=1)).timestamp() * 1000)
            mock_query.query_end_time_ms = int(now.timestamp() * 1000)
            mock_queries.append(mock_query)

        dbsql_admin.ws.query_history.list.return_value = mock_queries

        result = dbsql_admin.user_query_summary(user_name="user@example.com")

        assert result["total_queries"] == 4
        # Should have unique warehouses, sorted
        assert result["warehouses_used"] == ["wh-1", "wh-2", "wh-3"]
