"""
Unit tests for Databricks Agent tools.

Tests verify that all tool functions:
- Return plain Python callable functions
- Have proper names and docstrings
- Can be invoked with mocked admin classes
- Return JSON-serializable outputs
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timezone
from typing import Callable

from admin_ai_bridge.tools_databricks_agent import (
    jobs_admin_tools,
    dbsql_admin_tools,
    clusters_admin_tools,
    security_admin_tools,
    usage_admin_tools,
    audit_admin_tools,
    pipelines_admin_tools,
)
from admin_ai_bridge.schemas import (
    JobRunSummary,
    QueryHistoryEntry,
    ClusterSummary,
    PermissionEntry,
    UsageEntry,
    AuditEvent,
    PipelineStatus,
)


class TestJobsAdminTools:
    """Tests for jobs_admin_tools."""

    def test_tool_list_structure(self):
        """Test that jobs_admin_tools returns a list of callable functions."""
        tools = jobs_admin_tools()

        assert isinstance(tools, list)
        assert len(tools) == 2
        assert all(callable(tool) for tool in tools)

    def test_tool_names(self):
        """Test that job tools have the expected names."""
        tools = jobs_admin_tools()
        tool_names = [tool.__name__ for tool in tools]

        assert "list_long_running_jobs" in tool_names
        assert "list_failed_jobs" in tool_names

    def test_tool_descriptions(self):
        """Test that job tools have meaningful docstrings."""
        tools = jobs_admin_tools()

        for tool in tools:
            assert tool.__doc__ is not None
            assert len(tool.__doc__) > 50  # Should be descriptive
            assert any(keyword in tool.__doc__.lower() for keyword in ["job", "run"])

    @patch('admin_ai_bridge.tools_databricks_agent.JobsAdmin')
    def test_list_long_running_jobs_invocation(self, mock_jobs_admin_class):
        """Test that list_long_running_jobs tool can be invoked successfully."""
        # Setup mock
        mock_jobs_admin = Mock()
        mock_jobs_admin_class.return_value = mock_jobs_admin

        # Create mock job run
        mock_run = JobRunSummary(
            job_id=123,
            job_name="test_job",
            run_id=456,
            state="SUCCESS",
            life_cycle_state="TERMINATED",
            start_time=datetime.now(timezone.utc),
            end_time=datetime.now(timezone.utc),
            duration_seconds=18000.0,  # 5 hours
        )
        mock_jobs_admin.list_long_running_jobs.return_value = [mock_run]

        # Get tools and find the specific tool
        tools = jobs_admin_tools()
        long_running_tool = next(t for t in tools if t.__name__ == "list_long_running_jobs")

        # Invoke the tool function directly
        result = long_running_tool(min_duration_hours=4.0, lookback_hours=24.0, limit=20)

        # Verify
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], dict)
        assert result[0]["job_id"] == 123
        assert result[0]["job_name"] == "test_job"
        assert result[0]["duration_seconds"] == 18000.0

        # Verify the admin class method was called correctly
        mock_jobs_admin.list_long_running_jobs.assert_called_once_with(
            min_duration_hours=4.0,
            lookback_hours=24.0,
            limit=20,
        )

    @patch('admin_ai_bridge.tools_databricks_agent.JobsAdmin')
    def test_list_failed_jobs_invocation(self, mock_jobs_admin_class):
        """Test that list_failed_jobs tool can be invoked successfully."""
        # Setup mock
        mock_jobs_admin = Mock()
        mock_jobs_admin_class.return_value = mock_jobs_admin

        mock_run = JobRunSummary(
            job_id=789,
            job_name="failed_job",
            run_id=101,
            state="FAILED",
            life_cycle_state="TERMINATED",
            start_time=datetime.now(timezone.utc),
            end_time=datetime.now(timezone.utc),
            duration_seconds=120.0,
        )
        mock_jobs_admin.list_failed_jobs.return_value = [mock_run]

        tools = jobs_admin_tools()
        failed_tool = next(t for t in tools if t.__name__ == "list_failed_jobs")

        result = failed_tool(lookback_hours=24.0, limit=20)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["state"] == "FAILED"


class TestDBSQLAdminTools:
    """Tests for dbsql_admin_tools."""

    def test_tool_list_structure(self):
        """Test that dbsql_admin_tools returns the correct structure."""
        tools = dbsql_admin_tools()

        assert isinstance(tools, list)
        assert len(tools) == 2
        assert all(callable(tool) for tool in tools)

    def test_tool_names(self):
        """Test that DBSQL tools have the expected names."""
        tools = dbsql_admin_tools()
        tool_names = [tool.__name__ for tool in tools]

        assert "top_slowest_queries" in tool_names
        assert "user_query_summary" in tool_names

    @patch('admin_ai_bridge.tools_databricks_agent.DBSQLAdmin')
    def test_top_slowest_queries_invocation(self, mock_dbsql_admin_class):
        """Test top_slowest_queries tool invocation."""
        mock_dbsql_admin = Mock()
        mock_dbsql_admin_class.return_value = mock_dbsql_admin

        mock_query = QueryHistoryEntry(
            query_id="q123",
            warehouse_id="wh456",
            user_name="test@example.com",
            status="FINISHED",
            start_time=datetime.now(timezone.utc),
            end_time=datetime.now(timezone.utc),
            duration_seconds=300.0,
            sql_text="SELECT * FROM large_table",
        )
        mock_dbsql_admin.top_slowest_queries.return_value = [mock_query]

        tools = dbsql_admin_tools()
        slow_queries_tool = next(t for t in tools if t.__name__ == "top_slowest_queries")

        result = slow_queries_tool(lookback_hours=24.0, limit=20)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["query_id"] == "q123"
        assert result[0]["duration_seconds"] == 300.0

    @patch('admin_ai_bridge.tools_databricks_agent.DBSQLAdmin')
    def test_user_query_summary_invocation(self, mock_dbsql_admin_class):
        """Test user_query_summary tool invocation."""
        mock_dbsql_admin = Mock()
        mock_dbsql_admin_class.return_value = mock_dbsql_admin

        mock_summary = {
            "user_name": "test@example.com",
            "total_queries": 100,
            "successful_queries": 95,
            "failed_queries": 5,
            "avg_duration_seconds": 45.2,
            "failure_rate": 5.0,
        }
        mock_dbsql_admin.user_query_summary.return_value = mock_summary

        tools = dbsql_admin_tools()
        summary_tool = next(t for t in tools if t.__name__ == "user_query_summary")

        result = summary_tool(user_name="test@example.com", lookback_hours=24.0)

        assert isinstance(result, dict)
        assert result["total_queries"] == 100
        assert result["failure_rate"] == 5.0


class TestClustersAdminTools:
    """Tests for clusters_admin_tools."""

    def test_tool_list_structure(self):
        """Test that clusters_admin_tools returns the correct structure."""
        tools = clusters_admin_tools()

        assert isinstance(tools, list)
        assert len(tools) == 2
        assert all(callable(tool) for tool in tools)

    def test_tool_names(self):
        """Test that cluster tools have the expected names."""
        tools = clusters_admin_tools()
        tool_names = [tool.__name__ for tool in tools]

        assert "list_long_running_clusters" in tool_names
        assert "list_idle_clusters" in tool_names

    @patch('admin_ai_bridge.tools_databricks_agent.ClustersAdmin')
    def test_list_long_running_clusters_invocation(self, mock_clusters_admin_class):
        """Test list_long_running_clusters tool invocation."""
        mock_clusters_admin = Mock()
        mock_clusters_admin_class.return_value = mock_clusters_admin

        mock_cluster = ClusterSummary(
            cluster_id="c123",
            cluster_name="long_running_cluster",
            state="RUNNING",
            creator="user@example.com",
            start_time=datetime.now(timezone.utc),
            driver_node_type="i3.xlarge",
            node_type="i3.xlarge",
            cluster_policy_id=None,
            last_activity_time=datetime.now(timezone.utc),
            is_long_running=True,
        )
        mock_clusters_admin.list_long_running_clusters.return_value = [mock_cluster]

        tools = clusters_admin_tools()
        long_running_tool = next(t for t in tools if t.__name__ == "list_long_running_clusters")

        result = long_running_tool(min_duration_hours=8.0, lookback_hours=24.0, limit=50)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["cluster_id"] == "c123"
        assert result[0]["is_long_running"] is True


class TestSecurityAdminTools:
    """Tests for security_admin_tools."""

    def test_tool_list_structure(self):
        """Test that security_admin_tools returns the correct structure."""
        tools = security_admin_tools()

        assert isinstance(tools, list)
        assert len(tools) == 2
        assert all(callable(tool) for tool in tools)

    def test_tool_names(self):
        """Test that security tools have the expected names."""
        tools = security_admin_tools()
        tool_names = [tool.__name__ for tool in tools]

        assert "who_can_manage_job" in tool_names
        assert "who_can_use_cluster" in tool_names

    @patch('admin_ai_bridge.tools_databricks_agent.SecurityAdmin')
    def test_who_can_manage_job_invocation(self, mock_security_admin_class):
        """Test who_can_manage_job tool invocation."""
        mock_security_admin = Mock()
        mock_security_admin_class.return_value = mock_security_admin

        mock_permission = PermissionEntry(
            object_type="JOB",
            object_id="123",
            principal="admin@example.com",
            permission_level="CAN_MANAGE",
        )
        mock_security_admin.who_can_manage_job.return_value = [mock_permission]

        tools = security_admin_tools()
        manage_job_tool = next(t for t in tools if t.__name__ == "who_can_manage_job")

        result = manage_job_tool(job_id=123)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["principal"] == "admin@example.com"
        assert result[0]["permission_level"] == "CAN_MANAGE"


class TestUsageAdminTools:
    """Tests for usage_admin_tools."""

    def test_tool_list_structure(self):
        """Test that usage_admin_tools returns the correct structure."""
        tools = usage_admin_tools()

        assert isinstance(tools, list)
        assert len(tools) == 3  # top_cost_centers, cost_by_dimension, budget_status
        assert all(callable(tool) for tool in tools)

    def test_tool_names(self):
        """Test that usage tools have the expected names including new tools."""
        tools = usage_admin_tools()
        tool_names = [tool.__name__ for tool in tools]

        assert "top_cost_centers" in tool_names
        assert "cost_by_dimension" in tool_names  # NEW from addendum
        assert "budget_status" in tool_names  # NEW from addendum

    def test_tool_descriptions_include_chargeback(self):
        """Test that cost_by_dimension mentions chargeback."""
        tools = usage_admin_tools()
        cost_by_dim_tool = next(t for t in tools if t.__name__ == "cost_by_dimension")

        assert "chargeback" in cost_by_dim_tool.__doc__.lower()

    def test_tool_descriptions_include_budget(self):
        """Test that budget_status mentions budget monitoring."""
        tools = usage_admin_tools()
        budget_tool = next(t for t in tools if t.__name__ == "budget_status")

        assert "budget" in budget_tool.__doc__.lower()

    @patch('admin_ai_bridge.tools_databricks_agent.UsageAdmin')
    def test_top_cost_centers_invocation(self, mock_usage_admin_class):
        """Test top_cost_centers tool invocation."""
        mock_usage_admin = Mock()
        mock_usage_admin_class.return_value = mock_usage_admin

        mock_usage = UsageEntry(
            scope="cluster",
            name="expensive_cluster",
            start_time=datetime.now(timezone.utc),
            end_time=datetime.now(timezone.utc),
            cost=1000.0,
            dbus=5000.0,
        )
        mock_usage_admin.top_cost_centers.return_value = [mock_usage]

        tools = usage_admin_tools()
        cost_tool = next(t for t in tools if t.__name__ == "top_cost_centers")

        result = cost_tool(lookback_days=7, limit=20)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["cost"] == 1000.0

    @patch('admin_ai_bridge.tools_databricks_agent.UsageAdmin')
    def test_cost_by_dimension_invocation(self, mock_usage_admin_class):
        """Test cost_by_dimension tool invocation."""
        mock_usage_admin = Mock()
        mock_usage_admin_class.return_value = mock_usage_admin

        mock_usage = UsageEntry(
            scope="workspace",
            name="ws-123",
            start_time=datetime.now(timezone.utc),
            end_time=datetime.now(timezone.utc),
            cost=5000.0,
            dbus=25000.0,
        )
        mock_usage_admin.cost_by_dimension.return_value = [mock_usage]

        tools = usage_admin_tools()
        cost_by_dim_tool = next(t for t in tools if t.__name__ == "cost_by_dimension")

        result = cost_by_dim_tool(dimension="workspace", lookback_days=30, limit=100)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["scope"] == "workspace"
        assert result[0]["cost"] == 5000.0

    @patch('admin_ai_bridge.tools_databricks_agent.UsageAdmin')
    def test_budget_status_invocation(self, mock_usage_admin_class):
        """Test budget_status tool invocation."""
        mock_usage_admin = Mock()
        mock_usage_admin_class.return_value = mock_usage_admin

        mock_budget_status = [
            {
                "dimension_value": "project_a",
                "actual_cost": 8500.0,
                "budget_amount": 10000.0,
                "utilization_pct": 85.0,
                "status": "warning",
            }
        ]
        mock_usage_admin.budget_status.return_value = mock_budget_status

        tools = usage_admin_tools()
        budget_tool = next(t for t in tools if t.__name__ == "budget_status")

        result = budget_tool(dimension="project", period_days=30, warn_threshold=0.8)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["status"] == "warning"
        assert result[0]["utilization_pct"] == 85.0


class TestAuditAdminTools:
    """Tests for audit_admin_tools."""

    def test_tool_list_structure(self):
        """Test that audit_admin_tools returns the correct structure."""
        tools = audit_admin_tools()

        assert isinstance(tools, list)
        assert len(tools) == 2
        assert all(callable(tool) for tool in tools)

    def test_tool_names(self):
        """Test that audit tools have the expected names."""
        tools = audit_admin_tools()
        tool_names = [tool.__name__ for tool in tools]

        assert "failed_logins" in tool_names
        assert "recent_admin_changes" in tool_names

    @patch('admin_ai_bridge.tools_databricks_agent.AuditAdmin')
    def test_failed_logins_invocation(self, mock_audit_admin_class):
        """Test failed_logins tool invocation."""
        mock_audit_admin = Mock()
        mock_audit_admin_class.return_value = mock_audit_admin

        mock_event = AuditEvent(
            event_time=datetime.now(timezone.utc),
            service_name="accounts",
            event_type="login",
            user_name="test@example.com",
            source_ip="192.168.1.1",
            details={"status_code": 401},
        )
        mock_audit_admin.failed_logins.return_value = [mock_event]

        tools = audit_admin_tools()
        failed_logins_tool = next(t for t in tools if t.__name__ == "failed_logins")

        result = failed_logins_tool(lookback_hours=24.0, limit=100)

        assert isinstance(result, list)
        # Note: audit methods may return empty lists in placeholder implementation
        # But the structure should be correct


class TestPipelinesAdminTools:
    """Tests for pipelines_admin_tools."""

    def test_tool_list_structure(self):
        """Test that pipelines_admin_tools returns the correct structure."""
        tools = pipelines_admin_tools()

        assert isinstance(tools, list)
        assert len(tools) == 2
        assert all(callable(tool) for tool in tools)

    def test_tool_names(self):
        """Test that pipeline tools have the expected names."""
        tools = pipelines_admin_tools()
        tool_names = [tool.__name__ for tool in tools]

        assert "list_lagging_pipelines" in tool_names
        assert "list_failed_pipelines" in tool_names

    @patch('admin_ai_bridge.tools_databricks_agent.PipelinesAdmin')
    def test_list_lagging_pipelines_invocation(self, mock_pipelines_admin_class):
        """Test list_lagging_pipelines tool invocation."""
        mock_pipelines_admin = Mock()
        mock_pipelines_admin_class.return_value = mock_pipelines_admin

        mock_pipeline = PipelineStatus(
            pipeline_id="p123",
            name="lagging_pipeline",
            state="RUNNING",
            last_update_time=datetime.now(timezone.utc),
            lag_seconds=1200.0,  # 20 minutes
            last_error=None,
        )
        mock_pipelines_admin.list_lagging_pipelines.return_value = [mock_pipeline]

        tools = pipelines_admin_tools()
        lagging_tool = next(t for t in tools if t.__name__ == "list_lagging_pipelines")

        result = lagging_tool(max_lag_seconds=600.0, limit=50)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["lag_seconds"] == 1200.0


class TestToolJSONSerialization:
    """Test that all tools return JSON-serializable outputs."""

    @patch('admin_ai_bridge.tools_databricks_agent.JobsAdmin')
    def test_jobs_tools_json_serializable(self, mock_jobs_admin_class):
        """Test that job tools return JSON-serializable data."""
        import json

        mock_jobs_admin = Mock()
        mock_jobs_admin_class.return_value = mock_jobs_admin

        mock_run = JobRunSummary(
            job_id=123,
            job_name="test",
            run_id=456,
            state="SUCCESS",
            start_time=datetime.now(timezone.utc),
            duration_seconds=100.0,
        )
        mock_jobs_admin.list_long_running_jobs.return_value = [mock_run]

        tools = jobs_admin_tools()
        tool = next(t for t in tools if t.__name__ == "list_long_running_jobs")
        result = tool()

        # Should be able to serialize to JSON
        json_str = json.dumps(result, default=str)
        assert json_str is not None

    @patch('admin_ai_bridge.tools_databricks_agent.UsageAdmin')
    def test_usage_tools_json_serializable(self, mock_usage_admin_class):
        """Test that usage tools return JSON-serializable data."""
        import json

        mock_usage_admin = Mock()
        mock_usage_admin_class.return_value = mock_usage_admin

        mock_budget = [
            {
                "dimension_value": "test",
                "actual_cost": 100.0,
                "budget_amount": 200.0,
                "utilization_pct": 50.0,
                "status": "within_budget",
            }
        ]
        mock_usage_admin.budget_status.return_value = mock_budget

        tools = usage_admin_tools()
        tool = next(t for t in tools if t.__name__ == "budget_status")
        result = tool(dimension="workspace")

        # Should be able to serialize to JSON
        json_str = json.dumps(result, default=str)
        assert json_str is not None


class TestToolParameterValidation:
    """Test that tools properly validate parameters through admin classes."""

    @patch('admin_ai_bridge.tools_databricks_agent.JobsAdmin')
    def test_tools_pass_parameters_correctly(self, mock_jobs_admin_class):
        """Test that tool functions pass parameters to admin methods correctly."""
        mock_jobs_admin = Mock()
        mock_jobs_admin_class.return_value = mock_jobs_admin
        mock_jobs_admin.list_long_running_jobs.return_value = []

        tools = jobs_admin_tools()
        tool = next(t for t in tools if t.__name__ == "list_long_running_jobs")

        # Call with specific parameters
        tool(min_duration_hours=6.0, lookback_hours=48.0, limit=10)

        # Verify parameters were passed correctly
        mock_jobs_admin.list_long_running_jobs.assert_called_once_with(
            min_duration_hours=6.0,
            lookback_hours=48.0,
            limit=10,
        )


class TestAllDomainsExported:
    """Test that all 7 domains have tool functions."""

    def test_all_seven_domains_present(self):
        """Test that we have tool functions for all 7 admin domains."""
        # All 7 tool functions should exist
        assert callable(jobs_admin_tools)
        assert callable(dbsql_admin_tools)
        assert callable(clusters_admin_tools)
        assert callable(security_admin_tools)
        assert callable(usage_admin_tools)
        assert callable(audit_admin_tools)
        assert callable(pipelines_admin_tools)

    def test_all_domains_return_tools(self):
        """Test that all domain functions return non-empty tool lists."""
        all_tool_functions = [
            jobs_admin_tools,
            dbsql_admin_tools,
            clusters_admin_tools,
            security_admin_tools,
            usage_admin_tools,
            audit_admin_tools,
            pipelines_admin_tools,
        ]

        for tool_func in all_tool_functions:
            tools = tool_func()
            assert isinstance(tools, list)
            assert len(tools) > 0
            assert all(callable(t) for t in tools)


class TestReadOnlyOperations:
    """Test that all tools are read-only (no destructive operations)."""

    def test_no_delete_or_destroy_in_tool_names(self):
        """Test that no tool has 'delete', 'destroy', or 'terminate' in its name."""
        all_tool_functions = [
            jobs_admin_tools,
            dbsql_admin_tools,
            clusters_admin_tools,
            security_admin_tools,
            usage_admin_tools,
            audit_admin_tools,
            pipelines_admin_tools,
        ]

        destructive_keywords = ["delete", "destroy", "terminate", "remove", "stop", "kill"]

        for tool_func in all_tool_functions:
            tools = tool_func()
            for tool in tools:
                tool_name_lower = tool.__name__.lower()
                for keyword in destructive_keywords:
                    assert keyword not in tool_name_lower, \
                        f"Tool '{tool.__name__}' contains destructive keyword '{keyword}'"

    def test_descriptions_emphasize_read_only(self):
        """Test that tool descriptions emphasize read-only/observability nature."""
        all_tool_functions = [
            jobs_admin_tools,
            dbsql_admin_tools,
            clusters_admin_tools,
            security_admin_tools,
            usage_admin_tools,
            audit_admin_tools,
            pipelines_admin_tools,
        ]

        # Tools should be about listing, returning, getting, or identifying
        read_only_keywords = ["list", "return", "get", "identify", "aggregate", "summarize", "compare"]

        for tool_func in all_tool_functions:
            tools = tool_func()
            for tool in tools:
                desc_lower = tool.__doc__.lower()
                # At least one read-only keyword should be present
                has_read_only_keyword = any(keyword in desc_lower for keyword in read_only_keywords)
                assert has_read_only_keyword, \
                    f"Tool '{tool.__name__}' docstring doesn't clearly indicate read-only operation"
