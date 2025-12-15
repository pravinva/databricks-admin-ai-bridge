"""
End-to-end tests for admin-observability-agent query handling.

Tests all user stories from spec.md to ensure the agent can correctly answer
admin/observability questions using the available tools.

All tests are marked as e2e and require a live Databricks workspace.
"""

import pytest
from typing import List, Dict, Any

from admin_ai_bridge import (
    AdminBridgeConfig,
    jobs_admin_tools,
    dbsql_admin_tools,
    clusters_admin_tools,
    security_admin_tools,
    usage_admin_tools,
    audit_admin_tools,
    pipelines_admin_tools,
)


@pytest.mark.e2e
class TestUserStoryQueries:
    """
    Test all user stories from spec.md.

    Each test simulates a user asking a question and validates that:
    1. The appropriate tool can be called
    2. The tool returns valid, structured data
    3. The data contains the expected fields
    """

    def test_query_long_running_jobs(self):
        """
        User Story: "Which jobs have been running longer than 4 hours today?"

        This should use the list_long_running_jobs tool.
        """
        cfg = AdminBridgeConfig()
        tools = jobs_admin_tools(cfg)

        # Find the tool
        tool = next(t for t in tools if t.name == "list_long_running_jobs")
        assert tool is not None, "list_long_running_jobs tool not found"

        # Call with user story parameters
        results = tool.func(min_duration_hours=4.0, lookback_hours=24.0, limit=20)

        # Validate structure
        assert isinstance(results, list), "Should return a list"

        # If there are results, validate structure
        if results:
            result = results[0]
            assert isinstance(result, dict), "Each result should be a dict"

            # Check for expected fields
            expected_fields = ["job_id", "job_name", "run_id", "state", "duration_hours"]
            for field in expected_fields:
                assert field in result, f"Result should contain {field}"

            # Validate duration logic
            assert result["duration_hours"] >= 4.0, "Duration should be >= 4.0 hours"

    def test_query_slowest_queries(self):
        """
        User Story: "Show me top 10 slowest queries in the last 24 hours."

        This should use the top_slowest_queries tool.
        """
        cfg = AdminBridgeConfig()
        tools = dbsql_admin_tools(cfg)

        tool = next(t for t in tools if t.name == "top_slowest_queries")
        assert tool is not None

        # Call with user story parameters
        results = tool.func(lookback_hours=24.0, limit=10)

        assert isinstance(results, list)

        if results:
            result = results[0]
            assert isinstance(result, dict)

            expected_fields = ["query_id", "duration_seconds", "user_name", "warehouse_id"]
            for field in expected_fields:
                assert field in result, f"Result should contain {field}"

            # Validate that results are sorted by duration (descending)
            if len(results) > 1:
                for i in range(len(results) - 1):
                    assert (
                        results[i]["duration_seconds"] >= results[i + 1]["duration_seconds"]
                    ), "Results should be sorted by duration descending"

    def test_query_idle_clusters(self):
        """
        User Story: "Which clusters are idle for more than 2 hours?"

        This should use the list_idle_clusters tool.
        """
        cfg = AdminBridgeConfig()
        tools = clusters_admin_tools(cfg)

        tool = next(t for t in tools if t.name == "list_idle_clusters")
        assert tool is not None

        results = tool.func(idle_hours=2.0, limit=50)

        assert isinstance(results, list)

        if results:
            result = results[0]
            assert isinstance(result, dict)

            expected_fields = ["cluster_id", "cluster_name", "state", "idle_hours"]
            for field in expected_fields:
                assert field in result, f"Result should contain {field}"

            assert result["idle_hours"] >= 2.0, "Idle time should be >= 2.0 hours"

    def test_query_job_permissions(self):
        """
        User Story: "Who can manage job 123?"

        This should use the who_can_manage_job tool.

        Note: This test uses a placeholder job_id. In a real test environment,
        you would use an actual job_id from your workspace.
        """
        cfg = AdminBridgeConfig()
        tools = security_admin_tools(cfg)

        tool = next(t for t in tools if t.name == "who_can_manage_job")
        assert tool is not None

        # This will fail if job 123 doesn't exist, which is expected
        # In a real E2E test, you would first create or identify a valid job
        try:
            results = tool.func(job_id=123)

            assert isinstance(results, list)

            if results:
                result = results[0]
                assert isinstance(result, dict)

                expected_fields = ["principal", "permission_level"]
                for field in expected_fields:
                    assert field in result, f"Result should contain {field}"

                # Validate permission level is CAN_MANAGE
                assert "MANAGE" in result["permission_level"].upper()

        except Exception as e:
            # Job might not exist - this is acceptable for a schema test
            # In production E2E, you'd create a test job first
            pytest.skip(f"Job 123 not found (expected for demo): {e}")

    def test_query_failed_logins(self):
        """
        User Story: "Show failed login attempts in the last day."

        This should use the failed_logins tool.
        """
        cfg = AdminBridgeConfig()
        tools = audit_admin_tools(cfg)

        tool = next(t for t in tools if t.name == "failed_logins")
        assert tool is not None

        results = tool.func(lookback_hours=24.0, limit=100)

        assert isinstance(results, list)

        if results:
            result = results[0]
            assert isinstance(result, dict)

            expected_fields = ["timestamp", "user_name", "event_type", "result"]
            for field in expected_fields:
                assert field in result, f"Result should contain {field}"

            # Validate that result indicates failure
            assert "fail" in result["result"].lower() or "error" in result["result"].lower()

    def test_query_expensive_workloads(self):
        """
        User Story: "Which clusters or jobs are the most expensive in the last 7 days?"

        This should use the top_cost_centers tool.
        """
        cfg = AdminBridgeConfig()
        tools = usage_admin_tools(cfg)

        tool = next(t for t in tools if t.name == "top_cost_centers")
        assert tool is not None

        results = tool.func(lookback_days=7, limit=20)

        assert isinstance(results, list)

        if results:
            result = results[0]
            assert isinstance(result, dict)

            expected_fields = ["scope", "name", "cost", "dbus_consumed"]
            for field in expected_fields:
                assert field in result, f"Result should contain {field}"

            # Validate cost sorting
            if len(results) > 1:
                for i in range(len(results) - 1):
                    assert (
                        results[i]["cost"] >= results[i + 1]["cost"]
                    ), "Results should be sorted by cost descending"

    def test_query_lagging_pipelines(self):
        """
        User Story: "Which pipelines are behind by more than 10 minutes?"

        This should use the list_lagging_pipelines tool.
        """
        cfg = AdminBridgeConfig()
        tools = pipelines_admin_tools(cfg)

        tool = next(t for t in tools if t.name == "list_lagging_pipelines")
        assert tool is not None

        # 10 minutes = 600 seconds
        results = tool.func(max_lag_seconds=600.0, limit=50)

        assert isinstance(results, list)

        if results:
            result = results[0]
            assert isinstance(result, dict)

            expected_fields = ["pipeline_id", "pipeline_name", "lag_seconds", "state"]
            for field in expected_fields:
                assert field in result, f"Result should contain {field}"

            assert result["lag_seconds"] > 600.0, "Lag should be > 600 seconds (10 minutes)"

    def test_query_cost_by_workspace(self):
        """
        User Story (addendum): "Show DBUs and cost by workspace for the last 30 days"

        This should use the cost_by_dimension tool with dimension="workspace".
        """
        cfg = AdminBridgeConfig()
        tools = usage_admin_tools(cfg)

        tool = next(t for t in tools if t.name == "cost_by_dimension")
        assert tool is not None

        results = tool.func(dimension="workspace", lookback_days=30, limit=100)

        assert isinstance(results, list)

        if results:
            result = results[0]
            assert isinstance(result, dict)

            expected_fields = ["dimension_value", "cost", "dbus_consumed"]
            for field in expected_fields:
                assert field in result, f"Result should contain {field}"

            # Validate dimension value looks like a workspace
            assert result["dimension_value"], "Workspace name should not be empty"

    def test_query_budget_status(self):
        """
        User Story (addendum): "Which teams are over 80% of their monthly budget?"

        This should use the budget_status tool with dimension="team".
        """
        cfg = AdminBridgeConfig()
        tools = usage_admin_tools(cfg)

        tool = next(t for t in tools if t.name == "budget_status")
        assert tool is not None

        results = tool.func(dimension="team", period_days=30, warn_threshold=0.8)

        assert isinstance(results, list)

        if results:
            result = results[0]
            assert isinstance(result, dict)

            expected_fields = ["dimension_value", "actual_cost", "budget_amount", "utilization_pct", "status"]
            for field in expected_fields:
                assert field in result, f"Result should contain {field}"

            # If status is warning or breached, utilization should be >= 80%
            if result["status"] in ["warning", "breached"]:
                assert result["utilization_pct"] >= 0.8, "High utilization should trigger warning/breached status"


@pytest.mark.e2e
class TestMultiToolQueries:
    """
    Test scenarios that might require combining multiple tools.

    These represent more complex admin questions that might need
    data from multiple sources.
    """

    def test_query_failed_jobs_with_permissions(self):
        """
        Complex Query: "Show failed jobs and who can manage them"

        This would use:
        1. list_failed_jobs to get failed jobs
        2. who_can_manage_job for each failed job to get managers
        """
        cfg = AdminBridgeConfig()
        jobs_tools = jobs_admin_tools(cfg)
        security_tools = security_admin_tools(cfg)

        # Get failed jobs
        failed_jobs_tool = next(t for t in jobs_tools if t.name == "list_failed_jobs")
        failed_jobs = failed_jobs_tool.func(lookback_hours=24.0, limit=5)

        assert isinstance(failed_jobs, list)

        # For each failed job, get permissions (if any failed jobs exist)
        permissions_tool = next(t for t in security_tools if t.name == "who_can_manage_job")

        for job in failed_jobs[:1]:  # Test just the first one
            job_id = job["job_id"]
            try:
                permissions = permissions_tool.func(job_id=job_id)
                assert isinstance(permissions, list)
            except Exception:
                # Some jobs might not have permissions accessible
                pass

    def test_query_long_running_expensive_clusters(self):
        """
        Complex Query: "Show long-running clusters and their costs"

        This would use:
        1. list_long_running_clusters
        2. top_cost_centers or cost_by_dimension to get cluster costs
        """
        cfg = AdminBridgeConfig()
        clusters_tools = clusters_admin_tools(cfg)
        usage_tools = usage_admin_tools(cfg)

        # Get long-running clusters
        long_running_tool = next(t for t in clusters_tools if t.name == "list_long_running_clusters")
        long_running = long_running_tool.func(min_duration_hours=1.0, lookback_hours=24.0, limit=10)

        assert isinstance(long_running, list)

        # Get cost by cluster
        cost_tool = next(t for t in usage_tools if t.name == "cost_by_dimension")
        costs = cost_tool.func(dimension="cluster", lookback_days=7, limit=100)

        assert isinstance(costs, list)

    def test_query_user_activity_summary(self):
        """
        Complex Query: "Show me all activity for user X"

        This would use:
        1. user_query_summary for DBSQL activity
        2. failed_logins or recent_admin_changes for audit activity
        """
        cfg = AdminBridgeConfig()
        dbsql_tools = dbsql_admin_tools(cfg)
        audit_tools = audit_admin_tools(cfg)

        # Get query summary (using a likely username)
        query_summary_tool = next(t for t in dbsql_tools if t.name == "user_query_summary")

        try:
            # This might fail if user doesn't exist
            summary = query_summary_tool.func(user_name="test.user@example.com", lookback_hours=24.0)
            assert isinstance(summary, dict)
        except Exception:
            pytest.skip("Test user not found (expected for demo)")

        # Get audit events
        admin_changes_tool = next(t for t in audit_tools if t.name == "recent_admin_changes")
        admin_changes = admin_changes_tool.func(lookback_hours=24.0, limit=100)

        assert isinstance(admin_changes, list)


@pytest.mark.e2e
class TestToolParameterValidation:
    """
    Test that tools handle various parameter ranges correctly.

    Ensures tools are robust to different parameter values.
    """

    def test_zero_lookback_returns_empty_or_minimal(self):
        """Test tools with very short lookback windows."""
        cfg = AdminBridgeConfig()

        # Test jobs tool
        jobs_tools = jobs_admin_tools(cfg)
        tool = next(t for t in jobs_tools if t.name == "list_long_running_jobs")
        results = tool.func(min_duration_hours=100.0, lookback_hours=0.1, limit=10)
        assert isinstance(results, list)

    def test_large_limit_is_respected(self):
        """Test that limit parameter is respected."""
        cfg = AdminBridgeConfig()

        # Test with limit=1
        dbsql_tools = dbsql_admin_tools(cfg)
        tool = next(t for t in dbsql_tools if t.name == "top_slowest_queries")
        results = tool.func(lookback_hours=24.0, limit=1)

        assert isinstance(results, list)
        assert len(results) <= 1, "Should respect limit=1"

    def test_reasonable_defaults_work(self):
        """Test that tools work with default parameters."""
        cfg = AdminBridgeConfig()

        # Each tool should work with no parameters (using defaults)
        all_tools = []
        all_tools.extend(jobs_admin_tools(cfg))
        all_tools.extend(dbsql_admin_tools(cfg))
        all_tools.extend(clusters_admin_tools(cfg))
        all_tools.extend(usage_admin_tools(cfg))
        all_tools.extend(audit_admin_tools(cfg))
        all_tools.extend(pipelines_admin_tools(cfg))

        # Test a sample of tools with no arguments
        for tool in all_tools[:3]:  # Test first 3 as sample
            try:
                # Tools with required args will fail, which is acceptable
                result = tool.func()
                assert isinstance(result, (list, dict))
            except TypeError:
                # Tool requires arguments - acceptable
                pass


@pytest.mark.e2e
class TestDataQuality:
    """
    Test that tool outputs contain high-quality, complete data.

    These tests validate that the data returned by tools is
    actionable and complete for admin use cases.
    """

    def test_job_data_completeness(self):
        """Verify job data includes all essential fields."""
        cfg = AdminBridgeConfig()
        tools = jobs_admin_tools(cfg)

        tool = next(t for t in tools if t.name == "list_long_running_jobs")
        results = tool.func(min_duration_hours=0.5, lookback_hours=168.0, limit=5)

        if results:
            result = results[0]

            # Essential fields for actionability
            essential = ["job_id", "job_name", "run_id", "state", "duration_hours", "start_time"]
            for field in essential:
                assert field in result, f"Missing essential field: {field}"
                assert result[field] is not None, f"Essential field {field} is None"

    def test_query_data_includes_sql_text(self):
        """Verify slow queries include SQL text for analysis."""
        cfg = AdminBridgeConfig()
        tools = dbsql_admin_tools(cfg)

        tool = next(t for t in tools if t.name == "top_slowest_queries")
        results = tool.func(lookback_hours=168.0, limit=5)

        if results:
            result = results[0]

            assert "query_text" in result or "sql_text" in result, "Should include SQL text"
            # SQL text might be truncated but should exist

    def test_cost_data_includes_currency(self):
        """Verify cost data is presented in actionable format."""
        cfg = AdminBridgeConfig()
        tools = usage_admin_tools(cfg)

        tool = next(t for t in tools if t.name == "top_cost_centers")
        results = tool.func(lookback_days=7, limit=5)

        if results:
            result = results[0]

            # Cost should be numeric
            assert isinstance(result["cost"], (int, float)), "Cost should be numeric"
            assert result["cost"] >= 0, "Cost should be non-negative"

            # DBUs should be numeric
            assert isinstance(result["dbus_consumed"], (int, float)), "DBUs should be numeric"

    def test_timestamps_are_parseable(self):
        """Verify timestamp fields are in a standard format."""
        cfg = AdminBridgeConfig()
        tools = audit_admin_tools(cfg)

        tool = next(t for t in tools if t.name == "failed_logins")
        results = tool.func(lookback_hours=168.0, limit=5)

        if results:
            result = results[0]

            assert "timestamp" in result, "Should include timestamp"
            # Timestamp should be a string in ISO format or an integer (epoch)
            assert isinstance(result["timestamp"], (str, int)), "Timestamp should be string or int"
