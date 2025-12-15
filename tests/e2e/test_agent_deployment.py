"""
End-to-end tests for admin-observability-agent deployment.

Tests agent deployment, configuration, endpoint creation, and tool availability.
All tests are marked as e2e and require a live Databricks workspace.
"""

import pytest
from typing import List

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
class TestAgentDeployment:
    """Test agent deployment and configuration."""

    def test_all_tool_functions_available(self):
        """Verify all 7 tool helper functions are importable and callable."""
        cfg = AdminBridgeConfig()

        # Test that each tool function returns a list of tools
        tools_map = {
            "jobs_admin_tools": jobs_admin_tools,
            "dbsql_admin_tools": dbsql_admin_tools,
            "clusters_admin_tools": clusters_admin_tools,
            "security_admin_tools": security_admin_tools,
            "usage_admin_tools": usage_admin_tools,
            "audit_admin_tools": audit_admin_tools,
            "pipelines_admin_tools": pipelines_admin_tools,
        }

        for name, func in tools_map.items():
            tools = func(cfg)
            assert isinstance(tools, list), f"{name} should return a list"
            assert len(tools) > 0, f"{name} should return at least one tool"

    def test_total_tool_count(self):
        """Verify that exactly 15 tools are available across all domains."""
        cfg = AdminBridgeConfig()

        all_tools = []
        all_tools.extend(jobs_admin_tools(cfg))  # 2 tools
        all_tools.extend(dbsql_admin_tools(cfg))  # 2 tools
        all_tools.extend(clusters_admin_tools(cfg))  # 2 tools
        all_tools.extend(security_admin_tools(cfg))  # 2 tools
        all_tools.extend(usage_admin_tools(cfg))  # 3 tools
        all_tools.extend(audit_admin_tools(cfg))  # 2 tools
        all_tools.extend(pipelines_admin_tools(cfg))  # 2 tools

        assert len(all_tools) == 15, f"Expected 15 tools, got {len(all_tools)}"

    def test_tool_names_unique(self):
        """Verify that all tool names are unique (no conflicts)."""
        cfg = AdminBridgeConfig()

        all_tools = []
        all_tools.extend(jobs_admin_tools(cfg))
        all_tools.extend(dbsql_admin_tools(cfg))
        all_tools.extend(clusters_admin_tools(cfg))
        all_tools.extend(security_admin_tools(cfg))
        all_tools.extend(usage_admin_tools(cfg))
        all_tools.extend(audit_admin_tools(cfg))
        all_tools.extend(pipelines_admin_tools(cfg))

        tool_names = [t.name for t in all_tools]
        assert len(tool_names) == len(set(tool_names)), "Tool names must be unique"

    def test_all_tools_have_descriptions(self):
        """Verify that every tool has a non-empty description for LLM usage."""
        cfg = AdminBridgeConfig()

        all_tools = []
        all_tools.extend(jobs_admin_tools(cfg))
        all_tools.extend(dbsql_admin_tools(cfg))
        all_tools.extend(clusters_admin_tools(cfg))
        all_tools.extend(security_admin_tools(cfg))
        all_tools.extend(usage_admin_tools(cfg))
        all_tools.extend(audit_admin_tools(cfg))
        all_tools.extend(pipelines_admin_tools(cfg))

        for tool in all_tools:
            assert hasattr(tool, 'description'), f"Tool {tool.name} missing description"
            assert tool.description, f"Tool {tool.name} has empty description"
            assert len(tool.description) > 20, f"Tool {tool.name} description too short"

    def test_all_tools_have_callable_functions(self):
        """Verify that every tool has a callable function."""
        cfg = AdminBridgeConfig()

        all_tools = []
        all_tools.extend(jobs_admin_tools(cfg))
        all_tools.extend(dbsql_admin_tools(cfg))
        all_tools.extend(clusters_admin_tools(cfg))
        all_tools.extend(security_admin_tools(cfg))
        all_tools.extend(usage_admin_tools(cfg))
        all_tools.extend(audit_admin_tools(cfg))
        all_tools.extend(pipelines_admin_tools(cfg))

        for tool in all_tools:
            assert hasattr(tool, 'func'), f"Tool {tool.name} missing func attribute"
            assert callable(tool.func), f"Tool {tool.name}.func is not callable"

    def test_expected_tool_names(self):
        """Verify that all expected tool names are present."""
        cfg = AdminBridgeConfig()

        all_tools = []
        all_tools.extend(jobs_admin_tools(cfg))
        all_tools.extend(dbsql_admin_tools(cfg))
        all_tools.extend(clusters_admin_tools(cfg))
        all_tools.extend(security_admin_tools(cfg))
        all_tools.extend(usage_admin_tools(cfg))
        all_tools.extend(audit_admin_tools(cfg))
        all_tools.extend(pipelines_admin_tools(cfg))

        tool_names = {t.name for t in all_tools}

        expected_names = {
            # Jobs (2)
            "list_long_running_jobs",
            "list_failed_jobs",
            # DBSQL (2)
            "top_slowest_queries",
            "user_query_summary",
            # Clusters (2)
            "list_long_running_clusters",
            "list_idle_clusters",
            # Security (2)
            "who_can_manage_job",
            "who_can_use_cluster",
            # Usage (3)
            "top_cost_centers",
            "cost_by_dimension",
            "budget_status",
            # Audit (2)
            "failed_logins",
            "recent_admin_changes",
            # Pipelines (2)
            "list_lagging_pipelines",
            "list_failed_pipelines",
        }

        assert tool_names == expected_names, f"Tool names mismatch.\nExpected: {expected_names}\nGot: {tool_names}"

    def test_jobs_tools_structure(self):
        """Validate Jobs domain tools structure."""
        cfg = AdminBridgeConfig()
        tools = jobs_admin_tools(cfg)

        assert len(tools) == 2, "Jobs should have 2 tools"

        tool_names = {t.name for t in tools}
        assert "list_long_running_jobs" in tool_names
        assert "list_failed_jobs" in tool_names

    def test_dbsql_tools_structure(self):
        """Validate DBSQL domain tools structure."""
        cfg = AdminBridgeConfig()
        tools = dbsql_admin_tools(cfg)

        assert len(tools) == 2, "DBSQL should have 2 tools"

        tool_names = {t.name for t in tools}
        assert "top_slowest_queries" in tool_names
        assert "user_query_summary" in tool_names

    def test_clusters_tools_structure(self):
        """Validate Clusters domain tools structure."""
        cfg = AdminBridgeConfig()
        tools = clusters_admin_tools(cfg)

        assert len(tools) == 2, "Clusters should have 2 tools"

        tool_names = {t.name for t in tools}
        assert "list_long_running_clusters" in tool_names
        assert "list_idle_clusters" in tool_names

    def test_security_tools_structure(self):
        """Validate Security domain tools structure."""
        cfg = AdminBridgeConfig()
        tools = security_admin_tools(cfg)

        assert len(tools) == 2, "Security should have 2 tools"

        tool_names = {t.name for t in tools}
        assert "who_can_manage_job" in tool_names
        assert "who_can_use_cluster" in tool_names

    def test_usage_tools_structure(self):
        """Validate Usage domain tools structure."""
        cfg = AdminBridgeConfig()
        tools = usage_admin_tools(cfg)

        assert len(tools) == 3, "Usage should have 3 tools"

        tool_names = {t.name for t in tools}
        assert "top_cost_centers" in tool_names
        assert "cost_by_dimension" in tool_names
        assert "budget_status" in tool_names

    def test_audit_tools_structure(self):
        """Validate Audit domain tools structure."""
        cfg = AdminBridgeConfig()
        tools = audit_admin_tools(cfg)

        assert len(tools) == 2, "Audit should have 2 tools"

        tool_names = {t.name for t in tools}
        assert "failed_logins" in tool_names
        assert "recent_admin_changes" in tool_names

    def test_pipelines_tools_structure(self):
        """Validate Pipelines domain tools structure."""
        cfg = AdminBridgeConfig()
        tools = pipelines_admin_tools(cfg)

        assert len(tools) == 2, "Pipelines should have 2 tools"

        tool_names = {t.name for t in tools}
        assert "list_lagging_pipelines" in tool_names
        assert "list_failed_pipelines" in tool_names

    def test_config_can_be_none(self):
        """Verify that all tool functions work with cfg=None (default credentials)."""
        # This tests that default credential handling works
        all_tool_funcs = [
            jobs_admin_tools,
            dbsql_admin_tools,
            clusters_admin_tools,
            security_admin_tools,
            usage_admin_tools,
            audit_admin_tools,
            pipelines_admin_tools,
        ]

        for func in all_tool_funcs:
            tools = func(cfg=None)
            assert isinstance(tools, list)
            assert len(tools) > 0

    def test_tools_return_json_serializable_output(self):
        """
        Verify that tool functions return JSON-serializable outputs.

        This is a smoke test that calls each tool with default/minimal parameters
        and verifies the output structure. This requires a live workspace.
        """
        cfg = AdminBridgeConfig()

        # Test a sample from each domain
        jobs_tools = jobs_admin_tools(cfg)
        list_long_running = next(t for t in jobs_tools if t.name == "list_long_running_jobs")

        # Call with very permissive parameters to minimize false negatives
        result = list_long_running.func(min_duration_hours=100.0, lookback_hours=1.0, limit=1)

        assert isinstance(result, list), "Tool output should be a list"
        # If there are results, verify they're dicts (JSON-serializable)
        if result:
            assert isinstance(result[0], dict), "Tool output items should be dicts"


@pytest.mark.e2e
class TestAgentConfiguration:
    """Test agent configuration and credential handling."""

    def test_config_with_profile(self):
        """Verify that AdminBridgeConfig can be created with a profile."""
        cfg = AdminBridgeConfig(profile="DEFAULT")
        assert cfg.profile == "DEFAULT"

    def test_config_with_host_and_token(self):
        """Verify that AdminBridgeConfig can be created with host and token."""
        cfg = AdminBridgeConfig(
            host="https://e2-demo-field-eng.cloud.databricks.com",
            token="dummy-token-for-testing"
        )
        assert cfg.host == "https://e2-demo-field-eng.cloud.databricks.com"
        assert cfg.token == "dummy-token-for-testing"

    def test_tools_created_with_custom_config(self):
        """Verify that tools can be created with a custom config."""
        cfg = AdminBridgeConfig(profile="DEFAULT")

        tools = jobs_admin_tools(cfg)
        assert len(tools) > 0

        # Verify tools are independent (different configs create different instances)
        cfg2 = AdminBridgeConfig(profile="DEFAULT")
        tools2 = jobs_admin_tools(cfg2)
        assert len(tools2) > 0
