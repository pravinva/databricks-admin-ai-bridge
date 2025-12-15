"""
Databricks Admin AI Bridge

A Python package that provides strongly typed, high-level classes for Databricks admin domains
with agent-friendly tool specifications for MCP clients and Databricks agents.
"""

from .config import AdminBridgeConfig, get_workspace_client
from .schemas import (
    JobRunSummary,
    QueryHistoryEntry,
    ClusterSummary,
    PermissionEntry,
    UsageEntry,
    AuditEvent,
    PipelineStatus,
    BudgetStatus,
)
from .jobs import JobsAdmin
from .dbsql import DBSQLAdmin
from .clusters import ClustersAdmin
from .security import SecurityAdmin
from .usage import UsageAdmin
from .audit import AuditAdmin
from .pipelines import PipelinesAdmin
from .tools_databricks_agent import (
    jobs_admin_tools,
    dbsql_admin_tools,
    clusters_admin_tools,
    security_admin_tools,
    usage_admin_tools,
    audit_admin_tools,
    pipelines_admin_tools,
)

__version__ = "0.1.0"

__all__ = [
    # Configuration
    "AdminBridgeConfig",
    "get_workspace_client",
    # Schemas
    "JobRunSummary",
    "QueryHistoryEntry",
    "ClusterSummary",
    "PermissionEntry",
    "UsageEntry",
    "AuditEvent",
    "PipelineStatus",
    "BudgetStatus",
    # Admin Classes
    "JobsAdmin",
    "DBSQLAdmin",
    "ClustersAdmin",
    "SecurityAdmin",
    "UsageAdmin",
    "AuditAdmin",
    "PipelinesAdmin",
    # Databricks Agent Framework Tools
    "jobs_admin_tools",
    "dbsql_admin_tools",
    "clusters_admin_tools",
    "security_admin_tools",
    "usage_admin_tools",
    "audit_admin_tools",
    "pipelines_admin_tools",
]
