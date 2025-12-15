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
)
from .jobs import JobsAdmin
from .dbsql import DBSQLAdmin
from .clusters import ClustersAdmin
from .security import SecurityAdmin
from .usage import UsageAdmin
from .audit import AuditAdmin
from .pipelines import PipelinesAdmin

__version__ = "0.1.0"

__all__ = [
    "AdminBridgeConfig",
    "get_workspace_client",
    "JobRunSummary",
    "QueryHistoryEntry",
    "ClusterSummary",
    "PermissionEntry",
    "UsageEntry",
    "AuditEvent",
    "PipelineStatus",
    "JobsAdmin",
    "DBSQLAdmin",
    "ClustersAdmin",
    "SecurityAdmin",
    "UsageAdmin",
    "AuditAdmin",
    "PipelinesAdmin",
]
