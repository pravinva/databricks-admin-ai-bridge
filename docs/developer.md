```markdown
# Databricks Admin AI Bridge – Developer Guide

This document describes how to implement an “Admin AI Bridge” library for Databricks platform APIs (Jobs, DBSQL, clusters, security, usage, audit logs, pipelines, etc.), similar in spirit to `databricks-ai-bridge` for Genie and Vector Search. The goal is to expose safe, high‑level, **agent‑friendly** tools that can be called from MCP clients and Databricks agents, and used via Slack, Teams, or Claude Desktop. [web:68][web:9]

---

## 1. Problem & Goals

### 1.1 Problem

- Developers repeatedly hand‑craft calls to Databricks Jobs, DBSQL, clusters, audit logs, and other REST APIs inside each bot or agent. [web:81][web:120]  
- There is no unified, “AI‑friendly” abstraction over these admin APIs, unlike `databricks-ai-bridge` for Genie and Vector Search. [web:68]  
- This causes:
  - Boilerplate (auth, pagination, filtering) copied into every project.
  - Inconsistent tool schemas and prompts across agent frameworks. [web:61]  
  - Higher risk of unsafe or overly broad actions.

### 1.2 Goals

Build a Python package (`admin_ai_bridge`, working name) that:

- Provides **strongly typed, high‑level classes** for core admin domains:
  - Jobs & Workflows
  - DBSQL / query history / warehouse monitoring
  - Clusters & SQL warehouses
  - Identity, groups, and permissions
  - Cost & usage
  - Audit logs & security events
  - Pipelines / streaming / Lakeflow observability [web:120][web:128][web:129]
- Exposes **agent‑ready tools**:
  - Functions that return tool specifications compatible with:
    - Databricks Agent Framework tools
    - LangChain / LangGraph tools
    - MCP servers (via a thin wrapper) [web:61][web:9]
- Encapsulates:
  - Databricks SDK usage (`WorkspaceClient`)
  - Input validation & parameter normalization
  - Paging / filtering patterns [web:81]

Non‑goals:

- Not a full SDK replacement.
- Not a general REST client generator.
- No destructive admin operations in v1 (read‑only only).

---

## 2. Architecture Overview

### 2.1 High‑Level Modules

Implement a Python package with modules:

- `admin_ai_bridge/`
  - `__init__.py`
  - `config.py`
  - `schemas.py`
  - `errors.py`
  - `jobs.py`
  - `dbsql.py`
  - `clusters.py`
  - `security.py`
  - `usage.py`
  - `audit.py`
  - `pipelines.py`
  - `tools_databricks_agent.py`
  - `tools_mcp.py` (optional)

Each domain module:

- `jobs.py`: Jobs & Workflows observability. [web:128]  
- `dbsql.py`: DBSQL query history & performance. [web:81]  
- `clusters.py`: clusters and SQL warehouses (utilization, long‑running, idle, basic policy checks). [web:115][web:119][web:120]  
- `security.py`: identity, groups, permissions, workspace ACL questions. [web:117][web:131]  
- `usage.py`: cost & usage metrics by scope (cluster/job/warehouse/workspace). [web:125][web:127]  
- `audit.py`: audit log queries for security & compliance events. [web:122][web:126][web:130]  
- `pipelines.py`: pipelines / DLT / Lakeflow jobs (lags, failures, SLA). [web:129][web:133]

### 2.2 Dependencies

- `databricks-sdk`
- `pydantic`
- Optional:
  - `databricks-agents` (Databricks Agent Framework integration) [web:62]
  - MCP server runtime (if we ship a default MCP server entrypoint) [web:9]

Use Python 3.10+.

---

## 3. Environment & Workspace

- Workspace: **`https://e2-demo-field-eng.cloud.databricks.com`** is the target for all development, testing, and demo notebooks. [web:81]  
- Auth:
  - Use a Databricks CLI profile in `~/.databrickscfg` (for example, `DEFAULT`).
  - All code must obtain credentials via `WorkspaceClient(profile=...)` or equivalent config, not hard‑coded tokens. [web:81]  
- Distribution:
  - All examples and test flows must be implemented as **Databricks notebooks** so they can be bundled into the **`db-demos`** repository and run end‑to‑end in this workspace. [web:99]

---

## 4. Core APIs to Implement

### 4.1 Config & Client Management

```
# admin_ai_bridge/config.py
from databricks.sdk import WorkspaceClient
from pydantic import BaseModel

class AdminBridgeConfig(BaseModel):
    profile: str | None = None
    host: str | None = None
    token: str | None = None

def get_workspace_client(cfg: AdminBridgeConfig | None = None) -> WorkspaceClient:
    """
    Resolve a WorkspaceClient from:
    - profile (preferred; must be defined in ~/.databrickscfg and point to e2-demo-field-eng.cloud.databricks.com)
    - host + token
    - environment variables / default config
    """
    if cfg and cfg.profile:
        return WorkspaceClient(profile=cfg.profile)
    if cfg and cfg.host and cfg.token:
        return WorkspaceClient(host=cfg.host, token=cfg.token)
    # Fallback: rely on default env/config
    return WorkspaceClient()
```

### 4.2 Shared Schemas

```
# admin_ai_bridge/schemas.py
from pydantic import BaseModel
from datetime import datetime

class JobRunSummary(BaseModel):
    job_id: int
    job_name: str
    run_id: int
    state: str
    life_cycle_state: str | None
    start_time: datetime | None
    end_time: datetime | None
    duration_seconds: float | None

class QueryHistoryEntry(BaseModel):
    query_id: str
    warehouse_id: str | None
    user_name: str | None
    status: str | None
    start_time: datetime | None
    end_time: datetime | None
    duration_seconds: float | None
    sql_text: str | None

class ClusterSummary(BaseModel):
    cluster_id: str
    cluster_name: str
    state: str
    creator: str | None
    start_time: datetime | None
    driver_node_type: str | None
    node_type: str | None
    cluster_policy_id: str | None
    last_activity_time: datetime | None
    is_long_running: bool | None

class PermissionEntry(BaseModel):
    object_type: str
    object_id: str
    principal: str
    permission_level: str

class UsageEntry(BaseModel):
    scope: str  # cluster/job/warehouse/workspace
    name: str
    start_time: datetime
    end_time: datetime
    cost: float | None
    dbus: float | None

class AuditEvent(BaseModel):
    event_time: datetime
    service_name: str
    event_type: str
    user_name: str | None
    source_ip: str | None
    details: dict | None

class PipelineStatus(BaseModel):
    pipeline_id: str
    name: str
    state: str
    last_update_time: datetime | None
    lag_seconds: float | None
    last_error: str | None
```

---

## 5. Domain Modules

### 5.1 Jobs Admin

```
# admin_ai_bridge/jobs.py
from datetime import datetime, timedelta
from .config import get_workspace_client
from .schemas import JobRunSummary

class JobsAdmin:
    def __init__(self, cfg=None):
        self.ws = get_workspace_client(cfg)

    def list_long_running_jobs(
        self,
        min_duration_hours: float = 4.0,
        lookback_hours: float = 24.0,
        limit: int = 100,
    ) -> list[JobRunSummary]:
        """
        List job runs with duration > min_duration_hours within the last lookback_hours.
        """
        # Use jobs/runs APIs via self.ws.jobs, filter by timestamps and duration.
        ...

    def list_failed_jobs(
        self,
        lookback_hours: float = 24.0,
        limit: int = 100,
    ) -> list[JobRunSummary]:
        """
        List failed job runs within the last lookback_hours.
        """
        ...
```

Use Jobs APIs. Filter by `start_time`, `end_time`, `state`. [web:81][web:128]

### 5.2 DBSQL Admin

```
# admin_ai_bridge/dbsql.py
from datetime import datetime, timedelta
from .config import get_workspace_client
from .schemas import QueryHistoryEntry

class DBSQLAdmin:
    def __init__(self, cfg=None):
        self.ws = get_workspace_client(cfg)

    def top_slowest_queries(
        self,
        lookback_hours: float = 24.0,
        limit: int = 20,
    ) -> list[QueryHistoryEntry]:
        """
        Return the top N slowest queries by duration in the given time window.
        """
        ...

    def user_query_summary(
        self,
        user_name: str,
        lookback_hours: float = 24.0,
    ) -> dict:
        """
        Summarize queries for a given user in the last window:
        counts, average duration, failure rate, etc.
        """
        ...
```

Use DBSQL query history APIs or system tables, apply time filter and sorting. [web:81]

### 5.3 Clusters & Warehouses Admin

```
# admin_ai_bridge/clusters.py
from datetime import datetime, timedelta
from .config import get_workspace_client
from .schemas import ClusterSummary

class ClustersAdmin:
    def __init__(self, cfg=None):
        self.ws = get_workspace_client(cfg)

    def list_long_running_clusters(
        self,
        min_duration_hours: float = 8.0,
        lookback_hours: float = 24.0,
        limit: int = 100,
    ) -> list[ClusterSummary]:
        """
        List clusters that have been running longer than min_duration_hours
        within the lookback_hours window.
        """
        ...

    def list_idle_clusters(
        self,
        idle_hours: float = 2.0,
        limit: int = 100,
    ) -> list[ClusterSummary]:
        """
        List clusters with no activity in the last idle_hours.
        """
        ...
```

Use cluster list APIs and last activity metrics. [web:115][web:119][web:120]

### 5.4 Security Admin

```
# admin_ai_bridge/security.py
from .config import get_workspace_client
from .schemas import PermissionEntry

class SecurityAdmin:
    def __init__(self, cfg=None):
        self.ws = get_workspace_client(cfg)

    def who_can_manage_job(self, job_id: int) -> list[PermissionEntry]:
        """
        Return principals (users/groups/service principals) with CAN_MANAGE on the given job.
        """
        ...

    def who_can_use_cluster(self, cluster_id: str) -> list[PermissionEntry]:
        """
        Return principals with permission to attach/use the given cluster.
        """
        ...
```

Use permissions APIs / workspace ACL metadata. [web:117][web:128][web:131]

### 5.5 Usage & Cost Admin

```
# admin_ai_bridge/usage.py
from .config import get_workspace_client
from .schemas import UsageEntry

class UsageAdmin:
    def __init__(self, cfg=None):
        self.ws = get_workspace_client(cfg)

    def top_cost_centers(
        self,
        lookback_days: int = 7,
        limit: int = 20,
    ) -> list[UsageEntry]:
        """
        Return the top N cost contributors (clusters/jobs/warehouses/workspaces)
        over the given time window.
        """
        ...
```

Query usage/cost tables or billing exports where available. [web:125][web:127]

### 5.6 Audit Logs Admin

```
# admin_ai_bridge/audit.py
from datetime import datetime, timedelta
from .config import get_workspace_client
from .schemas import AuditEvent

class AuditAdmin:
    def __init__(self, cfg=None):
        self.ws = get_workspace_client(cfg)

    def failed_logins(
        self,
        lookback_hours: float = 24.0,
        limit: int = 100,
    ) -> list[AuditEvent]:
        """
        Return failed login attempts in the audit logs.
        """
        ...

    def recent_admin_changes(
        self,
        lookback_hours: float = 24.0,
        limit: int = 100,
    ) -> list[AuditEvent]:
        """
        Return audit events related to admin group membership changes or
        sensitive permission changes.
        """
        ...
```

Query audit log tables / log sinks. [web:122][web:126][web:130]

### 5.7 Pipelines / Streaming Admin

```
# admin_ai_bridge/pipelines.py
from .config import get_workspace_client
from .schemas import PipelineStatus

class PipelinesAdmin:
    def __init__(self, cfg=None):
        self.ws = get_workspace_client(cfg)

    def list_lagging_pipelines(
        self,
        max_lag_seconds: float = 600.0,
        limit: int = 50,
    ) -> list[PipelineStatus]:
        """
        List streaming/Lakeflow pipelines whose lag exceeds max_lag_seconds.
        """
        ...

    def list_failed_pipelines(
        self,
        lookback_hours: float = 24.0,
        limit: int = 50,
    ) -> list[PipelineStatus]:
        """
        List pipelines that have failed in the last lookback_hours.
        """
        ...
```

Use jobs/pipelines APIs and observability endpoints. [web:129][web:133]

---

## 6. Agent‑Friendly Tool Layer

### 6.1 Databricks Agent Framework Tools

```
# admin_ai_bridge/tools_databricks_agent.py
from databricks.agents import ToolSpec
from .jobs import JobsAdmin
from .dbsql import DBSQLAdmin
from .clusters import ClustersAdmin
from .security import SecurityAdmin
from .usage import UsageAdmin
from .audit import AuditAdmin
from .pipelines import PipelinesAdmin

def jobs_admin_tools(cfg=None) -> list[ToolSpec]:
    jobs = JobsAdmin(cfg)

    def _list_long_running_jobs(min_duration_hours: float = 4.0, lookback_hours: float = 24.0, limit: int = 20):
        return [j.model_dump() for j in jobs.list_long_running_jobs(
            min_duration_hours=min_duration_hours,
            lookback_hours=lookback_hours,
            limit=limit,
        )]

    def _list_failed_jobs(lookback_hours: float = 24.0, limit: int = 20):
        return [j.model_dump() for j in jobs.list_failed_jobs(
            lookback_hours=lookback_hours,
            limit=limit,
        )]

    return [
        ToolSpec.python(
            func=_list_long_running_jobs,
            name="list_long_running_jobs",
            description="List job runs longer than a specified number of hours in a recent time window.",
        ),
        ToolSpec.python(
            func=_list_failed_jobs,
            name="list_failed_jobs",
            description="List failed job runs in a recent time window.",
        ),
    ]


def dbsql_admin_tools(cfg=None) -> list[ToolSpec]:
    db = DBSQLAdmin(cfg)

    def _top_slowest_queries(lookback_hours: float = 24.0, limit: int = 20):
        return [q.model_dump() for q in db.top_slowest_queries(
            lookback_hours=lookback_hours,
            limit=limit,
        )]

    return [
        ToolSpec.python(
            func=_top_slowest_queries,
            name="top_slowest_queries",
            description="Return the top slowest queries by duration within a time window.",
        )
    ]


def clusters_admin_tools(cfg=None) -> list[ToolSpec]:
    clusters = ClustersAdmin(cfg)

    def _list_long_running_clusters(min_duration_hours: float = 8.0, lookback_hours: float = 24.0, limit: int = 50):
        return [c.model_dump() for c in clusters.list_long_running_clusters(
            min_duration_hours=min_duration_hours,
            lookback_hours=lookback_hours,
            limit=limit,
        )]

    def _list_idle_clusters(idle_hours: float = 2.0, limit: int = 50):
        return [c.model_dump() for c in clusters.list_idle_clusters(
            idle_hours=idle_hours,
            limit=limit,
        )]

    return [
        ToolSpec.python(
            func=_list_long_running_clusters,
            name="list_long_running_clusters",
            description="List clusters running longer than a specified number of hours.",
        ),
        ToolSpec.python(
            func=_list_idle_clusters,
            name="list_idle_clusters",
            description="List clusters that have been idle longer than a threshold.",
        ),
    ]


def security_admin_tools(cfg=None) -> list[ToolSpec]:
    sec = SecurityAdmin(cfg)

    def _who_can_manage_job(job_id: int):
        return [p.model_dump() for p in sec.who_can_manage_job(job_id)]

    def _who_can_use_cluster(cluster_id: str):
        return [p.model_dump() for p in sec.who_can_use_cluster(cluster_id)]

    return [
        ToolSpec.python(
            func=_who_can_manage_job,
            name="who_can_manage_job",
            description="Return principals with CAN_MANAGE on a given job.",
        ),
        ToolSpec.python(
            func=_who_can_use_cluster,
            name="who_can_use_cluster",
            description="Return principals allowed to use a given cluster.",
        ),
    ]


def usage_admin_tools(cfg=None) -> list[ToolSpec]:
    usage = UsageAdmin(cfg)

    def _top_cost_centers(lookback_days: int = 7, limit: int = 20):
        return [u.model_dump() for u in usage.top_cost_centers(
            lookback_days=lookback_days,
            limit=limit,
        )]

    return [
        ToolSpec.python(
            func=_top_cost_centers,
            name="top_cost_centers",
            description="Return the top cost‑contributing workloads in the given time window.",
        )
    ]


def audit_admin_tools(cfg=None) -> list[ToolSpec]:
    audit = AuditAdmin(cfg)

    def _failed_logins(lookback_hours: float = 24.0, limit: int = 100):
        return [e.model_dump() for e in audit.failed_logins(
            lookback_hours=lookback_hours,
            limit=limit,
        )]

    def _recent_admin_changes(lookback_hours: float = 24.0, limit: int = 100):
        return [e.model_dump() for e in audit.recent_admin_changes(
            lookback_hours=lookback_hours,
            limit=limit,
        )]

    return [
        ToolSpec.python(
            func=_failed_logins,
            name="failed_logins",
            description="Return failed login events in the given time window.",
        ),
        ToolSpec.python(
            func=_recent_admin_changes,
            name="recent_admin_changes",
            description="Return recent admin or permission change events.",
        ),
    ]


def pipelines_admin_tools(cfg=None) -> list[ToolSpec]:
    pipes = PipelinesAdmin(cfg)

    def _list_lagging_pipelines(max_lag_seconds: float = 600.0, limit: int = 50):
        return [p.model_dump() for p in pipes.list_lagging_pipelines(
            max_lag_seconds=max_lag_seconds,
            limit=limit,
        )]

    def _list_failed_pipelines(lookback_hours: float = 24.0, limit: int = 50):
        return [p.model_dump() for p in pipes.list_failed_pipelines(
            lookback_hours=lookback_hours,
            limit=limit,
        )]

    return [
        ToolSpec.python(
            func=_list_lagging_pipelines,
            name="list_lagging_pipelines",
            description="List pipelines whose lag exceeds a threshold.",
        ),
        ToolSpec.python(
            func=_list_failed_pipelines,
            name="list_failed_pipelines",
            description="List pipelines that have failed recently.",
        ),
    ]
```

All tools must be **read‑only**, return JSON‑serializable outputs, and have clear descriptions. [web:61]

### 6.2 MCP Tool Layer (Optional)

Optionally add `tools_mcp.py` or an MCP server entrypoint to expose the same methods as MCP tools, using the same underlying admin classes. [web:9][web:96]

---

## 7. Example: Admin Agent Notebook for `db-demos`

Create a Databricks notebook targeting `e2-demo-field-eng.cloud.databricks.com` that:

- Configures `AdminBridgeConfig(profile="DEFAULT")`.
- Aggregates tools from all domains.
- Deploys an `admin-observability-agent` endpoint.

Example cell:

```
from databricks.sdk import WorkspaceClient
from databricks import agents

from admin_ai_bridge.config import AdminBridgeConfig
from admin_ai_bridge.tools_databricks_agent import (
    jobs_admin_tools,
    dbsql_admin_tools,
    clusters_admin_tools,
    security_admin_tools,
    usage_admin_tools,
    audit_admin_tools,
    pipelines_admin_tools,
)

cfg = AdminBridgeConfig(profile="DEFAULT")

tools = (
    jobs_admin_tools(cfg)
    + dbsql_admin_tools(cfg)
    + clusters_admin_tools(cfg)
    + security_admin_tools(cfg)
    + usage_admin_tools(cfg)
    + audit_admin_tools(cfg)
    + pipelines_admin_tools(cfg)
)

agent_spec = agents.AgentSpec(
    name="admin_observability_agent",
    system_prompt=(
        "You are a Databricks admin assistant. "
        "Use the tools to answer questions about jobs, queries, clusters, permissions, "
        "usage, audit logs, and pipelines. Never perform destructive operations."
    ),
    llm_endpoint="databricks-claude-3-7-sonnet",
    tools=tools,
)

deployed = agents.deploy(model=agent_spec, name="admin-observability-agent")
deployed.endpoint_name
```

Add further cells showing example queries and validating responses.

---

## 8. Security & Guardrails

- All methods and tools are **read‑only** in v1:
  - No job/cluster deletion, no stopping jobs, no changing permissions. [web:61]  
- Hard‑code or document safe defaults:
  - `lookback_hours` / `lookback_days` with reasonable maximums.
  - `limit` on number of returned items.
- Add warnings in docstrings and tool descriptions:
  - “This tool is for observability and reporting only; do not perform destructive actions.”

---

## 9. Deliverables

- Python package `admin_ai_bridge` with:
  - `config.py`, `schemas.py`, `errors.py`
  - `jobs.py`, `dbsql.py`, `clusters.py`, `security.py`, `usage.py`, `audit.py`, `pipelines.py`
  - `tools_databricks_agent.py` (and optionally `tools_mcp.py`).
- One or more **Databricks notebooks** for `db-demos` that:
  - Run on `https://e2-demo-field-eng.cloud.databricks.com`. [web:81][web:99]  
  - Demonstrate each domain’s methods.
  - Deploy and exercise `admin-observability-agent`.
- Optional MCP server entrypoint exposing the same tools via MCP for external clients. [web:9][web:96]
```
