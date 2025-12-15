# Databricks Admin AI Bridge – Product Spec

## 1. Overview

We want a small, opinionated library that makes **Databricks admin APIs** (Jobs, DBSQL, clusters, security, usage, audit logs, pipelines) easy to use from **AI agents and MCP clients**. It is inspired by Databricks AI Bridge for Genie and Vector Search, but focused on **platform / observability / admin** use cases. [web:68][web:61][web:120]

Primary users:

- Platform engineers / admins building Slack/Teams/Claude assistants.
- Data engineers building Databricks agents that answer infra questions (jobs, queries, clusters, permissions, cost). [web:62][web:89][web:120]

Target environment:

- Workspace: **`https://e2-demo-field-eng.cloud.databricks.com`**. [web:81]  
- Auth: Databricks CLI profile configured in `~/.databrickscfg` (for example, `DEFAULT`).  
- Deliverables must include **Databricks notebooks** suitable for bundling into **`db-demos`**. [web:99]

## 2. Objectives & Non‑Objectives

### 2.1 Objectives

- Provide a Python library that:
  - Wraps Databricks Jobs, DBSQL, clusters, security, usage, audit, and pipelines APIs with **high‑level, safe methods**. [web:81][web:120][web:128][web:129][web:130]  
  - Exposes those methods as **tools** for:
    - Databricks Agent Framework.
    - (Optionally) MCP servers. [web:61][web:9]
- Make it trivial to answer questions like:
  - “Which jobs have been running longer than 4 hours in the last day?”
  - “Which queries are the slowest in the last 24 hours?”
  - “Which clusters have been idle more than 2 hours?”
  - “Who can manage this job or attach to this cluster?”
  - “Which workloads are the most expensive this week?”
  - “Show failed login attempts in the last 24 hours.”
  - “Which pipelines are behind by more than 10 minutes?” [web:115][web:117][web:125][web:122][web:129]

### 2.2 Non‑Objectives

- Not a full SDK replacement.
- Not a UI project; UI is Slack/Teams/Claude/etc.
- No destructive operations (no job/cluster deletion, policy changes) in v1.

## 3. User Stories

1. **Admin asks in Slack**: “Which jobs have been running longer than 4 hours today?”  
   - Slack bot forwards to a Databricks agent endpoint backed by this library.  
   - Agent calls a `list_long_running_jobs` tool and summarizes results. [web:61][web:62]

2. **Ops engineer asks in Teams**: “Show me top 10 slowest queries in the last 24 hours.”  
   - Teams app calls the same agent or MCP server.  
   - Agent calls `top_slowest_queries` tool and returns a table. [web:61][web:81]

3. **Platform admin asks**: “Which clusters are idle for more than 2 hours?”  
   - Agent calls `list_idle_clusters` and returns cluster names, owners, idle durations. [web:115][web:119]

4. **Security engineer asks**: “Who can manage job 123?” and “Show failed login attempts in the last day.”  
   - Agent uses `who_can_manage_job` and `failed_logins` tools. [web:117][web:122][web:130]

5. **Cost owner asks**: “Which clusters or jobs are the most expensive in the last 7 days?”  
   - Agent calls `top_cost_centers` and shows a ranked list. [web:125][web:127]

6. **DataOps asks**: “Which pipelines are behind by more than 10 minutes?”  
   - Agent calls `list_lagging_pipelines` and reports problematic pipelines. [web:129][web:133]

## 4. Scope

### 4.1 In Scope (v1)

- **Domains**:
  - Jobs observability. [web:128]
  - DBSQL query history / performance. [web:81]
  - Clusters and SQL warehouses (runtime, idle, basic policy checks). [web:115][web:120]
  - Identity & permissions (who can do what). [web:117][web:131]
  - Cost & usage (basic “top cost centers” from usage/billing data). [web:125][web:127]
  - Audit logs (failed logins, admin changes, resource activity). [web:122][web:126][web:130]
  - Pipelines / streaming / Lakeflow (lags, failures, SLA breaches). [web:129][web:133]

- **Integration**:
  - Databricks Agent Framework tools and one sample `admin-observability-agent` endpoint. [web:61][web:62]  
  - Example Databricks notebooks runnable in `e2-demo-field-eng.cloud.databricks.com` and packaged into `db-demos`. [web:99]

- **Security**:
  - Read‑only behavior.
  - Reasonable defaults (limited time windows and row limits).

### 4.2 Future Scope

- More detailed cluster policy diagnostics.
- Workspace/account‑level inventory and configuration comparisons. [web:120][web:127]
- Model serving endpoints stats.
- MCP server distribution as a first‑class artifact. [web:9][web:113]

## 5. Functional Requirements

### 5.1 Library APIs

Provide classes and methods (names may evolve, but the shape should be similar):

- `JobsAdmin`:
  - `list_long_running_jobs(min_duration_hours, lookback_hours, limit)`
  - `list_failed_jobs(lookback_hours, limit)`

- `DBSQLAdmin`:
  - `top_slowest_queries(lookback_hours, limit)`
  - `user_query_summary(user_name, lookback_hours)`

- `ClustersAdmin`:
  - `list_long_running_clusters(min_duration_hours, lookback_hours, limit)`
  - `list_idle_clusters(idle_hours, limit)`

- `SecurityAdmin`:
  - `who_can_manage_job(job_id)`
  - `who_can_use_cluster(cluster_id)`

- `UsageAdmin`:
  - `top_cost_centers(lookback_days, limit)`

- `AuditAdmin`:
  - `failed_logins(lookback_hours, limit)`
  - `recent_admin_changes(lookback_hours, limit)`

- `PipelinesAdmin`:
  - `list_lagging_pipelines(max_lag_seconds, limit)`
  - `list_failed_pipelines(lookback_hours, limit)`

Each method:

- Takes simple, numeric/time parameters.
- Returns Pydantic models (`JobRunSummary`, `QueryHistoryEntry`, `ClusterSummary`, `PermissionEntry`, `UsageEntry`, `AuditEvent`, `PipelineStatus`), easily converted to JSON. [web:81][web:120][web:129][web:130]

### 5.2 Tool Helper Functions

- `jobs_admin_tools(cfg)` → tools for Jobs.
- `dbsql_admin_tools(cfg)` → tools for DBSQL.
- `clusters_admin_tools(cfg)` → tools for Clusters.
- `security_admin_tools(cfg)` → tools for Security.
- `usage_admin_tools(cfg)` → tools for Usage.
- `audit_admin_tools(cfg)` → tools for Audit.
- `pipelines_admin_tools(cfg)` → tools for Pipelines.

Each tool:

- Wraps exactly one method of the corresponding admin class.
- Has a clear, concise description and parameter schema (for LLM tool‑calling). [web:61]

### 5.3 Behavior

- Methods must:
  - Handle Databricks auth via CLI profile or host/token (preferring CLI profile in `~/.databrickscfg` pointing at `e2-demo-field-eng.cloud.databricks.com`). [web:81]  
  - Filter by time windows (UTC).  
  - Sort and limit results appropriately.  
- Tools must:
  - Be read‑only.
  - Accept only safe parameters (numbers, simple strings).
  - Expose human‑readable descriptions.

### 5.4 Performance

- Must handle typical admin workloads:
  - Up to thousands of jobs, clusters, queries, and audit events in the lookback period. [web:129][web:130]
- Must apply sensible paging and early exits to avoid timeouts.

## 6. Non‑Functional Requirements

- **Observability**:
  - Basic logging of method name, input window, number of results, and errors. [web:125]
- **Testability**:
  - Unit tests with mocked `WorkspaceClient`.  
  - Integration tests in `e2-demo-field-eng.cloud.databricks.com` plus notebook equivalents in `db-demos`. [web:99]  
- **Documentation**:
  - README with quickstart.
  - Databricks notebooks demonstrating each domain and the combined `admin-observability-agent`.

## 7. Integration Points

- **Databricks SDK** for all underlying API calls. [web:81][web:120]  
- **Databricks Agent Framework** for tool registration and agent deployment. [web:61][web:62]  
- Optional:
  - MCP server runtime if we ship a default MCP entrypoint. [web:9][web:113]

## 8. Risks & Mitigations

- **Risk**: Over‑fetching data.  
  - Mitigation: enforce configurable but bounded time windows and `limit`s.

- **Risk**: LLMs misusing tools for sensitive operations.  
  - Mitigation: v1 is read‑only; no destructive APIs.

- **Risk**: API changes in Jobs/DBSQL/clusters/audit.  
  - Mitigation: keep logic thin and aligned with official SDKs and documentation. [web:81][web:120][web:130]

## 9. Success Metrics

- Internal / community adoption:
  - # of projects and agents using the library.
- Developer effort:
  - Fewer lines of custom Databricks REST code in Slack/Teams/Claude bots.
- Operational:
  - Notebooks and examples run successfully in `e2-demo-field-eng.cloud.databricks.com` and are accepted into `db-demos`. [web:99]

