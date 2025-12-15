# Databricks Admin AI Bridge

An AI-friendly Python library for Databricks platform admin APIs (Jobs, DBSQL, clusters, security, usage, audit logs, pipelines, etc.). Provides strongly typed, high-level classes with agent-ready tool specifications for MCP clients and Databricks agents.

## Overview

`databricks-admin-ai-bridge` simplifies working with Databricks admin APIs by:

- Providing **strongly typed, high-level classes** for core admin domains
- Exposing **agent-ready tools** compatible with:
  - Databricks Agent Framework
  - LangChain / LangGraph
  - MCP servers
- Encapsulating SDK usage, input validation, and pagination patterns
- Offering **read-only operations** for safe observability and monitoring

## Features

### Supported Domains

- **Jobs & Workflows**: Long-running jobs, failed runs, job observability
- **DBSQL**: Query history, performance analysis, warehouse monitoring
- **Clusters**: Long-running clusters, idle detection, utilization monitoring
- **Security**: Permissions, identity, groups, workspace ACL queries
- **Usage & Cost**: Cost centers, DBU consumption, resource usage tracking
- **Audit Logs**: Security events, compliance monitoring, login tracking
- **Pipelines**: DLT/Lakeflow status, lag detection, failure monitoring

### Core Capabilities

- **Read-only operations**: Safe for production monitoring without risk of destructive actions
- **Pydantic schemas**: Strongly typed data models for all operations
- **Databricks SDK integration**: Built on top of the official Databricks SDK
- **Agent-friendly**: Tool specifications optimized for AI agents and chatbots

## Installation

### From PyPI (when published)

```bash
pip install databricks-admin-ai-bridge
```

### From source

```bash
git clone https://github.com/databricks/databricks-admin-ai-bridge.git
cd databricks-admin-ai-bridge
pip install -e .
```

### With optional dependencies

```bash
# For Databricks Agent Framework integration
pip install databricks-admin-ai-bridge[agents]

# For development
pip install -r requirements-dev.txt
```

## Quick Start

### Authentication

Configure your Databricks workspace credentials using one of these methods:

1. **Using CLI profile** (recommended):
```python
from admin_ai_bridge import AdminBridgeConfig, get_workspace_client

cfg = AdminBridgeConfig(profile="DEFAULT")
client = get_workspace_client(cfg)
```

2. **Using host + token**:
```python
cfg = AdminBridgeConfig(
    host="https://e2-demo-field-eng.cloud.databricks.com",
    token="dapi..."
)
client = get_workspace_client(cfg)
```

3. **Using environment variables**:
```bash
export DATABRICKS_HOST="https://e2-demo-field-eng.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
```

```python
client = get_workspace_client()
```

### Basic Usage

```python
from admin_ai_bridge import AdminBridgeConfig
from admin_ai_bridge.jobs import JobsAdmin
from admin_ai_bridge.dbsql import DBSQLAdmin
from admin_ai_bridge.clusters import ClustersAdmin

# Configure connection
cfg = AdminBridgeConfig(profile="DEFAULT")

# Jobs monitoring
jobs = JobsAdmin(cfg)
long_running = jobs.list_long_running_jobs(min_duration_hours=4.0, lookback_hours=24.0)
failed = jobs.list_failed_jobs(lookback_hours=24.0)

# DBSQL monitoring
dbsql = DBSQLAdmin(cfg)
slow_queries = dbsql.top_slowest_queries(lookback_hours=24.0, limit=20)
user_summary = dbsql.user_query_summary(user_name="admin@example.com", lookback_hours=24.0)

# Cluster monitoring
clusters = ClustersAdmin(cfg)
long_running_clusters = clusters.list_long_running_clusters(min_duration_hours=8.0)
idle_clusters = clusters.list_idle_clusters(idle_hours=2.0)
```

### Using with Databricks Agent Framework

```python
from databricks import agents
from admin_ai_bridge.config import AdminBridgeConfig
from admin_ai_bridge.tools_databricks_agent import (
    jobs_admin_tools,
    dbsql_admin_tools,
    clusters_admin_tools,
)

cfg = AdminBridgeConfig(profile="DEFAULT")

tools = (
    jobs_admin_tools(cfg)
    + dbsql_admin_tools(cfg)
    + clusters_admin_tools(cfg)
)

agent_spec = agents.AgentSpec(
    name="admin_observability_agent",
    system_prompt=(
        "You are a Databricks admin assistant. "
        "Use the tools to answer questions about jobs, queries, and clusters. "
        "Never perform destructive operations."
    ),
    llm_endpoint="databricks-claude-3-7-sonnet",
    tools=tools,
)

deployed = agents.deploy(model=agent_spec, name="admin-observability-agent")
print(f"Agent deployed at: {deployed.endpoint_name}")
```

## Target Workspace

This library is designed and tested for use with:

**Workspace**: `https://e2-demo-field-eng.cloud.databricks.com`

All examples and demo notebooks target this workspace and use authentication via Databricks CLI profiles (`~/.databrickscfg`).

## Security & Guardrails

- **Read-only operations**: Version 1.0 implements only safe, read-only operations
- **No destructive actions**: No job deletion, cluster termination, or permission changes
- **Safe defaults**: Reasonable limits on lookback periods and result counts
- **Input validation**: Pydantic schemas validate all inputs

## Project Structure

```
databricks-admin-ai-bridge/
├── admin_ai_bridge/          # Main package
│   ├── __init__.py           # Package exports
│   ├── config.py             # Configuration and client management
│   ├── schemas.py            # Pydantic models
│   ├── errors.py             # Custom exceptions
│   ├── jobs.py               # Jobs admin (Phase 2)
│   ├── dbsql.py              # DBSQL admin (Phase 2)
│   ├── clusters.py           # Clusters admin (Phase 2)
│   ├── security.py           # Security admin (Phase 2)
│   ├── usage.py              # Usage admin (Phase 2)
│   ├── audit.py              # Audit logs admin (Phase 2)
│   ├── pipelines.py          # Pipelines admin (Phase 2)
│   ├── tools_databricks_agent.py  # Agent tools (Phase 3)
│   └── tools_mcp.py          # MCP tools (Phase 3)
├── tests/                    # Test suite
│   ├── unit/                 # Unit tests
│   └── integration/          # Integration tests
├── notebooks/                # Databricks notebooks for db-demos
├── examples/                 # Example scripts and usage
├── README.md                 # This file
├── pyproject.toml            # Project metadata and dependencies
├── requirements.txt          # Core dependencies
└── requirements-dev.txt      # Development dependencies
```

## Development

### Running tests

```bash
pytest tests/
```

### Code formatting

```bash
black admin_ai_bridge tests
ruff check admin_ai_bridge tests
```

### Type checking

```bash
mypy admin_ai_bridge
```

## Roadmap

- **Phase 1** (Current): Core framework and schemas
- **Phase 2**: Domain module implementations (jobs, dbsql, clusters, security, usage, audit, pipelines)
- **Phase 3**: Agent tool layers (Databricks Agent Framework, MCP)
- **Phase 4**: Databricks notebooks for db-demos
- **Future**: Write operations with safety guardrails, additional domains

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

Apache License 2.0

## Support

For issues and questions:

- GitHub Issues: https://github.com/databricks/databricks-admin-ai-bridge/issues
- Documentation: https://github.com/databricks/databricks-admin-ai-bridge/blob/main/README.md
