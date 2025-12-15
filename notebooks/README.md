# Databricks Notebooks

This directory contains Databricks notebooks for demonstration and integration with `db-demos`.

## Target Workspace

All notebooks are designed to run on:
- **Workspace**: `https://e2-demo-field-eng.cloud.databricks.com`
- **Authentication**: Databricks CLI profile (`DEFAULT`)

## Notebooks (Coming in Phase 4)

### Admin Observability Agent
- `01_admin_agent_setup.py` - Setup and deploy admin observability agent
- `02_jobs_monitoring.py` - Jobs monitoring demonstrations
- `03_query_analysis.py` - DBSQL query analysis demonstrations
- `04_cluster_monitoring.py` - Cluster monitoring demonstrations
- `05_security_audit.py` - Security and permissions demonstrations
- `06_usage_tracking.py` - Usage and cost tracking demonstrations
- `07_pipeline_monitoring.py` - Pipeline monitoring demonstrations
- `08_end_to_end_demo.py` - Complete end-to-end demonstration

## Installation in Databricks

### Option 1: Install from Notebook

Add this cell at the beginning of any notebook:

```python
%pip install git+https://github.com/databricks/databricks-admin-ai-bridge.git
dbutils.library.restartPython()
```

### Option 2: Create Cluster Library

1. Go to your cluster configuration
2. Click "Libraries" tab
3. Click "Install New"
4. Select "PyPI"
5. Enter package name: `databricks-admin-ai-bridge`
6. Click "Install"

## Usage in Notebooks

```python
from admin_ai_bridge import AdminBridgeConfig
from admin_ai_bridge.jobs import JobsAdmin

# Use default authentication (service principal or personal token)
cfg = AdminBridgeConfig()

# Initialize admin class
jobs = JobsAdmin(cfg)

# Query data
long_running = jobs.list_long_running_jobs(min_duration_hours=4.0)
display(long_running)
```

## Integration with db-demos

These notebooks are designed to be bundled into the `db-demos` repository and run end-to-end in the target workspace. They demonstrate:

1. Core admin API patterns
2. Agent deployment and usage
3. Real-world monitoring scenarios
4. Best practices for observability

## Requirements

- Databricks Runtime 13.3 LTS or higher
- Python 3.10 or higher
- Access to workspace admin APIs (requires appropriate permissions)
