# Examples

This directory contains example scripts demonstrating how to use the Databricks Admin AI Bridge library.

## Prerequisites

1. Install the library:
```bash
pip install -e ..
```

2. Configure Databricks authentication:
   - Option A: Set up a CLI profile in `~/.databrickscfg`
   - Option B: Set environment variables `DATABRICKS_HOST` and `DATABRICKS_TOKEN`

## Examples (Coming in Phase 2)

- `basic_usage.py` - Basic usage of core admin classes
- `jobs_monitoring.py` - Job monitoring and analysis
- `query_analysis.py` - DBSQL query performance analysis
- `cluster_monitoring.py` - Cluster utilization monitoring
- `security_audit.py` - Security and permissions analysis
- `cost_tracking.py` - Usage and cost tracking
- `pipeline_monitoring.py` - Pipeline status and lag monitoring

## Running Examples

```bash
python examples/basic_usage.py
```

## Target Workspace

All examples are configured to work with:
- Workspace: `https://e2-demo-field-eng.cloud.databricks.com`
- Profile: `DEFAULT` (in `~/.databrickscfg`)
