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

## Available Examples

### 1. basic_usage.py
Basic usage demonstration of core admin classes.

**Features:**
- AdminBridgeConfig configuration patterns
- JobsAdmin usage (long-running and failed jobs)
- DBSQLAdmin usage (slowest queries)
- ClustersAdmin usage (long-running clusters)
- Error handling examples

**Usage:**
```bash
# Run all demos
python examples/basic_usage.py

# Run specific demo
python examples/basic_usage.py --demo jobs
python examples/basic_usage.py --demo dbsql
python examples/basic_usage.py --demo clusters
python examples/basic_usage.py --demo errors

# Use different profile
python examples/basic_usage.py --profile PROD

# Enable verbose logging
python examples/basic_usage.py --verbose
```

### 2. jobs_monitoring.py
Comprehensive job monitoring and analysis.

**Features:**
- Long-running job detection with configurable thresholds
- Failed job identification and reporting
- Job trend analysis across multiple dimensions
- Formatted tabular reports

**Usage:**
```bash
# Run all reports
python examples/jobs_monitoring.py

# Run specific report
python examples/jobs_monitoring.py --report long-running
python examples/jobs_monitoring.py --report failed
python examples/jobs_monitoring.py --report trends

# Customize parameters
python examples/jobs_monitoring.py --min-duration 6.0 --lookback 48.0 --limit 100
```

### 3. query_analysis.py
DBSQL query performance analysis and optimization.

**Features:**
- Top slowest queries identification
- User-specific query summaries
- Performance metrics by user and warehouse
- Duration formatting and statistics

**Usage:**
```bash
# Run all reports
python examples/query_analysis.py

# Run specific reports
python examples/query_analysis.py --report slowest
python examples/query_analysis.py --report metrics

# User-specific analysis
python examples/query_analysis.py --report user --user john.doe@company.com

# Customize parameters
python examples/query_analysis.py --lookback 48.0 --limit 50
```

### 4. cluster_monitoring.py
Cluster utilization monitoring and cost optimization.

**Features:**
- Long-running cluster detection
- Idle cluster identification
- Cluster utilization summaries by creator and node type
- Cost optimization recommendations

**Usage:**
```bash
# Run all reports
python examples/cluster_monitoring.py

# Run specific reports
python examples/cluster_monitoring.py --report long-running
python examples/cluster_monitoring.py --report idle
python examples/cluster_monitoring.py --report utilization

# Customize thresholds
python examples/cluster_monitoring.py --min-duration 12.0 --idle-threshold 6.0
```

### 5. security_audit.py
Security and permissions analysis.

**Features:**
- Job permissions queries (who can manage specific jobs)
- Cluster permissions queries
- Comprehensive permissions audit across multiple resources
- Access control best practices

**Usage:**
```bash
# Run summary report
python examples/security_audit.py

# Audit specific job
python examples/security_audit.py --report job --job-id 12345

# Audit specific cluster
python examples/security_audit.py --report cluster --cluster-id abc-123-def

# Comprehensive audit across multiple resources
python examples/security_audit.py --report comprehensive

# Run all reports
python examples/security_audit.py --report all
```

### 6. cost_tracking.py
Usage and cost tracking with budget monitoring.

**Features:**
- Top cost centers by dimension (cluster, job, warehouse, workspace, project)
- Budget vs actuals status monitoring
- Budget breach and warning alerts
- Cost optimization recommendations

**Usage:**
```bash
# Run all reports
python examples/cost_tracking.py

# Run specific reports
python examples/cost_tracking.py --report costs
python examples/cost_tracking.py --report budget
python examples/cost_tracking.py --report recommendations

# Analyze different dimensions
python examples/cost_tracking.py --dimension cluster
python examples/cost_tracking.py --dimension job
python examples/cost_tracking.py --dimension warehouse

# Customize tables (if not using defaults)
python examples/cost_tracking.py --usage-table my_catalog.my_schema.usage --budget-table my_catalog.my_schema.budgets
```

### 7. pipeline_monitoring.py
Pipeline status and lag monitoring for DLT and Lakeflow.

**Features:**
- Lagging pipeline detection with configurable thresholds
- Failed pipeline identification
- Pipeline health summary with severity assessment
- Lag categorization (critical, high, medium)
- Troubleshooting recommendations

**Usage:**
```bash
# Run all reports
python examples/pipeline_monitoring.py

# Run specific reports
python examples/pipeline_monitoring.py --report lagging
python examples/pipeline_monitoring.py --report failed
python examples/pipeline_monitoring.py --report health

# Customize lag threshold (in seconds)
python examples/pipeline_monitoring.py --max-lag 1800.0  # 30 minutes

# Customize lookback for failures
python examples/pipeline_monitoring.py --lookback 48.0  # 48 hours
```

## Common Command-Line Options

All example scripts support the following common options:

- `--profile PROFILE`: Databricks CLI profile name (default: DEFAULT)
- `--verbose` or `-v`: Enable verbose logging for debugging
- `--help` or `-h`: Show help message with all available options

## Running Examples

### Basic execution:
```bash
# From repository root
python examples/basic_usage.py
python examples/jobs_monitoring.py
python examples/query_analysis.py
```

### With custom profile:
```bash
python examples/jobs_monitoring.py --profile PROD
```

### With verbose logging:
```bash
python examples/cluster_monitoring.py --verbose
```

## Target Workspace

All examples are configured to work with:
- Workspace: `https://e2-demo-field-eng.cloud.databricks.com`
- Profile: `DEFAULT` (in `~/.databrickscfg`)
