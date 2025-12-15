# Integration Test Guide

This guide explains how to run integration tests for the Databricks Admin AI Bridge library against a real Databricks workspace.

## Overview

Integration tests validate that the library works correctly with real Databricks APIs and data. Unlike unit tests that use mocks, integration tests make actual API calls to a configured workspace.

**Target Workspace:** `https://e2-demo-field-eng.cloud.databricks.com`

## Prerequisites

### 1. Databricks Workspace Access

You need access to a Databricks workspace with:

- **Jobs:** Some scheduled/running jobs (for testing JobsAdmin)
- **DBSQL:** Query history from SQL warehouses (for testing DBSQLAdmin)
- **Clusters:** Active or terminated clusters (for testing ClustersAdmin)
- **Security:** Permissions configured on jobs/clusters (for testing SecurityAdmin)
- **Usage Data:** Optional - usage/cost tracking (for testing UsageAdmin)
- **Audit Logs:** Optional - audit log export configured (for testing AuditAdmin)
- **Pipelines:** Optional - Delta Live Tables pipelines (for testing PipelinesAdmin)

### 2. Databricks CLI Configuration

Install the Databricks CLI and configure authentication:

```bash
# Install Databricks CLI (if not already installed)
pip install databricks-cli

# Configure authentication profile
databricks configure --token
```

When prompted:
- **Databricks Host:** `https://e2-demo-field-eng.cloud.databricks.com`
- **Token:** Your personal access token (generate in workspace settings)

This creates a `~/.databrickscfg` file with your credentials.

### 3. Python Environment

Ensure you have Python 3.8+ with required dependencies:

```bash
# Install the package in development mode
pip install -e .

# Install test dependencies
pip install -r requirements-dev.txt
```

## Configuration

### ~/.databrickscfg File

Your `~/.databrickscfg` should look like this:

```ini
[DEFAULT]
host = https://e2-demo-field-eng.cloud.databricks.com
token = dapi1234567890abcdef...
```

**Note:** The integration tests use the `DEFAULT` profile. If you use a different profile name, you'll need to modify the test fixtures.

### Alternative: Environment Variables

You can also set credentials via environment variables:

```bash
export DATABRICKS_HOST="https://e2-demo-field-eng.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi1234567890abcdef..."
```

## Running Integration Tests

### Run All Integration Tests

```bash
# Run all integration tests with verbose output
pytest -m integration -v

# Run with logging output
pytest -m integration -v --log-cli-level=INFO
```

### Run Specific Domain Tests

```bash
# Test only JobsAdmin
pytest tests/integration/test_jobs_integration.py -v

# Test only DBSQLAdmin
pytest tests/integration/test_dbsql_integration.py -v

# Test only ClustersAdmin
pytest tests/integration/test_clusters_integration.py -v

# Test only SecurityAdmin
pytest tests/integration/test_security_integration.py -v

# Test only UsageAdmin
pytest tests/integration/test_usage_integration.py -v

# Test only AuditAdmin
pytest tests/integration/test_audit_integration.py -v

# Test only PipelinesAdmin
pytest tests/integration/test_pipelines_integration.py -v
```

### Run Specific Test Methods

```bash
# Run a single test method
pytest tests/integration/test_jobs_integration.py::TestJobsAdminIntegration::test_list_long_running_jobs_real_workspace -v

# Run all tests in a class
pytest tests/integration/test_dbsql_integration.py::TestDBSQLAdminIntegration -v
```

### Generate Coverage Report

```bash
# Run integration tests with coverage
pytest -m integration --cov=admin_ai_bridge --cov-report=html

# View coverage report
open htmlcov/index.html
```

## Expected Behavior

### Successful Tests

When tests pass, you'll see output like:

```
tests/integration/test_jobs_integration.py::TestJobsAdminIntegration::test_list_long_running_jobs_real_workspace PASSED
tests/integration/test_dbsql_integration.py::TestDBSQLAdminIntegration::test_top_slowest_queries_real_workspace PASSED
tests/integration/test_clusters_integration.py::TestClustersAdminIntegration::test_list_all_clusters_real_workspace PASSED
```

### Skipped Tests

Some tests may be skipped if required data is not available:

```
tests/integration/test_usage_integration.py::TestUsageAdminIntegration::test_top_cost_centers_real_workspace SKIPPED
Reason: Usage data not available in workspace
```

This is normal behavior. The tests gracefully handle:
- Empty result sets (e.g., no failed jobs in the time window)
- Missing features (e.g., audit logs not configured)
- Optional functionality (e.g., usage tracking not enabled)

### Empty Results vs Errors

**Empty results are OK:**
- "No long-running jobs found" - workspace may have no qualifying jobs
- "No failed logins found" - workspace may have no recent failures
- "No lagging pipelines found" - pipelines may all be healthy

**These are errors:**
- Authentication failures (bad token or host)
- Permission denied (insufficient workspace access)
- API timeouts (network issues)
- Invalid parameters (negative durations, etc.)

## Troubleshooting

### Authentication Errors

**Error:** `DatabricksError: Authentication failed`

**Solution:**
1. Verify your token is valid: `databricks workspace ls /`
2. Check `~/.databrickscfg` format
3. Regenerate token in workspace settings if expired

### Permission Errors

**Error:** `DatabricksError: User does not have permission`

**Solution:**
1. Ensure your user has workspace admin or appropriate permissions
2. Check with workspace administrator
3. Some tests require specific permissions (e.g., viewing all jobs)

### Connection Timeouts

**Error:** `requests.exceptions.ConnectTimeout`

**Solution:**
1. Check network connectivity to workspace
2. Verify workspace URL is correct
3. Check for proxy/firewall issues
4. Increase timeout if on slow network

### Module Import Errors

**Error:** `ModuleNotFoundError: No module named 'admin_ai_bridge'`

**Solution:**
1. Install package in development mode: `pip install -e .`
2. Ensure virtual environment is activated
3. Check Python path: `python -c "import sys; print(sys.path)"`

### Workspace Data Issues

**Issue:** Most tests are skipped due to missing data

**Solution:**
1. Ensure workspace has some activity (jobs, queries, clusters)
2. Wait for system tables to populate (can take hours for new workspaces)
3. Consider creating test jobs/clusters/queries
4. Some features (audit, usage) require explicit configuration

## Test Data Requirements

### Minimal Workspace Setup

For integration tests to be meaningful, your workspace should have:

```bash
# Create a simple test job (via CLI or UI)
databricks jobs create --json '{
  "name": "Integration Test Job",
  "tasks": [{
    "task_key": "test_task",
    "notebook_task": {
      "notebook_path": "/Shared/test_notebook",
      "source": "WORKSPACE"
    },
    "new_cluster": {
      "spark_version": "13.3.x-scala2.12",
      "node_type_id": "i3.xlarge",
      "num_workers": 1
    }
  }]
}'

# Create a test cluster (via CLI or UI)
databricks clusters create --json '{
  "cluster_name": "Integration Test Cluster",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "i3.xlarge",
  "num_workers": 1
}'

# Run some SQL queries (via SQL editor or API)
databricks sql execute "SELECT * FROM samples.nyctaxi.trips LIMIT 10"
```

### Optional: Enable Audit Logs

To test AuditAdmin functionality:

1. Go to workspace **Admin Settings** > **Audit Logs**
2. Configure delivery to cloud storage (S3/ADLS/GCS)
3. Wait 1-2 hours for logs to be delivered
4. Configure system tables to read audit logs

### Optional: Enable Usage Tracking

To test UsageAdmin functionality:

1. Go to workspace **Admin Settings** > **Usage**
2. Enable usage tracking
3. Wait 24 hours for data to populate
4. Access `system.billing.usage` tables

## Continuous Integration

### GitHub Actions Example

```yaml
name: Integration Tests

on:
  schedule:
    - cron: '0 2 * * *'  # Run daily at 2 AM
  workflow_dispatch:  # Allow manual trigger

jobs:
  integration:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          pip install -e .
          pip install -r requirements-dev.txt

      - name: Configure Databricks
        run: |
          mkdir -p ~/.databrickscfg
          echo "[DEFAULT]" > ~/.databrickscfg
          echo "host = ${{ secrets.DATABRICKS_HOST }}" >> ~/.databrickscfg
          echo "token = ${{ secrets.DATABRICKS_TOKEN }}" >> ~/.databrickscfg

      - name: Run integration tests
        run: pytest -m integration -v --log-cli-level=INFO
```

**Required GitHub Secrets:**
- `DATABRICKS_HOST`: Workspace URL
- `DATABRICKS_TOKEN`: Service principal or bot token

## Best Practices

### 1. Use Isolated Workspace

Run integration tests against a dedicated test/development workspace, not production.

### 2. Use Service Principal

For CI/CD, use a service principal token instead of personal access token:

```bash
# Create service principal in workspace
databricks service-principals create --display-name "Integration Tests"

# Generate token for service principal
databricks tokens create --lifetime-seconds 3600 --comment "Integration test token"
```

### 3. Clean Up Test Resources

After running tests, clean up any created resources:

```bash
# List and delete test jobs
databricks jobs list | grep "Integration Test"

# List and delete test clusters
databricks clusters list | grep "Integration Test"
```

### 4. Monitor Test Results

Track integration test results over time:
- Set up alerts for test failures
- Monitor test execution time
- Track API rate limits/quotas
- Review logs for warnings

### 5. Limit Test Scope

Integration tests can be slow and expensive. Consider:
- Running only critical tests in CI/CD
- Using smaller lookback windows
- Limiting result counts
- Running full suite only nightly

## Support

### Issues and Questions

- **Library Issues:** Open issue on GitHub repository
- **Workspace Access:** Contact your Databricks administrator
- **API Questions:** Refer to [Databricks API Documentation](https://docs.databricks.com/api/)
- **Test Failures:** Check logs, enable debug logging with `--log-cli-level=DEBUG`

### Useful Commands

```bash
# Check Databricks CLI configuration
databricks configure --token

# Test workspace connectivity
databricks workspace ls /

# List available jobs
databricks jobs list --output JSON

# List available clusters
databricks clusters list --output JSON

# Check current user
databricks current-user me
```

## Summary

Integration tests ensure the Admin AI Bridge library works correctly with real Databricks workspaces. Key points:

- Tests use the `DEFAULT` profile from `~/.databrickscfg`
- Target workspace: `https://e2-demo-field-eng.cloud.databricks.com`
- Tests handle missing data gracefully (skips are OK)
- Run with: `pytest -m integration -v`
- Some tests require workspace configuration (audit logs, usage tracking)
- Use service principals for CI/CD
- Monitor test results and clean up resources

For questions or issues, consult this guide or open an issue on the project repository.
