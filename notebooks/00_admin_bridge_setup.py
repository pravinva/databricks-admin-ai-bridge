# Databricks notebook source
# MAGIC %md
# MAGIC # Admin AI Bridge - Setup and Validation
# MAGIC
# MAGIC This notebook installs and validates the `admin_ai_bridge` library for use in the Databricks workspace.
# MAGIC
# MAGIC **Target Workspace:** `https://e2-demo-field-eng.cloud.databricks.com`
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Databricks Runtime 13.3 LTS or higher
# MAGIC - Python 3.10+
# MAGIC - Appropriate workspace permissions for admin API access
# MAGIC
# MAGIC **What This Notebook Does:**
# MAGIC 1. Installs the admin_ai_bridge library
# MAGIC 2. Configures authentication using workspace credentials
# MAGIC 3. Validates connection to Databricks APIs
# MAGIC 4. Displays library version and available tools
# MAGIC 5. Runs basic connectivity tests

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Admin AI Bridge Library
# MAGIC
# MAGIC Install the library directly from the local source or from PyPI/Git repository.

# COMMAND ----------

# Install dependencies and the admin_ai_bridge library from GitHub
%pip install --upgrade databricks-sdk>=0.23.0 pydantic>=2.0.0 "databricks-agents>=0.3.0"
%pip install git+https://github.com/pravinva/databricks-admin-ai-bridge.git

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import and Configure
# MAGIC
# MAGIC Import the library and configure authentication. When running in a Databricks notebook,
# MAGIC the library automatically uses the notebook's execution context for authentication.

# COMMAND ----------

from admin_ai_bridge.config import AdminBridgeConfig, get_workspace_client
from admin_ai_bridge.jobs import JobsAdmin
from admin_ai_bridge.dbsql import DBSQLAdmin
from admin_ai_bridge.clusters import ClustersAdmin
from admin_ai_bridge.security import SecurityAdmin
from admin_ai_bridge.usage import UsageAdmin
from admin_ai_bridge.audit import AuditAdmin
from admin_ai_bridge.pipelines import PipelinesAdmin
import admin_ai_bridge
__version__ = admin_ai_bridge.__version__
from databricks.sdk import WorkspaceClient

print(f"Admin AI Bridge version: {__version__}")
print(f"Successfully imported admin_ai_bridge modules")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configure Authentication
# MAGIC
# MAGIC When running in a Databricks notebook, we can use the default authentication
# MAGIC which leverages the notebook's execution context. No explicit profile is needed.

# COMMAND ----------

# Initialize configuration
# When cfg is None or empty, it uses the notebook's execution context
cfg = AdminBridgeConfig()

# Test workspace client connection
ws = get_workspace_client(cfg)
print(f"✓ Successfully connected to workspace")
print(f"✓ Workspace URL: {ws.config.host}")

# Get current user to verify authentication
try:
    current_user = ws.current_user.me()
    print(f"✓ Authenticated as: {current_user.user_name}")
    print(f"✓ User active: {current_user.active}")
except Exception as e:
    print(f"✗ Error getting current user: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Initialize Admin Classes
# MAGIC
# MAGIC Create instances of each admin class to verify they can be instantiated properly.
# MAGIC
# MAGIC **Performance Optimization:** For fast queries, provide a `warehouse_id` to use system tables
# MAGIC instead of slower API calls (10-100x faster for large workspaces).

# COMMAND ----------

import os

# Get warehouse ID from environment or find the first available
warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
if not warehouse_id:
    # Try to find the first available warehouse
    try:
        warehouses = list(ws.warehouses.list(max_results=1))
        if warehouses:
            warehouse_id = warehouses[0].id
            print(f"✓ Using warehouse: {warehouse_id}")
    except Exception as e:
        print(f"⚠ Could not find warehouse ID, will use API methods: {e}")

# Initialize all admin classes with warehouse_id for fast system table queries
jobs_admin = JobsAdmin(cfg, warehouse_id=warehouse_id)
dbsql_admin = DBSQLAdmin(cfg, warehouse_id=warehouse_id)
clusters_admin = ClustersAdmin(cfg, warehouse_id=warehouse_id)
security_admin = SecurityAdmin(cfg)  # No system table queries
usage_admin = UsageAdmin(cfg, warehouse_id=warehouse_id)
audit_admin = AuditAdmin(cfg)  # No system table queries
pipelines_admin = PipelinesAdmin(cfg)  # No system table queries

print("✓ Successfully initialized all admin classes:")
print("  - JobsAdmin: List and monitor job runs (system tables enabled)")
print("  - DBSQLAdmin: Query history and performance analysis (system tables enabled)")
print("  - ClustersAdmin: Cluster monitoring and utilization (system tables enabled)")
print("  - SecurityAdmin: Permissions and access control")
print("  - UsageAdmin: Cost tracking and budget monitoring (system tables enabled)")
print("  - AuditAdmin: Audit log queries and security events")
print("  - PipelinesAdmin: Pipeline monitoring and observability")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Test Basic Connectivity
# MAGIC
# MAGIC Run simple API calls to verify each admin class can communicate with Databricks APIs.

# COMMAND ----------

import pandas as pd
from datetime import datetime

print("Testing connectivity for each admin domain...\n")

# Test Jobs Admin
try:
    failed_jobs = jobs_admin.list_failed_jobs(lookback_hours=24.0, limit=5)
    print(f"✓ JobsAdmin: Found {len(failed_jobs)} failed jobs in last 24 hours")
except Exception as e:
    print(f"✗ JobsAdmin error: {e}")

# Test DBSQL Admin
try:
    slow_queries = dbsql_admin.top_slowest_queries(lookback_hours=24.0, limit=5)
    print(f"✓ DBSQLAdmin: Found {len(slow_queries)} slow queries in last 24 hours")
except Exception as e:
    print(f"✗ DBSQLAdmin error: {e}")

# Test Clusters Admin
try:
    idle_clusters = clusters_admin.list_idle_clusters(idle_hours=2.0, limit=5)
    print(f"✓ ClustersAdmin: Found {len(idle_clusters)} idle clusters")
except Exception as e:
    print(f"✗ ClustersAdmin error: {e}")

# Test Usage Admin
try:
    cost_centers = usage_admin.top_cost_centers(lookback_days=7, limit=5)
    print(f"✓ UsageAdmin: Found {len(cost_centers)} cost centers in last 7 days")
except Exception as e:
    print(f"✗ UsageAdmin error: {e}")

# Test Audit Admin
try:
    failed_logins = audit_admin.failed_logins(lookback_hours=24.0, limit=5)
    print(f"✓ AuditAdmin: Found {len(failed_logins)} failed logins in last 24 hours")
except Exception as e:
    print(f"✗ AuditAdmin error: {e}")

# Test Pipelines Admin
try:
    failed_pipelines = pipelines_admin.list_failed_pipelines(lookback_hours=24.0, limit=5)
    print(f"✓ PipelinesAdmin: Found {len(failed_pipelines)} failed pipelines in last 24 hours")
except Exception as e:
    print(f"✗ PipelinesAdmin error: {e}")

print("\n✓ All connectivity tests completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Display Available Tools
# MAGIC
# MAGIC Show all the tools that can be used with Databricks Agent Framework.

# COMMAND ----------

from admin_ai_bridge import (
    jobs_admin_tools,
    dbsql_admin_tools,
    clusters_admin_tools,
    security_admin_tools,
    usage_admin_tools,
    audit_admin_tools,
    pipelines_admin_tools,
)

# Get all tools
all_tools = (
    jobs_admin_tools(cfg)
    + dbsql_admin_tools(cfg)
    + clusters_admin_tools(cfg)
    + security_admin_tools(cfg)
    + usage_admin_tools(cfg)
    + audit_admin_tools(cfg)
    + pipelines_admin_tools(cfg)
)

print(f"Total tools available: {len(all_tools)}\n")
print("Available tools by domain:\n")

tool_info = []
for tool in all_tools:
    tool_info.append({
        "Tool Name": tool.name,
        "Description": tool.description[:60] + "..." if len(tool.description) > 60 else tool.description
    })

tools_df = pd.DataFrame(tool_info)
display(tools_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Workspace Information
# MAGIC
# MAGIC Display key information about the connected workspace.

# COMMAND ----------

# Get workspace details
workspace_info = {
    "Workspace URL": ws.config.host,
    "Library Version": __version__,
    "Python Version": __import__('sys').version.split()[0],
    "Current User": current_user.user_name if 'current_user' in locals() else "Unknown",
    "Timestamp": datetime.now().isoformat(),
    "Total Tools Available": len(all_tools)
}

print("=" * 60)
print("WORKSPACE INFORMATION")
print("=" * 60)
for key, value in workspace_info.items():
    print(f"{key:.<30} {value}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC The Admin AI Bridge library has been successfully installed and validated!
# MAGIC
# MAGIC **Next Steps:**
# MAGIC 1. Explore individual domain notebooks (Jobs, DBSQL, Clusters, Security, Usage, Audit, Pipelines)
# MAGIC 2. Deploy the full admin observability agent (see notebook 07)
# MAGIC 3. Integrate with Slack, Teams, or Claude Desktop
# MAGIC
# MAGIC **Available Notebooks:**
# MAGIC - `01_jobs_admin_demo.py` - Jobs monitoring and observability
# MAGIC - `02_dbsql_admin_demo.py` - Query performance analysis
# MAGIC - `03_clusters_admin_demo.py` - Cluster monitoring and cost optimization
# MAGIC - `04_security_admin_demo.py` - Permissions and security auditing
# MAGIC - `05_usage_cost_budget_demo.py` - Cost tracking and budget monitoring
# MAGIC - `06_audit_pipelines_demo.py` - Audit logs and pipeline observability
# MAGIC - `07_agent_deployment.py` - Full agent deployment and testing

# COMMAND ----------
