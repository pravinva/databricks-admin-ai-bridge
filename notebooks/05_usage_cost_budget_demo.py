# Databricks notebook source
# MAGIC %md
# MAGIC # Usage, Cost, and Budget Demo
# MAGIC
# MAGIC This notebook demonstrates the **UsageAdmin** class for cost tracking, chargeback, and budget monitoring.
# MAGIC
# MAGIC **Target Workspace:** `https://e2-demo-field-eng.cloud.databricks.com`
# MAGIC
# MAGIC **What You'll Learn:**
# MAGIC - Track top cost centers
# MAGIC - Implement chargeback by dimension (workspace, cluster, project, team)
# MAGIC - Monitor budget status and utilization
# MAGIC - Generate cost visualizations
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - "What are the top cost centers in the last 7 days?"
# MAGIC - "Show me cost by project for the last 30 days"
# MAGIC - "Which teams are over 80% of their monthly budget?"
# MAGIC - "Calculate chargeback by workspace"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from admin_ai_bridge import AdminBridgeConfig, UsageAdmin
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

# Initialize configuration
cfg = AdminBridgeConfig()

# Databricks notebook widget for warehouse_id
dbutils.widgets.text("warehouse_id", "4b9b953939869799", "SQL Warehouse ID")
warehouse_id = dbutils.widgets.get("warehouse_id")

# Initialize with warehouse_id for fast system table queries
usage_admin = UsageAdmin(cfg, warehouse_id=warehouse_id)

print(f"✓ UsageAdmin initialized successfully (system tables enabled with warehouse: {warehouse_id})")
print(f"Usage table: {usage_admin.usage_table}")
print(f"Budget table: {usage_admin.budget_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Top Cost Centers
# MAGIC
# MAGIC Identify the resources consuming the most costs/DBUs.

# COMMAND ----------

# Get top 20 cost centers from last 7 days
top_costs = usage_admin.top_cost_centers(
    lookback_days=7,
    limit=20
)

print(f"Found {len(top_costs)} cost centers\n")

if top_costs:
    # Convert to DataFrame
    cost_data = []
    for entry in top_costs:
        cost_data.append({
            "Scope": entry.scope,
            "Name": entry.name,
            "DBUs": round(entry.dbus, 2) if entry.dbus else None,
            "Cost": f"${entry.cost:.2f}" if entry.cost else "N/A",
            "Start Time": entry.start_time.strftime("%Y-%m-%d") if entry.start_time else None,
            "End Time": entry.end_time.strftime("%Y-%m-%d") if entry.end_time else None
        })

    df = pd.DataFrame(cost_data)
    display(df)

    # Show summary
    total_dbus = sum(e.dbus for e in top_costs if e.dbus)
    print(f"\n" + "=" * 60)
    print("COST CENTER SUMMARY")
    print("=" * 60)
    print(f"Total DBUs (top {len(top_costs)}): {total_dbus:,.1f}")

    # Count by scope
    from collections import Counter
    scopes = [e.scope for e in top_costs]
    scope_counts = Counter(scopes)
    print(f"\nBreakdown by scope:")
    for scope, count in scope_counts.items():
        print(f"  {scope}: {count}")
    print("=" * 60)
else:
    print("No cost center data available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cost by Dimension (Chargeback)
# MAGIC
# MAGIC Aggregate costs by different dimensions for chargeback analysis.
# MAGIC
# MAGIC **Note:** This requires usage tables with appropriate schema. If tables don't exist,
# MAGIC example code is provided showing how to query them.

# COMMAND ----------

# Example: Cost by workspace
print("=" * 70)
print("CHARGEBACK ANALYSIS: COST BY WORKSPACE")
print("=" * 70)

try:
    workspace_costs = usage_admin.cost_by_dimension(
        dimension="workspace",
        lookback_days=30,
        limit=20
    )

    if workspace_costs:
        print(f"\nFound {len(workspace_costs)} workspaces with cost data:\n")

        workspace_data = []
        for entry in workspace_costs:
            workspace_data.append({
                "Workspace": entry.name,
                "Total Cost": f"${entry.cost:.2f}" if entry.cost else "N/A",
                "Total DBUs": f"{entry.dbus:,.1f}" if entry.dbus else "N/A",
                "Period": f"{entry.start_time.strftime('%Y-%m-%d')} to {entry.end_time.strftime('%Y-%m-%d')}"
            })

        df_workspace = pd.DataFrame(workspace_data)
        display(df_workspace)

        # Calculate total
        total_cost = sum(e.cost for e in workspace_costs if e.cost)
        total_dbus = sum(e.dbus for e in workspace_costs if e.dbus)
        print(f"\nTotal across all workspaces:")
        print(f"  Cost: ${total_cost:,.2f}")
        print(f"  DBUs: {total_dbus:,.1f}")
    else:
        print("\n⚠ No workspace cost data found")
        print("This may be because:")
        print("  1. Usage table doesn't exist yet")
        print("  2. No data in the specified time period")
        print("  3. Table schema needs to be created")

except Exception as e:
    print(f"\n⚠ Error querying workspace costs: {e}")
    print("\nThis is expected if usage tables haven't been set up yet.")
    print("See the setup section below for creating sample data.")

print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cost by Project (Tag-Based Chargeback)
# MAGIC
# MAGIC Example of tag-based chargeback for project tracking.

# COMMAND ----------

print("=" * 70)
print("CHARGEBACK ANALYSIS: COST BY PROJECT")
print("=" * 70)

try:
    project_costs = usage_admin.cost_by_dimension(
        dimension="tag:project",
        lookback_days=30,
        limit=20
    )

    if project_costs:
        print(f"\nFound {len(project_costs)} projects with cost data:\n")

        project_data = []
        for entry in project_costs:
            project_data.append({
                "Project": entry.name,
                "Total Cost": f"${entry.cost:.2f}" if entry.cost else "N/A",
                "Total DBUs": f"{entry.dbus:,.1f}" if entry.dbus else "N/A"
            })

        df_project = pd.DataFrame(project_data)
        display(df_project)

        # Visualize top 10 projects
        if len(project_costs) >= 3:
            top_10 = project_costs[:10]
            plt.figure(figsize=(10, 6))
            plt.barh([e.name for e in top_10], [e.cost or 0 for e in top_10])
            plt.xlabel('Cost ($)')
            plt.ylabel('Project')
            plt.title('Top 10 Projects by Cost (Last 30 Days)')
            plt.tight_layout()
            plt.show()
    else:
        print("\n⚠ No project cost data found")
        print("This requires usage tables with project tags")

except Exception as e:
    print(f"\n⚠ Error querying project costs: {e}")
    print("This is expected if usage tables haven't been configured with tags.")

print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cost by Cluster (Detailed Chargeback)
# MAGIC
# MAGIC Track costs at the cluster level for detailed chargeback.

# COMMAND ----------

print("=" * 70)
print("CHARGEBACK ANALYSIS: COST BY CLUSTER")
print("=" * 70)

try:
    cluster_costs = usage_admin.cost_by_dimension(
        dimension="cluster",
        lookback_days=30,
        limit=50
    )

    if cluster_costs:
        print(f"\nFound {len(cluster_costs)} clusters with cost data:\n")

        cluster_data = []
        for entry in cluster_costs:
            cluster_data.append({
                "Cluster ID": entry.name[:20] + "..." if len(entry.name) > 20 else entry.name,
                "Total Cost": f"${entry.cost:.2f}" if entry.cost else "N/A",
                "Total DBUs": f"{entry.dbus:,.1f}" if entry.dbus else "N/A"
            })

        df_cluster = pd.DataFrame(cluster_data)
        display(df_cluster.head(20))  # Show top 20

        # Summary statistics
        total_cost = sum(e.cost for e in cluster_costs if e.cost)
        avg_cost = total_cost / len(cluster_costs) if cluster_costs else 0

        print(f"\nCluster Cost Summary:")
        print(f"  Total clusters: {len(cluster_costs)}")
        print(f"  Total cost: ${total_cost:,.2f}")
        print(f"  Average cost per cluster: ${avg_cost:,.2f}")
    else:
        print("\n⚠ No cluster cost data found")

except Exception as e:
    print(f"\n⚠ Error querying cluster costs: {e}")

print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Budget Status Monitoring
# MAGIC
# MAGIC Track budget utilization and identify overspending.

# COMMAND ----------

print("=" * 70)
print("BUDGET STATUS MONITORING")
print("=" * 70)

try:
    # Check workspace budgets
    budget_status = usage_admin.budget_status(
        dimension="workspace",
        period_days=30,
        warn_threshold=0.8
    )

    if budget_status:
        print(f"\nFound budget data for {len(budget_status)} entities:\n")

        budget_data = []
        breached = 0
        warning = 0
        within = 0

        for status in budget_status:
            budget_data.append({
                "Entity": status["dimension_value"],
                "Budget": f"${status['budget_amount']:,.2f}",
                "Actual": f"${status['actual_cost']:,.2f}",
                "Utilization": f"{status['utilization_pct']:.1f}%",
                "Status": status["status"]
            })

            if status["status"] == "breached":
                breached += 1
            elif status["status"] == "warning":
                warning += 1
            else:
                within += 1

        df_budget = pd.DataFrame(budget_data)
        display(df_budget)

        # Summary
        print(f"\nBudget Status Summary:")
        print(f"  Within budget: {within}")
        print(f"  Warning (>80%): {warning}")
        print(f"  Breached (>100%): {breached}")

        if breached > 0:
            print(f"\n⚠ ALERT: {breached} entities have exceeded their budget!")

        # Visualize budget utilization
        if len(budget_status) >= 3:
            plt.figure(figsize=(12, 6))
            entities = [s["dimension_value"][:15] for s in budget_status[:10]]
            utilizations = [s["utilization_pct"] for s in budget_status[:10]]

            colors = ['red' if u >= 100 else 'orange' if u >= 80 else 'green' for u in utilizations]

            plt.barh(entities, utilizations, color=colors)
            plt.axvline(x=80, color='orange', linestyle='--', label='Warning (80%)')
            plt.axvline(x=100, color='red', linestyle='--', label='Budget Limit')
            plt.xlabel('Budget Utilization (%)')
            plt.ylabel('Entity')
            plt.title('Budget Utilization Status')
            plt.legend()
            plt.tight_layout()
            plt.show()
    else:
        print("\n⚠ No budget data found")
        print("This requires a budget table to be configured")

except Exception as e:
    print(f"\n⚠ Error querying budget status: {e}")
    print("This is expected if budget tables haven't been set up yet.")

print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Budget Status by Project
# MAGIC
# MAGIC Monitor project-level budgets.

# COMMAND ----------

print("=" * 70)
print("PROJECT BUDGET MONITORING")
print("=" * 70)

try:
    project_budgets = usage_admin.budget_status(
        dimension="project",
        period_days=30,
        warn_threshold=0.8
    )

    if project_budgets:
        print(f"\nFound budget data for {len(project_budgets)} projects:\n")

        # Show projects needing attention
        alerts = [b for b in project_budgets if b["status"] in ["warning", "breached"]]

        if alerts:
            print(f"⚠ {len(alerts)} projects need attention:\n")

            alert_data = []
            for alert in alerts:
                alert_data.append({
                    "Project": alert["dimension_value"],
                    "Budget": f"${alert['budget_amount']:,.2f}",
                    "Actual": f"${alert['actual_cost']:,.2f}",
                    "Over Budget": f"${alert['actual_cost'] - alert['budget_amount']:,.2f}",
                    "Status": alert["status"].upper()
                })

            df_alerts = pd.DataFrame(alert_data)
            display(df_alerts)
        else:
            print("✓ All projects are within budget!")

        # Show all projects
        all_project_data = []
        for proj in project_budgets:
            all_project_data.append({
                "Project": proj["dimension_value"],
                "Utilization": f"{proj['utilization_pct']:.1f}%",
                "Status": proj["status"]
            })

        print(f"\nAll project budget status:")
        df_all_projects = pd.DataFrame(all_project_data)
        display(df_all_projects)
    else:
        print("\n⚠ No project budget data found")

except Exception as e:
    print(f"\n⚠ Error querying project budgets: {e}")

print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Cost Trend Analysis
# MAGIC
# MAGIC Analyze cost trends over time.

# COMMAND ----------

print("=" * 70)
print("COST TREND ANALYSIS")
print("=" * 70)

# Compare costs across different time periods
periods = [
    (7, "Last 7 days"),
    (14, "Last 14 days"),
    (30, "Last 30 days")
]

trend_data = []
for days, label in periods:
    costs = usage_admin.top_cost_centers(lookback_days=days, limit=100)
    total_dbus = sum(c.dbus for c in costs if c.dbus)

    trend_data.append({
        "Period": label,
        "Total DBUs": f"{total_dbus:,.1f}",
        "Resource Count": len(costs)
    })

df_trend = pd.DataFrame(trend_data)
print("\nCost trends across time periods:\n")
display(df_trend)

# Visualize if we have data
if trend_data and any(float(t["Total DBUs"].replace(",", "")) > 0 for t in trend_data):
    plt.figure(figsize=(10, 6))
    periods_labels = [t["Period"] for t in trend_data]
    dbus_values = [float(t["Total DBUs"].replace(",", "")) for t in trend_data]

    plt.bar(periods_labels, dbus_values, color='skyblue')
    plt.ylabel('Total DBUs')
    plt.title('DBU Consumption Trends')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Example Natural Language Queries
# MAGIC
# MAGIC These are the types of questions you can answer with UsageAdmin:

# COMMAND ----------

print("=" * 70)
print("EXAMPLE QUESTIONS YOU CAN ANSWER WITH USAGEADMIN")
print("=" * 70)

examples = [
    {
        "Question": "What are the top cost centers in the last 7 days?",
        "Method": "top_cost_centers(lookback_days=7, limit=20)",
        "Use Case": "Cost visibility"
    },
    {
        "Question": "Show me cost by project for the last 30 days",
        "Method": "cost_by_dimension(dimension='tag:project', lookback_days=30)",
        "Use Case": "Project chargeback"
    },
    {
        "Question": "Which teams are over 80% of their monthly budget?",
        "Method": "budget_status(dimension='team', warn_threshold=0.8)",
        "Use Case": "Budget monitoring"
    },
    {
        "Question": "Calculate chargeback by workspace",
        "Method": "cost_by_dimension(dimension='workspace', lookback_days=30)",
        "Use Case": "Workspace chargeback"
    },
    {
        "Question": "Which clusters are the most expensive?",
        "Method": "cost_by_dimension(dimension='cluster') + sort by cost",
        "Use Case": "Cost optimization"
    },
    {
        "Question": "Are any projects over budget this month?",
        "Method": "budget_status(dimension='project') + filter breached",
        "Use Case": "Budget alerts"
    }
]

df_examples = pd.DataFrame(examples)
display(df_examples)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Setup Guide for Usage Tables
# MAGIC
# MAGIC **Note:** The cost_by_dimension and budget_status methods require usage and budget tables.
# MAGIC Here's an example of how to create sample data for testing:

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Create usage table (example schema)
# MAGIC CREATE TABLE IF NOT EXISTS billing.usage_events (
# MAGIC   timestamp TIMESTAMP,
# MAGIC   workspace_id STRING,
# MAGIC   cluster_id STRING,
# MAGIC   job_id STRING,
# MAGIC   warehouse_id STRING,
# MAGIC   cost DOUBLE,
# MAGIC   dbu_consumed DOUBLE,
# MAGIC   tags MAP<STRING, STRING>
# MAGIC );
# MAGIC
# MAGIC -- Create budget table (example schema)
# MAGIC CREATE TABLE IF NOT EXISTS billing.budgets (
# MAGIC   dimension_type STRING,
# MAGIC   dimension_value STRING,
# MAGIC   budget_amount DOUBLE,
# MAGIC   period STRING
# MAGIC );
# MAGIC
# MAGIC -- Insert sample usage data
# MAGIC INSERT INTO billing.usage_events VALUES
# MAGIC   (current_timestamp(), 'workspace-1', 'cluster-abc', 'job-123', 'warehouse-x', 150.00, 300.0, map('project', 'ml-training', 'team', 'data-science')),
# MAGIC   (current_timestamp(), 'workspace-2', 'cluster-def', 'job-456', 'warehouse-y', 200.00, 400.0, map('project', 'analytics', 'team', 'analytics'));
# MAGIC
# MAGIC -- Insert sample budget data
# MAGIC INSERT INTO billing.budgets VALUES
# MAGIC   ('workspace', 'workspace-1', 5000.00, '2024-01'),
# MAGIC   ('workspace', 'workspace-2', 7500.00, '2024-01'),
# MAGIC   ('project', 'ml-training', 3000.00, '2024-01'),
# MAGIC   ('project', 'analytics', 4000.00, '2024-01');
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC The UsageAdmin class provides comprehensive cost tracking and budget management:
# MAGIC
# MAGIC **Key Features:**
# MAGIC - ✓ Track top cost centers by scope (cluster, warehouse, workspace)
# MAGIC - ✓ Implement chargeback by multiple dimensions (workspace, cluster, project, team)
# MAGIC - ✓ Monitor budget status with configurable warning thresholds
# MAGIC - ✓ Generate cost trend analysis and visualizations
# MAGIC - ✓ Support for tag-based cost allocation
# MAGIC
# MAGIC **Cost/Chargeback Capabilities:**
# MAGIC - ✓ cost_by_dimension(): Aggregate costs for chargeback reports
# MAGIC - ✓ budget_status(): Monitor budget vs actuals with status tracking
# MAGIC - ✓ Flexible dimensions: workspace, cluster, job, warehouse, custom tags
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - Set up usage and budget tables for full functionality
# MAGIC - Explore audit logs in `06_audit_pipelines_demo.py`
# MAGIC - Deploy the full agent with budget/chargeback support in `07_agent_deployment.py`

# COMMAND ----------
