# Databricks notebook source
# MAGIC %md
# MAGIC # Clusters Admin Demo
# MAGIC
# MAGIC This notebook demonstrates the **ClustersAdmin** class for cluster monitoring and cost optimization.
# MAGIC
# MAGIC **Target Workspace:** `https://e2-demo-field-eng.cloud.databricks.com`
# MAGIC
# MAGIC **What You'll Learn:**
# MAGIC - Find long-running clusters
# MAGIC - Identify idle clusters
# MAGIC - Analyze cluster utilization patterns
# MAGIC - Identify cost optimization opportunities
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - "Which clusters have been running longer than 8 hours?"
# MAGIC - "Show me clusters that have been idle for more than 2 hours"
# MAGIC - "What clusters are consuming the most resources?"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from admin_ai_bridge import AdminBridgeConfig, ClustersAdmin
from databricks.sdk import WorkspaceClient
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import os

# Initialize configuration
cfg = AdminBridgeConfig()

# Get warehouse ID for fast system table queries
warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
if not warehouse_id:
    ws = WorkspaceClient()
    try:
        warehouses = list(ws.warehouses.list(max_results=1))
        if warehouses:
            warehouse_id = warehouses[0].id
            print(f"✓ Using warehouse: {warehouse_id}")
    except Exception as e:
        print(f"⚠ Could not find warehouse ID, will use API methods: {e}")

# Initialize with warehouse_id for fast queries
clusters_admin = ClustersAdmin(cfg, warehouse_id=warehouse_id)

print("✓ ClustersAdmin initialized successfully (system tables enabled)" if warehouse_id else "✓ ClustersAdmin initialized successfully (API mode)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. List Long-Running Clusters
# MAGIC
# MAGIC Find clusters that have been running for extended periods.
# MAGIC This helps identify clusters that may have been forgotten or are inefficiently managed.

# COMMAND ----------

# Find clusters running longer than 8 hours
long_running = clusters_admin.list_long_running_clusters(
    min_duration_hours=8.0,
    lookback_hours=24.0,
    limit=50
)

print(f"Found {len(long_running)} long-running clusters\n")

if long_running:
    # Convert to DataFrame
    clusters_data = []
    for cluster in long_running:
        clusters_data.append({
            "Cluster ID": cluster.cluster_id[:16] + "...",
            "Cluster Name": cluster.cluster_name,
            "State": cluster.state,
            "Creator": cluster.creator,
            "Node Type": cluster.node_type,
            "Start Time": cluster.start_time.strftime("%Y-%m-%d %H:%M:%S") if cluster.start_time else None,
            "Last Activity": cluster.last_activity_time.strftime("%Y-%m-%d %H:%M:%S") if cluster.last_activity_time else None
        })

    df = pd.DataFrame(clusters_data)
    display(df)

    # Calculate total runtime
    print("\n" + "=" * 60)
    print("LONG-RUNNING CLUSTER SUMMARY")
    print("=" * 60)
    print(f"Total clusters: {len(long_running)}")

    # Count by state
    from collections import Counter
    states = [c.state for c in long_running]
    state_counts = Counter(states)
    print(f"\nClusters by state:")
    for state, count in state_counts.items():
        print(f"  {state}: {count}")
else:
    print("✓ No long-running clusters found - good resource management!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. List Idle Clusters
# MAGIC
# MAGIC Find clusters with no recent activity.
# MAGIC These are prime candidates for termination to reduce costs.

# COMMAND ----------

# Find clusters idle for more than 2 hours
idle_clusters = clusters_admin.list_idle_clusters(
    idle_hours=2.0,
    limit=50
)

print(f"Found {len(idle_clusters)} idle clusters\n")

if idle_clusters:
    # Convert to DataFrame
    idle_data = []
    for cluster in idle_clusters:
        # Calculate idle time if we have last_activity_time
        idle_hours = None
        if cluster.last_activity_time:
            from datetime import timezone
            now = datetime.now(timezone.utc)
            idle_seconds = (now - cluster.last_activity_time).total_seconds()
            idle_hours = idle_seconds / 3600

        idle_data.append({
            "Cluster ID": cluster.cluster_id[:16] + "...",
            "Cluster Name": cluster.cluster_name,
            "State": cluster.state,
            "Creator": cluster.creator,
            "Node Type": cluster.node_type,
            "Last Activity": cluster.last_activity_time.strftime("%Y-%m-%d %H:%M:%S") if cluster.last_activity_time else "Unknown",
            "Idle Hours": f"{idle_hours:.1f}" if idle_hours else "N/A"
        })

    df = pd.DataFrame(idle_data)
    display(df)

    # Show potential cost savings
    print("\n" + "=" * 60)
    print("IDLE CLUSTER ANALYSIS")
    print("=" * 60)
    print(f"Total idle clusters: {len(idle_clusters)}")
    print(f"\nPotential cost optimization:")
    print(f"  Terminating these clusters could reduce costs")
    print(f"  Consider setting auto-termination policies")
    print("=" * 60)
else:
    print("✓ No idle clusters found - excellent utilization!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cluster Duration Analysis
# MAGIC
# MAGIC Analyze cluster runtime with different thresholds.

# COMMAND ----------

# Check different duration thresholds
duration_thresholds = [4, 8, 12, 24]

print("Long-running clusters by duration threshold:\n")
threshold_data = []

for threshold in duration_thresholds:
    clusters = clusters_admin.list_long_running_clusters(
        min_duration_hours=float(threshold),
        lookback_hours=48.0,
        limit=100
    )
    threshold_data.append({
        "Threshold": f"> {threshold}h",
        "Cluster Count": len(clusters)
    })
    print(f"  Clusters running > {threshold} hours: {len(clusters)}")

df_thresholds = pd.DataFrame(threshold_data)
display(df_thresholds)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cluster Utilization Report
# MAGIC
# MAGIC Generate a comprehensive view of cluster usage patterns.

# COMMAND ----------

print("=" * 70)
print("CLUSTER UTILIZATION REPORT")
print("=" * 70)
print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# Get all clusters for analysis
all_long_running = clusters_admin.list_long_running_clusters(
    min_duration_hours=4.0,
    lookback_hours=24.0,
    limit=100
)

all_idle = clusters_admin.list_idle_clusters(
    idle_hours=2.0,
    limit=100
)

print(f"\nCluster Summary (Last 24 Hours):")
print(f"  Long-running clusters (>4h): {len(all_long_running)}")
print(f"  Idle clusters (>2h): {len(all_idle)}")

# Analyze by creator
if all_long_running:
    creators = [c.creator for c in all_long_running if c.creator]
    from collections import Counter
    creator_counts = Counter(creators)

    print(f"\nTop 5 users with most long-running clusters:")
    for creator, count in creator_counts.most_common(5):
        print(f"  {creator}: {count} clusters")

# Analyze by node type
if all_long_running:
    node_types = [c.node_type for c in all_long_running if c.node_type]
    node_type_counts = Counter(node_types)

    print(f"\nClusters by node type:")
    for node_type, count in node_type_counts.most_common():
        print(f"  {node_type}: {count}")

print("\n" + "=" * 70)

# Optimization recommendations
total_issues = len(all_long_running) + len(all_idle)
if total_issues == 0:
    print("Cluster Health: EXCELLENT ✓✓✓")
    print("No optimization recommendations at this time")
elif total_issues < 5:
    print("Cluster Health: GOOD ✓✓")
    print("Minor optimizations possible")
elif total_issues < 15:
    print("Cluster Health: FAIR ✓")
    print("Several optimization opportunities identified")
else:
    print("Cluster Health: NEEDS ATTENTION ⚠")
    print("Significant optimization opportunities available")

print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cost Optimization Opportunities
# MAGIC
# MAGIC Identify specific opportunities to reduce cluster costs.

# COMMAND ----------

print("=" * 70)
print("COST OPTIMIZATION OPPORTUNITIES")
print("=" * 70)

recommendations = []

# Check for idle clusters
if idle_clusters:
    recommendations.append({
        "Issue": f"{len(idle_clusters)} idle clusters",
        "Impact": "High",
        "Action": "Terminate idle clusters or enable auto-termination",
        "Potential Savings": "Immediate cost reduction"
    })

# Check for very long-running clusters
very_long = clusters_admin.list_long_running_clusters(
    min_duration_hours=24.0,
    lookback_hours=48.0,
    limit=100
)

if very_long:
    recommendations.append({
        "Issue": f"{len(very_long)} clusters running >24h",
        "Impact": "Medium",
        "Action": "Review if these clusters need to run continuously",
        "Potential Savings": "Consider scheduled clusters or jobs"
    })

# Check for clusters without auto-termination
clusters_no_policy = [c for c in all_long_running if not c.cluster_policy_id]
if clusters_no_policy:
    recommendations.append({
        "Issue": f"{len(clusters_no_policy)} clusters without policies",
        "Impact": "Medium",
        "Action": "Apply cluster policies with auto-termination",
        "Potential Savings": "Prevent accidental long-running clusters"
    })

if recommendations:
    df_recommendations = pd.DataFrame(recommendations)
    display(df_recommendations)
    print(f"\n✓ Found {len(recommendations)} optimization opportunities")
else:
    print("✓ No major optimization opportunities found")
    print("Cluster management is excellent!")

print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Example Natural Language Queries
# MAGIC
# MAGIC These are the types of questions you can answer with ClustersAdmin:

# COMMAND ----------

print("=" * 70)
print("EXAMPLE QUESTIONS YOU CAN ANSWER WITH CLUSTERSADMIN")
print("=" * 70)

examples = [
    {
        "Question": "Which clusters have been running longer than 8 hours?",
        "Method": "list_long_running_clusters(min_duration_hours=8.0)",
        "Use Case": "Resource management"
    },
    {
        "Question": "Show me clusters idle for more than 2 hours",
        "Method": "list_idle_clusters(idle_hours=2.0)",
        "Use Case": "Cost optimization"
    },
    {
        "Question": "Which user has the most long-running clusters?",
        "Method": "list_long_running_clusters() + group by creator",
        "Use Case": "User behavior analysis"
    },
    {
        "Question": "Are there any clusters running for more than 24 hours?",
        "Method": "list_long_running_clusters(min_duration_hours=24.0)",
        "Use Case": "Cost control"
    },
    {
        "Question": "What clusters can I terminate to save costs?",
        "Method": "list_idle_clusters() + analysis",
        "Use Case": "Budget management"
    }
]

df_examples = pd.DataFrame(examples)
display(df_examples)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Cluster Policy Compliance
# MAGIC
# MAGIC Check which clusters are following recommended policies.

# COMMAND ----------

# Get all clusters
all_clusters = clusters_admin.list_long_running_clusters(
    min_duration_hours=0.0,  # Get all running clusters
    lookback_hours=24.0,
    limit=200
)

if all_clusters:
    print("=" * 70)
    print("CLUSTER POLICY COMPLIANCE")
    print("=" * 70)

    total = len(all_clusters)
    with_policy = len([c for c in all_clusters if c.cluster_policy_id])
    without_policy = total - with_policy

    compliance_rate = (with_policy / total * 100) if total > 0 else 0

    print(f"\nTotal clusters: {total}")
    print(f"Clusters with policies: {with_policy} ({compliance_rate:.1f}%)")
    print(f"Clusters without policies: {without_policy}")

    if compliance_rate >= 90:
        status = "EXCELLENT ✓✓✓"
    elif compliance_rate >= 70:
        status = "GOOD ✓✓"
    elif compliance_rate >= 50:
        status = "FAIR ✓"
    else:
        status = "NEEDS IMPROVEMENT ⚠"

    print(f"\nPolicy Compliance Status: {status}")

    if without_policy > 0:
        print(f"\nRecommendation:")
        print(f"  Apply cluster policies to {without_policy} unmanaged clusters")
        print(f"  This will enforce auto-termination and resource limits")

    print("=" * 70)
else:
    print("No running clusters found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC The ClustersAdmin class provides powerful cluster monitoring and cost optimization capabilities:
# MAGIC
# MAGIC **Key Features:**
# MAGIC - ✓ Identify long-running clusters for cost control
# MAGIC - ✓ Find idle clusters to terminate
# MAGIC - ✓ Analyze utilization patterns by user and node type
# MAGIC - ✓ Track cluster policy compliance
# MAGIC - ✓ Generate cost optimization recommendations
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - Explore security and permissions in `04_security_admin_demo.py`
# MAGIC - Review cost and budget tracking in `05_usage_cost_budget_demo.py`
# MAGIC - Deploy the full agent in `07_agent_deployment.py`

# COMMAND ----------
