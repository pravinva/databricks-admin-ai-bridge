# Databricks notebook source
# MAGIC %md
# MAGIC # DBSQL Admin Demo
# MAGIC
# MAGIC This notebook demonstrates the **DBSQLAdmin** class for query performance monitoring and analysis.
# MAGIC
# MAGIC **Target Workspace:** `https://e2-demo-field-eng.cloud.databricks.com`
# MAGIC
# MAGIC **What You'll Learn:**
# MAGIC - Find the slowest queries
# MAGIC - Analyze query performance by user
# MAGIC - Identify optimization opportunities
# MAGIC - Visualize query patterns
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - "What are the top 10 slowest queries in the last 24 hours?"
# MAGIC - "Show me query performance for user john.doe@company.com"
# MAGIC - "Which queries are consuming the most resources?"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from admin_ai_bridge import AdminBridgeConfig, DBSQLAdmin
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

# Initialize configuration
cfg = AdminBridgeConfig()

# Databricks notebook widget for warehouse_id
dbutils.widgets.text("warehouse_id", "4b9b953939869799", "SQL Warehouse ID")
warehouse_id = dbutils.widgets.get("warehouse_id")

# Initialize with warehouse_id for fast system table queries
dbsql_admin = DBSQLAdmin(cfg, warehouse_id=warehouse_id)

print(f"✓ DBSQLAdmin initialized successfully (system tables enabled with warehouse: {warehouse_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Top Slowest Queries
# MAGIC
# MAGIC Find queries with the longest execution times.

# COMMAND ----------

# Get top 20 slowest queries from last 24 hours
slowest_queries = dbsql_admin.top_slowest_queries(
    lookback_hours=24.0,
    limit=20
)

print(f"Found {len(slowest_queries)} slow queries\n")

if slowest_queries:
    # Convert to DataFrame
    queries_data = []
    for query in slowest_queries:
        queries_data.append({
            "Query ID": query.query_id[:16] + "...",  # Truncate for display
            "Warehouse ID": query.warehouse_id[:16] + "..." if query.warehouse_id else None,
            "User": query.user_name,
            "Status": query.status,
            "Duration (seconds)": round(query.duration_seconds, 2) if query.duration_seconds else None,
            "Start Time": query.start_time.strftime("%Y-%m-%d %H:%M:%S") if query.start_time else None,
            "SQL Preview": query.sql_text[:50] + "..." if query.sql_text else None
        })

    df = pd.DataFrame(queries_data)
    display(df)

    # Show summary statistics
    durations = [q.duration_seconds for q in slowest_queries if q.duration_seconds]
    if durations:
        print("\n" + "=" * 60)
        print("SLOW QUERY STATISTICS")
        print("=" * 60)
        print(f"Average duration: {sum(durations) / len(durations):.2f} seconds")
        print(f"Maximum duration: {max(durations):.2f} seconds")
        print(f"Minimum duration: {min(durations):.2f} seconds")
        print("=" * 60)
else:
    print("No slow queries found in the specified time window")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. User Query Summary
# MAGIC
# MAGIC Analyze query patterns for a specific user.

# COMMAND ----------

# Get current user for analysis
from databricks.sdk import WorkspaceClient
ws = WorkspaceClient()
current_user = ws.current_user.me()
user_name = current_user.user_name

print(f"Analyzing queries for user: {user_name}\n")

# Get user query summary
user_summary = dbsql_admin.user_query_summary(
    user_name=user_name,
    lookback_hours=24.0
)

print("=" * 60)
print(f"QUERY SUMMARY FOR {user_name}")
print("=" * 60)
for key, value in user_summary.items():
    if isinstance(value, float):
        print(f"{key:.<40} {value:.2f}")
    else:
        print(f"{key:.<40} {value}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Query Performance by Time Window
# MAGIC
# MAGIC Compare query performance across different time windows.

# COMMAND ----------

time_windows = [
    (1, "1 hour"),
    (6, "6 hours"),
    (24, "24 hours"),
    (72, "3 days")
]

print("Slow query counts by time window (> 10 seconds):\n")
window_data = []

for hours, label in time_windows:
    queries = dbsql_admin.top_slowest_queries(lookback_hours=float(hours), limit=100)
    # Filter for queries > 10 seconds
    slow = [q for q in queries if q.duration_seconds and q.duration_seconds > 10]
    window_data.append({
        "Time Window": label,
        "Total Queries": len(queries),
        "Slow Queries (>10s)": len(slow)
    })
    print(f"  {label:.<20} {len(slow)} slow queries")

df_windows = pd.DataFrame(window_data)
display(df_windows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Query Duration Analysis
# MAGIC
# MAGIC Visualize the distribution of query durations.

# COMMAND ----------

# Get more queries for analysis
all_queries = dbsql_admin.top_slowest_queries(lookback_hours=24.0, limit=100)

if all_queries:
    # Extract durations
    durations = [q.duration_seconds for q in all_queries if q.duration_seconds]

    if durations:
        # Create visualization
        plt.figure(figsize=(12, 6))

        # Histogram
        plt.subplot(1, 2, 1)
        plt.hist(durations, bins=20, color='skyblue', edgecolor='black')
        plt.xlabel('Duration (seconds)')
        plt.ylabel('Number of Queries')
        plt.title('Query Duration Distribution')
        plt.grid(True, alpha=0.3)

        # Box plot
        plt.subplot(1, 2, 2)
        plt.boxplot(durations, vert=True)
        plt.ylabel('Duration (seconds)')
        plt.title('Query Duration Box Plot')
        plt.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.show()

        # Show percentiles
        import numpy as np
        print("\nQuery Duration Percentiles:")
        percentiles = [50, 75, 90, 95, 99]
        for p in percentiles:
            value = np.percentile(durations, p)
            print(f"  P{p}: {value:.2f} seconds")
else:
    print("Not enough data for visualization")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Query Status Analysis
# MAGIC
# MAGIC Analyze query outcomes (success, failure, etc.).

# COMMAND ----------

all_queries = dbsql_admin.top_slowest_queries(lookback_hours=24.0, limit=200)

if all_queries:
    # Count by status
    from collections import Counter
    statuses = [q.status for q in all_queries if q.status]
    status_counts = Counter(statuses)

    print("Query Status Distribution:\n")
    status_data = []
    for status, count in status_counts.most_common():
        percentage = (count / len(all_queries)) * 100
        status_data.append({
            "Status": status,
            "Count": count,
            "Percentage": f"{percentage:.1f}%"
        })
        print(f"  {status:.<30} {count} ({percentage:.1f}%)")

    df_status = pd.DataFrame(status_data)
    display(df_status)
else:
    print("No queries found for status analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Example Natural Language Queries
# MAGIC
# MAGIC These are the types of questions you can answer with DBSQLAdmin:

# COMMAND ----------

print("=" * 70)
print("EXAMPLE QUESTIONS YOU CAN ANSWER WITH DBSQLADMIN")
print("=" * 70)

examples = [
    {
        "Question": "What are the top 10 slowest queries in the last 24 hours?",
        "Method": "top_slowest_queries(lookback_hours=24.0, limit=10)",
        "Use Case": "Performance optimization"
    },
    {
        "Question": "Show me query performance for user john.doe@company.com",
        "Method": "user_query_summary(user_name='john.doe@company.com')",
        "Use Case": "User activity monitoring"
    },
    {
        "Question": "Which queries took longer than 60 seconds today?",
        "Method": "top_slowest_queries() + filter by duration",
        "Use Case": "SLA compliance"
    },
    {
        "Question": "How many queries failed in the last hour?",
        "Method": "top_slowest_queries(lookback_hours=1.0) + count failures",
        "Use Case": "Error monitoring"
    },
    {
        "Question": "What's the average query duration per user?",
        "Method": "user_query_summary() for each user",
        "Use Case": "Resource allocation"
    }
]

df_examples = pd.DataFrame(examples)
display(df_examples)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Query Performance Report
# MAGIC
# MAGIC Generate a comprehensive report on query health.

# COMMAND ----------

print("=" * 70)
print("DBSQL QUERY PERFORMANCE REPORT")
print("=" * 70)
print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# Get queries for last 24 hours
queries_24h = dbsql_admin.top_slowest_queries(lookback_hours=24.0, limit=500)

if queries_24h:
    # Calculate statistics
    total_queries = len(queries_24h)
    durations = [q.duration_seconds for q in queries_24h if q.duration_seconds]
    failed = [q for q in queries_24h if q.status and 'FAILED' in q.status.upper()]

    print(f"\nQuery Summary (Last 24 Hours):")
    print(f"  Total queries analyzed: {total_queries}")
    print(f"  Failed queries: {len(failed)}")

    if durations:
        avg_duration = sum(durations) / len(durations)
        max_duration = max(durations)
        min_duration = min(durations)

        print(f"\nQuery Performance:")
        print(f"  Average duration: {avg_duration:.2f} seconds")
        print(f"  Maximum duration: {max_duration:.2f} seconds")
        print(f"  Minimum duration: {min_duration:.2f} seconds")

        # Categorize queries by duration
        fast = len([d for d in durations if d < 1])
        medium = len([d for d in durations if 1 <= d < 10])
        slow = len([d for d in durations if 10 <= d < 60])
        very_slow = len([d for d in durations if d >= 60])

        print(f"\nQuery Categories:")
        print(f"  Fast (< 1s): {fast}")
        print(f"  Medium (1-10s): {medium}")
        print(f"  Slow (10-60s): {slow}")
        print(f"  Very Slow (>= 60s): {very_slow}")

    # User analysis
    users = [q.user_name for q in queries_24h if q.user_name]
    unique_users = len(set(users))
    print(f"\nUser Activity:")
    print(f"  Unique users: {unique_users}")
    print(f"  Average queries per user: {len(users) / unique_users:.1f}")

    print("\n" + "=" * 70)

    # Health score
    failure_rate = (len(failed) / total_queries) * 100 if total_queries > 0 else 0
    slow_rate = (very_slow / len(durations)) * 100 if durations else 0

    if failure_rate < 1 and slow_rate < 5:
        health_status = "EXCELLENT"
    elif failure_rate < 3 and slow_rate < 10:
        health_status = "GOOD"
    elif failure_rate < 5 and slow_rate < 20:
        health_status = "FAIR"
    else:
        health_status = "NEEDS ATTENTION"

    print(f"Overall Query Health: {health_status}")
    print(f"  Failure Rate: {failure_rate:.2f}%")
    print(f"  Very Slow Query Rate: {slow_rate:.2f}%")
    print("=" * 70)
else:
    print("\nNo queries found in the last 24 hours")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC The DBSQLAdmin class provides comprehensive query monitoring capabilities:
# MAGIC
# MAGIC **Key Features:**
# MAGIC - ✓ Find slowest queries for performance optimization
# MAGIC - ✓ Analyze query patterns by user
# MAGIC - ✓ Track query success/failure rates
# MAGIC - ✓ Visualize performance distributions
# MAGIC - ✓ Generate detailed performance reports
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - Explore cluster monitoring in `03_clusters_admin_demo.py`
# MAGIC - Set up alerts for slow queries
# MAGIC - Deploy the full agent in `07_agent_deployment.py`

# COMMAND ----------
