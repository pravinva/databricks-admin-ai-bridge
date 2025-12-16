# Databricks notebook source
# MAGIC %md
# MAGIC # Jobs Admin Demo
# MAGIC
# MAGIC This notebook demonstrates the **JobsAdmin** class from the Admin AI Bridge library.
# MAGIC
# MAGIC **Target Workspace:** `https://e2-demo-field-eng.cloud.databricks.com`
# MAGIC
# MAGIC **What You'll Learn:**
# MAGIC - List long-running jobs
# MAGIC - Find failed jobs
# MAGIC - Analyze job performance patterns
# MAGIC - Answer natural language questions about jobs
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - "Which jobs have been running longer than 4 hours today?"
# MAGIC - "Show me all failed jobs in the last 24 hours"
# MAGIC - "What jobs are consuming the most compute time?"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from admin_ai_bridge import AdminBridgeConfig, JobsAdmin
import pandas as pd
from datetime import datetime

# Initialize configuration
cfg = AdminBridgeConfig()

# Databricks notebook widget for warehouse_id
dbutils.widgets.text("warehouse_id", "4b9b953939869799", "SQL Warehouse ID")
warehouse_id = dbutils.widgets.get("warehouse_id")

# Initialize with warehouse_id for fast system table queries
jobs_admin = JobsAdmin(cfg, warehouse_id=warehouse_id)

print(f"✓ JobsAdmin initialized successfully (system tables enabled with warehouse: {warehouse_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. List Long-Running Jobs
# MAGIC
# MAGIC Find jobs that have been running longer than a specified duration.
# MAGIC This is useful for identifying stuck jobs or inefficient workflows.

# COMMAND ----------

# Find jobs running longer than 4 hours in the last 24 hours
long_running = jobs_admin.list_long_running_jobs(
    min_duration_hours=4.0,
    lookback_hours=24.0,
    limit=20
)

print(f"Found {len(long_running)} long-running jobs\n")

if long_running:
    # Convert to DataFrame for better display
    jobs_data = []
    for job in long_running:
        jobs_data.append({
            "Job ID": job.job_id,
            "Job Name": job.job_name,
            "Run ID": job.run_id,
            "State": job.state,
            "Duration (hours)": round(job.duration_seconds / 3600, 2) if job.duration_seconds else None,
            "Start Time": job.start_time.strftime("%Y-%m-%d %H:%M:%S") if job.start_time else None
        })

    df = pd.DataFrame(jobs_data)
    display(df)
else:
    print("No long-running jobs found in the specified time window")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. List Failed Jobs
# MAGIC
# MAGIC Find all jobs that failed in a recent time window.
# MAGIC This helps with troubleshooting and identifying systemic issues.

# COMMAND ----------

# Find failed jobs in the last 24 hours
failed_jobs = jobs_admin.list_failed_jobs(
    lookback_hours=24.0,
    limit=50
)

print(f"Found {len(failed_jobs)} failed jobs in the last 24 hours\n")

if failed_jobs:
    # Convert to DataFrame
    failed_data = []
    for job in failed_jobs:
        failed_data.append({
            "Job ID": job.job_id,
            "Job Name": job.job_name,
            "Run ID": job.run_id,
            "State": job.state,
            "Lifecycle State": job.life_cycle_state,
            "Duration (minutes)": round(job.duration_seconds / 60, 2) if job.duration_seconds else None,
            "Start Time": job.start_time.strftime("%Y-%m-%d %H:%M:%S") if job.start_time else None,
            "End Time": job.end_time.strftime("%Y-%m-%d %H:%M:%S") if job.end_time else None
        })

    df = pd.DataFrame(failed_data)
    display(df)

    # Show summary statistics
    print("\n" + "=" * 60)
    print("FAILED JOBS SUMMARY")
    print("=" * 60)
    print(f"Total failed jobs: {len(failed_jobs)}")

    # Count by job name
    job_names = [j.job_name for j in failed_jobs]
    from collections import Counter
    job_counts = Counter(job_names)

    print(f"\nTop 5 jobs with most failures:")
    for job_name, count in job_counts.most_common(5):
        print(f"  {job_name}: {count} failures")

else:
    print("✓ No failed jobs found in the last 24 hours - all systems operational!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Different Time Windows
# MAGIC
# MAGIC Demonstrate using different time windows for analysis.

# COMMAND ----------

# Compare different time windows
time_windows = [
    (6, "6 hours"),
    (24, "24 hours"),
    (72, "3 days"),
    (168, "1 week")
]

print("Failed job counts by time window:\n")
for hours, label in time_windows:
    jobs = jobs_admin.list_failed_jobs(lookback_hours=float(hours), limit=1000)
    print(f"  {label:.<20} {len(jobs)} failed jobs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Long-Running Jobs Analysis
# MAGIC
# MAGIC Find jobs with different duration thresholds.

# COMMAND ----------

# Find jobs with different duration thresholds
duration_thresholds = [2, 4, 8, 12]

print("Long-running jobs by duration threshold:\n")
results = []
for threshold in duration_thresholds:
    jobs = jobs_admin.list_long_running_jobs(
        min_duration_hours=float(threshold),
        lookback_hours=24.0,
        limit=100
    )
    results.append({
        "Threshold (hours)": f"> {threshold}h",
        "Job Count": len(jobs)
    })
    print(f"  Jobs running > {threshold} hours: {len(jobs)}")

# Visualize as a table
df_thresholds = pd.DataFrame(results)
display(df_thresholds)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Example Natural Language Queries
# MAGIC
# MAGIC These are the types of questions you can answer with JobsAdmin:

# COMMAND ----------

print("=" * 70)
print("EXAMPLE QUESTIONS YOU CAN ANSWER WITH JOBSADMIN")
print("=" * 70)

examples = [
    {
        "Question": "Which jobs have been running longer than 4 hours today?",
        "Method": "list_long_running_jobs(min_duration_hours=4.0, lookback_hours=24.0)",
        "Answer": f"{len(long_running)} jobs found"
    },
    {
        "Question": "Show me all failed jobs in the last 24 hours",
        "Method": "list_failed_jobs(lookback_hours=24.0)",
        "Answer": f"{len(failed_jobs)} failed jobs"
    },
    {
        "Question": "Are there any jobs stuck for more than 12 hours?",
        "Method": "list_long_running_jobs(min_duration_hours=12.0, lookback_hours=24.0)",
        "Answer": "Check for long-running jobs with high threshold"
    },
    {
        "Question": "What jobs failed in the last 6 hours?",
        "Method": "list_failed_jobs(lookback_hours=6.0)",
        "Answer": "Recent failure analysis"
    },
    {
        "Question": "Which workflows are taking the longest time?",
        "Method": "list_long_running_jobs() + sort by duration",
        "Answer": "Performance optimization candidates"
    }
]

df_examples = pd.DataFrame(examples)
display(df_examples)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Job Performance Report
# MAGIC
# MAGIC Generate a comprehensive report of job health.

# COMMAND ----------

print("=" * 70)
print("JOB PERFORMANCE REPORT")
print("=" * 70)
print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# Get data for last 24 hours
long_running_24h = jobs_admin.list_long_running_jobs(
    min_duration_hours=4.0,
    lookback_hours=24.0,
    limit=100
)

failed_24h = jobs_admin.list_failed_jobs(
    lookback_hours=24.0,
    limit=100
)

# Calculate statistics
print(f"\nJobs Summary (Last 24 Hours):")
print(f"  Long-running jobs (>4h): {len(long_running_24h)}")
print(f"  Failed jobs: {len(failed_24h)}")

if long_running_24h:
    avg_duration = sum(j.duration_seconds or 0 for j in long_running_24h) / len(long_running_24h)
    max_duration = max(j.duration_seconds or 0 for j in long_running_24h)
    print(f"\nLong-Running Job Statistics:")
    print(f"  Average duration: {avg_duration / 3600:.2f} hours")
    print(f"  Maximum duration: {max_duration / 3600:.2f} hours")

if failed_24h:
    # Count unique jobs that failed
    unique_failed_jobs = len(set(j.job_id for j in failed_24h))
    print(f"\nFailed Job Statistics:")
    print(f"  Total failures: {len(failed_24h)}")
    print(f"  Unique jobs with failures: {unique_failed_jobs}")
    print(f"  Average retries per job: {len(failed_24h) / unique_failed_jobs:.2f}")

print("\n" + "=" * 70)

# Health score calculation
total_issues = len(long_running_24h) + len(failed_24h)
if total_issues == 0:
    health_status = "EXCELLENT"
    health_emoji = "✓✓✓"
elif total_issues < 5:
    health_status = "GOOD"
    health_emoji = "✓✓"
elif total_issues < 15:
    health_status = "FAIR"
    health_emoji = "✓"
else:
    health_status = "NEEDS ATTENTION"
    health_emoji = "⚠"

print(f"Overall Job Health: {health_status} {health_emoji}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC The JobsAdmin class provides powerful capabilities for monitoring and analyzing Databricks jobs:
# MAGIC
# MAGIC **Key Features:**
# MAGIC - ✓ Find long-running jobs with customizable duration thresholds
# MAGIC - ✓ Identify failed jobs for troubleshooting
# MAGIC - ✓ Flexible time windows (hours to weeks)
# MAGIC - ✓ Performance analysis and reporting
# MAGIC - ✓ Integration with Databricks Agent Framework
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - Explore DBSQL query monitoring in `02_dbsql_admin_demo.py`
# MAGIC - Set up automated alerts for job failures
# MAGIC - Deploy the full agent in `07_agent_deployment.py`

# COMMAND ----------
