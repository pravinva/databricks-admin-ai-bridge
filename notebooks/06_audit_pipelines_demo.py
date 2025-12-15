# Databricks notebook source
# MAGIC %md
# MAGIC # Audit and Pipelines Admin Demo
# MAGIC
# MAGIC This notebook demonstrates the **AuditAdmin** and **PipelinesAdmin** classes for security monitoring and pipeline observability.
# MAGIC
# MAGIC **Target Workspace:** `https://e2-demo-field-eng.cloud.databricks.com`
# MAGIC
# MAGIC **What You'll Learn:**
# MAGIC - Query audit logs for security events
# MAGIC - Track failed login attempts
# MAGIC - Monitor admin permission changes
# MAGIC - Track pipeline health and performance
# MAGIC - Identify lagging or failed pipelines
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - "Show me failed login attempts in the last 24 hours"
# MAGIC - "What admin changes were made recently?"
# MAGIC - "Which pipelines are behind by more than 10 minutes?"
# MAGIC - "List all failed pipelines today"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from admin_ai_bridge import AdminBridgeConfig, AuditAdmin, PipelinesAdmin
import pandas as pd
from datetime import datetime

# Initialize
cfg = AdminBridgeConfig()
audit_admin = AuditAdmin(cfg)
pipelines_admin = PipelinesAdmin(cfg)

print("✓ AuditAdmin initialized successfully")
print("✓ PipelinesAdmin initialized successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Audit Admin
# MAGIC
# MAGIC ### 1.1 Failed Login Attempts
# MAGIC
# MAGIC Track failed authentication attempts for security monitoring.

# COMMAND ----------

# Get failed logins in the last 24 hours
failed_logins = audit_admin.failed_logins(
    lookback_hours=24.0,
    limit=50
)

print(f"Found {len(failed_logins)} failed login attempts in the last 24 hours\n")

if failed_logins:
    # Convert to DataFrame
    login_data = []
    for event in failed_logins:
        login_data.append({
            "Event Time": event.event_time.strftime("%Y-%m-%d %H:%M:%S") if event.event_time else None,
            "User": event.user_name,
            "Source IP": event.source_ip,
            "Service": event.service_name,
            "Event Type": event.event_type
        })

    df = pd.DataFrame(login_data)
    display(df)

    # Analyze patterns
    from collections import Counter
    users = [e.user_name for e in failed_logins if e.user_name]
    ips = [e.source_ip for e in failed_logins if e.source_ip]

    print("\n" + "=" * 60)
    print("FAILED LOGIN ANALYSIS")
    print("=" * 60)

    if users:
        user_counts = Counter(users)
        print(f"\nTop 5 users with failed logins:")
        for user, count in user_counts.most_common(5):
            print(f"  {user}: {count} attempts")

    if ips:
        ip_counts = Counter(ips)
        print(f"\nTop 5 source IPs with failed logins:")
        for ip, count in ip_counts.most_common(5):
            print(f"  {ip}: {count} attempts")

    print("=" * 60)

    # Security alerts
    if len(failed_logins) > 10:
        print("\n⚠ HIGH ALERT: More than 10 failed login attempts detected!")
    elif len(failed_logins) > 5:
        print("\n⚠ MODERATE ALERT: Multiple failed login attempts detected")
else:
    print("✓ No failed login attempts found - good security posture!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Recent Admin Changes
# MAGIC
# MAGIC Monitor administrative changes for compliance and security.

# COMMAND ----------

# Get recent admin changes
admin_changes = audit_admin.recent_admin_changes(
    lookback_hours=24.0,
    limit=50
)

print(f"Found {len(admin_changes)} admin changes in the last 24 hours\n")

if admin_changes:
    # Convert to DataFrame
    changes_data = []
    for event in admin_changes:
        changes_data.append({
            "Event Time": event.event_time.strftime("%Y-%m-%d %H:%M:%S") if event.event_time else None,
            "User": event.user_name,
            "Service": event.service_name,
            "Event Type": event.event_type,
            "Source IP": event.source_ip,
            "Details": str(event.details)[:50] + "..." if event.details else None
        })

    df = pd.DataFrame(changes_data)
    display(df)

    # Analyze change patterns
    print("\n" + "=" * 60)
    print("ADMIN CHANGE ANALYSIS")
    print("=" * 60)

    # Count by event type
    from collections import Counter
    event_types = [e.event_type for e in admin_changes if e.event_type]
    type_counts = Counter(event_types)

    print(f"\nChanges by event type:")
    for event_type, count in type_counts.most_common():
        print(f"  {event_type}: {count}")

    # Count by user
    users = [e.user_name for e in admin_changes if e.user_name]
    user_counts = Counter(users)

    print(f"\nChanges by user (top 5):")
    for user, count in user_counts.most_common(5):
        print(f"  {user}: {count} changes")

    print("=" * 60)
else:
    print("No admin changes found in the specified time window")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Audit Timeline Analysis
# MAGIC
# MAGIC Analyze audit events across different time periods.

# COMMAND ----------

# Compare audit activity across time windows
time_windows = [
    (1, "1 hour"),
    (6, "6 hours"),
    (24, "24 hours"),
    (72, "3 days")
]

print("Audit activity by time window:\n")
audit_timeline = []

for hours, label in time_windows:
    logins = audit_admin.failed_logins(lookback_hours=float(hours), limit=500)
    changes = audit_admin.recent_admin_changes(lookback_hours=float(hours), limit=500)

    audit_timeline.append({
        "Time Window": label,
        "Failed Logins": len(logins),
        "Admin Changes": len(changes)
    })
    print(f"  {label:.<20} {len(logins)} failed logins, {len(changes)} admin changes")

df_timeline = pd.DataFrame(audit_timeline)
display(df_timeline)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Security Report
# MAGIC
# MAGIC Generate a comprehensive security report.

# COMMAND ----------

print("=" * 70)
print("SECURITY AUDIT REPORT")
print("=" * 70)
print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# Collect 24-hour data
logins_24h = audit_admin.failed_logins(lookback_hours=24.0, limit=500)
changes_24h = audit_admin.recent_admin_changes(lookback_hours=24.0, limit=500)

print(f"\nSecurity Events (Last 24 Hours):")
print(f"  Failed login attempts: {len(logins_24h)}")
print(f"  Admin changes: {len(changes_24h)}")

if logins_24h:
    unique_users = len(set(e.user_name for e in logins_24h if e.user_name))
    unique_ips = len(set(e.source_ip for e in logins_24h if e.source_ip))
    print(f"\nFailed Login Details:")
    print(f"  Unique users: {unique_users}")
    print(f"  Unique source IPs: {unique_ips}")

if changes_24h:
    change_users = len(set(e.user_name for e in changes_24h if e.user_name))
    print(f"\nAdmin Change Details:")
    print(f"  Users making changes: {change_users}")

print("\n" + "=" * 70)

# Security posture assessment
total_events = len(logins_24h) + len(changes_24h)
if len(logins_24h) == 0:
    security_status = "EXCELLENT - No failed logins"
elif len(logins_24h) < 5:
    security_status = "GOOD - Few failed login attempts"
elif len(logins_24h) < 20:
    security_status = "FAIR - Some failed login activity"
else:
    security_status = "ALERT - High failed login activity"

print(f"Security Posture: {security_status}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Pipelines Admin
# MAGIC
# MAGIC ### 2.1 Lagging Pipelines
# MAGIC
# MAGIC Identify pipelines that are behind schedule.

# COMMAND ----------

# Find pipelines lagging more than 10 minutes
lagging_pipelines = pipelines_admin.list_lagging_pipelines(
    max_lag_seconds=600.0,  # 10 minutes
    limit=50
)

print(f"Found {len(lagging_pipelines)} lagging pipelines (lag > 10 minutes)\n")

if lagging_pipelines:
    # Convert to DataFrame
    lag_data = []
    for pipeline in lagging_pipelines:
        lag_data.append({
            "Pipeline ID": pipeline.pipeline_id[:16] + "...",
            "Name": pipeline.name,
            "State": pipeline.state,
            "Lag (minutes)": round(pipeline.lag_seconds / 60, 2) if pipeline.lag_seconds else None,
            "Last Update": pipeline.last_update_time.strftime("%Y-%m-%d %H:%M:%S") if pipeline.last_update_time else None
        })

    df = pd.DataFrame(lag_data)
    display(df)

    # Show summary
    print("\n" + "=" * 60)
    print("LAGGING PIPELINE SUMMARY")
    print("=" * 60)

    if any(p.lag_seconds for p in lagging_pipelines):
        avg_lag = sum(p.lag_seconds for p in lagging_pipelines if p.lag_seconds) / len(lagging_pipelines)
        max_lag = max(p.lag_seconds for p in lagging_pipelines if p.lag_seconds)

        print(f"Average lag: {avg_lag / 60:.1f} minutes")
        print(f"Maximum lag: {max_lag / 60:.1f} minutes")

    # Count by state
    from collections import Counter
    states = [p.state for p in lagging_pipelines]
    state_counts = Counter(states)

    print(f"\nPipelines by state:")
    for state, count in state_counts.items():
        print(f"  {state}: {count}")

    print("=" * 60)

    if max_lag > 3600:  # More than 1 hour
        print("\n⚠ CRITICAL: Some pipelines are lagging by more than 1 hour!")
else:
    print("✓ No lagging pipelines found - all pipelines are on schedule!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Failed Pipelines
# MAGIC
# MAGIC Track pipeline failures for troubleshooting.

# COMMAND ----------

# Get failed pipelines from last 24 hours
failed_pipelines = pipelines_admin.list_failed_pipelines(
    lookback_hours=24.0,
    limit=50
)

print(f"Found {len(failed_pipelines)} failed pipelines in the last 24 hours\n")

if failed_pipelines:
    # Convert to DataFrame
    failed_data = []
    for pipeline in failed_pipelines:
        failed_data.append({
            "Pipeline ID": pipeline.pipeline_id[:16] + "...",
            "Name": pipeline.name,
            "State": pipeline.state,
            "Last Update": pipeline.last_update_time.strftime("%Y-%m-%d %H:%M:%S") if pipeline.last_update_time else None,
            "Error": pipeline.last_error[:50] + "..." if pipeline.last_error else None
        })

    df = pd.DataFrame(failed_data)
    display(df)

    # Analyze failures
    print("\n" + "=" * 60)
    print("FAILED PIPELINE ANALYSIS")
    print("=" * 60)
    print(f"Total failures: {len(failed_pipelines)}")

    # Count unique pipelines
    unique_pipelines = len(set(p.pipeline_id for p in failed_pipelines))
    print(f"Unique pipelines with failures: {unique_pipelines}")

    if failed_pipelines and failed_pipelines[0].last_error:
        print(f"\nMost recent error:")
        print(f"  {failed_pipelines[0].last_error[:200]}")

    print("=" * 60)
else:
    print("✓ No failed pipelines found - all systems operational!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Pipeline Health Report
# MAGIC
# MAGIC Generate a comprehensive pipeline health report.

# COMMAND ----------

print("=" * 70)
print("PIPELINE HEALTH REPORT")
print("=" * 70)
print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# Check different lag thresholds
lag_thresholds = [
    (300, "5 minutes"),
    (600, "10 minutes"),
    (1800, "30 minutes"),
    (3600, "1 hour")
]

print(f"\nLagging Pipelines by Threshold:")
for seconds, label in lag_thresholds:
    lagging = pipelines_admin.list_lagging_pipelines(
        max_lag_seconds=float(seconds),
        limit=100
    )
    print(f"  Lag > {label:.<15} {len(lagging)} pipelines")

# Check failures over different time windows
print(f"\nFailed Pipelines by Time Window:")
for hours, label in [(6, "6 hours"), (24, "24 hours"), (72, "3 days")]:
    failed = pipelines_admin.list_failed_pipelines(
        lookback_hours=float(hours),
        limit=100
    )
    print(f"  Last {label:.<15} {len(failed)} failures")

print("\n" + "=" * 70)

# Health score
lagging_count = len(pipelines_admin.list_lagging_pipelines(max_lag_seconds=600.0, limit=100))
failed_count = len(pipelines_admin.list_failed_pipelines(lookback_hours=24.0, limit=100))

total_issues = lagging_count + failed_count

if total_issues == 0:
    health_status = "EXCELLENT ✓✓✓"
elif total_issues < 5:
    health_status = "GOOD ✓✓"
elif total_issues < 15:
    health_status = "FAIR ✓"
else:
    health_status = "NEEDS ATTENTION ⚠"

print(f"Pipeline Health Status: {health_status}")
print(f"  Lagging pipelines (>10 min): {lagging_count}")
print(f"  Failed pipelines (24h): {failed_count}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Example Natural Language Queries
# MAGIC
# MAGIC These are the types of questions you can answer with AuditAdmin and PipelinesAdmin:

# COMMAND ----------

print("=" * 70)
print("EXAMPLE QUESTIONS YOU CAN ANSWER")
print("=" * 70)

examples = [
    {
        "Domain": "Audit",
        "Question": "Show me failed login attempts in the last 24 hours",
        "Method": "failed_logins(lookback_hours=24.0)",
        "Use Case": "Security monitoring"
    },
    {
        "Domain": "Audit",
        "Question": "What admin changes were made recently?",
        "Method": "recent_admin_changes(lookback_hours=24.0)",
        "Use Case": "Compliance tracking"
    },
    {
        "Domain": "Audit",
        "Question": "Which user has the most failed login attempts?",
        "Method": "failed_logins() + group by user",
        "Use Case": "Account security"
    },
    {
        "Domain": "Pipelines",
        "Question": "Which pipelines are behind by more than 10 minutes?",
        "Method": "list_lagging_pipelines(max_lag_seconds=600)",
        "Use Case": "SLA monitoring"
    },
    {
        "Domain": "Pipelines",
        "Question": "List all failed pipelines today",
        "Method": "list_failed_pipelines(lookback_hours=24.0)",
        "Use Case": "Troubleshooting"
    },
    {
        "Domain": "Pipelines",
        "Question": "Are any pipelines lagging by more than an hour?",
        "Method": "list_lagging_pipelines(max_lag_seconds=3600)",
        "Use Case": "Critical alerts"
    }
]

df_examples = pd.DataFrame(examples)
display(df_examples)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Combined Security and Pipeline Dashboard
# MAGIC
# MAGIC A unified view of security and pipeline health.

# COMMAND ----------

print("=" * 70)
print("UNIFIED SECURITY & PIPELINE DASHBOARD")
print("=" * 70)
print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# Security metrics
failed_logins_24h = audit_admin.failed_logins(lookback_hours=24.0, limit=500)
admin_changes_24h = audit_admin.recent_admin_changes(lookback_hours=24.0, limit=500)

# Pipeline metrics
lagging_pipelines = pipelines_admin.list_lagging_pipelines(max_lag_seconds=600.0, limit=100)
failed_pipelines_24h = pipelines_admin.list_failed_pipelines(lookback_hours=24.0, limit=100)

print("\n📊 SECURITY METRICS (Last 24 Hours)")
print("-" * 70)
print(f"  Failed login attempts: {len(failed_logins_24h)}")
print(f"  Admin changes: {len(admin_changes_24h)}")

print("\n📊 PIPELINE METRICS")
print("-" * 70)
print(f"  Lagging pipelines (>10 min): {len(lagging_pipelines)}")
print(f"  Failed pipelines (24h): {len(failed_pipelines_24h)}")

print("\n🎯 OVERALL STATUS")
print("-" * 70)

# Calculate overall health
security_score = 100 - (len(failed_logins_24h) * 2)  # -2 points per failed login
pipeline_score = 100 - (len(lagging_pipelines) * 5) - (len(failed_pipelines_24h) * 10)

overall_score = (max(0, security_score) + max(0, pipeline_score)) / 2

if overall_score >= 90:
    status = "EXCELLENT ✓✓✓"
elif overall_score >= 70:
    status = "GOOD ✓✓"
elif overall_score >= 50:
    status = "FAIR ✓"
else:
    status = "NEEDS ATTENTION ⚠"

print(f"  Security Health: {max(0, security_score):.0f}/100")
print(f"  Pipeline Health: {max(0, pipeline_score):.0f}/100")
print(f"  Overall Status: {status}")

print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC The AuditAdmin and PipelinesAdmin classes provide comprehensive monitoring capabilities:
# MAGIC
# MAGIC **AuditAdmin Features:**
# MAGIC - ✓ Track failed login attempts for security monitoring
# MAGIC - ✓ Monitor admin permission changes for compliance
# MAGIC - ✓ Analyze audit patterns and trends
# MAGIC - ✓ Generate security reports
# MAGIC
# MAGIC **PipelinesAdmin Features:**
# MAGIC - ✓ Identify lagging pipelines with configurable thresholds
# MAGIC - ✓ Track pipeline failures for troubleshooting
# MAGIC - ✓ Monitor pipeline health and SLA compliance
# MAGIC - ✓ Generate pipeline observability reports
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - Deploy the full admin observability agent in `07_agent_deployment.py`
# MAGIC - Integrate with Slack/Teams for real-time alerts
# MAGIC - Set up automated monitoring dashboards

# COMMAND ----------
