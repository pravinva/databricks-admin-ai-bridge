# Databricks notebook source
# MAGIC %md
# MAGIC # Security Admin Demo
# MAGIC
# MAGIC This notebook demonstrates the **SecurityAdmin** class for permissions and access control auditing.
# MAGIC
# MAGIC **Target Workspace:** `https://e2-demo-field-eng.cloud.databricks.com`
# MAGIC
# MAGIC **What You'll Learn:**
# MAGIC - Query job permissions
# MAGIC - Check cluster access control
# MAGIC - Audit who can manage resources
# MAGIC - Analyze permission patterns
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - "Who can manage job 12345?"
# MAGIC - "Which users can attach to cluster abc123?"
# MAGIC - "Show me all permissions for a specific resource"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from admin_ai_bridge import AdminBridgeConfig, SecurityAdmin, JobsAdmin, ClustersAdmin
import pandas as pd
from datetime import datetime
from databricks.sdk import WorkspaceClient

# Initialize
cfg = AdminBridgeConfig()
security_admin = SecurityAdmin(cfg)
jobs_admin = JobsAdmin(cfg)
clusters_admin = ClustersAdmin(cfg)
ws = WorkspaceClient()

print("✓ SecurityAdmin initialized successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Job Permissions
# MAGIC
# MAGIC Query who can manage a specific job.

# COMMAND ----------

# Get a list of jobs to demonstrate with
from databricks.sdk.service.jobs import RunLifeCycleState

jobs_list = list(ws.jobs.list(limit=10))

if jobs_list:
    # Pick the first job for demonstration
    demo_job = jobs_list[0]
    job_id = demo_job.job_id

    print(f"Checking permissions for Job ID: {job_id}")
    print(f"Job Name: {demo_job.settings.name if demo_job.settings else 'Unknown'}\n")

    # Get who can manage this job
    try:
        managers = security_admin.who_can_manage_job(job_id)

        print(f"Found {len(managers)} principals with CAN_MANAGE permission:\n")

        if managers:
            permissions_data = []
            for perm in managers:
                permissions_data.append({
                    "Principal": perm.principal,
                    "Permission Level": perm.permission_level,
                    "Object Type": perm.object_type,
                    "Object ID": perm.object_id
                })

            df = pd.DataFrame(permissions_data)
            display(df)
        else:
            print("No explicit managers found (may inherit from workspace admins)")

    except Exception as e:
        print(f"Error querying permissions: {e}")
        print("Note: Some jobs may have restricted permission access")
else:
    print("No jobs found in workspace for demonstration")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cluster Permissions
# MAGIC
# MAGIC Query who can use a specific cluster.

# COMMAND ----------

# Get a list of clusters to demonstrate with
clusters = list(ws.clusters.list())

if clusters:
    # Pick the first cluster for demonstration
    demo_cluster = clusters[0]
    cluster_id = demo_cluster.cluster_id

    print(f"Checking permissions for Cluster ID: {cluster_id}")
    print(f"Cluster Name: {demo_cluster.cluster_name}\n")

    # Get who can use this cluster
    try:
        users = security_admin.who_can_use_cluster(cluster_id)

        print(f"Found {len(users)} principals with cluster usage permission:\n")

        if users:
            cluster_perms_data = []
            for perm in users:
                cluster_perms_data.append({
                    "Principal": perm.principal,
                    "Permission Level": perm.permission_level,
                    "Object Type": perm.object_type,
                    "Object ID": perm.object_id[:16] + "..."
                })

            df = pd.DataFrame(cluster_perms_data)
            display(df)
        else:
            print("No explicit users found (may be open access or workspace admins)")

    except Exception as e:
        print(f"Error querying permissions: {e}")
        print("Note: Some clusters may have restricted permission access")
else:
    print("No clusters found in workspace for demonstration")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Permission Analysis Across Multiple Jobs
# MAGIC
# MAGIC Analyze permission patterns across several jobs.

# COMMAND ----------

# Analyze permissions for multiple jobs
print("=" * 70)
print("PERMISSION ANALYSIS ACROSS JOBS")
print("=" * 70)

all_jobs = list(ws.jobs.list(limit=20))

if all_jobs:
    print(f"\nAnalyzing permissions for {len(all_jobs)} jobs...\n")

    all_principals = set()
    jobs_analyzed = 0
    jobs_with_perms = 0

    for job in all_jobs:
        try:
            managers = security_admin.who_can_manage_job(job.job_id)
            jobs_analyzed += 1

            if managers:
                jobs_with_perms += 1
                for perm in managers:
                    all_principals.add(perm.principal)

        except Exception as e:
            # Some jobs may not be accessible
            continue

    print(f"Jobs analyzed: {jobs_analyzed}")
    print(f"Jobs with explicit permissions: {jobs_with_perms}")
    print(f"Unique principals with job management rights: {len(all_principals)}")

    if all_principals:
        print(f"\nPrincipals with job management access:")
        for principal in sorted(all_principals)[:10]:  # Show first 10
            print(f"  - {principal}")
        if len(all_principals) > 10:
            print(f"  ... and {len(all_principals) - 10} more")

    print("=" * 70)
else:
    print("No jobs available for analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Permission Distribution Analysis
# MAGIC
# MAGIC Analyze how permissions are distributed across resources.

# COMMAND ----------

# Analyze permission levels
from collections import Counter

print("=" * 70)
print("PERMISSION LEVEL DISTRIBUTION")
print("=" * 70)

all_permission_levels = []

# Collect permissions from jobs
jobs_sample = list(ws.jobs.list(limit=30))
for job in jobs_sample:
    try:
        perms = security_admin.who_can_manage_job(job.job_id)
        all_permission_levels.extend([p.permission_level for p in perms])
    except:
        continue

if all_permission_levels:
    level_counts = Counter(all_permission_levels)

    print(f"\nPermission levels found:")
    perm_data = []
    for level, count in level_counts.most_common():
        perm_data.append({
            "Permission Level": level,
            "Count": count,
            "Percentage": f"{(count / len(all_permission_levels) * 100):.1f}%"
        })
        print(f"  {level}: {count}")

    df_perms = pd.DataFrame(perm_data)
    display(df_perms)
else:
    print("No permission data collected")

print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Access Control Report
# MAGIC
# MAGIC Generate a comprehensive access control report.

# COMMAND ----------

print("=" * 70)
print("ACCESS CONTROL REPORT")
print("=" * 70)
print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# Collect statistics
total_jobs_checked = 0
total_clusters_checked = 0
total_job_permissions = 0
total_cluster_permissions = 0

# Analyze jobs
jobs_sample = list(ws.jobs.list(limit=50))
for job in jobs_sample:
    try:
        perms = security_admin.who_can_manage_job(job.job_id)
        total_jobs_checked += 1
        total_job_permissions += len(perms)
    except:
        continue

# Analyze clusters
clusters_sample = list(ws.clusters.list())[:20]
for cluster in clusters_sample:
    try:
        perms = security_admin.who_can_use_cluster(cluster.cluster_id)
        total_clusters_checked += 1
        total_cluster_permissions += len(perms)
    except:
        continue

print(f"\nResources Analyzed:")
print(f"  Jobs: {total_jobs_checked}")
print(f"  Clusters: {total_clusters_checked}")

print(f"\nPermissions Found:")
print(f"  Job permissions: {total_job_permissions}")
print(f"  Cluster permissions: {total_cluster_permissions}")

if total_jobs_checked > 0:
    avg_job_perms = total_job_permissions / total_jobs_checked
    print(f"\nAverage permissions per job: {avg_job_perms:.1f}")

if total_clusters_checked > 0:
    avg_cluster_perms = total_cluster_permissions / total_clusters_checked
    print(f"Average permissions per cluster: {avg_cluster_perms:.1f}")

print("\n" + "=" * 70)

# Security posture assessment
if total_job_permissions == 0 and total_cluster_permissions == 0:
    print("Security Posture: RESTRICTED - Very few explicit permissions")
elif avg_job_perms > 5 or avg_cluster_perms > 10:
    print("Security Posture: OPEN - Many users have access")
else:
    print("Security Posture: BALANCED - Reasonable access control")

print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Example Natural Language Queries
# MAGIC
# MAGIC These are the types of questions you can answer with SecurityAdmin:

# COMMAND ----------

print("=" * 70)
print("EXAMPLE QUESTIONS YOU CAN ANSWER WITH SECURITYADMIN")
print("=" * 70)

examples = [
    {
        "Question": "Who can manage job 12345?",
        "Method": "who_can_manage_job(job_id=12345)",
        "Use Case": "Access control audit"
    },
    {
        "Question": "Which users can attach to cluster abc123?",
        "Method": "who_can_use_cluster(cluster_id='abc123')",
        "Use Case": "Resource access review"
    },
    {
        "Question": "Show me all principals with job management rights",
        "Method": "who_can_manage_job() for all jobs + aggregate",
        "Use Case": "Permission inventory"
    },
    {
        "Question": "Which jobs does user john.doe have access to?",
        "Method": "who_can_manage_job() + filter by principal",
        "Use Case": "User access report"
    },
    {
        "Question": "Are there any jobs with no explicit managers?",
        "Method": "who_can_manage_job() + check for empty results",
        "Use Case": "Security gap analysis"
    }
]

df_examples = pd.DataFrame(examples)
display(df_examples)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Security Best Practices Check
# MAGIC
# MAGIC Identify potential security issues or areas for improvement.

# COMMAND ----------

print("=" * 70)
print("SECURITY BEST PRACTICES CHECK")
print("=" * 70)

recommendations = []

# Check 1: Jobs with no explicit permissions
jobs_no_perms = 0
jobs_with_perms = 0

for job in list(ws.jobs.list(limit=30)):
    try:
        perms = security_admin.who_can_manage_job(job.job_id)
        if len(perms) == 0:
            jobs_no_perms += 1
        else:
            jobs_with_perms += 1
    except:
        continue

if jobs_no_perms > 0:
    recommendations.append({
        "Issue": f"{jobs_no_perms} jobs have no explicit permissions",
        "Severity": "Low",
        "Recommendation": "Consider applying explicit permissions for better access control"
    })

# Check 2: Overly permissive resources
if total_job_permissions > 0 and total_jobs_checked > 0:
    avg_perms = total_job_permissions / total_jobs_checked
    if avg_perms > 10:
        recommendations.append({
            "Issue": f"High average permissions per job ({avg_perms:.1f})",
            "Severity": "Medium",
            "Recommendation": "Review if all users need access to all jobs"
        })

# Check 3: Cluster access
if total_cluster_permissions > 0 and total_clusters_checked > 0:
    avg_cluster_perms = total_cluster_permissions / total_clusters_checked
    if avg_cluster_perms > 15:
        recommendations.append({
            "Issue": f"High average permissions per cluster ({avg_cluster_perms:.1f})",
            "Severity": "Medium",
            "Recommendation": "Consider using cluster policies to restrict access"
        })

if recommendations:
    print(f"\nFound {len(recommendations)} recommendations:\n")
    df_recommendations = pd.DataFrame(recommendations)
    display(df_recommendations)
else:
    print("\n✓ No major security issues found")
    print("Access control appears well-managed")

print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC The SecurityAdmin class provides essential security and access control capabilities:
# MAGIC
# MAGIC **Key Features:**
# MAGIC - ✓ Query job management permissions
# MAGIC - ✓ Check cluster usage permissions
# MAGIC - ✓ Analyze permission distribution patterns
# MAGIC - ✓ Generate access control reports
# MAGIC - ✓ Identify security best practice gaps
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - Explore cost and budget tracking in `05_usage_cost_budget_demo.py`
# MAGIC - Review audit logs in `06_audit_pipelines_demo.py`
# MAGIC - Deploy the full agent in `07_agent_deployment.py`

# COMMAND ----------
