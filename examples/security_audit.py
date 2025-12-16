#!/usr/bin/env python3
"""
Security Audit Example for Databricks Admin AI Bridge

This script demonstrates security and permissions analysis including:
- Job permissions queries
- Cluster permissions queries
- Permission reports
- Access control auditing

Target workspace: https://e2-demo-field-eng.cloud.databricks.com
"""

import argparse
import logging
from datetime import datetime
from collections import defaultdict

from admin_ai_bridge import AdminBridgeConfig, SecurityAdmin, JobsAdmin, ClustersAdmin
from admin_ai_bridge.errors import APIError, ValidationError, ResourceNotFoundError


def setup_logging(verbose: bool = False):
    """Configure logging for the script."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def print_section_header(title: str):
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)


def print_job_permissions_report(security_admin: SecurityAdmin, job_id: int):
    """Generate and print a permissions report for a specific job."""
    print_section_header(f"JOB PERMISSIONS AUDIT: Job ID {job_id}")

    try:
        # Get users who can manage the job
        managers = security_admin.who_can_manage_job(job_id)

        if not managers:
            print(f"\nNo CAN_MANAGE permissions found for job {job_id}.")
            return

        print(f"\nFound {len(managers)} principals with CAN_MANAGE permission:\n")

        # Group by permission level
        by_level = defaultdict(list)
        for perm in managers:
            by_level[perm.permission_level].append(perm)

        # Print by permission level
        for level in sorted(by_level.keys()):
            perms = by_level[level]
            print(f"\n{level} ({len(perms)} principals):")
            print(f"{'Principal':<50} {'Object Type':<15} {'Object ID':<20}")
            print("-" * 90)

            for perm in perms:
                principal_short = (perm.principal[:48] + "..") if len(perm.principal) > 50 else perm.principal
                print(f"{principal_short:<50} {perm.object_type:<15} {perm.object_id:<20}")

        # Security recommendations
        print("\n" + "-" * 90)
        print("Security Recommendations:")
        if len(managers) > 10:
            print("  NOTICE: Large number of principals with manage access")
            print("  ACTION: Review and minimize management access to essential users")
        if len(managers) == 0:
            print("  WARNING: No explicit managers found - job may be orphaned")

    except ResourceNotFoundError as e:
        print(f"\nResource Not Found: {e}")
    except ValidationError as e:
        print(f"\nValidation Error: {e}")
    except APIError as e:
        print(f"\nAPI Error: {e}")
    except Exception as e:
        print(f"\nUnexpected Error: {e}")


def print_cluster_permissions_report(security_admin: SecurityAdmin, cluster_id: str):
    """Generate and print a permissions report for a specific cluster."""
    print_section_header(f"CLUSTER PERMISSIONS AUDIT: Cluster ID {cluster_id}")

    try:
        # Get users who can manage the cluster
        managers = security_admin.who_can_manage_cluster(cluster_id)

        if not managers:
            print(f"\nNo CAN_MANAGE permissions found for cluster {cluster_id}.")
            return

        print(f"\nFound {len(managers)} principals with CAN_MANAGE permission:\n")

        # Group by permission level
        by_level = defaultdict(list)
        for perm in managers:
            by_level[perm.permission_level].append(perm)

        # Print by permission level
        for level in sorted(by_level.keys()):
            perms = by_level[level]
            print(f"\n{level} ({len(perms)} principals):")
            print(f"{'Principal':<50} {'Object Type':<15} {'Object ID':<20}")
            print("-" * 90)

            for perm in perms:
                principal_short = (perm.principal[:48] + "..") if len(perm.principal) > 50 else perm.principal
                print(f"{principal_short:<50} {perm.object_type:<15} {perm.object_id:<20}")

        # Security recommendations
        print("\n" + "-" * 90)
        print("Security Recommendations:")
        if len(managers) > 10:
            print("  NOTICE: Large number of principals with manage access")
            print("  ACTION: Review and minimize management access")
        if len(managers) == 0:
            print("  WARNING: No explicit managers found")

    except ResourceNotFoundError as e:
        print(f"\nResource Not Found: {e}")
    except ValidationError as e:
        print(f"\nValidation Error: {e}")
    except APIError as e:
        print(f"\nAPI Error: {e}")
    except Exception as e:
        print(f"\nUnexpected Error: {e}")


def print_comprehensive_permissions_audit(security_admin: SecurityAdmin, jobs_admin: JobsAdmin):
    """Generate a comprehensive permissions audit across multiple jobs."""
    print_section_header("COMPREHENSIVE PERMISSIONS AUDIT")

    try:
        # Get recent jobs
        print("\nScanning recent jobs for permissions analysis...")
        recent_jobs = jobs_admin.list_long_running_jobs(
            min_duration_hours=0.1,  # Very low threshold to get any recent jobs
            lookback_hours=168.0,  # Last week
            limit=20
        )

        if not recent_jobs:
            print("No recent jobs found to audit.")
            return

        print(f"\nAnalyzing permissions for {len(recent_jobs)} recent jobs:\n")

        # Collect permission statistics
        all_principals = set()
        job_permissions = {}
        errors = []

        for job in recent_jobs[:10]:  # Limit to 10 jobs to avoid API rate limits
            try:
                managers = security_admin.who_can_manage_job(job.job_id)
                job_permissions[job.job_name] = {
                    "job_id": job.job_id,
                    "managers": managers,
                    "principal_count": len(managers)
                }
                for perm in managers:
                    all_principals.add(perm.principal)
            except Exception as e:
                errors.append((job.job_name, str(e)))

        # Print summary
        print(f"{'Job Name':<40} {'Job ID':<12} {'Managers':>10}")
        print("-" * 65)

        sorted_jobs = sorted(
            job_permissions.items(),
            key=lambda x: x[1]["principal_count"],
            reverse=True
        )

        for job_name, data in sorted_jobs:
            job_name_short = (job_name[:38] + "..") if len(job_name) > 40 else job_name
            print(f"{job_name_short:<40} {data['job_id']:<12} {data['principal_count']:>10}")

        # Print statistics
        print("\n" + "-" * 65)
        print(f"Jobs Analyzed: {len(job_permissions)}")
        print(f"Unique Principals with Manage Access: {len(all_principals)}")
        if errors:
            print(f"Errors Encountered: {len(errors)}")

        # Print top principals
        if all_principals:
            print("\nPrincipals with Manage Access:")
            for i, principal in enumerate(sorted(all_principals)[:10], 1):
                print(f"  {i}. {principal}")
            if len(all_principals) > 10:
                print(f"  ... and {len(all_principals) - 10} more")

        # Security insights
        print("\nSecurity Insights:")
        avg_managers = sum(d["principal_count"] for d in job_permissions.values()) / len(job_permissions) if job_permissions else 0
        print(f"  Average Managers per Job: {avg_managers:.1f}")

        if avg_managers > 5:
            print("  RECOMMENDATION: Review permission model - high average may indicate overly broad access")
        if len(all_principals) > 20:
            print("  RECOMMENDATION: Large number of principals with manage access - consider role-based access")

        # Print errors if any
        if errors:
            print("\nErrors Encountered:")
            for job_name, error in errors[:5]:
                print(f"  {job_name}: {error}")

    except Exception as e:
        print(f"\nError generating comprehensive audit: {e}")


def print_access_control_summary(security_admin: SecurityAdmin):
    """Generate an access control summary report."""
    print_section_header("ACCESS CONTROL SUMMARY")

    print("\nThis report provides an overview of access control patterns across")
    print("the workspace. For detailed permissions on specific resources, use")
    print("the --job-id or --cluster-id options.")

    print("\nSecurity Best Practices:")
    print("  1. Apply principle of least privilege")
    print("  2. Use groups instead of individual user permissions")
    print("  3. Regularly audit and remove unused permissions")
    print("  4. Document permission grants and their justification")
    print("  5. Use service principals for automated workloads")
    print("  6. Enable audit logging for all permission changes")

    print("\nCommon Permission Levels:")
    print("  CAN_MANAGE: Full control including permission management")
    print("  CAN_RESTART: Can start, stop, and restart (for clusters)")
    print("  CAN_ATTACH_TO: Can attach notebooks (for clusters)")
    print("  CAN_RUN: Can trigger job runs")
    print("  CAN_VIEW: Read-only access")


def main():
    """Main entry point for the security audit script."""
    parser = argparse.ArgumentParser(
        description="Security and permissions audit for Databricks Admin AI Bridge",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--profile",
        default="DEFAULT",
        help="Databricks CLI profile name (default: DEFAULT)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "--job-id",
        type=int,
        help="Specific job ID to audit",
    )
    parser.add_argument(
        "--cluster-id",
        type=str,
        help="Specific cluster ID to audit",
    )
    parser.add_argument(
        "--report",
        choices=["job", "cluster", "comprehensive", "summary", "all"],
        default="summary",
        help="Which report to generate (default: summary)",
    )

    args = parser.parse_args()

    # Configure logging
    setup_logging(args.verbose)

    # Create configuration
    cfg = AdminBridgeConfig(profile=args.profile)

    # Initialize admin clients
    security_admin = SecurityAdmin(cfg)

    # Print header
    print("=" * 80)
    print(" DATABRICKS SECURITY AUDIT REPORT")
    print("=" * 80)
    print(f"\nWorkspace: https://e2-demo-field-eng.cloud.databricks.com")
    print(f"Profile: {args.profile}")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Generate requested reports
    if args.report in ["job", "all"]:
        if args.job_id:
            print_job_permissions_report(security_admin, args.job_id)
        elif args.report == "job":
            print("\nError: --job-id argument required for job permissions report")

    if args.report in ["cluster", "all"]:
        if args.cluster_id:
            print_cluster_permissions_report(security_admin, args.cluster_id)
        elif args.report == "cluster":
            print("\nError: --cluster-id argument required for cluster permissions report")

    if args.report in ["comprehensive", "all"]:
        jobs_admin = JobsAdmin(cfg)
        print_comprehensive_permissions_audit(security_admin, jobs_admin)

    if args.report in ["summary", "all"]:
        print_access_control_summary(security_admin)

    print("\n" + "=" * 80)
    print(" REPORT COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
