#!/usr/bin/env python3
"""
Basic Usage Example for Databricks Admin AI Bridge

This script demonstrates the basic usage of core admin classes including:
- AdminBridgeConfig configuration
- JobsAdmin for job monitoring
- DBSQLAdmin for query analysis
- ClustersAdmin for cluster monitoring
- Error handling patterns

Target workspace: https://e2-demo-field-eng.cloud.databricks.com
"""

import argparse
import logging
from datetime import datetime

from admin_ai_bridge import (
    AdminBridgeConfig,
    JobsAdmin,
    DBSQLAdmin,
    ClustersAdmin,
)
from admin_ai_bridge.errors import APIError, ValidationError


def setup_logging(verbose: bool = False):
    """Configure logging for the script."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def demo_jobs_admin(cfg: AdminBridgeConfig):
    """Demonstrate JobsAdmin basic functionality."""
    print("\n" + "=" * 80)
    print("JOBS ADMIN DEMO")
    print("=" * 80)

    try:
        jobs_admin = JobsAdmin(cfg)

        # List long-running jobs
        print("\n--- Long-Running Jobs (> 4 hours in last 24 hours) ---")
        long_jobs = jobs_admin.list_long_running_jobs(
            min_duration_hours=4.0,
            lookback_hours=24.0,
            limit=5
        )

        if long_jobs:
            for job in long_jobs:
                duration_hours = job.duration_seconds / 3600 if job.duration_seconds else 0
                print(f"  Job: {job.job_name}")
                print(f"    ID: {job.job_id}")
                print(f"    Run ID: {job.run_id}")
                print(f"    State: {job.state}")
                print(f"    Duration: {duration_hours:.2f} hours")
                print(f"    Start Time: {job.start_time}")
                print()
        else:
            print("  No long-running jobs found.")

        # List failed jobs
        print("\n--- Failed Jobs (last 24 hours) ---")
        failed_jobs = jobs_admin.list_failed_jobs(
            lookback_hours=24.0,
            limit=5
        )

        if failed_jobs:
            for job in failed_jobs:
                print(f"  Job: {job.job_name}")
                print(f"    ID: {job.job_id}")
                print(f"    Run ID: {job.run_id}")
                print(f"    State: {job.state}")
                print(f"    Start Time: {job.start_time}")
                print()
        else:
            print("  No failed jobs found.")

    except ValidationError as e:
        print(f"  Validation error: {e}")
    except APIError as e:
        print(f"  API error: {e}")
    except Exception as e:
        print(f"  Unexpected error: {e}")


def demo_dbsql_admin(cfg: AdminBridgeConfig):
    """Demonstrate DBSQLAdmin basic functionality."""
    print("\n" + "=" * 80)
    print("DBSQL ADMIN DEMO")
    print("=" * 80)

    try:
        dbsql_admin = DBSQLAdmin(cfg)

        # Get slowest queries
        print("\n--- Top 5 Slowest Queries (last 24 hours) ---")
        slow_queries = dbsql_admin.top_slowest_queries(
            lookback_hours=24.0,
            limit=5
        )

        if slow_queries:
            for query in slow_queries:
                print(f"  Query ID: {query.query_id}")
                print(f"    User: {query.user_name}")
                print(f"    Status: {query.status}")
                print(f"    Duration: {query.duration_seconds:.2f}s")
                print(f"    Warehouse: {query.warehouse_id}")
                if query.sql_text:
                    # Show first 100 chars of SQL
                    sql_preview = query.sql_text[:100].replace('\n', ' ')
                    print(f"    SQL: {sql_preview}...")
                print()
        else:
            print("  No queries found.")

    except ValidationError as e:
        print(f"  Validation error: {e}")
    except APIError as e:
        print(f"  API error: {e}")
    except Exception as e:
        print(f"  Unexpected error: {e}")


def demo_clusters_admin(cfg: AdminBridgeConfig):
    """Demonstrate ClustersAdmin basic functionality."""
    print("\n" + "=" * 80)
    print("CLUSTERS ADMIN DEMO")
    print("=" * 80)

    try:
        clusters_admin = ClustersAdmin(cfg)

        # List long-running clusters
        print("\n--- Long-Running Clusters (> 8 hours) ---")
        long_clusters = clusters_admin.list_long_running_clusters(
            min_duration_hours=8.0,
            lookback_hours=24.0,
            limit=5
        )

        if long_clusters:
            for cluster in long_clusters:
                runtime_hours = 0
                if cluster.start_time:
                    runtime = datetime.now(cluster.start_time.tzinfo) - cluster.start_time
                    runtime_hours = runtime.total_seconds() / 3600

                print(f"  Cluster: {cluster.cluster_name}")
                print(f"    ID: {cluster.cluster_id}")
                print(f"    State: {cluster.state}")
                print(f"    Creator: {cluster.creator}")
                print(f"    Runtime: {runtime_hours:.2f} hours")
                print(f"    Node Type: {cluster.node_type}")
                print()
        else:
            print("  No long-running clusters found.")

    except ValidationError as e:
        print(f"  Validation error: {e}")
    except APIError as e:
        print(f"  API error: {e}")
    except Exception as e:
        print(f"  Unexpected error: {e}")


def demo_error_handling(cfg: AdminBridgeConfig):
    """Demonstrate error handling patterns."""
    print("\n" + "=" * 80)
    print("ERROR HANDLING DEMO")
    print("=" * 80)

    jobs_admin = JobsAdmin(cfg)

    # Test validation errors
    print("\n--- Testing Validation Errors ---")
    try:
        # This should raise ValidationError - negative duration
        jobs_admin.list_long_running_jobs(min_duration_hours=-1.0)
    except ValidationError as e:
        print(f"  Caught expected ValidationError: {e}")

    try:
        # This should raise ValidationError - zero limit
        jobs_admin.list_failed_jobs(limit=0)
    except ValidationError as e:
        print(f"  Caught expected ValidationError: {e}")

    print("\n  Error handling demonstration complete.")


def main():
    """Main entry point for the basic usage example."""
    parser = argparse.ArgumentParser(
        description="Basic usage demonstration of Databricks Admin AI Bridge",
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
        "--demo",
        choices=["jobs", "dbsql", "clusters", "errors", "all"],
        default="all",
        help="Which demo to run (default: all)",
    )

    args = parser.parse_args()

    # Configure logging
    setup_logging(args.verbose)

    # Create configuration
    cfg = AdminBridgeConfig(profile=args.profile)

    print("=" * 80)
    print("DATABRICKS ADMIN AI BRIDGE - BASIC USAGE EXAMPLES")
    print("=" * 80)
    print(f"\nWorkspace: https://e2-demo-field-eng.cloud.databricks.com")
    print(f"Profile: {args.profile}")
    print(f"Timestamp: {datetime.now().isoformat()}")

    # Run selected demos
    if args.demo in ["jobs", "all"]:
        demo_jobs_admin(cfg)

    if args.demo in ["dbsql", "all"]:
        demo_dbsql_admin(cfg)

    if args.demo in ["clusters", "all"]:
        demo_clusters_admin(cfg)

    if args.demo in ["errors", "all"]:
        demo_error_handling(cfg)

    print("\n" + "=" * 80)
    print("DEMO COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
