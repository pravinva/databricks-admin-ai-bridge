#!/usr/bin/env python3
"""
Cluster Monitoring Example for Databricks Admin AI Bridge

This script demonstrates cluster utilization monitoring including:
- Long-running clusters detection
- Idle clusters identification
- Utilization reports
- Cost optimization recommendations

Target workspace: https://e2-demo-field-eng.cloud.databricks.com
"""

import argparse
import logging
from datetime import datetime, timezone

from admin_ai_bridge import AdminBridgeConfig, ClustersAdmin
from admin_ai_bridge.errors import APIError, ValidationError


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


def format_duration(hours: float) -> str:
    """Format duration in appropriate units."""
    if hours < 1:
        return f"{hours * 60:.0f}m"
    elif hours < 24:
        return f"{hours:.2f}h"
    else:
        days = hours / 24
        return f"{days:.2f}d"


def print_long_running_clusters_report(clusters_admin: ClustersAdmin, min_hours: float, lookback: float, limit: int):
    """Generate and print a report of long-running clusters."""
    print_section_header(f"LONG-RUNNING CLUSTERS (> {min_hours} hours)")

    try:
        clusters = clusters_admin.list_long_running_clusters(
            min_duration_hours=min_hours,
            lookback_hours=lookback,
            limit=limit
        )

        if not clusters:
            print("\nNo long-running clusters found.")
            return

        print(f"\nFound {len(clusters)} long-running clusters:\n")

        # Print table header
        print(f"{'Cluster Name':<35} {'State':<12} {'Creator':<25} {'Runtime':>12} {'Node Type':<20}")
        print("-" * 110)

        # Print each cluster
        total_runtime_hours = 0.0
        for cluster in clusters:
            runtime_hours = 0
            if cluster.start_time:
                runtime = datetime.now(timezone.utc) - cluster.start_time
                runtime_hours = runtime.total_seconds() / 3600
                total_runtime_hours += runtime_hours

            runtime_str = format_duration(runtime_hours)
            cluster_name_short = (cluster.cluster_name[:33] + "..") if len(cluster.cluster_name) > 35 else cluster.cluster_name
            creator_short = (cluster.creator[:23] + "..") if cluster.creator and len(cluster.creator) > 25 else (cluster.creator or "N/A")
            node_type_short = (cluster.node_type[:18] + "..") if cluster.node_type and len(cluster.node_type) > 20 else (cluster.node_type or "N/A")

            print(f"{cluster_name_short:<35} {cluster.state:<12} {creator_short:<25} {runtime_str:>12} {node_type_short:<20}")

        # Calculate statistics
        avg_runtime = total_runtime_hours / len(clusters) if clusters else 0

        print("\n" + "-" * 110)
        print(f"Total Clusters: {len(clusters)}")
        print(f"Average Runtime: {format_duration(avg_runtime)}")
        print(f"Total Runtime: {format_duration(total_runtime_hours)}")

        # Cost optimization recommendations
        print("\nCost Optimization Recommendations:")
        if total_runtime_hours > 100:
            print("  HIGH PRIORITY: Review auto-termination settings for these clusters")
        if len(clusters) > 5:
            print("  MEDIUM PRIORITY: Consider consolidating workloads onto fewer clusters")
        for cluster in clusters:
            if cluster.start_time:
                runtime = datetime.now(timezone.utc) - cluster.start_time
                runtime_hours = runtime.total_seconds() / 3600
                if runtime_hours > 48:
                    print(f"  NOTICE: Cluster '{cluster.cluster_name}' has been running for {format_duration(runtime_hours)}")

    except ValidationError as e:
        print(f"\nValidation Error: {e}")
    except APIError as e:
        print(f"\nAPI Error: {e}")
    except Exception as e:
        print(f"\nUnexpected Error: {e}")


def print_idle_clusters_report(clusters_admin: ClustersAdmin, idle_threshold: float, lookback: float, limit: int):
    """Generate and print a report of idle clusters."""
    print_section_header(f"IDLE CLUSTERS (no activity for > {idle_threshold} hours)")

    try:
        clusters = clusters_admin.list_idle_clusters(
            idle_threshold_hours=idle_threshold,
            lookback_hours=lookback,
            limit=limit
        )

        if not clusters:
            print("\nNo idle clusters found.")
            return

        print(f"\nFound {len(clusters)} idle clusters:\n")

        # Print table header
        print(f"{'Cluster Name':<35} {'State':<12} {'Creator':<25} {'Idle Time':>12} {'Last Activity':<20}")
        print("-" * 110)

        # Print each cluster
        for cluster in clusters:
            idle_hours = 0
            last_activity_str = "N/A"

            if cluster.last_activity_time:
                idle_time = datetime.now(timezone.utc) - cluster.last_activity_time
                idle_hours = idle_time.total_seconds() / 3600
                last_activity_str = cluster.last_activity_time.strftime("%Y-%m-%d %H:%M")

            idle_str = format_duration(idle_hours)
            cluster_name_short = (cluster.cluster_name[:33] + "..") if len(cluster.cluster_name) > 35 else cluster.cluster_name
            creator_short = (cluster.creator[:23] + "..") if cluster.creator and len(cluster.creator) > 25 else (cluster.creator or "N/A")

            print(f"{cluster_name_short:<35} {cluster.state:<12} {creator_short:<25} {idle_str:>12} {last_activity_str:<20}")

        print("\n" + "-" * 110)
        print(f"Total Idle Clusters: {len(clusters)}")

        # Recommendations
        print("\nRecommendations:")
        print("  1. Consider terminating idle clusters to reduce costs")
        print("  2. Review auto-termination settings (recommended: 30-60 minutes)")
        print("  3. Investigate why clusters were left running without activity")

    except ValidationError as e:
        print(f"\nValidation Error: {e}")
    except APIError as e:
        print(f"\nAPI Error: {e}")
    except Exception as e:
        print(f"\nUnexpected Error: {e}")


def print_cluster_utilization_report(clusters_admin: ClustersAdmin, lookback: float):
    """Generate and print a cluster utilization summary report."""
    print_section_header(f"CLUSTER UTILIZATION SUMMARY (last {lookback} hours)")

    try:
        # Get all clusters for analysis
        long_running = clusters_admin.list_long_running_clusters(
            min_duration_hours=1.0,  # Lower threshold for comprehensive analysis
            lookback_hours=lookback,
            limit=100
        )

        idle = clusters_admin.list_idle_clusters(
            idle_threshold_hours=2.0,  # 2+ hours idle
            lookback_hours=lookback,
            limit=100
        )

        print("\nCluster Activity Summary:")
        print(f"  Long-Running Clusters (>1h): {len(long_running)}")
        print(f"  Idle Clusters (>2h idle): {len(idle)}")

        # Analyze by creator
        creators = {}
        for cluster in long_running:
            creator = cluster.creator or "Unknown"
            if creator not in creators:
                creators[creator] = {"count": 0, "runtime_hours": 0.0}
            creators[creator]["count"] += 1
            if cluster.start_time:
                runtime = datetime.now(timezone.utc) - cluster.start_time
                creators[creator]["runtime_hours"] += runtime.total_seconds() / 3600

        if creators:
            print("\nTop Cluster Creators:")
            print(f"{'Creator':<40} {'Clusters':>10} {'Total Runtime':>15}")
            print("-" * 70)

            sorted_creators = sorted(
                creators.items(),
                key=lambda x: x[1]["runtime_hours"],
                reverse=True
            )[:10]

            for creator, stats in sorted_creators:
                creator_short = (creator[:38] + "..") if len(creator) > 40 else creator
                print(f"{creator_short:<40} {stats['count']:>10} {format_duration(stats['runtime_hours']):>15}")

        # Analyze by node type
        node_types = {}
        for cluster in long_running:
            node_type = cluster.node_type or "Unknown"
            if node_type not in node_types:
                node_types[node_type] = 0
            node_types[node_type] += 1

        if node_types:
            print("\nNode Type Distribution:")
            print(f"{'Node Type':<50} {'Count':>10}")
            print("-" * 65)

            sorted_node_types = sorted(
                node_types.items(),
                key=lambda x: x[1],
                reverse=True
            )[:10]

            for node_type, count in sorted_node_types:
                node_type_short = (node_type[:48] + "..") if len(node_type) > 50 else node_type
                print(f"{node_type_short:<50} {count:>10}")

        # Overall recommendations
        print("\n" + "-" * 70)
        print("Overall Recommendations:")
        if len(idle) > 3:
            print("  HIGH: Significant number of idle clusters detected - review auto-termination")
        if len(long_running) > 10:
            print("  MEDIUM: Many long-running clusters - consider job clusters instead of interactive")
        if len(creators) > 5 and any(c[1]["count"] > 5 for c in creators.items()):
            print("  LOW: Some users have many clusters - provide training on resource management")

    except Exception as e:
        print(f"\nError generating utilization report: {e}")


def main():
    """Main entry point for the cluster monitoring script."""
    parser = argparse.ArgumentParser(
        description="Cluster utilization monitoring for Databricks Admin AI Bridge",
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
        "--min-duration",
        type=float,
        default=8.0,
        help="Minimum duration in hours for long-running clusters (default: 8.0)",
    )
    parser.add_argument(
        "--idle-threshold",
        type=float,
        default=4.0,
        help="Idle threshold in hours (default: 4.0)",
    )
    parser.add_argument(
        "--lookback",
        type=float,
        default=24.0,
        help="Lookback period in hours (default: 24.0)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum number of results per query (default: 50)",
    )
    parser.add_argument(
        "--report",
        choices=["long-running", "idle", "utilization", "all"],
        default="all",
        help="Which report to generate (default: all)",
    )

    args = parser.parse_args()

    # Configure logging
    setup_logging(args.verbose)

    # Create configuration
    cfg = AdminBridgeConfig(profile=args.profile)

    # Initialize admin client
    clusters_admin = ClustersAdmin(cfg)

    # Print header
    print("=" * 80)
    print(" DATABRICKS CLUSTER MONITORING REPORT")
    print("=" * 80)
    print(f"\nWorkspace: https://e2-demo-field-eng.cloud.databricks.com")
    print(f"Profile: {args.profile}")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Generate requested reports
    if args.report in ["long-running", "all"]:
        print_long_running_clusters_report(clusters_admin, args.min_duration, args.lookback, args.limit)

    if args.report in ["idle", "all"]:
        print_idle_clusters_report(clusters_admin, args.idle_threshold, args.lookback, args.limit)

    if args.report in ["utilization", "all"]:
        print_cluster_utilization_report(clusters_admin, args.lookback)

    print("\n" + "=" * 80)
    print(" REPORT COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
