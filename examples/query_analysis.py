#!/usr/bin/env python3
"""
Query Analysis Example for Databricks Admin AI Bridge

This script demonstrates DBSQL query performance analysis including:
- Top slowest queries identification
- User query summaries
- Performance metrics analysis
- Formatted output and reports

Target workspace: https://e2-demo-field-eng.cloud.databricks.com
"""

import argparse
import logging
from datetime import datetime
from collections import defaultdict

from admin_ai_bridge import AdminBridgeConfig, DBSQLAdmin
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


def format_duration(seconds: float) -> str:
    """Format duration in appropriate units."""
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.2f}m"
    else:
        return f"{seconds / 3600:.2f}h"


def print_slowest_queries_report(dbsql_admin: DBSQLAdmin, lookback: float, limit: int):
    """Generate and print a report of slowest queries."""
    print_section_header(f"TOP {limit} SLOWEST QUERIES (last {lookback} hours)")

    try:
        queries = dbsql_admin.top_slowest_queries(
            lookback_hours=lookback,
            limit=limit
        )

        if not queries:
            print("\nNo queries found.")
            return

        print(f"\nFound {len(queries)} slow queries:\n")

        # Print table header
        print(f"{'Query ID':<40} {'User':<25} {'Duration':>12} {'Status':<12} {'Warehouse ID':<20}")
        print("-" * 115)

        # Print each query
        for query in queries:
            duration_str = format_duration(query.duration_seconds) if query.duration_seconds else "N/A"
            user_short = (query.user_name[:23] + "..") if query.user_name and len(query.user_name) > 25 else (query.user_name or "N/A")
            warehouse_short = (query.warehouse_id[:18] + "..") if query.warehouse_id and len(query.warehouse_id) > 20 else (query.warehouse_id or "N/A")

            print(f"{query.query_id:<40} {user_short:<25} {duration_str:>12} {query.status or 'N/A':<12} {warehouse_short:<20}")

            # Print SQL preview if available
            if query.sql_text:
                sql_preview = query.sql_text.replace('\n', ' ').replace('\r', ' ').strip()[:100]
                print(f"  SQL: {sql_preview}...")

        # Calculate statistics
        total_duration = sum(q.duration_seconds or 0 for q in queries)
        avg_duration = total_duration / len(queries) if queries else 0

        print("\n" + "-" * 115)
        print(f"Total Queries: {len(queries)}")
        print(f"Average Duration: {format_duration(avg_duration)}")
        print(f"Total Duration: {format_duration(total_duration)}")
        print(f"Slowest Query: {format_duration(queries[0].duration_seconds if queries[0].duration_seconds else 0)}")

    except ValidationError as e:
        print(f"\nValidation Error: {e}")
    except APIError as e:
        print(f"\nAPI Error: {e}")
    except Exception as e:
        print(f"\nUnexpected Error: {e}")


def print_user_summary_report(dbsql_admin: DBSQLAdmin, user_name: str, lookback: float):
    """Generate and print a user query summary report."""
    print_section_header(f"USER QUERY SUMMARY: {user_name}")

    try:
        summary = dbsql_admin.user_query_summary(
            user_name=user_name,
            lookback_hours=lookback
        )

        print(f"\nQuery Activity Summary:")
        print(f"  Time Window: {summary['time_window_start']} to {summary['time_window_end']}")
        print(f"\nQuery Statistics:")
        print(f"  Total Queries: {summary['total_queries']}")
        print(f"  Successful: {summary['successful_queries']}")
        print(f"  Failed: {summary['failed_queries']}")
        print(f"  Failure Rate: {summary['failure_rate']:.2f}%")

        print(f"\nPerformance Metrics:")
        print(f"  Average Duration: {format_duration(summary['avg_duration_seconds'])}")
        print(f"  Min Duration: {format_duration(summary['min_duration_seconds'])}")
        print(f"  Max Duration: {format_duration(summary['max_duration_seconds'])}")
        print(f"  Total Duration: {format_duration(summary['total_duration_seconds'])}")

        print(f"\nWarehouses Used:")
        if summary['warehouses_used']:
            for warehouse in summary['warehouses_used']:
                print(f"  - {warehouse}")
        else:
            print("  None")

        # Status indicators
        print(f"\nStatus Indicators:")
        if summary['failure_rate'] > 10:
            print("  WARNING: High failure rate detected (>10%)")
        if summary['avg_duration_seconds'] > 300:
            print("  NOTICE: High average query duration (>5 minutes)")
        if summary['total_queries'] == 0:
            print("  INFO: No query activity in the specified time window")

    except ValidationError as e:
        print(f"\nValidation Error: {e}")
    except APIError as e:
        print(f"\nAPI Error: {e}")
    except Exception as e:
        print(f"\nUnexpected Error: {e}")


def print_performance_metrics_report(dbsql_admin: DBSQLAdmin, lookback: float):
    """Generate and print a performance metrics report."""
    print_section_header(f"QUERY PERFORMANCE METRICS (last {lookback} hours)")

    try:
        # Get queries for analysis
        queries = dbsql_admin.top_slowest_queries(
            lookback_hours=lookback,
            limit=100
        )

        if not queries:
            print("\nNo queries found for analysis.")
            return

        # Aggregate metrics by user
        user_stats = defaultdict(lambda: {
            "count": 0,
            "total_duration": 0.0,
            "durations": [],
            "statuses": defaultdict(int)
        })

        # Aggregate metrics by warehouse
        warehouse_stats = defaultdict(lambda: {
            "count": 0,
            "total_duration": 0.0,
            "durations": []
        })

        for query in queries:
            if query.user_name:
                user_stats[query.user_name]["count"] += 1
                user_stats[query.user_name]["total_duration"] += query.duration_seconds or 0
                user_stats[query.user_name]["durations"].append(query.duration_seconds or 0)
                if query.status:
                    user_stats[query.user_name]["statuses"][query.status] += 1

            if query.warehouse_id:
                warehouse_stats[query.warehouse_id]["count"] += 1
                warehouse_stats[query.warehouse_id]["total_duration"] += query.duration_seconds or 0
                warehouse_stats[query.warehouse_id]["durations"].append(query.duration_seconds or 0)

        # Print user performance metrics
        print("\nTop 10 Users by Query Count:")
        print(f"{'User':<40} {'Queries':>10} {'Avg Duration':>15} {'Total Duration':>15}")
        print("-" * 85)

        sorted_users = sorted(
            user_stats.items(),
            key=lambda x: x[1]["count"],
            reverse=True
        )[:10]

        for user, stats in sorted_users:
            avg_duration = stats["total_duration"] / stats["count"] if stats["count"] > 0 else 0
            user_short = (user[:38] + "..") if len(user) > 40 else user

            print(f"{user_short:<40} {stats['count']:>10} {format_duration(avg_duration):>15} {format_duration(stats['total_duration']):>15}")

        # Print warehouse performance metrics
        print("\n\nWarehouse Performance:")
        print(f"{'Warehouse ID':<40} {'Queries':>10} {'Avg Duration':>15} {'Total Duration':>15}")
        print("-" * 85)

        sorted_warehouses = sorted(
            warehouse_stats.items(),
            key=lambda x: x[1]["count"],
            reverse=True
        )[:10]

        for warehouse, stats in sorted_warehouses:
            avg_duration = stats["total_duration"] / stats["count"] if stats["count"] > 0 else 0
            warehouse_short = (warehouse[:38] + "..") if len(warehouse) > 40 else warehouse

            print(f"{warehouse_short:<40} {stats['count']:>10} {format_duration(avg_duration):>15} {format_duration(stats['total_duration']):>15}")

        # Print overall statistics
        print("\n" + "-" * 85)
        print(f"Overall Statistics:")
        print(f"  Total Queries Analyzed: {len(queries)}")
        print(f"  Unique Users: {len(user_stats)}")
        print(f"  Unique Warehouses: {len(warehouse_stats)}")
        print(f"  Total Duration: {format_duration(sum(q.duration_seconds or 0 for q in queries))}")

    except Exception as e:
        print(f"\nError generating performance metrics: {e}")


def main():
    """Main entry point for the query analysis script."""
    parser = argparse.ArgumentParser(
        description="DBSQL query performance analysis for Databricks Admin AI Bridge",
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
        "--lookback",
        type=float,
        default=24.0,
        help="Lookback period in hours (default: 24.0)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of results for slowest queries (default: 20)",
    )
    parser.add_argument(
        "--user",
        type=str,
        help="User name for user-specific summary report",
    )
    parser.add_argument(
        "--report",
        choices=["slowest", "user", "metrics", "all"],
        default="all",
        help="Which report to generate (default: all)",
    )

    args = parser.parse_args()

    # Configure logging
    setup_logging(args.verbose)

    # Create configuration
    cfg = AdminBridgeConfig(profile=args.profile)

    # Initialize admin client
    dbsql_admin = DBSQLAdmin(cfg)

    # Print header
    print("=" * 80)
    print(" DATABRICKS QUERY ANALYSIS REPORT")
    print("=" * 80)
    print(f"\nWorkspace: https://e2-demo-field-eng.cloud.databricks.com")
    print(f"Profile: {args.profile}")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Generate requested reports
    if args.report in ["slowest", "all"]:
        print_slowest_queries_report(dbsql_admin, args.lookback, args.limit)

    if args.report in ["user", "all"]:
        if args.user:
            print_user_summary_report(dbsql_admin, args.user, args.lookback)
        elif args.report == "user":
            print("\nError: --user argument required for user report")

    if args.report in ["metrics", "all"]:
        print_performance_metrics_report(dbsql_admin, args.lookback)

    print("\n" + "=" * 80)
    print(" REPORT COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
