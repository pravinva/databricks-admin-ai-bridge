#!/usr/bin/env python3
"""
Cost Tracking Example for Databricks Admin AI Bridge

This script demonstrates usage and cost tracking including:
- Top cost centers by dimension
- Cost by workspace, project, etc.
- Budget status monitoring
- Cost reports with warnings

Target workspace: https://e2-demo-field-eng.cloud.databricks.com
"""

import argparse
import logging
from datetime import datetime

from admin_ai_bridge import AdminBridgeConfig, UsageAdmin
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


def format_cost(cost: float | None) -> str:
    """Format cost in currency."""
    if cost is None:
        return "N/A"
    return f"${cost:,.2f}"


def format_dbus(dbus: float | None) -> str:
    """Format DBUs."""
    if dbus is None:
        return "N/A"
    return f"{dbus:,.2f}"


def print_top_cost_centers_report(usage_admin: UsageAdmin, dimension: str, lookback: float, limit: int):
    """Generate and print a report of top cost centers."""
    print_section_header(f"TOP {limit} COST CENTERS BY {dimension.upper()} (last {lookback} hours)")

    try:
        cost_centers = usage_admin.top_cost_centers(
            dimension=dimension,
            lookback_hours=lookback,
            limit=limit
        )

        if not cost_centers:
            print(f"\nNo cost data found for dimension '{dimension}'.")
            print("\nNote: This may indicate that:")
            print("  1. The usage tables are not yet configured")
            print("  2. There is no usage data in the specified time window")
            print("  3. The dimension name is incorrect")
            return

        print(f"\nFound {len(cost_centers)} cost centers:\n")

        # Print table header
        print(f"{'Resource Name':<45} {'Scope':<15} {'Cost':>15} {'DBUs':>15} {'Duration (h)':>15}")
        print("-" * 110)

        # Print each cost center
        total_cost = 0.0
        total_dbus = 0.0

        for entry in cost_centers:
            duration_hours = (entry.end_time - entry.start_time).total_seconds() / 3600
            name_short = (entry.name[:43] + "..") if len(entry.name) > 45 else entry.name

            cost = entry.cost or 0.0
            dbus = entry.dbus or 0.0
            total_cost += cost
            total_dbus += dbus

            print(f"{name_short:<45} {entry.scope:<15} {format_cost(cost):>15} {format_dbus(dbus):>15} {duration_hours:>15.2f}")

        # Print totals
        print("-" * 110)
        print(f"{'TOTAL':<45} {'':<15} {format_cost(total_cost):>15} {format_dbus(total_dbus):>15}")

        # Cost warnings
        print("\n" + "-" * 110)
        print("Cost Alerts:")
        if total_cost > 10000:
            print(f"  HIGH COST ALERT: Total cost of {format_cost(total_cost)} in {lookback}h period")
        if total_cost > 5000:
            print(f"  WARNING: Significant spend detected - review top consumers")
        if len(cost_centers) > 0:
            top_cost = cost_centers[0].cost or 0
            if top_cost > total_cost * 0.5:
                print(f"  CONCENTRATION ALERT: Top resource accounts for >50% of total cost")

    except ValidationError as e:
        print(f"\nValidation Error: {e}")
    except APIError as e:
        print(f"\nAPI Error: {e}")
        print("\nNote: Cost tracking requires properly configured usage tables.")
        print("See documentation for setup instructions.")
    except Exception as e:
        print(f"\nUnexpected Error: {e}")


def print_budget_status_report(usage_admin: UsageAdmin, dimension: str):
    """Generate and print a budget vs actuals status report."""
    print_section_header(f"BUDGET STATUS BY {dimension.upper()}")

    try:
        budget_statuses = usage_admin.budget_status_by_dimension(dimension=dimension)

        if not budget_statuses:
            print(f"\nNo budget data found for dimension '{dimension}'.")
            print("\nNote: This requires:")
            print("  1. Budget tables to be configured")
            print("  2. Budget allocations to be defined")
            print("  3. Usage tracking to be active")
            return

        print(f"\nFound {len(budget_statuses)} budget entries:\n")

        # Separate by status
        within_budget = []
        warning = []
        breached = []

        for status in budget_statuses:
            if status.status == "within_budget":
                within_budget.append(status)
            elif status.status == "warning":
                warning.append(status)
            elif status.status == "breached":
                breached.append(status)

        # Print summary counts
        print(f"Budget Status Summary:")
        print(f"  Within Budget (<80%): {len(within_budget)}")
        print(f"  Warning (80-100%): {len(warning)}")
        print(f"  Breached (>100%): {len(breached)}")

        # Print breached budgets (highest priority)
        if breached:
            print("\n" + "=" * 80)
            print("BUDGET BREACHES - IMMEDIATE ACTION REQUIRED")
            print("=" * 80)
            print(f"\n{'Dimension Value':<35} {'Actual':>15} {'Budget':>15} {'Utilization':>15} {'Overage':>15}")
            print("-" * 100)

            for status in sorted(breached, key=lambda x: x.utilization_pct, reverse=True):
                dim_short = (status.dimension_value[:33] + "..") if len(status.dimension_value) > 35 else status.dimension_value
                overage = status.actual_cost - status.budget_amount
                util_str = f"{status.utilization_pct:.1f}%"

                print(f"{dim_short:<35} {format_cost(status.actual_cost):>15} {format_cost(status.budget_amount):>15} {util_str:>15} {format_cost(overage):>15}")

        # Print warnings
        if warning:
            print("\n" + "=" * 80)
            print("BUDGET WARNINGS - MONITOR CLOSELY")
            print("=" * 80)
            print(f"\n{'Dimension Value':<35} {'Actual':>15} {'Budget':>15} {'Utilization':>15} {'Remaining':>15}")
            print("-" * 100)

            for status in sorted(warning, key=lambda x: x.utilization_pct, reverse=True):
                dim_short = (status.dimension_value[:33] + "..") if len(status.dimension_value) > 35 else status.dimension_value
                remaining = status.budget_amount - status.actual_cost
                util_str = f"{status.utilization_pct:.1f}%"

                print(f"{dim_short:<35} {format_cost(status.actual_cost):>15} {format_cost(status.budget_amount):>15} {util_str:>15} {format_cost(remaining):>15}")

        # Print within budget (sample)
        if within_budget:
            print("\n" + "=" * 80)
            print(f"WITHIN BUDGET - TOP 10 BY UTILIZATION")
            print("=" * 80)
            print(f"\n{'Dimension Value':<35} {'Actual':>15} {'Budget':>15} {'Utilization':>15} {'Remaining':>15}")
            print("-" * 100)

            for status in sorted(within_budget, key=lambda x: x.utilization_pct, reverse=True)[:10]:
                dim_short = (status.dimension_value[:33] + "..") if len(status.dimension_value) > 35 else status.dimension_value
                remaining = status.budget_amount - status.actual_cost
                util_str = f"{status.utilization_pct:.1f}%"

                print(f"{dim_short:<35} {format_cost(status.actual_cost):>15} {format_cost(status.budget_amount):>15} {util_str:>15} {format_cost(remaining):>15}")

        # Overall statistics
        total_actual = sum(s.actual_cost for s in budget_statuses)
        total_budget = sum(s.budget_amount for s in budget_statuses)
        overall_util = (total_actual / total_budget * 100) if total_budget > 0 else 0

        print("\n" + "=" * 80)
        print("OVERALL BUDGET SUMMARY")
        print("=" * 80)
        print(f"Total Actual Cost: {format_cost(total_actual)}")
        print(f"Total Budget: {format_cost(total_budget)}")
        print(f"Overall Utilization: {overall_util:.1f}%")

        if overall_util > 100:
            print("\nCRITICAL: Overall budget has been exceeded!")
        elif overall_util > 90:
            print("\nWARNING: Overall budget utilization >90%")
        elif overall_util > 80:
            print("\nCAUTION: Overall budget utilization >80%")

    except ValidationError as e:
        print(f"\nValidation Error: {e}")
    except APIError as e:
        print(f"\nAPI Error: {e}")
        print("\nNote: Budget tracking requires properly configured budget tables.")
        print("See documentation for setup instructions.")
    except Exception as e:
        print(f"\nUnexpected Error: {e}")


def print_cost_optimization_recommendations(usage_admin: UsageAdmin, lookback: float):
    """Generate cost optimization recommendations."""
    print_section_header("COST OPTIMIZATION RECOMMENDATIONS")

    print("\nBased on usage patterns, consider these cost optimization strategies:\n")

    print("1. Compute Optimization:")
    print("   - Enable auto-termination on all interactive clusters (30-60 min idle)")
    print("   - Use job clusters instead of all-purpose clusters for scheduled workloads")
    print("   - Right-size clusters based on actual workload requirements")
    print("   - Consider Spot/Preemptible instances for non-critical batch jobs")

    print("\n2. SQL Warehouse Optimization:")
    print("   - Configure auto-suspend for SQL warehouses (10-15 min idle)")
    print("   - Use Serverless SQL warehouses where available")
    print("   - Scale warehouse size based on query concurrency needs")
    print("   - Review and optimize slow queries")

    print("\n3. Storage Optimization:")
    print("   - Implement data lifecycle policies")
    print("   - Use Delta Lake optimization (OPTIMIZE, Z-ORDER)")
    print("   - Archive or delete unused tables and files")
    print("   - Enable table statistics for query optimization")

    print("\n4. Monitoring & Governance:")
    print("   - Set up budget alerts for critical cost thresholds")
    print("   - Implement tagging strategy for cost allocation")
    print("   - Regular review of resource utilization")
    print("   - Establish cluster policies to enforce cost controls")

    print("\n5. Reserved Capacity:")
    print("   - Evaluate reserved capacity options for predictable workloads")
    print("   - Consider commitment discounts for stable usage patterns")

    try:
        # Try to get actual cost data for more specific recommendations
        clusters = usage_admin.top_cost_centers(
            dimension="cluster",
            lookback_hours=lookback,
            limit=10
        )

        if clusters and len(clusters) > 0:
            print("\n" + "-" * 80)
            print("SPECIFIC RECOMMENDATIONS BASED ON YOUR USAGE:")
            total_cost = sum(c.cost or 0 for c in clusters)
            if total_cost > 5000:
                print(f"  HIGH PRIORITY: Cluster costs are significant ({format_cost(total_cost)})")
                print("  ACTION: Review top 3 clusters for optimization opportunities")

    except:
        pass  # Ignore errors in recommendations


def main():
    """Main entry point for the cost tracking script."""
    parser = argparse.ArgumentParser(
        description="Cost tracking and budget monitoring for Databricks Admin AI Bridge",
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
        "--dimension",
        choices=["cluster", "job", "warehouse", "workspace", "project"],
        default="cluster",
        help="Cost dimension to analyze (default: cluster)",
    )
    parser.add_argument(
        "--lookback",
        type=float,
        default=168.0,  # 1 week
        help="Lookback period in hours (default: 168.0 = 1 week)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of results (default: 20)",
    )
    parser.add_argument(
        "--report",
        choices=["costs", "budget", "recommendations", "all"],
        default="all",
        help="Which report to generate (default: all)",
    )
    parser.add_argument(
        "--usage-table",
        default="billing.usage_events",
        help="Usage events table name (default: billing.usage_events)",
    )
    parser.add_argument(
        "--budget-table",
        default="billing.budgets",
        help="Budget table name (default: billing.budgets)",
    )

    args = parser.parse_args()

    # Configure logging
    setup_logging(args.verbose)

    # Create configuration
    cfg = AdminBridgeConfig(profile=args.profile)

    # Initialize admin client
    usage_admin = UsageAdmin(
        cfg,
        usage_table=args.usage_table,
        budget_table=args.budget_table
    )

    # Print header
    print("=" * 80)
    print(" DATABRICKS COST TRACKING REPORT")
    print("=" * 80)
    print(f"\nWorkspace: https://e2-demo-field-eng.cloud.databricks.com")
    print(f"Profile: {args.profile}")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nNote: Cost tracking requires properly configured usage and budget tables.")

    # Generate requested reports
    if args.report in ["costs", "all"]:
        print_top_cost_centers_report(usage_admin, args.dimension, args.lookback, args.limit)

    if args.report in ["budget", "all"]:
        print_budget_status_report(usage_admin, args.dimension)

    if args.report in ["recommendations", "all"]:
        print_cost_optimization_recommendations(usage_admin, args.lookback)

    print("\n" + "=" * 80)
    print(" REPORT COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
