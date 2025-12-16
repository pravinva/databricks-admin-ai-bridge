#!/usr/bin/env python3
"""
Pipeline Monitoring Example for Databricks Admin AI Bridge

This script demonstrates pipeline status and lag monitoring including:
- Lagging pipelines detection
- Failed pipelines identification
- Status reports
- DLT and Lakeflow monitoring

Target workspace: https://e2-demo-field-eng.cloud.databricks.com
"""

import argparse
import logging
from datetime import datetime

from admin_ai_bridge import AdminBridgeConfig, PipelinesAdmin
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


def format_lag(seconds: float | None) -> str:
    """Format lag in appropriate units."""
    if seconds is None:
        return "N/A"
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m"
    else:
        return f"{seconds / 3600:.2f}h"


def print_lagging_pipelines_report(pipelines_admin: PipelinesAdmin, max_lag: float, limit: int):
    """Generate and print a report of lagging pipelines."""
    print_section_header(f"LAGGING PIPELINES (lag > {max_lag}s)")

    try:
        pipelines = pipelines_admin.list_lagging_pipelines(
            max_lag_seconds=max_lag,
            limit=limit
        )

        if not pipelines:
            print(f"\nNo lagging pipelines found (lag threshold: {format_lag(max_lag)}).")
            print("This is a positive indicator - all pipelines are processing within acceptable lag limits.")
            return

        print(f"\nFound {len(pipelines)} lagging pipelines:\n")

        # Print table header
        print(f"{'Pipeline Name':<40} {'State':<12} {'Lag':>12} {'Last Update':<20} {'Pipeline ID':<25}")
        print("-" * 115)

        # Print each pipeline
        total_lag = 0.0
        for pipeline in pipelines:
            lag_str = format_lag(pipeline.lag_seconds)
            last_update_str = pipeline.last_update_time.strftime("%Y-%m-%d %H:%M") if pipeline.last_update_time else "N/A"
            pipeline_name_short = (pipeline.name[:38] + "..") if len(pipeline.name) > 40 else pipeline.name
            pipeline_id_short = (pipeline.pipeline_id[:23] + "..") if len(pipeline.pipeline_id) > 25 else pipeline.pipeline_id

            if pipeline.lag_seconds:
                total_lag += pipeline.lag_seconds

            print(f"{pipeline_name_short:<40} {pipeline.state:<12} {lag_str:>12} {last_update_str:<20} {pipeline_id_short:<25}")

            # Show error if present
            if pipeline.last_error:
                error_preview = pipeline.last_error[:80].replace('\n', ' ')
                print(f"  ERROR: {error_preview}...")

        # Calculate statistics
        avg_lag = total_lag / len(pipelines) if pipelines else 0

        print("\n" + "-" * 115)
        print(f"Total Pipelines: {len(pipelines)}")
        print(f"Average Lag: {format_lag(avg_lag)}")
        print(f"Maximum Lag: {format_lag(pipelines[0].lag_seconds if pipelines[0].lag_seconds else 0)}")

        # Lag severity alerts
        print("\nLag Severity Assessment:")
        critical = sum(1 for p in pipelines if p.lag_seconds and p.lag_seconds > 3600)
        high = sum(1 for p in pipelines if p.lag_seconds and 1800 < p.lag_seconds <= 3600)
        medium = sum(1 for p in pipelines if p.lag_seconds and 600 < p.lag_seconds <= 1800)

        if critical > 0:
            print(f"  CRITICAL: {critical} pipeline(s) with >1 hour lag")
        if high > 0:
            print(f"  HIGH: {high} pipeline(s) with 30min-1h lag")
        if medium > 0:
            print(f"  MEDIUM: {medium} pipeline(s) with 10-30min lag")

        # Recommendations
        print("\nRecommendations:")
        if critical > 0:
            print("  1. URGENT: Investigate critical lag pipelines immediately")
            print("  2. Check for resource constraints or data quality issues")
        if avg_lag > 1800:
            print("  3. Consider scaling up compute resources")
            print("  4. Review pipeline configuration and optimization")
        print("  5. Enable pipeline monitoring alerts for proactive detection")

    except ValidationError as e:
        print(f"\nValidation Error: {e}")
    except APIError as e:
        print(f"\nAPI Error: {e}")
    except Exception as e:
        print(f"\nUnexpected Error: {e}")


def print_failed_pipelines_report(pipelines_admin: PipelinesAdmin, lookback: float, limit: int):
    """Generate and print a report of failed pipelines."""
    print_section_header(f"FAILED PIPELINES (last {lookback} hours)")

    try:
        pipelines = pipelines_admin.list_failed_pipelines(
            lookback_hours=lookback,
            limit=limit
        )

        if not pipelines:
            print(f"\nNo failed pipelines found in the last {lookback} hours.")
            print("This indicates healthy pipeline execution.")
            return

        print(f"\nFound {len(pipelines)} failed pipelines:\n")

        # Print table header
        print(f"{'Pipeline Name':<40} {'State':<12} {'Last Update':<20} {'Pipeline ID':<25}")
        print("-" * 105)

        # Print each pipeline
        for pipeline in pipelines:
            last_update_str = pipeline.last_update_time.strftime("%Y-%m-%d %H:%M") if pipeline.last_update_time else "N/A"
            pipeline_name_short = (pipeline.name[:38] + "..") if len(pipeline.name) > 40 else pipeline.name
            pipeline_id_short = (pipeline.pipeline_id[:23] + "..") if len(pipeline.pipeline_id) > 25 else pipeline.pipeline_id

            print(f"{pipeline_name_short:<40} {pipeline.state:<12} {last_update_str:<20} {pipeline_id_short:<25}")

            # Show error if present
            if pipeline.last_error:
                error_preview = pipeline.last_error[:100].replace('\n', ' ')
                print(f"  ERROR: {error_preview}...")
                print()

        print("-" * 105)
        print(f"Total Failed Pipelines: {len(pipelines)}")

        # Error analysis
        pipelines_with_errors = sum(1 for p in pipelines if p.last_error)
        print(f"Pipelines with Error Messages: {pipelines_with_errors}")

        # Recommendations
        print("\nTroubleshooting Steps:")
        print("  1. Review error messages for each failed pipeline")
        print("  2. Check pipeline event logs for detailed failure information")
        print("  3. Verify source data quality and availability")
        print("  4. Ensure sufficient cluster resources")
        print("  5. Review recent schema or configuration changes")
        print("  6. Consider implementing retry logic and error handling")

    except ValidationError as e:
        print(f"\nValidation Error: {e}")
    except APIError as e:
        print(f"\nAPI Error: {e}")
    except Exception as e:
        print(f"\nUnexpected Error: {e}")


def print_pipeline_health_summary(pipelines_admin: PipelinesAdmin):
    """Generate and print a pipeline health summary."""
    print_section_header("PIPELINE HEALTH SUMMARY")

    try:
        # Get various pipeline metrics
        lagging = pipelines_admin.list_lagging_pipelines(
            max_lag_seconds=600.0,  # 10 minutes
            limit=100
        )

        failed = pipelines_admin.list_failed_pipelines(
            lookback_hours=24.0,
            limit=100
        )

        print("\nOverall Pipeline Health Metrics:\n")

        # Lag analysis
        print("Lag Analysis (>10 minutes):")
        print(f"  Lagging Pipelines: {len(lagging)}")
        if lagging:
            avg_lag = sum(p.lag_seconds or 0 for p in lagging) / len(lagging)
            max_lag = max(p.lag_seconds or 0 for p in lagging)
            print(f"  Average Lag: {format_lag(avg_lag)}")
            print(f"  Maximum Lag: {format_lag(max_lag)}")

            # Categorize by severity
            critical = sum(1 for p in lagging if p.lag_seconds and p.lag_seconds > 3600)
            high = sum(1 for p in lagging if p.lag_seconds and 1800 < p.lag_seconds <= 3600)
            medium = sum(1 for p in lagging if p.lag_seconds and 600 < p.lag_seconds <= 1800)

            print(f"\n  By Severity:")
            print(f"    Critical (>1h): {critical}")
            print(f"    High (30m-1h): {high}")
            print(f"    Medium (10-30m): {medium}")

        # Failure analysis
        print("\nFailure Analysis (last 24 hours):")
        print(f"  Failed Pipelines: {len(failed)}")
        if failed:
            with_errors = sum(1 for p in failed if p.last_error)
            print(f"  Pipelines with Error Messages: {with_errors}")

        # Health score calculation
        print("\n" + "-" * 80)
        print("HEALTH SCORE:")

        total_issues = len(lagging) + len(failed)
        critical_issues = sum(1 for p in lagging if p.lag_seconds and p.lag_seconds > 3600)

        if total_issues == 0:
            health_score = "EXCELLENT"
            score_desc = "All pipelines are healthy"
        elif critical_issues > 0:
            health_score = "CRITICAL"
            score_desc = "Immediate attention required"
        elif len(failed) > 5:
            health_score = "POOR"
            score_desc = "Multiple pipeline failures"
        elif len(lagging) > 10:
            health_score = "FAIR"
            score_desc = "Significant lag detected"
        elif total_issues < 5:
            health_score = "GOOD"
            score_desc = "Minor issues detected"
        else:
            health_score = "FAIR"
            score_desc = "Some issues need attention"

        print(f"  Status: {health_score}")
        print(f"  Assessment: {score_desc}")

        # Key recommendations
        print("\n" + "-" * 80)
        print("KEY RECOMMENDATIONS:")

        if health_score == "EXCELLENT":
            print("  - Continue monitoring for proactive issue detection")
            print("  - Review and optimize pipeline configurations periodically")
        elif health_score in ["CRITICAL", "POOR"]:
            print("  - PRIORITY 1: Address failed and critically lagging pipelines")
            print("  - PRIORITY 2: Scale compute resources if needed")
            print("  - PRIORITY 3: Review and optimize data processing logic")
        else:
            print("  - Review lagging pipelines for optimization opportunities")
            print("  - Implement monitoring alerts for early detection")
            print("  - Consider resource scaling for consistently lagging pipelines")

        print("\nBest Practices:")
        print("  1. Set up alerts for lag >30 minutes and any failures")
        print("  2. Implement pipeline monitoring dashboards")
        print("  3. Regular review of pipeline performance metrics")
        print("  4. Document and track pipeline SLAs")
        print("  5. Automate scaling and retry logic where appropriate")

    except Exception as e:
        print(f"\nError generating health summary: {e}")


def main():
    """Main entry point for the pipeline monitoring script."""
    parser = argparse.ArgumentParser(
        description="Pipeline monitoring for Databricks Admin AI Bridge",
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
        "--max-lag",
        type=float,
        default=600.0,
        help="Maximum acceptable lag in seconds (default: 600.0 = 10 minutes)",
    )
    parser.add_argument(
        "--lookback",
        type=float,
        default=24.0,
        help="Lookback period in hours for failed pipelines (default: 24.0)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum number of results per query (default: 50)",
    )
    parser.add_argument(
        "--report",
        choices=["lagging", "failed", "health", "all"],
        default="all",
        help="Which report to generate (default: all)",
    )

    args = parser.parse_args()

    # Configure logging
    setup_logging(args.verbose)

    # Create configuration
    cfg = AdminBridgeConfig(profile=args.profile)

    # Initialize admin client
    pipelines_admin = PipelinesAdmin(cfg)

    # Print header
    print("=" * 80)
    print(" DATABRICKS PIPELINE MONITORING REPORT")
    print("=" * 80)
    print(f"\nWorkspace: https://e2-demo-field-eng.cloud.databricks.com")
    print(f"Profile: {args.profile}")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Generate requested reports
    if args.report in ["lagging", "all"]:
        print_lagging_pipelines_report(pipelines_admin, args.max_lag, args.limit)

    if args.report in ["failed", "all"]:
        print_failed_pipelines_report(pipelines_admin, args.lookback, args.limit)

    if args.report in ["health", "all"]:
        print_pipeline_health_summary(pipelines_admin)

    print("\n" + "=" * 80)
    print(" REPORT COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
