#!/usr/bin/env python3
"""
Jobs Monitoring Example for Databricks Admin AI Bridge

This script demonstrates comprehensive job monitoring and analysis including:
- Long-running job detection
- Failed job identification
- Job run trend analysis
- Formatted reporting

Target workspace: https://e2-demo-field-eng.cloud.databricks.com
"""

import argparse
import logging
from datetime import datetime
from collections import defaultdict

from admin_ai_bridge import AdminBridgeConfig, JobsAdmin
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


def print_long_running_jobs_report(jobs_admin: JobsAdmin, min_hours: float, lookback: float, limit: int):
    """Generate and print a report of long-running jobs."""
    print_section_header(f"LONG-RUNNING JOBS (> {min_hours} hours)")

    try:
        jobs = jobs_admin.list_long_running_jobs(
            min_duration_hours=min_hours,
            lookback_hours=lookback,
            limit=limit
        )

        if not jobs:
            print("\nNo long-running jobs found.")
            return

        print(f"\nFound {len(jobs)} long-running jobs in the last {lookback} hours:\n")

        # Print table header
        print(f"{'Job Name':<40} {'Run ID':<12} {'State':<15} {'Duration':>12} {'Start Time':<20}")
        print("-" * 105)

        # Print each job
        for job in jobs:
            duration_hours = job.duration_seconds / 3600 if job.duration_seconds else 0
            duration_str = f"{duration_hours:.2f}h"
            start_time_str = job.start_time.strftime("%Y-%m-%d %H:%M") if job.start_time else "N/A"
            job_name_short = job.job_name[:38] + ".." if len(job.job_name) > 40 else job.job_name

            print(f"{job_name_short:<40} {job.run_id:<12} {job.state:<15} {duration_str:>12} {start_time_str:<20}")

        # Calculate statistics
        total_duration = sum(j.duration_seconds or 0 for j in jobs)
        avg_duration = total_duration / len(jobs) if jobs else 0

        print("\n" + "-" * 105)
        print(f"Total Jobs: {len(jobs)}")
        print(f"Average Duration: {avg_duration / 3600:.2f} hours")
        print(f"Total Duration: {total_duration / 3600:.2f} hours")

    except ValidationError as e:
        print(f"\nValidation Error: {e}")
    except APIError as e:
        print(f"\nAPI Error: {e}")
    except Exception as e:
        print(f"\nUnexpected Error: {e}")


def print_failed_jobs_report(jobs_admin: JobsAdmin, lookback: float, limit: int):
    """Generate and print a report of failed jobs."""
    print_section_header(f"FAILED JOBS (last {lookback} hours)")

    try:
        jobs = jobs_admin.list_failed_jobs(
            lookback_hours=lookback,
            limit=limit
        )

        if not jobs:
            print("\nNo failed jobs found.")
            return

        print(f"\nFound {len(jobs)} failed jobs:\n")

        # Print table header
        print(f"{'Job Name':<40} {'Run ID':<12} {'State':<15} {'Duration':>12} {'Start Time':<20}")
        print("-" * 105)

        # Print each job
        for job in jobs:
            duration_str = "N/A"
            if job.duration_seconds:
                if job.duration_seconds < 60:
                    duration_str = f"{job.duration_seconds:.1f}s"
                elif job.duration_seconds < 3600:
                    duration_str = f"{job.duration_seconds / 60:.1f}m"
                else:
                    duration_str = f"{job.duration_seconds / 3600:.2f}h"

            start_time_str = job.start_time.strftime("%Y-%m-%d %H:%M") if job.start_time else "N/A"
            job_name_short = job.job_name[:38] + ".." if len(job.job_name) > 40 else job.job_name

            print(f"{job_name_short:<40} {job.run_id:<12} {job.state:<15} {duration_str:>12} {start_time_str:<20}")

        # Group by failure state
        state_counts = defaultdict(int)
        for job in jobs:
            state_counts[job.state] += 1

        print("\n" + "-" * 105)
        print(f"Total Failed Jobs: {len(jobs)}")
        print("\nFailure Breakdown:")
        for state, count in sorted(state_counts.items()):
            print(f"  {state}: {count}")

    except ValidationError as e:
        print(f"\nValidation Error: {e}")
    except APIError as e:
        print(f"\nAPI Error: {e}")
    except Exception as e:
        print(f"\nUnexpected Error: {e}")


def print_job_trends_report(jobs_admin: JobsAdmin, lookback: float):
    """Generate and print a job trends analysis report."""
    print_section_header(f"JOB TRENDS ANALYSIS (last {lookback} hours)")

    try:
        # Get both long-running and failed jobs for trend analysis
        long_jobs = jobs_admin.list_long_running_jobs(
            min_duration_hours=1.0,  # Lower threshold for trend analysis
            lookback_hours=lookback,
            limit=100
        )

        failed_jobs = jobs_admin.list_failed_jobs(
            lookback_hours=lookback,
            limit=100
        )

        # Analyze by job name
        job_stats = defaultdict(lambda: {"runs": 0, "failures": 0, "total_duration": 0.0})

        for job in long_jobs:
            job_stats[job.job_name]["runs"] += 1
            job_stats[job.job_name]["total_duration"] += job.duration_seconds or 0

        for job in failed_jobs:
            job_stats[job.job_name]["failures"] += 1

        if not job_stats:
            print("\nInsufficient data for trend analysis.")
            return

        print(f"\nAnalyzed {len(job_stats)} unique jobs:\n")

        # Sort by number of failures, then by runs
        sorted_jobs = sorted(
            job_stats.items(),
            key=lambda x: (x[1]["failures"], x[1]["runs"]),
            reverse=True
        )[:20]  # Top 20

        print(f"{'Job Name':<50} {'Runs':>8} {'Failures':>10} {'Avg Duration':>15} {'Failure Rate':>15}")
        print("-" * 105)

        for job_name, stats in sorted_jobs:
            runs = stats["runs"]
            failures = stats["failures"]
            avg_duration = (stats["total_duration"] / runs / 3600) if runs > 0 else 0
            failure_rate = (failures / (runs + failures) * 100) if (runs + failures) > 0 else 0

            job_name_short = job_name[:48] + ".." if len(job_name) > 50 else job_name

            print(f"{job_name_short:<50} {runs:>8} {failures:>10} {avg_duration:>13.2f}h {failure_rate:>13.1f}%")

        print("\n" + "-" * 105)
        total_runs = sum(s["runs"] for s in job_stats.values())
        total_failures = sum(s["failures"] for s in job_stats.values())
        overall_failure_rate = (total_failures / (total_runs + total_failures) * 100) if (total_runs + total_failures) > 0 else 0

        print(f"Overall Statistics:")
        print(f"  Total Job Runs: {total_runs}")
        print(f"  Total Failures: {total_failures}")
        print(f"  Overall Failure Rate: {overall_failure_rate:.2f}%")

    except Exception as e:
        print(f"\nError generating trends report: {e}")


def main():
    """Main entry point for the jobs monitoring script."""
    parser = argparse.ArgumentParser(
        description="Jobs monitoring and analysis for Databricks Admin AI Bridge",
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
        default=4.0,
        help="Minimum duration in hours for long-running jobs (default: 4.0)",
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
        choices=["long-running", "failed", "trends", "all"],
        default="all",
        help="Which report to generate (default: all)",
    )

    args = parser.parse_args()

    # Configure logging
    setup_logging(args.verbose)

    # Create configuration
    cfg = AdminBridgeConfig(profile=args.profile)

    # Initialize admin client
    jobs_admin = JobsAdmin(cfg)

    # Print header
    print("=" * 80)
    print(" DATABRICKS JOBS MONITORING REPORT")
    print("=" * 80)
    print(f"\nWorkspace: https://e2-demo-field-eng.cloud.databricks.com")
    print(f"Profile: {args.profile}")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Generate requested reports
    if args.report in ["long-running", "all"]:
        print_long_running_jobs_report(jobs_admin, args.min_duration, args.lookback, args.limit)

    if args.report in ["failed", "all"]:
        print_failed_jobs_report(jobs_admin, args.lookback, args.limit)

    if args.report in ["trends", "all"]:
        print_job_trends_report(jobs_admin, args.lookback)

    print("\n" + "=" * 80)
    print(" REPORT COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
