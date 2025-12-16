"""
Get detailed run output from notebook 07 execution.
"""
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

run_id = 831516396258047

print("=" * 80)
print("FETCHING RUN DETAILS")
print("=" * 80)

# Get run details
run = w.jobs.get_run(run_id)

print(f"\nRun ID: {run_id}")
print(f"Run URL: {w.config.host}/#job/{run_id}/run/1")
print(f"State: {run.state.result_state}")

# Get task runs
if run.tasks:
    for task in run.tasks:
        print(f"\n{'='*80}")
        print(f"Task: {task.task_key}")
        print(f"{'='*80}")
        print(f"State: {task.state.result_state if task.state else 'N/A'}")

        # Get task run output
        if task.run_id:
            try:
                task_output = w.jobs.get_run_output(task.run_id)

                if task_output.error:
                    print(f"\n❌ Error:")
                    print("-" * 80)
                    print(task_output.error)
                    print("-" * 80)

                if task_output.error_trace:
                    print(f"\n📋 Error Trace:")
                    print("-" * 80)
                    print(task_output.error_trace)
                    print("-" * 80)

                if task_output.logs:
                    print(f"\n📄 Logs:")
                    print("-" * 80)
                    print(task_output.logs)
                    print("-" * 80)

                if task_output.notebook_output:
                    print(f"\n📓 Notebook Output:")
                    print("-" * 80)
                    if task_output.notebook_output.result:
                        print(task_output.notebook_output.result)
                    if task_output.notebook_output.truncated:
                        print("\n⚠️  Output was truncated")
                    print("-" * 80)
            except Exception as e:
                print(f"⚠️  Error getting task output: {e}")
