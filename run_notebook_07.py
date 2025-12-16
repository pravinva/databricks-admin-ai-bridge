"""
Execute notebook 07 on serverless cluster and monitor output.
"""
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterSpec, RuntimeEngine
from databricks.sdk.service.jobs import Task, NotebookTask, Source

# Initialize workspace client
w = WorkspaceClient()

print("=" * 80)
print("EXECUTING NOTEBOOK 07: Agent Deployment")
print("=" * 80)

# Submit one-time run
print("\n📝 Submitting notebook run...")
run = w.jobs.submit(
    run_name="notebook_07_agent_deployment_test",
    tasks=[
        Task(
            task_key="agent_deployment",
            notebook_task=NotebookTask(
                notebook_path="/Users/pravin.varma@databricks.com/databricks-admin-ai-bridge/notebooks/07_agent_deployment",
                source=Source.WORKSPACE,
                base_parameters={"warehouse_id": "4b9b953939869799"}
            ),
            new_cluster=ClusterSpec(
                spark_version="14.3.x-scala2.12",
                node_type_id="i3.xlarge",
                num_workers=0,
                runtime_engine=RuntimeEngine.STANDARD
            )
        )
    ]
)

run_id = run.run_id
print(f"✅ Run submitted: {run_id}")
print(f"🔗 View run: {w.config.host}/#job/{run_id}/run/1")

# Monitor run status
print("\n⏳ Monitoring run status...\n")
start_time = time.time()
last_state = None

while True:
    run_status = w.jobs.get_run(run_id)
    current_state = run_status.state.life_cycle_state.value

    if current_state != last_state:
        elapsed = time.time() - start_time
        print(f"[{elapsed:.0f}s] State: {current_state}")
        last_state = current_state

    # Check if run is complete
    if run_status.state.life_cycle_state.value in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        result_state = run_status.state.result_state
        print(f"\n{'='*80}")
        print(f"Run completed: {result_state}")
        print(f"Total time: {time.time() - start_time:.1f}s")
        print(f"{'='*80}\n")

        if result_state and result_state.value == "SUCCESS":
            print("✅ Notebook executed successfully!")

            # Try to get output
            try:
                output = w.jobs.get_run_output(run_id)
                if output.notebook_output:
                    print("\n📄 Notebook Output:")
                    print("-" * 80)
                    print(output.notebook_output.result)
                    print("-" * 80)
            except Exception as e:
                print(f"⚠️  Could not retrieve output: {e}")
        else:
            print(f"❌ Run failed: {result_state}")

            # Try to get error details
            try:
                output = w.jobs.get_run_output(run_id)
                if output.error:
                    print("\n❌ Error:")
                    print("-" * 80)
                    print(output.error)
                    print("-" * 80)
                if output.error_trace:
                    print("\n📋 Error Trace:")
                    print("-" * 80)
                    print(output.error_trace)
                    print("-" * 80)
            except Exception as e:
                print(f"⚠️  Could not retrieve error details: {e}")

        break

    time.sleep(10)  # Poll every 10 seconds

print("\n🏁 Done!")
