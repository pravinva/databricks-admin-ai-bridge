"""
Test databricks.agents.deploy with simplified approach (no LangChain).
"""
import mlflow
from databricks.sdk import WorkspaceClient
from databricks import agents

from admin_ai_bridge import AdminBridgeConfig
from admin_ai_bridge import jobs_admin_tools

print("=" * 80)
print("TESTING AGENT DEPLOYMENT (SIMPLIFIED - NO LANGCHAIN)")
print("=" * 80)

# Initialize workspace client
w = WorkspaceClient()
print(f"\n✓ Connected to: {w.config.host}")

# Configure MLflow to use Databricks
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
print(f"✓ MLflow tracking URI: databricks")
print(f"✓ MLflow registry URI: databricks-uc")

# Get current user
current_user = w.current_user.me()
print(f"✓ Authenticated as: {current_user.user_name}")

# Load tools
print("\n📦 Loading tools...")
cfg = AdminBridgeConfig()
warehouse_id = "4b9b953939869799"
all_tools = jobs_admin_tools(cfg, warehouse_id=warehouse_id)[:2]  # Just 2 tools for testing

print(f"✓ Loaded {len(all_tools)} tools:")
for i, tool in enumerate(all_tools, 1):
    print(f"  {i}. {tool.__name__}")

# Set MLflow experiment
experiment_name = f"/Users/{current_user.user_name}/admin_observability_agent_test"
mlflow.set_experiment(experiment_name)
print(f"\n📊 MLflow experiment: {experiment_name}")

# Log agent with MLflow using databricks.agents.log_model
print("\n📝 Logging agent to MLflow...")
uc_model_name = "main.default.admin_observability_agent_test"

with mlflow.start_run(run_name="simplified_agent_test"):
    # Use databricks.agents.log_model() directly - no LangChain wrapping
    model_info = agents.log_model(
        model=f"{w.config.host}/serving-endpoints/databricks-meta-llama-3-1-70b-instruct",
        task="chat",
        artifacts={},
        tools=all_tools,  # Pass Python functions directly
        registered_model_name=uc_model_name,
        example={"messages": [{"role": "user", "content": "Show me failed jobs in the last 24 hours"}]}
    )

print(f"✓ Model logged: {model_info.model_uri}")
print(f"✓ Model version: {model_info.registered_model_version}")

# Deploy using databricks.agents
print("\n🚀 Deploying with databricks.agents.deploy()...")

try:
    deployment = agents.deploy(
        model_name=uc_model_name,
        model_version=model_info.registered_model_version,
        endpoint_name="admin-agent-test"
    )

    print(f"\n✅ Deployment successful!")
    print(f"   Endpoint: {deployment.endpoint_name}")
    print(f"   URL: {w.config.host}/ml/endpoints/{deployment.endpoint_name}")

except Exception as e:
    print(f"\n❌ Deployment failed: {e}")
    print(f"   Error type: {type(e).__name__}")
    import traceback
    traceback.print_exc()

print("\n🏁 Test complete!")
