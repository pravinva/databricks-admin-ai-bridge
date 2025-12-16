"""
Test databricks.agents.deploy locally to understand the API.
"""
from databricks.sdk import WorkspaceClient
from databricks import agents
import mlflow
from admin_ai_bridge import AdminBridgeConfig
from admin_ai_bridge import (
    jobs_admin_tools,
    dbsql_admin_tools,
    clusters_admin_tools,
)

print("=" * 80)
print("TESTING DATABRICKS.AGENTS.DEPLOY")
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

# Initialize config and get tools
cfg = AdminBridgeConfig()
warehouse_id = "4b9b953939869799"

print("\n📦 Loading tools...")
all_tools = (
    jobs_admin_tools(cfg, warehouse_id=warehouse_id)[:2]  # First 2 jobs tools
    + dbsql_admin_tools(cfg, warehouse_id=warehouse_id)[:2]  # First 2 dbsql tools
    + clusters_admin_tools(cfg, warehouse_id=warehouse_id)[:2]  # First 2 clusters tools
)

print(f"✓ Loaded {len(all_tools)} tools:")
for i, tool in enumerate(all_tools, 1):
    print(f"  {i}. {tool.__name__}")

# Set MLflow experiment
experiment_name = f"/Users/{current_user.user_name}/admin_observability_agent_test"
mlflow.set_experiment(experiment_name)
print(f"\n📊 MLflow experiment: {experiment_name}")

# Create a simple Python function-based model
print("\n🔨 Creating function-based model...")

def admin_agent(messages):
    """
    Simple admin agent that answers questions about Databricks workspace health.

    This is a minimal implementation to test databricks.agents.deploy().
    """
    import json

    # Get the last user message
    if not messages or len(messages) == 0:
        return {"role": "assistant", "content": "No messages provided"}

    last_message = messages[-1]
    user_query = last_message.get("content", "")

    # Simple response
    response = f"I am the Admin AI Agent. You asked: '{user_query}'. "
    response += f"I have access to {len(all_tools)} tools to help monitor your Databricks workspace."

    return {"role": "assistant", "content": response}

# Log the model with MLflow
print("📝 Logging model to MLflow...")

# Use Unity Catalog model name (3-level namespace)
uc_model_name = "main.default.admin_observability_agent_test"

# Define model signature for Unity Catalog
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec

input_schema = Schema([
    ColSpec("string", "messages")
])
output_schema = Schema([
    ColSpec("string", "content")
])
signature = ModelSignature(inputs=input_schema, outputs=output_schema)

# Create input example
input_example = {
    "messages": '[{"role": "user", "content": "Show me failed jobs"}]'
}

with mlflow.start_run(run_name="admin_agent_test"):
    # Log as Python function model with signature
    model_info = mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=mlflow.pyfunc.PythonModel(),
        code_paths=["."],  # Include current directory
        pip_requirements=[
            "databricks-sdk>=0.23.0",
            "mlflow",
        ],
        signature=signature,
        input_example=input_example,
        registered_model_name=uc_model_name
    )

    print(f"✓ Model logged: {model_info.model_uri}")
    print(f"✓ Model version: {model_info.registered_model_version}")

# Now try to deploy using databricks.agents
print("\n🚀 Deploying with databricks.agents.deploy()...")

try:
    deployment = agents.deploy(
        model_name=uc_model_name,
        model_version=model_info.registered_model_version,
        endpoint_name="admin-agent-test"
    )

    print(f"✅ Deployment successful!")
    print(f"   Endpoint: {deployment}")

except Exception as e:
    print(f"❌ Deployment failed: {e}")
    print(f"   Error type: {type(e).__name__}")
    import traceback
    traceback.print_exc()

print("\n🏁 Test complete!")
