# Databricks notebook source
# MAGIC %md
# MAGIC # Admin Observability Agent Deployment
# MAGIC
# MAGIC This notebook demonstrates the **full deployment** of an Admin Observability Agent using the Databricks Agent Framework.
# MAGIC
# MAGIC **Target Workspace:** `https://e2-demo-field-eng.cloud.databricks.com`
# MAGIC
# MAGIC **What You'll Learn:**
# MAGIC - Aggregate all admin tools into a single agent
# MAGIC - Deploy the agent as a Databricks endpoint
# MAGIC - Test the agent with natural language queries
# MAGIC - Query about jobs, queries, clusters, security, usage/budget, audit, and pipelines
# MAGIC
# MAGIC **Example Queries:**
# MAGIC - "Which jobs have been running longer than 4 hours?"
# MAGIC - "Show me the top 10 slowest queries"
# MAGIC - "Which teams are over 80% of their monthly budget?"
# MAGIC - "List failed login attempts in the last 24 hours"
# MAGIC - "Which pipelines are lagging by more than 10 minutes?"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Dependencies

# COMMAND ----------

%pip install --upgrade databricks-sdk>=0.23.0 pydantic>=2.0.0 "databricks-agents>=0.3.0" mlflow
%pip install git+https://github.com/pravinva/databricks-admin-ai-bridge.git

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import and Configure

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks import agents
from mlflow.models import ModelConfig

from admin_ai_bridge.config import AdminBridgeConfig
from admin_ai_bridge.tools_databricks_agent import (
    jobs_admin_tools,
    dbsql_admin_tools,
    clusters_admin_tools,
    security_admin_tools,
    usage_admin_tools,
    audit_admin_tools,
    pipelines_admin_tools,
)

print("✓ All modules imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configure Admin Bridge
# MAGIC
# MAGIC Set up the configuration to use workspace credentials.

# COMMAND ----------

# Initialize configuration
# When running in a notebook, this uses the notebook execution context
cfg = AdminBridgeConfig()

# Verify configuration
ws = WorkspaceClient()
current_user = ws.current_user.me()

print(f"✓ Configured for workspace: {ws.config.host}")
print(f"✓ Running as user: {current_user.user_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Aggregate All Tools
# MAGIC
# MAGIC Combine tools from all admin domains into a single tool collection.
# MAGIC
# MAGIC **Performance Optimization:** For fast queries, provide a `warehouse_id` to use system tables
# MAGIC instead of slower API calls (10-100x faster for large workspaces).

# COMMAND ----------

# Databricks notebook widget for warehouse_id
dbutils.widgets.text("warehouse_id", "4b9b953939869799", "SQL Warehouse ID")
warehouse_id = dbutils.widgets.get("warehouse_id")

print(f"✓ Using warehouse: {warehouse_id}")

# Collect all tools with warehouse_id for fast system table queries
all_tools = (
    jobs_admin_tools(cfg, warehouse_id=warehouse_id)
    + dbsql_admin_tools(cfg, warehouse_id=warehouse_id)
    + clusters_admin_tools(cfg, warehouse_id=warehouse_id)
    + security_admin_tools(cfg)
    + usage_admin_tools(cfg, warehouse_id=warehouse_id)
    + audit_admin_tools(cfg)
    + pipelines_admin_tools(cfg)
)

print(f"✓ Aggregated {len(all_tools)} tools from all domains\n")

# Display tool inventory
print("Tool Inventory by Domain:")
print("-" * 60)
print(f"  Jobs Admin: {len(jobs_admin_tools(cfg, warehouse_id))} tools (system tables enabled)")
print(f"  DBSQL Admin: {len(dbsql_admin_tools(cfg, warehouse_id))} tools (system tables enabled)")
print(f"  Clusters Admin: {len(clusters_admin_tools(cfg, warehouse_id))} tools (system tables enabled)")
print(f"  Security Admin: {len(security_admin_tools(cfg))} tools")
print(f"  Usage Admin: {len(usage_admin_tools(cfg, warehouse_id))} tools (system tables enabled)")
print(f"  Audit Admin: {len(audit_admin_tools(cfg))} tools")
print(f"  Pipelines Admin: {len(pipelines_admin_tools(cfg))} tools")
print("-" * 60)

# List all tool names
print("\nAvailable Tools:")
for i, tool in enumerate(all_tools, 1):
    print(f"  {i}. {tool.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Define Agent Specification
# MAGIC
# MAGIC Create the agent with a comprehensive system prompt and all tools.

# COMMAND ----------

# Define system prompt
system_prompt = """
You are an intelligent Databricks Admin Assistant powered by the Admin AI Bridge library.

Your capabilities span across multiple domains:

1. **Jobs & Workflows**: Monitor job runs, identify long-running or failed jobs, analyze performance
2. **DBSQL Queries**: Track query performance, identify slow queries, analyze user query patterns
3. **Clusters & Warehouses**: Monitor cluster utilization, find idle clusters, optimize costs
4. **Security & Permissions**: Query access control, audit who can manage resources
5. **Usage & Cost**: Track costs, implement chargeback by dimension, monitor budgets
6. **Audit Logs**: Monitor failed logins, track admin changes, ensure compliance
7. **Pipelines**: Monitor pipeline health, identify lagging or failed pipelines

**Important Guidelines:**
- Always use the tools provided to answer questions
- Be specific and data-driven in your responses
- When asked about costs or budgets, use the cost_by_dimension and budget_status tools
- Format large numbers with commas for readability
- Highlight critical issues (failures, budget breaches, security events)
- Provide actionable recommendations when appropriate
- NEVER perform destructive operations - all tools are read-only

**Example Queries You Can Answer:**
- "Which jobs have been running longer than 4 hours?"
- "Show me the top 10 slowest queries in the last 24 hours"
- "Which clusters are idle for more than 2 hours?"
- "Who can manage job 12345?"
- "What are the top cost centers in the last 7 days?"
- "Which teams are over 80% of their monthly budget?"
- "Show me failed login attempts in the last 24 hours"
- "Which pipelines are lagging by more than 10 minutes?"

Always provide clear, concise, and actionable information to help admins maintain a healthy Databricks environment.
"""

# Create agent specification
agent_spec = agents.AgentSpec(
    name="admin_observability_agent",
    system_prompt=system_prompt,
    llm_endpoint="databricks-meta-llama-3-1-70b-instruct",  # or "databricks-claude-3-5-sonnet"
    tools=all_tools,
)

print("✓ Agent specification created successfully")
print(f"  Name: {agent_spec.name}")
print(f"  LLM Endpoint: {agent_spec.llm_endpoint}")
print(f"  Total Tools: {len(agent_spec.tools)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Deploy Agent Endpoint
# MAGIC
# MAGIC Deploy the agent as a Databricks serving endpoint.

# COMMAND ----------

import mlflow

# Set MLflow experiment
mlflow.set_experiment("/Users/{}/admin_observability_agent".format(current_user.user_name))

print("Deploying admin-observability-agent endpoint...")
print("This may take a few minutes...\n")

try:
    # Deploy the agent
    deployed_agent = agents.deploy(
        model=agent_spec,
        name="admin-observability-agent",
        # Configure endpoint settings
        workload_size="Small",
        workload_type="CPU",
    )

    print("✓ Agent deployed successfully!")
    print(f"  Endpoint name: {deployed_agent.endpoint_name}")
    print(f"  Model name: {deployed_agent.model_name}")
    print(f"  Model version: {deployed_agent.model_version}")

except Exception as e:
    print(f"⚠ Deployment encountered an issue: {e}")
    print("\nNote: If the endpoint already exists, it may need to be updated manually.")
    print("You can also use the endpoint from a previous deployment.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Test the Agent
# MAGIC
# MAGIC Test the deployed agent with various natural language queries.

# COMMAND ----------

# Function to test the agent
def test_agent_query(question: str, endpoint_name: str = "admin-observability-agent"):
    """Test the agent with a natural language query."""
    print("=" * 70)
    print(f"QUESTION: {question}")
    print("=" * 70)

    try:
        # Query the agent endpoint
        response = ws.serving_endpoints.query(
            name=endpoint_name,
            inputs=[{"query": question}]
        )

        print("\nAGENT RESPONSE:")
        print("-" * 70)
        if hasattr(response, 'predictions') and response.predictions:
            print(response.predictions[0])
        else:
            print(response)
        print("-" * 70)

    except Exception as e:
        print(f"\n⚠ Error querying agent: {e}")
        print("Make sure the agent endpoint is deployed and ready.")

    print("\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Test Jobs Queries

# COMMAND ----------

# Test job-related queries
test_agent_query("Which jobs have been running longer than 4 hours in the last 24 hours?")

# COMMAND ----------

test_agent_query("Show me all failed jobs in the last 24 hours")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Test DBSQL Queries

# COMMAND ----------

test_agent_query("What are the top 10 slowest queries in the last 24 hours?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Test Cluster Queries

# COMMAND ----------

test_agent_query("Which clusters have been idle for more than 2 hours?")

# COMMAND ----------

test_agent_query("Show me clusters running longer than 8 hours")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.4 Test Security Queries

# COMMAND ----------

# Note: Replace with actual job/cluster IDs from your workspace
test_agent_query("Show me who can manage jobs in the workspace")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.5 Test Cost and Budget Queries (NEW!)

# COMMAND ----------

test_agent_query("What are the top cost centers in the last 7 days?")

# COMMAND ----------

test_agent_query("Show me cost by workspace for the last 30 days")

# COMMAND ----------

test_agent_query("Which teams are over 80% of their monthly budget?")

# COMMAND ----------

test_agent_query("Calculate chargeback by project for the last month")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.6 Test Audit Queries

# COMMAND ----------

test_agent_query("Show me failed login attempts in the last 24 hours")

# COMMAND ----------

test_agent_query("What admin changes were made in the last 24 hours?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.7 Test Pipeline Queries

# COMMAND ----------

test_agent_query("Which pipelines are lagging by more than 10 minutes?")

# COMMAND ----------

test_agent_query("List all failed pipelines in the last 24 hours")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.8 Test Complex Multi-Domain Queries

# COMMAND ----------

test_agent_query(
    "Give me a comprehensive health report covering jobs, queries, clusters, "
    "security events, and pipeline status for the last 24 hours"
)

# COMMAND ----------

test_agent_query(
    "What are the top 3 areas where we can optimize costs? "
    "Consider idle clusters, long-running jobs, and resource utilization"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Agent Usage Examples
# MAGIC
# MAGIC The deployed agent can be used from multiple interfaces.

# COMMAND ----------

print("=" * 70)
print("AGENT DEPLOYMENT SUMMARY")
print("=" * 70)
print(f"\nEndpoint Name: admin-observability-agent")
print(f"Workspace: {ws.config.host}")
print(f"Total Tools: {len(all_tools)}")
print("\n" + "=" * 70)

print("\nUSAGE OPTIONS:")
print("-" * 70)

print("\n1. Direct API Call (Python):")
print("""
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
response = ws.serving_endpoints.query(
    name="admin-observability-agent",
    inputs=[{"query": "Which jobs are running longer than 4 hours?"}]
)
print(response.predictions[0])
""")

print("\n2. Via Databricks Chat Interface:")
print("   - Navigate to the AI Gateway in Databricks UI")
print("   - Select 'admin-observability-agent' endpoint")
print("   - Ask questions in natural language")

print("\n3. Via Slack/Teams Integration:")
print("   - Configure Databricks bot in Slack/Teams")
print("   - Point to 'admin-observability-agent' endpoint")
print("   - Ask questions directly in chat channels")

print("\n4. Via Claude Desktop (MCP):")
print("   - Configure MCP server with this endpoint")
print("   - Access from Claude Desktop application")

print("\n" + "=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Example Question Library
# MAGIC
# MAGIC Here's a comprehensive list of questions the agent can answer:

# COMMAND ----------

import pandas as pd

questions = [
    {"Domain": "Jobs", "Question": "Which jobs have been running longer than 4 hours?"},
    {"Domain": "Jobs", "Question": "Show me all failed jobs in the last 24 hours"},
    {"Domain": "Jobs", "Question": "What jobs are consuming the most compute time?"},
    {"Domain": "DBSQL", "Question": "What are the top 10 slowest queries?"},
    {"Domain": "DBSQL", "Question": "Show me query performance for a specific user"},
    {"Domain": "DBSQL", "Question": "Which queries took longer than 60 seconds?"},
    {"Domain": "Clusters", "Question": "Which clusters are idle for more than 2 hours?"},
    {"Domain": "Clusters", "Question": "Show me clusters running longer than 8 hours"},
    {"Domain": "Clusters", "Question": "Which clusters can I terminate to save costs?"},
    {"Domain": "Security", "Question": "Who can manage job 12345?"},
    {"Domain": "Security", "Question": "Show me all users with cluster access"},
    {"Domain": "Security", "Question": "Which jobs have no explicit permissions?"},
    {"Domain": "Usage", "Question": "What are the top cost centers in the last 7 days?"},
    {"Domain": "Usage", "Question": "Show me cost by workspace for chargeback"},
    {"Domain": "Usage", "Question": "Calculate cost by project for the last month"},
    {"Domain": "Budget", "Question": "Which teams are over 80% of their monthly budget?"},
    {"Domain": "Budget", "Question": "Are any projects over budget this month?"},
    {"Domain": "Budget", "Question": "Show me budget utilization by workspace"},
    {"Domain": "Audit", "Question": "Show me failed login attempts in the last 24 hours"},
    {"Domain": "Audit", "Question": "What admin changes were made recently?"},
    {"Domain": "Audit", "Question": "Which users have multiple failed login attempts?"},
    {"Domain": "Pipelines", "Question": "Which pipelines are lagging by more than 10 minutes?"},
    {"Domain": "Pipelines", "Question": "List all failed pipelines today"},
    {"Domain": "Pipelines", "Question": "Are any pipelines behind schedule?"},
]

df_questions = pd.DataFrame(questions)
display(df_questions)

print(f"\n✓ The agent can answer {len(questions)} types of questions across {len(set([q['Domain'] for q in questions]))} domains")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Monitoring and Maintenance

# COMMAND ----------

print("=" * 70)
print("AGENT MONITORING & MAINTENANCE GUIDE")
print("=" * 70)

print("\n📊 MONITORING:")
print("-" * 70)
print("1. Track endpoint metrics in Databricks Serving UI")
print("2. Monitor query latency and token usage")
print("3. Review agent responses for accuracy")
print("4. Check for tool execution errors in logs")

print("\n🔧 MAINTENANCE:")
print("-" * 70)
print("1. Update tools when admin APIs change")
print("2. Refresh agent when adding new capabilities")
print("3. Tune system prompt based on user feedback")
print("4. Scale endpoint workload size if needed")

print("\n🚀 OPTIMIZATION:")
print("-" * 70)
print("1. Cache frequently requested data")
print("2. Batch similar queries for efficiency")
print("3. Set appropriate time windows for queries")
print("4. Use limits to control result size")

print("\n⚠ TROUBLESHOOTING:")
print("-" * 70)
print("1. Check endpoint status: ws.serving_endpoints.get('admin-observability-agent')")
print("2. Review logs for tool execution errors")
print("3. Verify workspace permissions for admin APIs")
print("4. Test individual tools in isolation")

print("\n" + "=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Congratulations! You have successfully deployed a comprehensive Admin Observability Agent!
# MAGIC
# MAGIC **What You've Accomplished:**
# MAGIC - ✓ Aggregated 15+ admin tools across 7 domains
# MAGIC - ✓ Deployed a production-ready agent endpoint
# MAGIC - ✓ Tested with natural language queries
# MAGIC - ✓ Covered Jobs, DBSQL, Clusters, Security, Usage/Budget, Audit, and Pipelines
# MAGIC
# MAGIC **Key Capabilities:**
# MAGIC - ✓ Jobs monitoring and performance analysis
# MAGIC - ✓ Query performance tracking
# MAGIC - ✓ Cluster utilization and cost optimization
# MAGIC - ✓ Security and permissions auditing
# MAGIC - ✓ **Cost tracking and chargeback by dimension**
# MAGIC - ✓ **Budget monitoring with utilization alerts**
# MAGIC - ✓ Audit log analysis
# MAGIC - ✓ Pipeline health monitoring
# MAGIC
# MAGIC **NEW Features in This Demo:**
# MAGIC - ✓ cost_by_dimension: Chargeback by workspace, cluster, project, team
# MAGIC - ✓ budget_status: Budget vs actuals with warning thresholds
# MAGIC - ✓ Multi-dimensional cost allocation
# MAGIC
# MAGIC **Next Steps:**
# MAGIC 1. Integrate with Slack/Teams for conversational access
# MAGIC 2. Set up automated alerts for critical issues
# MAGIC 3. Create custom dashboards using agent data
# MAGIC 4. Configure usage and budget tables for full cost tracking
# MAGIC 5. Expand to additional admin domains as needed
# MAGIC
# MAGIC **Integration Options:**
# MAGIC - 🤖 Slack Bot: Real-time admin queries in Slack
# MAGIC - 💬 Teams App: Enterprise admin assistant
# MAGIC - 🖥️ Claude Desktop: MCP integration for local access
# MAGIC - 📊 Dashboards: Automated reporting and visualization
# MAGIC
# MAGIC **Endpoint Details:**
# MAGIC - Name: `admin-observability-agent`
# MAGIC - Workspace: `https://e2-demo-field-eng.cloud.databricks.com`
# MAGIC - Tools: {len(all_tools)} across 7 domains

# COMMAND ----------
