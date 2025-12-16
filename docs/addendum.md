```markdown
# Addendum – Cost, Chargeback, and Budget Controls

This addendum extends the original design for the **Databricks Admin AI Bridge** to treat cost visibility, chargeback, and budget controls as first‑class requirements.

---

## 1. Goals for Cost & Budget Features

Platform admins must be able to: [web:120][web:125][web:127]

- Understand **who is spending what** (workspaces, projects, teams, cost centers).  
- Implement **chargeback** based on tags, jobs, clusters, or other dimensions.  
- Track **budget vs. actuals** and detect overspend early.  
- Ask questions in natural language via Slack/Teams/Claude like:
  - “Show me DBUs and cost by project for the last 30 days.”
  - “Which teams are over 80% of their monthly budget?”
  - “Which clusters or jobs are the most expensive this week?”

The Admin AI Bridge must provide structured APIs and tools to support these use cases.

---

## 2. Data Assumptions

These features assume the existence of one or more **usage/cost tables** in the lakehouse, following Databricks best practices for monitoring and chargeback: [web:125][web:127]

- A central usage table (for example, `billing.usage_events` or similar) with:
  - `timestamp`, `workspace_id`, `cluster_id`, `job_id`, `warehouse_id`
  - `dbu_consumed`, `cost` (or fields from which cost can be derived)
  - `tags` or metadata (for project/team/cost center)

- A budget table (for example, `billing.budgets`) with:
  - `dimension_type` (e.g. `project`, `team`, `workspace`)
  - `dimension_value`
  - `period` (e.g. `month`/`YYYY-MM`)
  - `budget_amount` (in cost or DBUs)

The exact table names and schemas are workspace‑specific and should be configurable via environment variables or a simple config file.

---

## 3. UsageAdmin Extensions

Extend `UsageAdmin` to support chargeback and budgets.

```
# admin_ai_bridge/usage.py
from datetime import datetime, timedelta
from .config import get_workspace_client
from .schemas import UsageEntry

class UsageAdmin:
    def __init__(self, cfg=None):
        self.ws = get_workspace_client(cfg)
        # Optionally, hold references to SQL warehouse or table names
        self.usage_table = "billing.usage_events"
        self.budget_table = "billing.budgets"

    def top_cost_centers(
        self,
        lookback_days: int = 7,
        limit: int = 20,
    ) -> list[UsageEntry]:
        """
        Return the top N cost contributors (clusters/jobs/warehouses/workspaces)
        over the given time window.
        """
        ...

    def cost_by_dimension(
        self,
        dimension: str,          # "workspace", "cluster", "job", "warehouse", "tag:project", etc.
        lookback_days: int = 30,
        limit: int = 100,
    ) -> list[UsageEntry]:
        """
        Aggregate cost and/or DBUs by a given dimension (for chargeback).
        """
        ...

    def budget_status(
        self,
        dimension: str,          # e.g. "workspace", "project", "team"
        period_days: int = 30,
        warn_threshold: float = 0.8,
    ) -> list[dict]:
        """
        For each entity (workspace/project/team), return:
        - dimension_value
        - actual_cost
        - budget_amount
        - utilization_pct (actual_cost / budget_amount)
        - status: "within_budget", "warning", "breached"
        """
        ...
```

Implementation guidance:

- `cost_by_dimension`:
  - Query the usage table, filter by time window (`NOW() - lookback_days`).  
  - Group by the requested dimension:
    - If `dimension` starts with `tag:`, use a tag column.  
    - Otherwise use workspace/cluster/job IDs or names.  
  - Compute `sum(cost)` and/or `sum(dbu_consumed)`. [web:125][web:127]

- `budget_status`:
  - Compute actual cost per dimension in the period.  
  - Join with the budget table on `(dimension_type, dimension_value)`.  
  - Compute utilization and status:
    - `< warn_threshold` → `within_budget`  
    - `warn_threshold ≤ utilization < 1` → `warning`  
    - `≥ 1` → `breached`

Both methods should return JSON‑friendly structures (Pydantic models or dicts).

---

## 4. Tools for Cost & Budget

Extend `usage_admin_tools` to expose the new capabilities as agent tools.

```
# admin_ai_bridge/tools_databricks_agent.py (additions)
from databricks.agents import ToolSpec
from .usage import UsageAdmin

def usage_admin_tools(cfg=None) -> list[ToolSpec]:
    usage = UsageAdmin(cfg)

    def _top_cost_centers(lookback_days: int = 7, limit: int = 20):
        return [u.model_dump() for u in usage.top_cost_centers(
            lookback_days=lookback_days,
            limit=limit,
        )]

    def _cost_by_dimension(dimension: str, lookback_days: int = 30, limit: int = 100):
        return [u.model_dump() for u in usage.cost_by_dimension(
            dimension=dimension,
            lookback_days=lookback_days,
            limit=limit,
        )]

    def _budget_status(dimension: str, period_days: int = 30, warn_threshold: float = 0.8):
        return usage.budget_status(
            dimension=dimension,
            period_days=period_days,
            warn_threshold=warn_threshold,
        )

    return [
        ToolSpec.python(
            func=_top_cost_centers,
            name="top_cost_centers",
            description="Return the top cost‑contributing workloads in the given time window.",
        ),
        ToolSpec.python(
            func=_cost_by_dimension,
            name="cost_by_dimension",
            description="Aggregate cost and DBUs by a dimension (workspace, cluster, job, tag:project, etc.) for chargeback.",
        ),
        ToolSpec.python(
            func=_budget_status,
            name="budget_status",
            description="Return budget vs actuals and status (within_budget, warning, breached) for each workspace/project/team.",
        ),
    ]
```

These tools let an LLM answer questions such as:

- “Cost by project over last 30 days.”  
- “Budget status by team for the current month.”

---

## 5. Spec Updates (Summary)

To ensure cost/chargeback is truly in scope, update the product spec to include:

- Under **Usage & Cost Admin**:

  - “The library SHALL implement `cost_by_dimension` and `budget_status` APIs to support chargeback and budget monitoring, using a configurable usage table and budget table in the lakehouse.” [web:125][web:127]  
  - “The library SHALL expose `cost_by_dimension` and `budget_status` as Databricks Agent Framework tools.”

- Under **User Stories**:

  - A dedicated story for “Budget owner asks: ‘Which teams are over 80% of their monthly budget?’”

- Under **Success Metrics**:

  - “Admins can answer chargeback and budget questions (by project/team/workspace) from a single agent endpoint, without writing SQL.”

---

## 6. QA Updates (Summary)

Extend `qa.md` to cover:

- Unit tests:
  - `cost_by_dimension`:
    - Aggregates correctly for synthetic usage data grouped by workspace, cluster, tag.  
  - `budget_status`:
    - Correct classification for under‑budget, warning, and breached cases.

- Integration tests:
  - On `e2-demo-field-eng.cloud.databricks.com`, use a small sample usage/budget dataset (or pre‑populated tables) to validate:
    - “Top cost centers” matches expected rankings.  
    - “Budget status” marks over 80% as warning and ≥100% as breached.

- Notebook tests:
  - In `db-demos`, a notebook that:
    - Calls the agent with “Which projects are over 80% of budget this month?”  
    - Verifies that the response matches the expected results from direct queries.

---

## 7. Agent UX Examples

Once implemented, the `admin-observability-agent` should be able to answer queries like:

- “Show DBUs and cost by workspace for the last 30 days.”  
- “Give me cost by tag:project for the last 7 days, sorted descending.”  
- “Which teams are over 80% of their monthly budget?”  
- “What is the total cost of our Lakeflow pipelines this week?”

The agent routes these to `top_cost_centers`, `cost_by_dimension`, or `budget_status` tools, and returns natural‑language summaries plus optional tables.

---
```
