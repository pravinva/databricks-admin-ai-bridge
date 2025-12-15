"""
Pydantic models for Databricks admin domain objects.

These schemas provide strongly typed data structures for all admin bridge operations.
"""

from datetime import datetime
from pydantic import BaseModel, Field


class JobRunSummary(BaseModel):
    """
    Summary of a Databricks job run.

    Attributes:
        job_id: Unique identifier for the job
        job_name: Human-readable name of the job
        run_id: Unique identifier for this specific run
        state: Current state of the run (e.g., SUCCESS, FAILED, RUNNING)
        life_cycle_state: Lifecycle state (e.g., PENDING, RUNNING, TERMINATED)
        start_time: When the run started
        end_time: When the run completed (if finished)
        duration_seconds: Total duration in seconds (if finished)
    """
    job_id: int = Field(description="Unique identifier for the job")
    job_name: str = Field(description="Human-readable name of the job")
    run_id: int = Field(description="Unique identifier for this specific run")
    state: str = Field(description="Current state of the run")
    life_cycle_state: str | None = Field(default=None, description="Lifecycle state of the run")
    start_time: datetime | None = Field(default=None, description="When the run started")
    end_time: datetime | None = Field(default=None, description="When the run completed")
    duration_seconds: float | None = Field(default=None, description="Total duration in seconds")


class QueryHistoryEntry(BaseModel):
    """
    Entry from Databricks SQL query history.

    Attributes:
        query_id: Unique identifier for the query
        warehouse_id: SQL warehouse that executed the query
        user_name: User who executed the query
        status: Query status (e.g., FINISHED, FAILED, RUNNING)
        start_time: When the query started
        end_time: When the query completed
        duration_seconds: Total execution duration in seconds
        sql_text: The SQL query text
    """
    query_id: str = Field(description="Unique identifier for the query")
    warehouse_id: str | None = Field(default=None, description="SQL warehouse that executed the query")
    user_name: str | None = Field(default=None, description="User who executed the query")
    status: str | None = Field(default=None, description="Query status")
    start_time: datetime | None = Field(default=None, description="When the query started")
    end_time: datetime | None = Field(default=None, description="When the query completed")
    duration_seconds: float | None = Field(default=None, description="Total execution duration in seconds")
    sql_text: str | None = Field(default=None, description="The SQL query text")


class ClusterSummary(BaseModel):
    """
    Summary of a Databricks cluster.

    Attributes:
        cluster_id: Unique identifier for the cluster
        cluster_name: Human-readable name of the cluster
        state: Current state (e.g., RUNNING, TERMINATED, PENDING)
        creator: Username of the cluster creator
        start_time: When the cluster was started
        driver_node_type: Instance type of the driver node
        node_type: Instance type of worker nodes
        cluster_policy_id: ID of the cluster policy applied (if any)
        last_activity_time: Timestamp of last activity on the cluster
        is_long_running: Whether the cluster has been running for an extended period
    """
    cluster_id: str = Field(description="Unique identifier for the cluster")
    cluster_name: str = Field(description="Human-readable name of the cluster")
    state: str = Field(description="Current state of the cluster")
    creator: str | None = Field(default=None, description="Username of the cluster creator")
    start_time: datetime | None = Field(default=None, description="When the cluster was started")
    driver_node_type: str | None = Field(default=None, description="Instance type of the driver node")
    node_type: str | None = Field(default=None, description="Instance type of worker nodes")
    cluster_policy_id: str | None = Field(default=None, description="ID of the cluster policy applied")
    last_activity_time: datetime | None = Field(default=None, description="Timestamp of last activity")
    is_long_running: bool | None = Field(default=None, description="Whether cluster has been running extended period")


class PermissionEntry(BaseModel):
    """
    Permission entry for a Databricks object.

    Attributes:
        object_type: Type of object (e.g., JOB, CLUSTER, NOTEBOOK)
        object_id: Unique identifier of the object
        principal: User, group, or service principal name
        permission_level: Permission level (e.g., CAN_MANAGE, CAN_USE, CAN_VIEW)
    """
    object_type: str = Field(description="Type of object")
    object_id: str = Field(description="Unique identifier of the object")
    principal: str = Field(description="User, group, or service principal name")
    permission_level: str = Field(description="Permission level")


class UsageEntry(BaseModel):
    """
    Usage and cost entry for a Databricks resource.

    Attributes:
        scope: Resource scope (cluster/job/warehouse/workspace)
        name: Human-readable name of the resource
        start_time: Start of the usage period
        end_time: End of the usage period
        cost: Cost in currency units (if available)
        dbus: Databricks Units consumed (if available)
    """
    scope: str = Field(description="Resource scope (cluster/job/warehouse/workspace)")
    name: str = Field(description="Human-readable name of the resource")
    start_time: datetime = Field(description="Start of the usage period")
    end_time: datetime = Field(description="End of the usage period")
    cost: float | None = Field(default=None, description="Cost in currency units")
    dbus: float | None = Field(default=None, description="Databricks Units consumed")


class AuditEvent(BaseModel):
    """
    Audit log event from Databricks.

    Attributes:
        event_time: When the event occurred
        service_name: Databricks service that generated the event
        event_type: Type of event (e.g., login, createCluster, deleteJob)
        user_name: User who performed the action
        source_ip: Source IP address of the request
        details: Additional event-specific details
    """
    event_time: datetime = Field(description="When the event occurred")
    service_name: str = Field(description="Databricks service that generated the event")
    event_type: str = Field(description="Type of event")
    user_name: str | None = Field(default=None, description="User who performed the action")
    source_ip: str | None = Field(default=None, description="Source IP address of the request")
    details: dict | None = Field(default=None, description="Additional event-specific details")


class PipelineStatus(BaseModel):
    """
    Status of a Databricks pipeline (DLT/Lakeflow).

    Attributes:
        pipeline_id: Unique identifier for the pipeline
        name: Human-readable name of the pipeline
        state: Current state (e.g., RUNNING, IDLE, FAILED)
        last_update_time: When the pipeline last updated
        lag_seconds: Current lag in seconds (for streaming pipelines)
        last_error: Last error message (if any)
    """
    pipeline_id: str = Field(description="Unique identifier for the pipeline")
    name: str = Field(description="Human-readable name of the pipeline")
    state: str = Field(description="Current state of the pipeline")
    last_update_time: datetime | None = Field(default=None, description="When the pipeline last updated")
    lag_seconds: float | None = Field(default=None, description="Current lag in seconds for streaming pipelines")
    last_error: str | None = Field(default=None, description="Last error message if any")


class BudgetStatus(BaseModel):
    """
    Budget vs actuals status for a dimension (workspace, project, team).

    Attributes:
        dimension_value: The specific value of the dimension (e.g., workspace ID, project name)
        actual_cost: Actual cost incurred during the period
        budget_amount: Allocated budget for the period
        utilization_pct: Percentage of budget utilized (actual_cost / budget_amount)
        status: Budget status - "within_budget" (<80%), "warning" (80-100%), or "breached" (>100%)
    """
    dimension_value: str = Field(description="The specific value of the dimension")
    actual_cost: float = Field(description="Actual cost incurred during the period")
    budget_amount: float = Field(description="Allocated budget for the period")
    utilization_pct: float = Field(description="Percentage of budget utilized")
    status: str = Field(description="Budget status: within_budget, warning, or breached")
