"""
Unit tests for schemas module.
"""

from datetime import datetime
import pytest
from admin_ai_bridge.schemas import (
    JobRunSummary,
    QueryHistoryEntry,
    ClusterSummary,
    PermissionEntry,
    UsageEntry,
    AuditEvent,
    PipelineStatus,
)


class TestJobRunSummary:
    """Test JobRunSummary schema."""

    def test_job_run_summary_minimal(self):
        """Test JobRunSummary with minimal fields."""
        summary = JobRunSummary(
            job_id=123,
            job_name="test_job",
            run_id=456,
            state="SUCCESS"
        )
        assert summary.job_id == 123
        assert summary.job_name == "test_job"
        assert summary.run_id == 456
        assert summary.state == "SUCCESS"
        assert summary.life_cycle_state is None

    def test_job_run_summary_full(self):
        """Test JobRunSummary with all fields."""
        now = datetime.now()
        summary = JobRunSummary(
            job_id=123,
            job_name="test_job",
            run_id=456,
            state="SUCCESS",
            life_cycle_state="TERMINATED",
            start_time=now,
            end_time=now,
            duration_seconds=120.5
        )
        assert summary.duration_seconds == 120.5
        assert summary.life_cycle_state == "TERMINATED"


class TestQueryHistoryEntry:
    """Test QueryHistoryEntry schema."""

    def test_query_history_minimal(self):
        """Test QueryHistoryEntry with minimal fields."""
        entry = QueryHistoryEntry(query_id="q123")
        assert entry.query_id == "q123"
        assert entry.warehouse_id is None

    def test_query_history_full(self):
        """Test QueryHistoryEntry with all fields."""
        now = datetime.now()
        entry = QueryHistoryEntry(
            query_id="q123",
            warehouse_id="w456",
            user_name="test@example.com",
            status="FINISHED",
            start_time=now,
            end_time=now,
            duration_seconds=5.2,
            sql_text="SELECT * FROM table"
        )
        assert entry.warehouse_id == "w456"
        assert entry.user_name == "test@example.com"
        assert entry.sql_text == "SELECT * FROM table"


class TestClusterSummary:
    """Test ClusterSummary schema."""

    def test_cluster_summary_minimal(self):
        """Test ClusterSummary with minimal fields."""
        cluster = ClusterSummary(
            cluster_id="c123",
            cluster_name="test_cluster",
            state="RUNNING"
        )
        assert cluster.cluster_id == "c123"
        assert cluster.cluster_name == "test_cluster"
        assert cluster.state == "RUNNING"

    def test_cluster_summary_full(self):
        """Test ClusterSummary with all fields."""
        now = datetime.now()
        cluster = ClusterSummary(
            cluster_id="c123",
            cluster_name="test_cluster",
            state="RUNNING",
            creator="user@example.com",
            start_time=now,
            driver_node_type="i3.xlarge",
            node_type="i3.xlarge",
            cluster_policy_id="policy123",
            last_activity_time=now,
            is_long_running=True
        )
        assert cluster.creator == "user@example.com"
        assert cluster.is_long_running is True


class TestPermissionEntry:
    """Test PermissionEntry schema."""

    def test_permission_entry(self):
        """Test PermissionEntry schema."""
        perm = PermissionEntry(
            object_type="JOB",
            object_id="123",
            principal="user@example.com",
            permission_level="CAN_MANAGE"
        )
        assert perm.object_type == "JOB"
        assert perm.object_id == "123"
        assert perm.principal == "user@example.com"
        assert perm.permission_level == "CAN_MANAGE"


class TestUsageEntry:
    """Test UsageEntry schema."""

    def test_usage_entry(self):
        """Test UsageEntry schema."""
        now = datetime.now()
        usage = UsageEntry(
            scope="cluster",
            name="prod_cluster",
            start_time=now,
            end_time=now,
            cost=125.50,
            dbus=1000.0
        )
        assert usage.scope == "cluster"
        assert usage.name == "prod_cluster"
        assert usage.cost == 125.50
        assert usage.dbus == 1000.0


class TestAuditEvent:
    """Test AuditEvent schema."""

    def test_audit_event(self):
        """Test AuditEvent schema."""
        now = datetime.now()
        event = AuditEvent(
            event_time=now,
            service_name="clusters",
            event_type="createCluster",
            user_name="admin@example.com",
            source_ip="192.168.1.1",
            details={"cluster_name": "test"}
        )
        assert event.service_name == "clusters"
        assert event.event_type == "createCluster"
        assert event.user_name == "admin@example.com"


class TestPipelineStatus:
    """Test PipelineStatus schema."""

    def test_pipeline_status(self):
        """Test PipelineStatus schema."""
        now = datetime.now()
        pipeline = PipelineStatus(
            pipeline_id="p123",
            name="test_pipeline",
            state="RUNNING",
            last_update_time=now,
            lag_seconds=30.5,
            last_error=None
        )
        assert pipeline.pipeline_id == "p123"
        assert pipeline.name == "test_pipeline"
        assert pipeline.state == "RUNNING"
        assert pipeline.lag_seconds == 30.5
