"""
Integration tests for SecurityAdmin.

Tests against real Databricks workspace: https://e2-demo-field-eng.cloud.databricks.com
"""

import pytest
import logging
from admin_ai_bridge.security import SecurityAdmin
from admin_ai_bridge.config import AdminBridgeConfig
from admin_ai_bridge.schemas import PermissionEntry

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def security_admin():
    """Create SecurityAdmin instance with real workspace client."""
    config = AdminBridgeConfig(profile="DEFAULT")
    admin = SecurityAdmin(config)
    logger.info(f"Connected to workspace: {config.host}")
    return admin


@pytest.fixture(scope="module")
def sample_job_id(security_admin):
    """Get a sample job ID from the workspace for testing."""
    try:
        # Try to get jobs from workspace
        from admin_ai_bridge.jobs import JobsAdmin
        jobs_admin = JobsAdmin(AdminBridgeConfig(profile="DEFAULT"))
        jobs = jobs_admin.ws.jobs.list(limit=1)
        job = next(iter(jobs), None)
        if job:
            logger.info(f"Using sample job_id: {job.job_id}")
            return job.job_id
    except Exception as e:
        logger.warning(f"Could not get sample job_id: {e}")
    return None


@pytest.fixture(scope="module")
def sample_cluster_id(security_admin):
    """Get a sample cluster ID from the workspace for testing."""
    try:
        # Try to get clusters from workspace
        from admin_ai_bridge.clusters import ClustersAdmin
        clusters_admin = ClustersAdmin(AdminBridgeConfig(profile="DEFAULT"))
        clusters = clusters_admin.ws.clusters.list()
        cluster = next(iter(clusters), None)
        if cluster:
            logger.info(f"Using sample cluster_id: {cluster.cluster_id}")
            return cluster.cluster_id
    except Exception as e:
        logger.warning(f"Could not get sample cluster_id: {e}")
    return None


@pytest.mark.integration
class TestSecurityAdminIntegration:
    """Integration tests for SecurityAdmin against real workspace."""

    def test_who_can_manage_job_real_workspace(self, security_admin, sample_job_id):
        """Test who_can_manage_job with real workspace data."""
        if not sample_job_id:
            pytest.skip("No job_id available in workspace for testing")

        logger.info(f"Testing who_can_manage_job for job_id: {sample_job_id}")

        result = security_admin.who_can_manage_job(sample_job_id)

        logger.info(f"Found {len(result)} principals who can manage job {sample_job_id}")

        # Validate result structure
        assert isinstance(result, list), "Result should be a list"

        # If we have results, validate structure
        if result:
            for entry in result:
                assert isinstance(entry, PermissionEntry), "Each item should be PermissionEntry"
                assert entry.principal is not None, "principal should be present"
                assert entry.permission_level is not None, "permission_level should be present"

                logger.info(
                    f"Principal {entry.principal}: "
                    f"permission={entry.permission_level}, "
                    f"type={entry.principal_type or 'N/A'}"
                )

            # Verify all returned permissions include CAN_MANAGE
            for entry in result:
                assert "CAN_MANAGE" in entry.permission_level or "IS_OWNER" in entry.permission_level, \
                    f"Expected CAN_MANAGE or IS_OWNER, got {entry.permission_level}"
        else:
            logger.warning("No principals with CAN_MANAGE found. This may indicate no explicit permissions set.")

    def test_who_can_use_cluster_real_workspace(self, security_admin, sample_cluster_id):
        """Test who_can_use_cluster with real workspace data."""
        if not sample_cluster_id:
            pytest.skip("No cluster_id available in workspace for testing")

        logger.info(f"Testing who_can_use_cluster for cluster_id: {sample_cluster_id}")

        result = security_admin.who_can_use_cluster(sample_cluster_id)

        logger.info(f"Found {len(result)} principals who can use cluster {sample_cluster_id}")

        # Validate result structure
        assert isinstance(result, list), "Result should be a list"

        # If we have results, validate structure
        if result:
            for entry in result:
                assert isinstance(entry, PermissionEntry), "Each item should be PermissionEntry"
                assert entry.principal is not None, "principal should be present"
                assert entry.permission_level is not None, "permission_level should be present"

                logger.info(
                    f"Principal {entry.principal}: "
                    f"permission={entry.permission_level}, "
                    f"type={entry.principal_type or 'N/A'}"
                )

            # Verify all returned permissions include CAN_ATTACH_TO or higher
            valid_permissions = ["CAN_ATTACH_TO", "CAN_RESTART", "CAN_MANAGE"]
            for entry in result:
                assert any(perm in entry.permission_level for perm in valid_permissions), \
                    f"Expected valid cluster permission, got {entry.permission_level}"
        else:
            logger.warning("No principals with cluster access found. This may indicate no explicit permissions set.")

    def test_list_workspace_groups_real_workspace(self, security_admin):
        """Test list_workspace_groups with real workspace data."""
        logger.info("Testing list_workspace_groups with real workspace")

        result = security_admin.list_workspace_groups()

        logger.info(f"Found {len(result)} workspace groups")

        # Validate result structure
        assert isinstance(result, list), "Result should be a list"

        # Workspace should have at least the 'users' group
        assert len(result) > 0, "Workspace should have at least one group"

        for group in result:
            assert "group_name" in group, "group_name should be present"
            assert "member_count" in group, "member_count should be present"

            logger.info(
                f"Group {group['group_name']}: "
                f"members={group['member_count']}"
            )

    def test_security_connection_timeout(self, security_admin):
        """Test that security API calls complete within reasonable timeout."""
        logger.info("Testing security API timeout handling")

        import time
        start_time = time.time()

        try:
            result = security_admin.list_workspace_groups()
            elapsed = time.time() - start_time
            logger.info(f"API call completed in {elapsed:.2f} seconds")

            # Should complete within 30 seconds for reasonable workspace
            assert elapsed < 30, f"API call took too long: {elapsed:.2f}s"

        except Exception as e:
            logger.error(f"API call failed: {e}")
            raise

    def test_security_error_handling(self, security_admin):
        """Test error handling with invalid parameters."""
        logger.info("Testing error handling with invalid parameters")

        # Test with invalid job_id (should handle gracefully)
        with pytest.raises(Exception):
            security_admin.who_can_manage_job("invalid_job_id")

        # Test with invalid cluster_id (should handle gracefully)
        with pytest.raises(Exception):
            security_admin.who_can_use_cluster("invalid_cluster_id")

    def test_permission_data_quality(self, security_admin, sample_job_id):
        """Test data quality of permission results."""
        if not sample_job_id:
            pytest.skip("No job_id available in workspace for testing")

        logger.info("Testing permission data quality")

        result = security_admin.who_can_manage_job(sample_job_id)

        if result:
            for entry in result:
                # Validate principal is not empty
                assert entry.principal, "principal should not be empty"

                # Validate permission_level is valid
                valid_levels = ["CAN_MANAGE", "CAN_MANAGE_RUN", "CAN_VIEW", "IS_OWNER"]
                assert any(level in entry.permission_level for level in valid_levels), \
                    f"Invalid permission level: {entry.permission_level}"

                logger.debug(f"Permission entry for {entry.principal} data quality OK")
        else:
            logger.warning("No permissions to validate data quality")

    def test_cross_validate_job_permissions(self, security_admin, sample_job_id):
        """Cross-validate job permissions with workspace UI expectations."""
        if not sample_job_id:
            pytest.skip("No job_id available in workspace for testing")

        logger.info("Cross-validating job permissions with workspace expectations")

        result = security_admin.who_can_manage_job(sample_job_id)

        # Every job should have at least one owner or manager
        # (either the creator or an admin group)
        if result:
            has_owner_or_manager = any(
                "IS_OWNER" in entry.permission_level or "CAN_MANAGE" in entry.permission_level
                for entry in result
            )
            assert has_owner_or_manager, \
                "Job should have at least one owner or manager"
            logger.info("Job permissions include owner or manager - OK")
        else:
            logger.warning("No explicit permissions found - job may use default permissions")
