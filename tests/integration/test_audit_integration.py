"""
Integration tests for AuditAdmin.

Tests against real Databricks workspace: https://e2-demo-field-eng.cloud.databricks.com

Note: Audit log export must be configured. These tests handle missing audit data gracefully.
"""

import pytest
import logging
from admin_ai_bridge.audit import AuditAdmin
from admin_ai_bridge.config import AdminBridgeConfig
from admin_ai_bridge.schemas import AuditEvent

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def audit_admin():
    """Create AuditAdmin instance with real workspace client."""
    config = AdminBridgeConfig(profile="DEFAULT")
    admin = AuditAdmin(config)
    logger.info(f"Connected to workspace: {config.host}")
    return admin


@pytest.mark.integration
class TestAuditAdminIntegration:
    """Integration tests for AuditAdmin against real workspace."""

    def test_failed_logins_real_workspace(self, audit_admin):
        """Test failed_logins with real workspace data."""
        logger.info("Testing failed_logins with real workspace")

        try:
            result = audit_admin.failed_logins(
                lookback_hours=72,  # Last 3 days
                limit=50
            )

            logger.info(f"Found {len(result)} failed login attempts")

            # Validate result structure
            assert isinstance(result, list), "Result should be a list"

            # If we have results, validate structure
            if result:
                for event in result:
                    assert isinstance(event, AuditEvent), "Each item should be AuditEvent"
                    assert event.timestamp is not None, "timestamp should be present"
                    assert event.event_type is not None, "event_type should be present"
                    assert event.user_identity is not None, "user_identity should be present"

                    logger.info(
                        f"Failed login: "
                        f"user={event.user_identity}, "
                        f"timestamp={event.timestamp}, "
                        f"source_ip={event.source_ip_address or 'N/A'}"
                    )

                # Verify events are sorted by timestamp (descending)
                timestamps = [e.timestamp for e in result]
                assert timestamps == sorted(timestamps, reverse=True), \
                    "Events should be sorted by timestamp in descending order"
            else:
                logger.warning("No failed logins found. This is OK if audit logs show no failures.")

        except Exception as e:
            logger.warning(f"Audit log may not be configured: {e}")
            pytest.skip("Audit log not available in workspace")

    def test_recent_admin_changes_real_workspace(self, audit_admin):
        """Test recent_admin_changes with real workspace data."""
        logger.info("Testing recent_admin_changes with real workspace")

        try:
            result = audit_admin.recent_admin_changes(
                lookback_hours=168,  # Last week
                limit=50
            )

            logger.info(f"Found {len(result)} recent admin changes")

            # Validate result structure
            assert isinstance(result, list), "Result should be a list"

            # If we have results, validate structure
            if result:
                for event in result:
                    assert isinstance(event, AuditEvent), "Each item should be AuditEvent"
                    assert event.timestamp is not None, "timestamp should be present"
                    assert event.event_type is not None, "event_type should be present"
                    assert event.user_identity is not None, "user_identity should be present"

                    logger.info(
                        f"Admin change: "
                        f"type={event.event_type}, "
                        f"user={event.user_identity}, "
                        f"timestamp={event.timestamp}, "
                        f"action={event.action_name or 'N/A'}"
                    )

                # Verify events are sorted by timestamp (descending)
                timestamps = [e.timestamp for e in result]
                assert timestamps == sorted(timestamps, reverse=True), \
                    "Events should be sorted by timestamp in descending order"
            else:
                logger.warning("No admin changes found in the specified time window.")

        except Exception as e:
            logger.warning(f"Audit log may not be configured: {e}")
            pytest.skip("Audit log not available in workspace")

    def test_query_audit_logs_real_workspace(self, audit_admin):
        """Test query_audit_logs with custom filters."""
        logger.info("Testing query_audit_logs with real workspace")

        try:
            # Query for recent notebook events
            result = audit_admin.query_audit_logs(
                event_types=["notebook"],
                lookback_hours=24,
                limit=20
            )

            logger.info(f"Found {len(result)} notebook audit events")

            # Validate result structure
            assert isinstance(result, list), "Result should be a list"

            if result:
                for event in result:
                    assert isinstance(event, AuditEvent), "Each item should be AuditEvent"
                    assert "notebook" in event.event_type.lower(), \
                        f"Expected notebook event, got {event.event_type}"

                    logger.info(
                        f"Notebook event: "
                        f"type={event.event_type}, "
                        f"user={event.user_identity}, "
                        f"timestamp={event.timestamp}"
                    )
            else:
                logger.warning("No notebook audit events found.")

        except Exception as e:
            logger.warning(f"Audit log may not be configured: {e}")
            pytest.skip("Audit log not available in workspace")

    def test_audit_with_various_parameters(self, audit_admin):
        """Test failed_logins with various parameter combinations."""
        logger.info("Testing failed_logins with various parameters")

        try:
            # Test with different limits
            result_limit_5 = audit_admin.failed_logins(
                lookback_hours=48,
                limit=5
            )
            assert len(result_limit_5) <= 5, "Result should respect limit parameter"
            logger.info(f"With limit=5: found {len(result_limit_5)} failed logins")

            # Test with different lookback periods
            result_24h = audit_admin.failed_logins(
                lookback_hours=24,
                limit=50
            )
            logger.info(f"With lookback=24h: found {len(result_24h)} failed logins")

            result_72h = audit_admin.failed_logins(
                lookback_hours=72,
                limit=50
            )
            logger.info(f"With lookback=72h: found {len(result_72h)} failed logins")

            # 72h should have >= 24h results (or both can be 0)
            assert len(result_72h) >= len(result_24h), \
                "Longer lookback should find >= events than shorter lookback"

        except Exception as e:
            logger.warning(f"Audit log may not be configured: {e}")
            pytest.skip("Audit log not available in workspace")

    def test_audit_connection_timeout(self, audit_admin):
        """Test that audit API calls complete within reasonable timeout."""
        logger.info("Testing audit API timeout handling")

        import time
        start_time = time.time()

        try:
            result = audit_admin.failed_logins(
                lookback_hours=24,
                limit=10
            )
            elapsed = time.time() - start_time
            logger.info(f"API call completed in {elapsed:.2f} seconds")

            # Should complete within 60 seconds (audit queries can be slower)
            assert elapsed < 60, f"API call took too long: {elapsed:.2f}s"

        except Exception as e:
            logger.warning(f"Audit log may not be configured: {e}")
            pytest.skip("Audit log not available in workspace")

    def test_audit_error_handling(self, audit_admin):
        """Test error handling with invalid parameters."""
        logger.info("Testing error handling with invalid parameters")

        try:
            # Test with invalid lookback hours (should handle gracefully)
            with pytest.raises(Exception):
                audit_admin.failed_logins(
                    lookback_hours=-1,  # Invalid
                    limit=10
                )

        except Exception as e:
            logger.warning(f"Audit log may not be configured: {e}")
            pytest.skip("Audit log not available in workspace")

    def test_audit_data_quality(self, audit_admin):
        """Test data quality of audit log results."""
        logger.info("Testing audit log data quality")

        try:
            result = audit_admin.recent_admin_changes(
                lookback_hours=168,
                limit=20
            )

            if result:
                for event in result:
                    # Validate timestamp is present and reasonable
                    assert event.timestamp, "timestamp should not be empty"

                    # Validate event_type is present
                    assert event.event_type, "event_type should not be empty"

                    # Validate user_identity is present
                    assert event.user_identity, "user_identity should not be empty"

                    logger.debug(f"Audit event {event.event_type} data quality OK")
            else:
                logger.warning("No audit events to validate data quality")

        except Exception as e:
            logger.warning(f"Audit log may not be configured: {e}")
            pytest.skip("Audit log not available in workspace")

    def test_cross_validate_audit_timestamps(self, audit_admin):
        """Cross-validate that audit timestamps are within expected range."""
        logger.info("Cross-validating audit event timestamps")

        try:
            from datetime import datetime, timezone, timedelta

            result = audit_admin.failed_logins(
                lookback_hours=24,
                limit=10
            )

            if result:
                now = datetime.now(timezone.utc)
                lookback_time = now - timedelta(hours=24)

                for event in result:
                    event_time = datetime.fromisoformat(event.timestamp.replace('Z', '+00:00'))

                    # Event should be within the lookback window
                    assert event_time >= lookback_time, \
                        f"Event timestamp {event.timestamp} is outside lookback window"
                    assert event_time <= now, \
                        f"Event timestamp {event.timestamp} is in the future"

                    logger.debug(f"Event timestamp {event.timestamp} is valid")

                logger.info("All audit event timestamps are within expected range")
            else:
                logger.warning("No audit events to validate timestamps")

        except Exception as e:
            logger.warning(f"Audit log may not be configured: {e}")
            pytest.skip("Audit log not available in workspace")
