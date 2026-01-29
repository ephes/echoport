"""Tests for the health status endpoint."""

import json
from datetime import timedelta

import pytest
from django.test import Client
from django.urls import reverse
from django.utils import timezone

from backups.models import BackupRun, BackupRunStatus, BackupTarget


@pytest.fixture
def client():
    return Client()


@pytest.fixture
def active_target(db):
    """Create an active backup target with a schedule."""
    return BackupTarget.objects.create(
        name="test-service",
        fastdeploy_service="echoport-backup",
        db_path="/tmp/test.db",
        schedule="0 2 * * *",  # Daily at 2am
        status="active",
    )


@pytest.fixture
def target_without_schedule(db):
    """Create an active backup target without a schedule."""
    return BackupTarget.objects.create(
        name="manual-only",
        fastdeploy_service="echoport-backup",
        db_path="/tmp/manual.db",
        schedule="",
        status="active",
    )


@pytest.fixture
def paused_target(db):
    """Create a paused backup target (should not appear in health)."""
    return BackupTarget.objects.create(
        name="paused-service",
        fastdeploy_service="echoport-backup",
        db_path="/tmp/paused.db",
        status="paused",
    )


@pytest.fixture
def target_with_invalid_schedule(db):
    """Create a target with an invalid cron expression."""
    return BackupTarget.objects.create(
        name="invalid-cron",
        fastdeploy_service="echoport-backup",
        db_path="/tmp/invalid.db",
        schedule="not a valid cron",
        status="active",
    )


@pytest.mark.django_db
class TestHealthEndpoint:
    def test_health_endpoint_no_auth_required(self, client):
        """Health endpoint should be accessible without authentication."""
        response = client.get(reverse("backups:health_status"))
        assert response.status_code == 200

    def test_health_endpoint_returns_json(self, client):
        """Health endpoint should return JSON."""
        response = client.get(reverse("backups:health_status"))
        assert response["Content-Type"] == "application/json"
        data = json.loads(response.content)
        assert "status" in data
        assert "targets" in data
        assert "recent_failures" in data
        assert "checked_at" in data

    def test_healthy_status_with_recent_backup(self, client, active_target):
        """Status should be healthy when backup completed after last scheduled time."""
        # Create a successful backup that completed recently
        BackupRun.objects.create(
            target=active_target,
            status=BackupRunStatus.SUCCESS,
            started_at=timezone.now() - timedelta(hours=1),
            finished_at=timezone.now() - timedelta(minutes=50),
        )

        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        assert data["status"] == "healthy"
        assert len(data["targets"]) == 1
        assert data["targets"][0]["name"] == "test-service"
        assert data["targets"][0]["status"] == "ok"
        assert data["targets"][0]["overdue"] is False

    def test_overdue_status_when_backup_missed(self, client, active_target):
        """Status should be overdue when backup is older than last scheduled time."""
        # Create a backup from 3 days ago (missed yesterday's 2am backup)
        BackupRun.objects.create(
            target=active_target,
            status=BackupRunStatus.SUCCESS,
            started_at=timezone.now() - timedelta(days=3),
            finished_at=timezone.now() - timedelta(days=3),
        )

        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        assert data["status"] == "unhealthy"
        assert data["targets"][0]["status"] == "overdue"
        assert data["targets"][0]["overdue"] is True
        assert "overdue_hours" in data["targets"][0]

    def test_overdue_when_no_successful_backup(self, client, active_target):
        """Status should be overdue when target has schedule but no successful backup."""
        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        assert data["status"] == "unhealthy"
        assert data["targets"][0]["status"] == "overdue"
        assert data["targets"][0]["overdue"] is True

    def test_degraded_status_on_recent_failure(self, client, active_target):
        """Status should be degraded when last backup failed but not overdue."""
        # Recent successful backup
        BackupRun.objects.create(
            target=active_target,
            status=BackupRunStatus.SUCCESS,
            started_at=timezone.now() - timedelta(hours=2),
            finished_at=timezone.now() - timedelta(hours=2),
        )
        # More recent failed backup
        BackupRun.objects.create(
            target=active_target,
            status=BackupRunStatus.FAILED,
            started_at=timezone.now() - timedelta(hours=1),
            finished_at=timezone.now() - timedelta(hours=1),
            error_message="Connection timeout",
        )

        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        assert data["status"] == "degraded"
        assert data["targets"][0]["status"] == "last_failed"
        assert len(data["recent_failures"]) == 1
        # Security: error message should NOT be exposed
        assert "error" not in data["recent_failures"][0]

    def test_timeout_treated_as_failure(self, client, active_target):
        """TIMEOUT status should be treated the same as FAILED."""
        # Recent successful backup
        BackupRun.objects.create(
            target=active_target,
            status=BackupRunStatus.SUCCESS,
            started_at=timezone.now() - timedelta(hours=2),
            finished_at=timezone.now() - timedelta(hours=2),
        )
        # More recent timeout
        BackupRun.objects.create(
            target=active_target,
            status=BackupRunStatus.TIMEOUT,
            started_at=timezone.now() - timedelta(hours=1),
            finished_at=timezone.now() - timedelta(hours=1),
        )

        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        assert data["status"] == "degraded"
        assert data["targets"][0]["status"] == "last_failed"
        assert len(data["recent_failures"]) == 1
        assert data["recent_failures"][0]["status"] == "timeout"

    def test_invalid_schedule_surfaces_as_degraded(self, client, target_with_invalid_schedule):
        """Invalid cron expression should surface as invalid_schedule status."""
        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        assert data["status"] == "degraded"
        assert data["targets"][0]["status"] == "invalid_schedule"
        assert data["targets"][0]["next_scheduled"] is None

    def test_paused_targets_excluded(self, client, active_target, paused_target):
        """Paused targets should not appear in health status."""
        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        target_names = [t["name"] for t in data["targets"]]
        assert "test-service" in target_names
        assert "paused-service" not in target_names

    def test_target_without_schedule(self, client, target_without_schedule):
        """Targets without schedule should show ok status (not overdue)."""
        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        assert data["status"] == "healthy"
        assert data["targets"][0]["status"] == "ok"
        assert data["targets"][0]["overdue"] is False
        assert data["targets"][0]["next_scheduled"] is None

    def test_recent_failures_limited(self, client, active_target):
        """Recent failures should be limited to prevent response bloat."""
        # Create many failed runs
        for i in range(15):
            BackupRun.objects.create(
                target=active_target,
                status=BackupRunStatus.FAILED,
                started_at=timezone.now() - timedelta(days=i % 7, hours=i),
                error_message=f"Error {i}",
            )

        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        # Should be limited to 10 most recent
        assert len(data["recent_failures"]) <= 10

    def test_failures_older_than_7_days_excluded(self, client, active_target):
        """Failures older than 7 days should not appear in recent_failures."""
        # Old failure
        BackupRun.objects.create(
            target=active_target,
            status=BackupRunStatus.FAILED,
            started_at=timezone.now() - timedelta(days=10),
            error_message="Old error",
        )
        # Recent successful backup
        BackupRun.objects.create(
            target=active_target,
            status=BackupRunStatus.SUCCESS,
            started_at=timezone.now() - timedelta(hours=1),
        )

        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        assert len(data["recent_failures"]) == 0

    def test_next_scheduled_includes_timezone(self, client, active_target):
        """next_scheduled should include timezone info in ISO format."""
        # Create successful backup so it's not overdue
        BackupRun.objects.create(
            target=active_target,
            status=BackupRunStatus.SUCCESS,
            started_at=timezone.now() - timedelta(hours=1),
        )

        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        next_scheduled = data["targets"][0]["next_scheduled"]
        assert next_scheduled is not None
        # Should have timezone offset (+ or Z)
        assert "+" in next_scheduled or "Z" in next_scheduled

    def test_mixed_overdue_and_failed_is_unhealthy(self, client, db):
        """When one target is overdue and another failed, overall is unhealthy."""
        # Target 1: overdue (no successful backup)
        BackupTarget.objects.create(
            name="overdue-target",
            fastdeploy_service="echoport-backup",
            schedule="0 2 * * *",
            status="active",
        )

        # Target 2: recent success + recent failure (degraded)
        target2 = BackupTarget.objects.create(
            name="failed-target",
            fastdeploy_service="echoport-backup",
            schedule="",  # No schedule so not overdue
            status="active",
        )
        BackupRun.objects.create(
            target=target2,
            status=BackupRunStatus.SUCCESS,
            started_at=timezone.now() - timedelta(hours=2),
        )
        BackupRun.objects.create(
            target=target2,
            status=BackupRunStatus.FAILED,
            started_at=timezone.now() - timedelta(hours=1),
        )

        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        # Overdue takes precedence -> unhealthy
        assert data["status"] == "unhealthy"

        # Check individual statuses
        statuses = {t["name"]: t["status"] for t in data["targets"]}
        assert statuses["overdue-target"] == "overdue"
        assert statuses["failed-target"] == "last_failed"
