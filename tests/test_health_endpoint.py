"""Tests for the health status endpoint."""

import json
from datetime import datetime, timedelta, timezone as datetime_timezone
from unittest.mock import patch

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
        service_name="test-service.service",
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
        service_name="manual-only.service",
        db_path="/tmp/manual.db",
        schedule="",
        status="active",
    )


@pytest.fixture
def target_missing_required_schedule(db):
    """Simulate configuration drift that bypassed model validation."""
    target = BackupTarget.objects.create(
        name="required-schedule",
        fastdeploy_service="echoport-backup",
        service_name="required-schedule.service",
        db_path="/tmp/required.db",
        schedule="0 2 * * *",
        schedule_required=True,
        status="active",
    )
    BackupTarget.objects.filter(pk=target.pk).update(schedule="")
    target.refresh_from_db()
    return target


@pytest.fixture
def paused_target(db):
    """Create a paused backup target (should not appear in health)."""
    return BackupTarget.objects.create(
        name="paused-service",
        fastdeploy_service="echoport-backup",
        service_name="paused-service.service",
        db_path="/tmp/paused.db",
        status="paused",
    )


@pytest.fixture
def paused_required_target(db):
    """Create a required target whose backup operation has been paused."""
    return BackupTarget.objects.create(
        name="paused-required",
        fastdeploy_service="echoport-backup",
        service_name="paused-required.service",
        db_path="/tmp/paused-required.db",
        schedule="0 2 * * *",
        schedule_required=True,
        status="paused",
    )


@pytest.fixture
def target_with_invalid_schedule(db):
    """Create a target with an invalid cron expression.

    Since model validation now rejects invalid schedules at save(),
    we create with a valid schedule then use update() to bypass validation.
    This simulates invalid data that might exist from before validation was added.
    """
    target = BackupTarget.objects.create(
        name="invalid-cron",
        fastdeploy_service="echoport-backup",
        service_name="invalid-cron.service",
        db_path="/tmp/invalid.db",
        schedule="0 2 * * *",  # Valid schedule for initial creation
        status="active",
    )
    # Bypass model validation to set invalid schedule
    BackupTarget.objects.filter(pk=target.pk).update(schedule="not a valid cron")
    target.refresh_from_db()
    return target


@pytest.fixture
def required_target_with_invalid_schedule(db):
    """Simulate malformed required-schedule drift that bypassed validation."""
    target = BackupTarget.objects.create(
        name="invalid-required-cron",
        fastdeploy_service="echoport-backup",
        service_name="invalid-required-cron.service",
        db_path="/tmp/invalid-required.db",
        schedule="0 2 * * *",
        schedule_required=True,
        status="active",
    )
    BackupTarget.objects.filter(pk=target.pk).update(schedule="not a valid cron")
    target.refresh_from_db()
    return target


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
        assert "targets_by_name" in data
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

    def test_required_target_healthy_contract(self, client, active_target):
        """A healthy required target exposes every named-monitor field."""
        now = datetime(2026, 7, 25, 12, 0, tzinfo=datetime_timezone.utc)
        active_target.schedule_required = True
        active_target.save()
        BackupRun.objects.create(
            target=active_target,
            status=BackupRunStatus.SUCCESS,
            started_at=now - timedelta(hours=1),
            finished_at=now - timedelta(minutes=50),
        )

        with patch("backups.views.timezone.now", return_value=now):
            response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)
        target = data["targets_by_name"]["test-service"]

        assert data["status"] == "healthy"
        assert target["status"] == "ok"
        assert target["target_status"] == "active"
        assert target["schedule_required"] is True
        assert target["schedule"] == "0 2 * * *"
        assert target["next_scheduled"] is not None

    def test_overdue_status_when_backup_missed(self, client, active_target):
        """Status should be overdue when backup is older than last scheduled time."""
        # Create a backup from 3 days ago (missed yesterday's 2am backup)
        now = datetime(2026, 7, 25, 12, 0, tzinfo=datetime_timezone.utc)
        BackupRun.objects.create(
            target=active_target,
            status=BackupRunStatus.SUCCESS,
            started_at=now - timedelta(days=3),
            finished_at=now - timedelta(days=3),
        )

        with patch("backups.views.timezone.now", return_value=now):
            response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        assert data["status"] == "unhealthy"
        assert data["targets"][0]["status"] == "overdue"
        assert data["targets"][0]["overdue"] is True
        assert "overdue_hours" in data["targets"][0]

    def test_overdue_when_no_successful_backup(self, client, active_target):
        """Status should be overdue when target has schedule but no successful backup."""
        now = datetime(2026, 7, 25, 12, 0, tzinfo=datetime_timezone.utc)
        with patch("backups.views.timezone.now", return_value=now):
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

    def test_invalid_required_schedule_is_unhealthy(
        self, client, required_target_with_invalid_schedule
    ):
        """A malformed required schedule is a data-risk contract violation."""
        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        assert data["status"] == "unhealthy"
        assert data["targets"][0]["status"] == "invalid_schedule"
        assert data["targets"][0]["schedule_required"] is True
        assert data["targets"][0]["next_scheduled"] is None
        named_target = data["targets_by_name"]["invalid-required-cron"]
        assert named_target["status"] == "invalid_schedule"
        assert named_target["schedule_required"] is True
        assert named_target["next_scheduled"] is None

    def test_paused_targets_excluded(self, client, active_target, paused_target):
        """Paused targets should not appear in health status."""
        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        target_names = [t["name"] for t in data["targets"]]
        assert "test-service" in target_names
        assert "paused-service" not in target_names

    def test_paused_required_target_is_unhealthy(
        self, client, paused_required_target
    ):
        """Pausing a required target remains visible as a Tier 1 failure."""
        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        assert data["status"] == "unhealthy"
        assert data["targets_by_name"]["paused-required"]["status"] == (
            "paused_required"
        )
        assert data["targets_by_name"]["paused-required"]["target_status"] == "paused"
        assert data["targets_by_name"]["paused-required"]["schedule_required"] is True
        assert data["targets_by_name"]["paused-required"]["next_scheduled"] is None
        assert data["targets_by_name"]["paused-required"]["overdue"] is False

    def test_paused_required_target_does_not_taint_healthy_required_entry(
        self, client, active_target, paused_required_target
    ):
        """A separate contract violation changes only aggregate health."""
        now = datetime(2026, 7, 25, 12, 0, tzinfo=datetime_timezone.utc)
        active_target.schedule_required = True
        active_target.save()
        BackupRun.objects.create(
            target=active_target,
            status=BackupRunStatus.SUCCESS,
            started_at=now - timedelta(hours=1),
            finished_at=now - timedelta(minutes=50),
        )

        with patch("backups.views.timezone.now", return_value=now):
            response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        active = data["targets_by_name"]["test-service"]
        paused = data["targets_by_name"]["paused-required"]
        assert data["status"] == "unhealthy"
        assert active["status"] == "ok"
        assert active["next_scheduled"] is not None
        assert paused["status"] == "paused_required"

    def test_paused_required_target_with_cleared_schedule_is_unhealthy(
        self, client, paused_required_target
    ):
        """Clearing a required schedule while paused cannot hide the target."""
        paused_required_target.schedule = ""
        paused_required_target.save()

        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        assert data["status"] == "unhealthy"
        target = data["targets_by_name"]["paused-required"]
        assert target["status"] == "paused_required"
        assert target["next_scheduled"] is None
        assert target["overdue"] is False

    def test_paused_required_target_omits_historical_failures(
        self, client, paused_required_target
    ):
        """Paused required state is the sole actionable health reason."""
        BackupRun.objects.create(
            target=paused_required_target,
            status=BackupRunStatus.FAILED,
            started_at=timezone.now() - timedelta(hours=1),
            finished_at=timezone.now() - timedelta(hours=1),
        )

        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        assert data["status"] == "unhealthy"
        assert data["targets_by_name"]["paused-required"]["status"] == (
            "paused_required"
        )
        assert data["recent_failures"] == []

    def test_disabled_required_target_is_absent_and_requires_external_assertion(
        self, client, db
    ):
        """Intentional retirement is absent from generic active-target health."""
        BackupTarget.objects.create(
            name="disabled-required",
            fastdeploy_service="echoport-backup",
            service_name="disabled-required.service",
            db_path="/tmp/disabled-required.db",
            schedule="",
            schedule_required=True,
            status="disabled",
        )

        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        assert data["status"] == "healthy"
        assert "disabled-required" not in data["targets_by_name"]

    def test_target_without_schedule(self, client, target_without_schedule):
        """Targets without schedule should show ok status (not overdue)."""
        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        assert data["status"] == "healthy"
        assert data["targets"][0]["status"] == "ok"
        assert data["targets"][0]["overdue"] is False
        assert data["targets"][0]["next_scheduled"] is None
        assert data["targets"][0]["schedule_required"] is False

    def test_required_target_without_schedule_is_unhealthy(
        self, client, target_missing_required_schedule
    ):
        """Required schedule drift should be visible as a data-risk condition."""
        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        assert data["status"] == "unhealthy"
        assert data["targets"][0]["status"] == "missing_schedule"
        assert data["targets"][0]["overdue"] is False
        assert data["targets"][0]["next_scheduled"] is None
        assert data["targets"][0]["schedule_required"] is True
        assert data["targets_by_name"]["required-schedule"] == data["targets"][0]

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

    def test_targets_by_name_contains_target_data(self, client, active_target):
        """targets_by_name should mirror target entries for stable lookup."""
        BackupRun.objects.create(
            target=active_target,
            status=BackupRunStatus.SUCCESS,
            started_at=timezone.now() - timedelta(hours=1),
            finished_at=timezone.now() - timedelta(minutes=50),
        )

        response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        assert "test-service" in data["targets_by_name"]
        target = data["targets_by_name"]["test-service"]
        assert target["name"] == "test-service"
        assert target["status"] == "ok"
        assert target["overdue"] is False

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
            service_name="overdue-target.service",
            db_path="/tmp/overdue.db",
            schedule="0 2 * * *",
            status="active",
        )

        # Target 2: recent success + recent failure (degraded)
        target2 = BackupTarget.objects.create(
            name="failed-target",
            fastdeploy_service="echoport-backup",
            service_name="failed-target.service",
            db_path="/tmp/failed.db",
            schedule="",  # No schedule so not overdue
            status="active",
        )
        now = datetime(2026, 7, 25, 12, 0, tzinfo=datetime_timezone.utc)
        BackupRun.objects.create(
            target=target2,
            status=BackupRunStatus.SUCCESS,
            started_at=now - timedelta(hours=2),
        )
        BackupRun.objects.create(
            target=target2,
            status=BackupRunStatus.FAILED,
            started_at=now - timedelta(hours=1),
        )

        with patch("backups.views.timezone.now", return_value=now):
            response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        # Overdue takes precedence -> unhealthy
        assert data["status"] == "unhealthy"

        # Check individual statuses
        statuses = {t["name"]: t["status"] for t in data["targets"]}
        assert statuses["overdue-target"] == "overdue"
        assert statuses["failed-target"] == "last_failed"

    def test_required_target_latest_failure_is_unhealthy(
        self, client, active_target
    ):
        """A current failed run on a required target is immediately critical."""
        now = datetime(2026, 7, 25, 12, 0, tzinfo=datetime_timezone.utc)
        active_target.schedule_required = True
        active_target.save()
        BackupRun.objects.create(
            target=active_target,
            status=BackupRunStatus.SUCCESS,
            started_at=now - timedelta(hours=2),
            finished_at=now - timedelta(hours=2),
        )
        BackupRun.objects.create(
            target=active_target,
            status=BackupRunStatus.FAILED,
            started_at=now - timedelta(hours=1),
            finished_at=now - timedelta(hours=1),
        )

        with patch("backups.views.timezone.now", return_value=now):
            response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        assert data["status"] == "unhealthy"
        assert data["targets_by_name"]["test-service"]["status"] == "last_failed"

    def test_required_target_recovery_keeps_only_degraded_failure_history(
        self, client, active_target
    ):
        """A later success clears critical state without erasing failure history."""
        now = datetime(2026, 7, 25, 12, 0, tzinfo=datetime_timezone.utc)
        active_target.schedule_required = True
        active_target.save()
        BackupRun.objects.create(
            target=active_target,
            status=BackupRunStatus.FAILED,
            started_at=now - timedelta(hours=2),
            finished_at=now - timedelta(hours=2),
        )
        BackupRun.objects.create(
            target=active_target,
            status=BackupRunStatus.SUCCESS,
            started_at=now - timedelta(hours=1),
            finished_at=now - timedelta(minutes=50),
        )

        with patch("backups.views.timezone.now", return_value=now):
            response = client.get(reverse("backups:health_status"))
        data = json.loads(response.content)

        assert data["status"] == "degraded"
        assert data["targets_by_name"]["test-service"]["status"] == "ok"
        assert len(data["recent_failures"]) == 1


class TestHealthOverdueGrace:
    """Scheduled runs in progress or within the grace window are not overdue."""

    # Cron "0 2 * * *": the previous scheduled time for these instants is
    # 2026-07-25 02:00 UTC.
    CRON_TIME = datetime(2026, 7, 25, 2, 0, tzinfo=datetime_timezone.utc)

    def _get(self, client, now):
        with patch("backups.views.timezone.now", return_value=now):
            response = client.get(reverse("backups:health_status"))
        return json.loads(response.content)

    def _yesterdays_success(self, target):
        started = self.CRON_TIME - timedelta(days=1)
        BackupRun.objects.create(
            target=target,
            status=BackupRunStatus.SUCCESS,
            started_at=started,
            finished_at=started + timedelta(minutes=5),
        )

    @pytest.fixture
    def required_target(self, active_target):
        active_target.schedule_required = True
        active_target.save()
        return active_target

    @pytest.mark.parametrize("run_status", [BackupRunStatus.PENDING, BackupRunStatus.RUNNING])
    def test_run_in_progress_after_cron_time_is_not_overdue(
        self, client, required_target, settings, run_status
    ):
        settings.ECHOPORT_HEALTH_OVERDUE_GRACE_MINUTES = 30
        self._yesterdays_success(required_target)
        BackupRun.objects.create(
            target=required_target,
            status=run_status,
            started_at=self.CRON_TIME + timedelta(minutes=40),
        )

        data = self._get(client, self.CRON_TIME + timedelta(minutes=50))
        target = data["targets_by_name"]["test-service"]

        assert data["status"] == "healthy"
        assert target["status"] == "ok"
        assert target["overdue"] is False
        assert target["active_run"] is True
        assert "overdue_hours" not in target

    def test_within_grace_window_without_run_is_not_overdue(
        self, client, required_target, settings
    ):
        settings.ECHOPORT_HEALTH_OVERDUE_GRACE_MINUTES = 30
        self._yesterdays_success(required_target)

        data = self._get(client, self.CRON_TIME + timedelta(minutes=10))
        target = data["targets_by_name"]["test-service"]

        assert data["status"] == "healthy"
        assert target["status"] == "ok"
        assert target["overdue"] is False
        assert target["active_run"] is False
        assert target["grace_until"] == (
            self.CRON_TIME + timedelta(minutes=30)
        ).isoformat()

    def test_grace_window_passed_without_success_is_overdue(
        self, client, required_target, settings
    ):
        settings.ECHOPORT_HEALTH_OVERDUE_GRACE_MINUTES = 30
        self._yesterdays_success(required_target)

        data = self._get(client, self.CRON_TIME + timedelta(minutes=90))
        target = data["targets_by_name"]["test-service"]

        assert data["status"] == "unhealthy"
        assert target["status"] == "overdue"
        assert target["overdue"] is True
        assert target["grace_until"] is None
        # Measured from the end of the grace window, not the cron time.
        assert target["overdue_hours"] == 1.0

    def test_zero_grace_restores_immediate_overdue(
        self, client, required_target, settings
    ):
        settings.ECHOPORT_HEALTH_OVERDUE_GRACE_MINUTES = 0
        self._yesterdays_success(required_target)

        data = self._get(client, self.CRON_TIME + timedelta(minutes=1))

        assert data["targets_by_name"]["test-service"]["status"] == "overdue"

    def test_failed_run_after_cron_time_within_grace_is_last_failed(
        self, client, required_target, settings
    ):
        settings.ECHOPORT_HEALTH_OVERDUE_GRACE_MINUTES = 30
        self._yesterdays_success(required_target)
        BackupRun.objects.create(
            target=required_target,
            status=BackupRunStatus.FAILED,
            started_at=self.CRON_TIME + timedelta(minutes=1),
            finished_at=self.CRON_TIME + timedelta(minutes=5),
        )

        data = self._get(client, self.CRON_TIME + timedelta(minutes=10))

        assert data["status"] == "unhealthy"
        assert data["targets_by_name"]["test-service"]["status"] == "last_failed"

    def test_failed_run_after_cron_time_past_grace_is_overdue(
        self, client, required_target, settings
    ):
        settings.ECHOPORT_HEALTH_OVERDUE_GRACE_MINUTES = 30
        self._yesterdays_success(required_target)
        BackupRun.objects.create(
            target=required_target,
            status=BackupRunStatus.FAILED,
            started_at=self.CRON_TIME + timedelta(minutes=1),
            finished_at=self.CRON_TIME + timedelta(minutes=5),
        )

        data = self._get(client, self.CRON_TIME + timedelta(hours=2))
        target = data["targets_by_name"]["test-service"]

        assert data["status"] == "unhealthy"
        assert target["status"] == "overdue"
        assert target["overdue"] is True
        assert len(data["recent_failures"]) == 1

    def test_stuck_run_does_not_hide_overdue_forever(
        self, client, required_target, settings
    ):
        settings.ECHOPORT_HEALTH_OVERDUE_GRACE_MINUTES = 30
        required_target.timeout_seconds = 600
        required_target.save()
        self._yesterdays_success(required_target)
        BackupRun.objects.create(
            target=required_target,
            status=BackupRunStatus.RUNNING,
            started_at=self.CRON_TIME + timedelta(minutes=1),
        )

        # Started 02:01; bound = 10 min timeout + 30 min grace -> 02:41.
        data = self._get(client, self.CRON_TIME + timedelta(minutes=45))
        target = data["targets_by_name"]["test-service"]

        assert target["status"] == "overdue"
        assert target["active_run"] is True

    def test_run_started_before_cron_time_does_not_count(
        self, client, required_target, settings
    ):
        settings.ECHOPORT_HEALTH_OVERDUE_GRACE_MINUTES = 30
        self._yesterdays_success(required_target)
        BackupRun.objects.create(
            target=required_target,
            status=BackupRunStatus.RUNNING,
            started_at=self.CRON_TIME - timedelta(minutes=5),
        )

        data = self._get(client, self.CRON_TIME + timedelta(minutes=40))

        assert data["targets_by_name"]["test-service"]["status"] == "overdue"

    def test_grace_is_not_renewed_by_later_cron_ticks(
        self, client, required_target, settings
    ):
        """With an hourly schedule, missed cycles must not keep renewing grace."""
        settings.ECHOPORT_HEALTH_OVERDUE_GRACE_MINUTES = 60
        required_target.schedule = "0 * * * *"
        required_target.save()
        BackupRun.objects.create(
            target=required_target,
            status=BackupRunStatus.SUCCESS,
            started_at=self.CRON_TIME,
            finished_at=self.CRON_TIME + timedelta(minutes=5),
        )

        # First missed cycle 03:00, deadline 04:00; at 04:10 the 04:00 tick is
        # inside a fresh 60-minute window but the target is still overdue.
        data = self._get(client, self.CRON_TIME + timedelta(hours=2, minutes=10))
        target = data["targets_by_name"]["test-service"]
        assert target["status"] == "overdue"
        assert target["overdue_hours"] == 0.2

        # Inside the first missed cycle's grace window it is not overdue.
        data = self._get(client, self.CRON_TIME + timedelta(hours=1, minutes=30))
        assert data["targets_by_name"]["test-service"]["status"] == "ok"

    def test_in_progress_run_does_not_cover_earlier_missed_cycle(
        self, client, required_target, settings
    ):
        settings.ECHOPORT_HEALTH_OVERDUE_GRACE_MINUTES = 30
        # Last success two days before the cron time: yesterday's cycle missed.
        BackupRun.objects.create(
            target=required_target,
            status=BackupRunStatus.SUCCESS,
            started_at=self.CRON_TIME - timedelta(days=2),
            finished_at=self.CRON_TIME - timedelta(days=2) + timedelta(minutes=5),
        )
        BackupRun.objects.create(
            target=required_target,
            status=BackupRunStatus.RUNNING,
            started_at=self.CRON_TIME + timedelta(minutes=1),
        )

        data = self._get(client, self.CRON_TIME + timedelta(minutes=5))

        assert data["targets_by_name"]["test-service"]["status"] == "overdue"

    def test_health_queries_do_not_scale_with_targets(
        self, client, db, settings, django_assert_max_num_queries
    ):
        for i in range(5):
            target = BackupTarget.objects.create(
                name=f"svc-{i}",
                fastdeploy_service="echoport-backup",
                service_name=f"svc-{i}.service",
                db_path=f"/tmp/svc-{i}.db",
                schedule="0 2 * * *",
                status="active",
            )
            self._yesterdays_success(target)

        with django_assert_max_num_queries(3):
            self._get(client, self.CRON_TIME + timedelta(minutes=10))
