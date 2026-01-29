"""
Tests for scheduled backup functionality.
"""

from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from django.utils import timezone

from backups.backup_engine import BackupError
from backups.management.commands.run_scheduled_backups import Command
from backups.models import BackupRun, BackupRunStatus, BackupStatus, BackupTarget, BackupTrigger
from backups.templatetags.backup_tags import next_scheduled_run


@pytest.fixture
def scheduled_target(db):
    """Create a test backup target with a schedule."""
    return BackupTarget.objects.create(
        name="scheduled-test",
        description="Test target with schedule",
        fastdeploy_service="echoport-backup",
        db_path="/tmp/test.db",
        schedule="0 2 * * *",  # Daily at 2am
        status=BackupStatus.ACTIVE,
    )


@pytest.fixture
def unscheduled_target(db):
    """Create a test backup target without a schedule."""
    return BackupTarget.objects.create(
        name="unscheduled-test",
        description="Test target without schedule",
        fastdeploy_service="echoport-backup",
        db_path="/tmp/test.db",
        schedule="",
        status=BackupStatus.ACTIVE,
    )


@pytest.fixture
def paused_target(db):
    """Create a paused backup target with a schedule."""
    return BackupTarget.objects.create(
        name="paused-test",
        description="Paused target",
        fastdeploy_service="echoport-backup",
        db_path="/tmp/test.db",
        schedule="0 2 * * *",
        status=BackupStatus.PAUSED,
    )


@pytest.fixture
def invalid_schedule_target(db):
    """Create a target with an invalid cron schedule."""
    return BackupTarget.objects.create(
        name="invalid-schedule-test",
        description="Target with invalid schedule",
        fastdeploy_service="echoport-backup",
        db_path="/tmp/test.db",
        schedule="not a valid cron expression",
        status=BackupStatus.ACTIVE,
    )


class TestIsDueForBackup:
    """Tests for the _is_due_for_backup logic."""

    def test_is_due_no_previous_run(self, scheduled_target):
        """Target with no previous scheduled run should be due."""
        command = Command()
        now = timezone.now()

        assert command._is_due_for_backup(scheduled_target, now) is True

    def test_is_due_after_scheduled_time(self, scheduled_target):
        """Target is due if last run was before the scheduled time."""
        # Create a run from 2 days ago
        old_run = BackupRun.objects.create(
            target=scheduled_target,
            status=BackupRunStatus.SUCCESS,
            trigger=BackupTrigger.SCHEDULED,
            triggered_by="scheduler",
        )
        old_run.started_at = timezone.now() - timedelta(days=2)
        old_run.save()

        command = Command()
        now = timezone.now()

        # The last scheduled time (2am today or yesterday) is after the old run
        assert command._is_due_for_backup(scheduled_target, now) is True

    def test_not_due_before_scheduled_time(self, scheduled_target):
        """Target is not due if last run was after the most recent scheduled time."""
        # Create a very recent run (within the last hour)
        recent_run = BackupRun.objects.create(
            target=scheduled_target,
            status=BackupRunStatus.SUCCESS,
            trigger=BackupTrigger.SCHEDULED,
            triggered_by="scheduler",
        )
        recent_run.started_at = timezone.now() - timedelta(minutes=30)
        recent_run.save()

        command = Command()
        # Check at a time shortly after the run
        check_time = recent_run.started_at + timedelta(minutes=5)

        # The run is more recent than the last scheduled 2am, so not due
        assert command._is_due_for_backup(scheduled_target, check_time) is False

    def test_not_due_empty_schedule(self, unscheduled_target):
        """Target without schedule should never be due."""
        command = Command()
        now = timezone.now()

        assert command._is_due_for_backup(unscheduled_target, now) is False

    def test_manual_runs_do_not_affect_schedule(self, scheduled_target):
        """Manual runs should not count as scheduled runs."""
        # Create a recent manual run
        manual_run = BackupRun.objects.create(
            target=scheduled_target,
            status=BackupRunStatus.SUCCESS,
            trigger=BackupTrigger.MANUAL,  # Manual, not scheduled
            triggered_by="user",
        )
        manual_run.started_at = timezone.now() - timedelta(minutes=30)
        manual_run.save()

        command = Command()
        now = timezone.now()

        # Should still be due because there are no scheduled runs
        assert command._is_due_for_backup(scheduled_target, now) is True

    def test_invalid_cron_returns_false(self, invalid_schedule_target):
        """Invalid cron expressions should return False, not raise."""
        command = Command()
        command.stderr = MagicMock()  # Suppress output
        now = timezone.now()

        # Should not raise, should return False
        assert command._is_due_for_backup(invalid_schedule_target, now) is False


class TestGetLastScheduledRun:
    """Tests for the get_last_scheduled_run model method."""

    def test_returns_none_when_no_scheduled_runs(self, scheduled_target):
        """Should return None if there are no scheduled runs."""
        assert scheduled_target.get_last_scheduled_run() is None

    def test_returns_latest_scheduled_run(self, scheduled_target):
        """Should return the most recent scheduled run."""
        # Create multiple runs
        old_run = BackupRun.objects.create(
            target=scheduled_target,
            status=BackupRunStatus.SUCCESS,
            trigger=BackupTrigger.SCHEDULED,
        )
        old_run.started_at = timezone.now() - timedelta(days=2)
        old_run.save()

        new_run = BackupRun.objects.create(
            target=scheduled_target,
            status=BackupRunStatus.SUCCESS,
            trigger=BackupTrigger.SCHEDULED,
        )
        new_run.started_at = timezone.now() - timedelta(hours=1)
        new_run.save()

        # Also create a manual run that should be ignored
        BackupRun.objects.create(
            target=scheduled_target,
            status=BackupRunStatus.SUCCESS,
            trigger=BackupTrigger.MANUAL,
        )

        last_scheduled = scheduled_target.get_last_scheduled_run()
        assert last_scheduled == new_run

    def test_includes_failed_runs(self, scheduled_target):
        """Should include failed scheduled runs in consideration."""
        failed_run = BackupRun.objects.create(
            target=scheduled_target,
            status=BackupRunStatus.FAILED,
            trigger=BackupTrigger.SCHEDULED,
            error_message="Test failure",
        )

        last_scheduled = scheduled_target.get_last_scheduled_run()
        assert last_scheduled == failed_run


class TestNextScheduledRun:
    """Tests for the next_scheduled_run template tag."""

    def test_returns_next_time(self, scheduled_target):
        """Should return the next scheduled time."""
        result = next_scheduled_run(scheduled_target)

        assert result is not None
        # The next run should be in the future
        assert result > timezone.now()

    def test_returns_none_for_no_schedule(self, unscheduled_target):
        """Should return None for targets without a schedule."""
        result = next_scheduled_run(unscheduled_target)
        assert result is None

    def test_returns_none_for_invalid_schedule(self, invalid_schedule_target):
        """Should return None for invalid cron expressions."""
        result = next_scheduled_run(invalid_schedule_target)
        assert result is None


@pytest.mark.django_db
class TestRunScheduledBackupsCommand:
    """Integration tests for the management command."""

    def test_command_skips_inactive_targets(self, paused_target, capsys):
        """Paused/disabled targets should be skipped."""
        command = Command()
        with pytest.raises(SystemExit) as exc_info:
            command.handle(dry_run=True)
        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert "paused-test" not in captured.out

    def test_command_skips_targets_without_schedule(self, unscheduled_target, capsys):
        """Targets without schedule should be skipped."""
        command = Command()
        with pytest.raises(SystemExit) as exc_info:
            command.handle(dry_run=True)
        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert "unscheduled-test" not in captured.out

    def test_dry_run_does_not_trigger_backups(self, scheduled_target, capsys):
        """Dry run should show what would run but not actually trigger."""
        command = Command()
        with pytest.raises(SystemExit) as exc_info:
            command.handle(dry_run=True)
        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert "DRY RUN" in captured.out
        assert "Would trigger backup" in captured.out
        assert scheduled_target.name in captured.out

        # No backup should have been created
        assert BackupRun.objects.filter(target=scheduled_target).count() == 0

    @patch("backups.management.commands.run_scheduled_backups.start_backup")
    def test_command_triggers_due_backups(self, mock_start_backup, scheduled_target, capsys):
        """Command should trigger backups for due targets."""
        # Mock successful backup
        mock_run = BackupRun(
            target=scheduled_target,
            status=BackupRunStatus.SUCCESS,
            trigger=BackupTrigger.SCHEDULED,
            storage_key="test/backup.tar.gz",
            size_bytes=12345,
        )
        mock_start_backup.return_value = mock_run

        command = Command()
        with pytest.raises(SystemExit) as exc_info:
            command.handle(dry_run=False)
        assert exc_info.value.code == 0

        # Verify backup was triggered
        mock_start_backup.assert_called_once()
        call_kwargs = mock_start_backup.call_args
        assert call_kwargs[0][0] == scheduled_target
        assert call_kwargs[1]["trigger"] == BackupTrigger.SCHEDULED
        assert call_kwargs[1]["triggered_by"] == "scheduler"

    @patch("backups.management.commands.run_scheduled_backups.start_backup")
    @patch("backups.management.commands.run_scheduled_backups.get_active_run")
    def test_command_skips_active_backup(
        self, mock_get_active_run, mock_start_backup, scheduled_target, capsys
    ):
        """Command should skip targets with active backups."""
        mock_get_active_run.return_value = BackupRun(
            target=scheduled_target,
            status=BackupRunStatus.RUNNING,
        )

        command = Command()
        with pytest.raises(SystemExit) as exc_info:
            command.handle(dry_run=False)
        assert exc_info.value.code == 0

        # Backup should not have been started
        mock_start_backup.assert_not_called()

        captured = capsys.readouterr()
        assert "backup already in progress" in captured.out

    @patch("backups.management.commands.run_scheduled_backups.start_backup")
    def test_command_exits_nonzero_on_backup_failure(
        self, mock_start_backup, scheduled_target, capsys
    ):
        """Command should exit with code 1 when backups fail."""
        # Mock failed backup
        mock_run = BackupRun(
            target=scheduled_target,
            status=BackupRunStatus.FAILED,
            trigger=BackupTrigger.SCHEDULED,
            error_message="Backup failed for testing",
        )
        mock_start_backup.return_value = mock_run

        command = Command()
        with pytest.raises(SystemExit) as exc_info:
            command.handle(dry_run=False)
        assert exc_info.value.code == 1

        captured = capsys.readouterr()
        assert "Failed" in captured.err
        assert "errors" in captured.err

    @patch("backups.management.commands.run_scheduled_backups.start_backup")
    def test_command_exits_nonzero_on_backup_error(
        self, mock_start_backup, scheduled_target, capsys
    ):
        """Command should exit with code 1 when BackupError is raised."""
        mock_start_backup.side_effect = BackupError("Test backup error")

        command = Command()
        with pytest.raises(SystemExit) as exc_info:
            command.handle(dry_run=False)
        assert exc_info.value.code == 1

        captured = capsys.readouterr()
        assert "Test backup error" in captured.err

    def test_command_handles_invalid_schedule(self, invalid_schedule_target, capsys):
        """Command should skip targets with invalid schedules gracefully."""
        command = Command()
        with pytest.raises(SystemExit) as exc_info:
            command.handle(dry_run=False)
        # Should still complete successfully (no backups to run = no errors)
        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert "invalid schedule" in captured.err
