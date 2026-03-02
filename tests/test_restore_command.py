"""
Tests for the restore management command and UI self-restore block.
"""

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from unittest.mock import MagicMock, patch

from backups.models import (
    BackupRun,
    BackupRunStatus,
    BackupTarget,
    RestoreRun,
    RestoreRunStatus,
)


@pytest.fixture
def restore_target(db):
    """Create a test backup target for restore tests."""
    return BackupTarget.objects.create(
        name="test-restore",
        description="Test restore target",
        fastdeploy_service="echoport-backup",
        service_name="test-restore.service",
        db_path="/tmp/test.db",
        backup_files=["/tmp/test.txt"],
        status="active",
    )


@pytest.fixture
def successful_backup(restore_target):
    """Create a successful backup run for restore tests."""
    return BackupRun.objects.create(
        target=restore_target,
        status=BackupRunStatus.SUCCESS,
        storage_bucket="backups",
        storage_key="test-restore/2026-01-01T00-00-00.tar.gz",
        size_bytes=1024,
        checksum_sha256="abc123def456" * 4 + "abcd1234abcd1234",
        file_count=3,
    )


@pytest.mark.django_db
class TestRestoreCommand:
    """Tests for manage.py restore command."""

    def test_target_not_found(self, capsys):
        with pytest.raises(CommandError, match="not found"):
            call_command("restore", "nonexistent", "1")

    def test_target_not_active(self, db):
        BackupTarget.objects.create(
            name="paused-target",
            fastdeploy_service="echoport-backup",
            service_name="paused-target.service",
            db_path="/tmp/test.db",
            status="paused",
        )
        with pytest.raises(CommandError, match="not active"):
            call_command("restore", "paused-target", "1")

    def test_backup_run_not_found(self, restore_target):
        with pytest.raises(CommandError, match="not found"):
            call_command("restore", "test-restore", "99999")

    def test_backup_run_not_successful(self, restore_target):
        run = BackupRun.objects.create(
            target=restore_target,
            status=BackupRunStatus.FAILED,
            error_message="test failure",
        )
        with pytest.raises(CommandError, match="expected 'success'"):
            call_command("restore", "test-restore", str(run.id))

    def test_backup_run_no_checksum(self, restore_target):
        run = BackupRun.objects.create(
            target=restore_target,
            status=BackupRunStatus.SUCCESS,
            checksum_sha256="",
        )
        with pytest.raises(CommandError, match="no checksum"):
            call_command("restore", "test-restore", str(run.id))

    @patch("backups.management.commands.restore.start_restore")
    def test_successful_restore(self, mock_start, restore_target, successful_backup, capsys):
        mock_run = MagicMock()
        mock_run.status = RestoreRunStatus.SUCCESS
        mock_run.id = 42
        mock_run.files_restored = 5
        mock_run.duration_seconds = 12.3
        mock_start.return_value = mock_run

        call_command("restore", "test-restore", str(successful_backup.id))

        mock_start.assert_called_once_with(
            successful_backup,
            triggered_by="cli",
        )
        captured = capsys.readouterr()
        assert "successfully" in captured.out
        assert "42" in captured.out

    @patch("backups.management.commands.restore.start_restore")
    def test_successful_restore_with_unknown_duration(self, mock_start, restore_target, successful_backup, capsys):
        mock_run = MagicMock()
        mock_run.status = RestoreRunStatus.SUCCESS
        mock_run.id = 43
        mock_run.files_restored = 5
        mock_run.duration_seconds = None
        mock_start.return_value = mock_run

        call_command("restore", "test-restore", str(successful_backup.id))

        captured = capsys.readouterr()
        assert "Duration: unknown" in captured.out

    @patch("backups.management.commands.restore.start_restore")
    def test_failed_restore(self, mock_start, restore_target, successful_backup):
        mock_run = MagicMock()
        mock_run.status = RestoreRunStatus.FAILED
        mock_run.error_message = "health check failed"
        mock_start.return_value = mock_run

        with pytest.raises(CommandError, match="Restore failed"):
            call_command("restore", "test-restore", str(successful_backup.id))

    @patch("backups.management.commands.restore.start_restore")
    def test_triggered_by_argument(self, mock_start, restore_target, successful_backup):
        mock_run = MagicMock()
        mock_run.status = RestoreRunStatus.SUCCESS
        mock_run.id = 1
        mock_run.files_restored = 0
        mock_run.duration_seconds = 1.0
        mock_start.return_value = mock_run

        call_command(
            "restore", "test-restore", str(successful_backup.id),
            triggered_by="operator-joe",
        )

        mock_start.assert_called_once_with(
            successful_backup,
            triggered_by="operator-joe",
        )

    def test_backup_run_wrong_target(self, restore_target, db):
        other = BackupTarget.objects.create(
            name="other-target",
            fastdeploy_service="echoport-backup",
            service_name="other-target.service",
            db_path="/tmp/other.db",
            status="active",
        )
        run = BackupRun.objects.create(
            target=other,
            status=BackupRunStatus.SUCCESS,
            checksum_sha256="x" * 64,
        )
        with pytest.raises(CommandError, match="not found"):
            call_command("restore", "test-restore", str(run.id))


@pytest.mark.django_db
class TestSelfRestoreUIBlock:
    """Tests for the UI self-restore block on echoport target."""

    def _make_echoport_target(self):
        return BackupTarget.objects.create(
            name="echoport",
            description="Echoport self-backup",
            fastdeploy_service="echoport-self-backup",
            service_name="echoport.service",
            db_path="/home/echoport/site/src/django/db.sqlite3",
            status="active",
        )

    def test_ui_restore_blocked_for_echoport_target(self, client, db):
        from django.contrib.auth.models import User

        user = User.objects.create_superuser("admin", "admin@test.com", "testpass")
        client.force_login(user)

        target = self._make_echoport_target()
        backup_run = BackupRun.objects.create(
            target=target,
            status=BackupRunStatus.SUCCESS,
            checksum_sha256="a" * 64,
            storage_key="echoport/test.tar.gz",
        )

        response = client.post(f"/runs/{backup_run.id}/restore/", follow=True)
        # Should redirect without creating a restore run
        assert response.status_code == 200
        assert RestoreRun.objects.count() == 0
        # Should surface a warning message to the user
        messages = list(response.context["messages"])
        assert len(messages) == 1
        assert "manage.py restore echoport" in str(messages[0])

    def test_ui_restore_allowed_for_non_echoport_target(self, client, db):
        from django.contrib.auth.models import User

        user = User.objects.create_superuser("admin", "admin@test.com", "testpass")
        client.force_login(user)

        target = BackupTarget.objects.create(
            name="other-service",
            fastdeploy_service="echoport-backup",
            service_name="other-service.service",
            db_path="/tmp/other.db",
            status="active",
        )
        backup_run = BackupRun.objects.create(
            target=target,
            status=BackupRunStatus.SUCCESS,
            checksum_sha256="b" * 64,
            storage_key="other/test.tar.gz",
        )

        with patch("backups.views._run_restore_in_thread"):
            response = client.post(f"/runs/{backup_run.id}/restore/")
        # Should proceed (redirect to restore detail)
        assert response.status_code == 302
        assert RestoreRun.objects.count() == 1
