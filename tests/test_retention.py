"""
Tests for backup retention policy enforcement.
"""

from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from django.utils import timezone

from backups.management.commands.cleanup_old_backups import Command, get_backups_to_delete
from backups.models import (
    BackupRun,
    BackupRunStatus,
    BackupStatus,
    BackupTarget,
    RestoreRun,
    RestoreRunStatus,
)


@pytest.fixture
def target_with_retention(db):
    """Create a test backup target with 7-day retention."""
    return BackupTarget.objects.create(
        name="retention-test",
        description="Test target for retention",
        fastdeploy_service="echoport-backup",
        db_path="/tmp/test.db",
        retention_days=7,
        status=BackupStatus.ACTIVE,
        storage_bucket="backups",
    )


@pytest.fixture
def old_successful_backup(target_with_retention):
    """Create a successful backup older than retention period."""
    backup = BackupRun.objects.create(
        target=target_with_retention,
        status=BackupRunStatus.SUCCESS,
        storage_bucket="backups",
        storage_key="retention-test/2026-01-01T02-00-00.tar.gz",
        size_bytes=12345,
        checksum_sha256="abc123",
    )
    # Set finished_at to 10 days ago (older than 7-day retention)
    backup.finished_at = timezone.now() - timedelta(days=10)
    backup.save()
    return backup


@pytest.fixture
def recent_successful_backup(target_with_retention):
    """Create a successful backup within retention period."""
    backup = BackupRun.objects.create(
        target=target_with_retention,
        status=BackupRunStatus.SUCCESS,
        storage_bucket="backups",
        storage_key="retention-test/2026-01-25T02-00-00.tar.gz",
        size_bytes=12345,
        checksum_sha256="def456",
    )
    backup.finished_at = timezone.now() - timedelta(days=3)
    backup.save()
    return backup


class TestGetBackupsToDelete:
    """Tests for the get_backups_to_delete function."""

    def test_returns_old_successful_backups(self, target_with_retention, old_successful_backup):
        """Old successful backups should be returned for deletion."""
        backups = get_backups_to_delete(target_with_retention)
        assert len(backups) == 1
        assert backups[0] == old_successful_backup

    def test_excludes_recent_backups(
        self, target_with_retention, old_successful_backup, recent_successful_backup
    ):
        """Recent backups within retention period should not be deleted."""
        backups = get_backups_to_delete(target_with_retention)
        assert len(backups) == 1
        assert old_successful_backup in backups
        assert recent_successful_backup not in backups

    def test_excludes_failed_backups(self, target_with_retention):
        """Failed backups should not be deleted (nothing in MinIO to delete)."""
        failed_backup = BackupRun.objects.create(
            target=target_with_retention,
            status=BackupRunStatus.FAILED,
            error_message="Test failure",
        )
        failed_backup.finished_at = timezone.now() - timedelta(days=10)
        failed_backup.save()

        backups = get_backups_to_delete(target_with_retention)
        assert len(backups) == 0

    def test_excludes_timeout_backups(self, target_with_retention):
        """Timeout backups should not be deleted."""
        timeout_backup = BackupRun.objects.create(
            target=target_with_retention,
            status=BackupRunStatus.TIMEOUT,
            error_message="Timeout",
        )
        timeout_backup.finished_at = timezone.now() - timedelta(days=10)
        timeout_backup.save()

        backups = get_backups_to_delete(target_with_retention)
        assert len(backups) == 0

    def test_excludes_backups_with_restore_runs(self, target_with_retention, old_successful_backup):
        """Backups that have been used for restore should not be deleted."""
        # Create a restore run referencing this backup
        RestoreRun.objects.create(
            backup_run=old_successful_backup,
            target=target_with_retention,
            status=RestoreRunStatus.SUCCESS,
        )

        backups = get_backups_to_delete(target_with_retention)
        assert len(backups) == 0

    def test_excludes_backups_with_failed_restore_runs(
        self, target_with_retention, old_successful_backup
    ):
        """Even failed restore attempts should protect the backup from deletion."""
        RestoreRun.objects.create(
            backup_run=old_successful_backup,
            target=target_with_retention,
            status=RestoreRunStatus.FAILED,
            error_message="Failed restore",
        )

        backups = get_backups_to_delete(target_with_retention)
        assert len(backups) == 0

    def test_respects_custom_retention_days(self, db):
        """Should respect different retention_days per target."""
        target_short = BackupTarget.objects.create(
            name="short-retention",
            fastdeploy_service="echoport-backup",
            retention_days=3,
            status=BackupStatus.ACTIVE,
        )
        target_long = BackupTarget.objects.create(
            name="long-retention",
            fastdeploy_service="echoport-backup",
            retention_days=30,
            status=BackupStatus.ACTIVE,
        )

        # Create backup 5 days old for both
        for target in [target_short, target_long]:
            backup = BackupRun.objects.create(
                target=target,
                status=BackupRunStatus.SUCCESS,
                storage_bucket="backups",
                storage_key=f"{target.name}/backup.tar.gz",
            )
            backup.finished_at = timezone.now() - timedelta(days=5)
            backup.save()

        # Short retention (3 days) should have the backup marked for deletion
        short_backups = get_backups_to_delete(target_short)
        assert len(short_backups) == 1

        # Long retention (30 days) should keep the backup
        long_backups = get_backups_to_delete(target_long)
        assert len(long_backups) == 0

    def test_returns_empty_for_no_backups(self, target_with_retention):
        """Should return empty list when no backups exist."""
        backups = get_backups_to_delete(target_with_retention)
        assert backups == []


class TestCleanupCommand:
    """Tests for the cleanup_old_backups management command."""

    @patch("backups.management.commands.cleanup_old_backups.delete_object")
    def test_dry_run_does_not_delete(
        self, mock_delete, target_with_retention, old_successful_backup, capsys
    ):
        """Dry run should show what would be deleted without deleting."""
        command = Command()
        with pytest.raises(SystemExit) as exc_info:
            command.handle(dry_run=True, target=None)
        assert exc_info.value.code == 0

        # delete_object should not be called in dry run
        mock_delete.assert_not_called()

        # Backup should still exist in DB
        assert BackupRun.objects.filter(pk=old_successful_backup.pk).exists()

        captured = capsys.readouterr()
        assert "DRY RUN" in captured.out
        assert "Would delete" in captured.out

    @patch("backups.management.commands.cleanup_old_backups.delete_object")
    def test_deletes_from_minio_then_database(
        self, mock_delete, target_with_retention, old_successful_backup
    ):
        """Should delete from MinIO first, then from database."""
        mock_delete.return_value = True

        command = Command()
        with pytest.raises(SystemExit) as exc_info:
            command.handle(dry_run=False, target=None)
        assert exc_info.value.code == 0

        # MinIO deletion should be called
        mock_delete.assert_called_once_with(
            old_successful_backup.storage_bucket,
            old_successful_backup.storage_key,
        )

        # Backup should be deleted from DB
        assert not BackupRun.objects.filter(pk=old_successful_backup.pk).exists()

    @patch("backups.management.commands.cleanup_old_backups.delete_object")
    def test_preserves_db_on_minio_failure(
        self, mock_delete, target_with_retention, old_successful_backup, capsys
    ):
        """Should preserve DB record if MinIO deletion fails."""
        mock_delete.return_value = False

        command = Command()
        with pytest.raises(SystemExit) as exc_info:
            command.handle(dry_run=False, target=None)
        assert exc_info.value.code == 1  # Error exit code

        # Backup should still exist in DB
        assert BackupRun.objects.filter(pk=old_successful_backup.pk).exists()

        captured = capsys.readouterr()
        assert "Failed to delete from MinIO" in captured.err

    @patch("backups.management.commands.cleanup_old_backups.delete_object")
    def test_target_filter(
        self, mock_delete, target_with_retention, old_successful_backup, capsys
    ):
        """Should only cleanup specified target when --target is used."""
        mock_delete.return_value = True

        # Create another target with old backup
        other_target = BackupTarget.objects.create(
            name="other-target",
            fastdeploy_service="echoport-backup",
            retention_days=7,
            status=BackupStatus.ACTIVE,
        )
        other_backup = BackupRun.objects.create(
            target=other_target,
            status=BackupRunStatus.SUCCESS,
            storage_bucket="backups",
            storage_key="other-target/backup.tar.gz",
        )
        other_backup.finished_at = timezone.now() - timedelta(days=10)
        other_backup.save()

        command = Command()
        with pytest.raises(SystemExit) as exc_info:
            command.handle(dry_run=False, target="retention-test")
        assert exc_info.value.code == 0

        # Only retention-test backup should be deleted
        assert not BackupRun.objects.filter(pk=old_successful_backup.pk).exists()
        assert BackupRun.objects.filter(pk=other_backup.pk).exists()

    def test_target_not_found(self, db, capsys):
        """Should error when target name doesn't exist."""
        command = Command()
        with pytest.raises(SystemExit) as exc_info:
            command.handle(dry_run=False, target="nonexistent")
        assert exc_info.value.code == 1

        captured = capsys.readouterr()
        assert "Target not found" in captured.err

    @patch("backups.management.commands.cleanup_old_backups.delete_object")
    def test_skips_inactive_targets(self, mock_delete, db, capsys):
        """Should skip paused and disabled targets."""
        paused_target = BackupTarget.objects.create(
            name="paused-target",
            fastdeploy_service="echoport-backup",
            retention_days=7,
            status=BackupStatus.PAUSED,
        )
        backup = BackupRun.objects.create(
            target=paused_target,
            status=BackupRunStatus.SUCCESS,
            storage_bucket="backups",
            storage_key="paused-target/backup.tar.gz",
        )
        backup.finished_at = timezone.now() - timedelta(days=10)
        backup.save()

        command = Command()
        with pytest.raises(SystemExit) as exc_info:
            command.handle(dry_run=False, target=None)
        assert exc_info.value.code == 0

        # Backup should not be deleted (target is paused)
        mock_delete.assert_not_called()
        assert BackupRun.objects.filter(pk=backup.pk).exists()

    @patch("backups.management.commands.cleanup_old_backups.delete_object")
    def test_skips_backup_without_storage_info(
        self, mock_delete, target_with_retention, capsys
    ):
        """Should skip backups with missing storage info to avoid orphans."""
        # This shouldn't happen for SUCCESS backups, but treat as error
        backup = BackupRun.objects.create(
            target=target_with_retention,
            status=BackupRunStatus.SUCCESS,
            storage_bucket="",  # No storage info
            storage_key="",
        )
        backup.finished_at = timezone.now() - timedelta(days=10)
        backup.save()

        command = Command()
        with pytest.raises(SystemExit) as exc_info:
            command.handle(dry_run=False, target=None)
        assert exc_info.value.code == 1  # Error exit code

        # Should not attempt MinIO deletion and should preserve DB record
        mock_delete.assert_not_called()
        assert BackupRun.objects.filter(pk=backup.pk).exists()

        captured = capsys.readouterr()
        assert "missing storage info" in captured.err

    def test_dry_run_validates_storage_info(self, target_with_retention, capsys):
        """Dry-run should report errors for backups with missing storage info."""
        backup = BackupRun.objects.create(
            target=target_with_retention,
            status=BackupRunStatus.SUCCESS,
            storage_bucket="",  # No storage info
            storage_key="",
        )
        backup.finished_at = timezone.now() - timedelta(days=10)
        backup.save()

        command = Command()
        with pytest.raises(SystemExit) as exc_info:
            command.handle(dry_run=True, target=None)
        assert exc_info.value.code == 0  # Dry-run always exits 0

        captured = capsys.readouterr()
        # Should report this would error, not "would delete"
        assert "Would ERROR" in captured.err
        assert "missing storage info" in captured.err
        assert "Would delete" not in captured.out
        # Summary should mention errors
        assert "would error" in captured.err
        assert "with issues" in captured.err

    def test_dry_run_summary_shows_both_counts(self, target_with_retention, capsys):
        """Dry-run summary should show both 'would delete' and 'would error' counts."""
        # Create one valid backup and one with missing storage info
        valid_backup = BackupRun.objects.create(
            target=target_with_retention,
            status=BackupRunStatus.SUCCESS,
            storage_bucket="backups",
            storage_key="test/valid.tar.gz",
        )
        valid_backup.finished_at = timezone.now() - timedelta(days=10)
        valid_backup.save()

        invalid_backup = BackupRun.objects.create(
            target=target_with_retention,
            status=BackupRunStatus.SUCCESS,
            storage_bucket="",
            storage_key="",
        )
        invalid_backup.finished_at = timezone.now() - timedelta(days=10)
        invalid_backup.save()

        command = Command()
        with pytest.raises(SystemExit) as exc_info:
            command.handle(dry_run=True, target=None)
        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        # Summary should include both counts
        assert "1 would be deleted" in captured.err
        assert "1 would error" in captured.err

    @patch("backups.management.commands.cleanup_old_backups.delete_object")
    @patch("backups.management.commands.cleanup_old_backups.get_backups_to_delete")
    def test_recheck_catches_restore_created_after_initial_query(
        self, mock_get_backups, mock_delete, target_with_retention, old_successful_backup, capsys
    ):
        """The re-check inside _delete_backup catches RestoreRuns created after initial query."""
        # Simulate: initial query returns backup (as if no RestoreRuns existed)
        mock_get_backups.return_value = [old_successful_backup]
        mock_delete.return_value = True

        # Create a RestoreRun - this simulates it being created after the initial query
        # but before _delete_backup processes this backup
        RestoreRun.objects.create(
            backup_run=old_successful_backup,
            target=target_with_retention,
            status=RestoreRunStatus.PENDING,
        )

        command = Command()
        with pytest.raises(SystemExit) as exc_info:
            command.handle(dry_run=False, target=None)
        assert exc_info.value.code == 0  # Skipped, not an error

        # MinIO should NOT have been called (re-check caught the RestoreRun)
        mock_delete.assert_not_called()

        # Backup should still exist
        assert BackupRun.objects.filter(pk=old_successful_backup.pk).exists()

        captured = capsys.readouterr()
        # Message varies by DB backend (SQLite vs PostgreSQL)
        assert "RestoreRun" in captured.out
        assert "Skipping" in captured.out

    @patch("backups.management.commands.cleanup_old_backups.delete_object")
    @patch("backups.management.commands.cleanup_old_backups.connection")
    def test_skips_on_lock_contention(
        self, mock_connection, mock_delete, target_with_retention, old_successful_backup, capsys
    ):
        """Should skip backup gracefully when target is locked by another operation."""
        from django.db import OperationalError

        # Simulate PostgreSQL with select_for_update support
        mock_connection.features.has_select_for_update = True

        # Make select_for_update raise OperationalError (lock contention)
        with patch.object(
            BackupTarget.objects, "select_for_update"
        ) as mock_select:
            mock_select.return_value.get.side_effect = OperationalError("could not obtain lock")

            command = Command()
            with pytest.raises(SystemExit) as exc_info:
                command.handle(dry_run=False, target=None)
            assert exc_info.value.code == 0  # Skipped, not an error

        # MinIO should NOT have been called
        mock_delete.assert_not_called()

        # Backup should still exist
        assert BackupRun.objects.filter(pk=old_successful_backup.pk).exists()

        captured = capsys.readouterr()
        assert "locked" in captured.out
        assert "Skipping" in captured.out


class TestFileLocking:
    """Tests for file lock behavior."""

    @patch("backups.management.commands.cleanup_old_backups.fcntl.flock")
    def test_exits_cleanly_when_locked(self, mock_flock, db, capsys):
        """Should exit cleanly when another instance is running."""
        import errno

        # Simulate lock held by another process
        mock_flock.side_effect = OSError(errno.EAGAIN, "Resource temporarily unavailable")

        command = Command()
        with pytest.raises(SystemExit) as exc_info:
            command.handle(dry_run=False, target=None)
        assert exc_info.value.code == 0  # Clean exit

        captured = capsys.readouterr()
        assert "Another cleanup instance is running" in captured.err

    def test_dry_run_skips_locking(self, target_with_retention, old_successful_backup, capsys):
        """Dry run should not acquire lock."""
        with patch(
            "backups.management.commands.cleanup_old_backups.Command._acquire_lock"
        ) as mock_acquire:
            command = Command()
            with pytest.raises(SystemExit):
                command.handle(dry_run=True, target=None)

            mock_acquire.assert_not_called()


class TestMinioClient:
    """Tests for the minio_client module."""

    @patch("backups.minio_client.subprocess.run")
    def test_delete_object_success(self, mock_run):
        """delete_object should return True on success."""
        from backups.minio_client import delete_object

        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        result = delete_object("backups", "test/backup.tar.gz")

        assert result is True
        mock_run.assert_called_once()
        # Verify the command includes --json flag
        call_args = mock_run.call_args
        assert call_args[0][0][1] == "rm"
        assert call_args[0][0][2] == "--json"
        assert "minio/backups/test/backup.tar.gz" in call_args[0][0][3]

    @patch("backups.minio_client.subprocess.run")
    def test_delete_object_failure(self, mock_run):
        """delete_object should return False on actual failure (not NoSuchKey)."""
        from backups.minio_client import delete_object

        # Permission denied - not a "not found" error
        error_json = '{"status":"error","error":{"message":"Access Denied","cause":{"error":{"Code":"AccessDenied"}}}}'
        mock_run.return_value = MagicMock(returncode=1, stdout=error_json, stderr="")

        result = delete_object("backups", "test/backup.tar.gz")

        assert result is False

    @patch("backups.minio_client.subprocess.run")
    def test_delete_object_not_found_nosuchkey(self, mock_run):
        """delete_object treats NoSuchKey error as success (idempotent)."""
        from backups.minio_client import delete_object

        # mc --json outputs JSON with NoSuchKey error code
        error_json = '{"status":"error","error":{"message":"Object does not exist.","cause":{"error":{"Code":"NoSuchKey","Message":"The specified key does not exist."}}}}'
        mock_run.return_value = MagicMock(returncode=1, stdout=error_json, stderr="")

        result = delete_object("backups", "test/backup.tar.gz")

        assert result is True

    @patch("backups.minio_client.subprocess.run")
    def test_delete_object_not_found_message_fallback(self, mock_run):
        """delete_object falls back to message check if no error code."""
        from backups.minio_client import delete_object

        # JSON without error code but with "object does not exist" message
        error_json = '{"status":"error","error":{"message":"Object does not exist.","cause":{}}}'
        mock_run.return_value = MagicMock(returncode=1, stdout=error_json, stderr="")

        result = delete_object("backups", "test/backup.tar.gz")

        assert result is True

    @patch("backups.minio_client.subprocess.run")
    def test_delete_object_bucket_not_found_is_error(self, mock_run):
        """delete_object should return False for bucket not found (not idempotent)."""
        from backups.minio_client import delete_object

        # Bucket not found - should NOT be treated as success
        error_json = '{"status":"error","error":{"message":"Bucket not found","cause":{"error":{"Code":"NoSuchBucket"}}}}'
        mock_run.return_value = MagicMock(returncode=1, stdout=error_json, stderr="")

        result = delete_object("backups", "test/backup.tar.gz")

        assert result is False

    @patch("backups.minio_client.subprocess.run")
    def test_delete_object_alias_not_found_is_error(self, mock_run):
        """delete_object should return False for alias not found."""
        from backups.minio_client import delete_object

        # Alias not found - should NOT be treated as success
        error_json = '{"status":"error","error":{"message":"Alias not found","cause":{}}}'
        mock_run.return_value = MagicMock(returncode=1, stdout=error_json, stderr="")

        result = delete_object("backups", "test/backup.tar.gz")

        assert result is False

    @patch("backups.minio_client.subprocess.run")
    def test_object_exists_true(self, mock_run):
        """object_exists should return True when object exists."""
        from backups.minio_client import object_exists

        mock_run.return_value = MagicMock(returncode=0)

        result = object_exists("backups", "test/backup.tar.gz")

        assert result is True

    @patch("backups.minio_client.subprocess.run")
    def test_object_exists_false(self, mock_run):
        """object_exists should return False when object doesn't exist."""
        from backups.minio_client import object_exists

        mock_run.return_value = MagicMock(returncode=1)

        result = object_exists("backups", "test/backup.tar.gz")

        assert result is False

    @patch("backups.minio_client.subprocess.run")
    def test_handles_timeout(self, mock_run):
        """Should handle subprocess timeout gracefully."""
        import subprocess

        from backups.minio_client import delete_object

        mock_run.side_effect = subprocess.TimeoutExpired(cmd="mc", timeout=60)

        result = delete_object("backups", "test/backup.tar.gz")

        assert result is False

    @patch("backups.minio_client.subprocess.run")
    def test_handles_mc_not_found(self, mock_run):
        """Should handle missing mc CLI gracefully."""
        from backups.minio_client import delete_object

        mock_run.side_effect = FileNotFoundError()

        result = delete_object("backups", "test/backup.tar.gz")

        assert result is False

    @patch("backups.minio_client.subprocess.run")
    def test_handles_invalid_json(self, mock_run):
        """Should return False if mc output is not valid JSON."""
        from backups.minio_client import delete_object

        # Invalid JSON output
        mock_run.return_value = MagicMock(returncode=1, stdout="not json", stderr="some error")

        result = delete_object("backups", "test/backup.tar.gz")

        assert result is False

    @patch("backups.minio_client.subprocess.run")
    def test_handles_mixed_json_non_json_lines(self, mock_run):
        """Should find NoSuchKey even with non-JSON lines mixed in output."""
        from backups.minio_client import delete_object

        # mc may emit warnings or progress before JSON error
        mixed_output = (
            "mc: WARNING some deprecation notice\n"
            '{"status":"error","error":{"message":"Object does not exist.","cause":{"error":{"Code":"NoSuchKey"}}}}'
        )
        mock_run.return_value = MagicMock(returncode=1, stdout=mixed_output, stderr="")

        result = delete_object("backups", "test/backup.tar.gz")

        assert result is True

    @patch("backups.minio_client.subprocess.run")
    def test_handles_nosuchkey_in_stderr(self, mock_run):
        """Should detect NoSuchKey even if mc emits JSON to stderr."""
        from backups.minio_client import delete_object

        # Some mc versions emit error JSON to stderr instead of stdout
        error_json = '{"status":"error","error":{"message":"Object does not exist.","cause":{"error":{"Code":"NoSuchKey"}}}}'
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr=error_json)

        result = delete_object("backups", "test/backup.tar.gz")

        assert result is True
