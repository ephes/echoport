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


@pytest.fixture(autouse=True)
def no_prefix_below_keys(request):
    """Command tests treat keys as plain objects unless a test says otherwise."""
    if request.node.get_closest_marker("real_list_prefix"):
        yield None
        return
    with patch(
        "backups.management.commands.cleanup_old_backups.list_prefix", return_value=[]
    ) as mock_list:
        yield mock_list


@pytest.fixture
def target_with_retention(db):
    """Create a test backup target with 7-day retention."""
    return BackupTarget.objects.create(
        name="retention-test",
        description="Test target for retention",
        fastdeploy_service="echoport-backup",
        service_name="retention-test.service",
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
            service_name="short-retention.service",
            db_path="/tmp/short.db",
            retention_days=3,
            status=BackupStatus.ACTIVE,
        )
        target_long = BackupTarget.objects.create(
            name="long-retention",
            fastdeploy_service="echoport-backup",
            service_name="long-retention.service",
            db_path="/tmp/long.db",
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
            service_name="other-target.service",
            db_path="/tmp/other.db",
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
            service_name="paused-target.service",
            db_path="/tmp/paused.db",
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
        """delete_object treats NoSuchKey error as success (idempotent).

        Synthetic S3-style shape kept for compatibility; real mc does not
        print it (see test_delete_object_not_found_real_mc_shape).
        """
        from backups.minio_client import delete_object

        # mc --json outputs JSON with NoSuchKey error code
        error_json = '{"status":"error","error":{"message":"Object does not exist.","cause":{"error":{"Code":"NoSuchKey","Message":"The specified key does not exist."}}}}'
        mock_run.return_value = MagicMock(returncode=1, stdout=error_json, stderr="")

        result = delete_object("backups", "test/backup.tar.gz")

        assert result is True

    @patch("backups.minio_client.subprocess.run")
    def test_delete_object_not_found_message_fallback(self, mock_run):
        """delete_object accepts an exact top-level "Object does not exist." message.

        Synthetic shape kept for compatibility; real mc puts the text in
        error.cause.message.
        """
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

    # Exact mc output from production cleanup.log on 2026-10-14
    # (mc RELEASE.2025-08-13T08-35-41Z against MinIO RELEASE.2025-10-15), rc 1.
    REAL_MC_NOT_FOUND = (
        '{"status":"error","error":{"message":"Failed to remove '
        '`minio/backups/python-podcast-media/2026-02-24T21-55-16`.",'
        '"cause":{"message":"Object does not exist","error":{}},"type":"error"}}'
    )

    @patch("backups.minio_client.subprocess.run")
    def test_delete_object_not_found_real_mc_shape(self, mock_run):
        """The real mc not-found output counts as already deleted."""
        from backups.minio_client import delete_object

        mock_run.return_value = MagicMock(returncode=1, stdout=self.REAL_MC_NOT_FOUND, stderr="")

        assert delete_object("backups", "python-podcast-media/2026-02-24T21-55-16") is True

    @patch("backups.minio_client.subprocess.run")
    def test_delete_object_unknown_alias_is_error(self, mock_run):
        """An unknown mc alias ("Requested path ... not found") stays an error."""
        from backups.minio_client import delete_object

        error_json = (
            '{"status":"error","error":{"message":"Failed to remove '
            '`minio/backups/x`.","cause":{"message":"Requested path '
            '`/home/echoport/site/minio/backups/x` not found","error":'
            '{"Path":"/home/echoport/site/minio/backups/x"}},"type":"error"}}'
        )
        mock_run.return_value = MagicMock(returncode=1, stdout=error_json, stderr="")

        assert delete_object("backups", "x") is False

    @patch("backups.minio_client.subprocess.run")
    def test_delete_object_missing_bucket_real_shape_is_error(self, mock_run):
        """A missing bucket ("Bucket `x` does not exist.") stays an error."""
        from backups.minio_client import delete_object

        error_json = (
            '{"status":"error","error":{"message":"Failed to remove '
            '`minio/backups/x`.","cause":{"message":"Bucket `backups` does not exist.",'
            '"error":{"Bucket":"backups"}},"type":"error"}}'
        )
        mock_run.return_value = MagicMock(returncode=1, stdout=error_json, stderr="")

        assert delete_object("backups", "x") is False

    @pytest.mark.parametrize(
        ("message", "expected"),
        [
            ("Object does not exist", True),
            ("Object does not exist.", True),
            ("  OBJECT DOES NOT EXIST.  ", True),
            ("Object does not exist in bucket", False),
            ("Requested path `/object does not exist` not found", False),
            ("Bucket `x` does not exist.", False),
            ("", False),
        ],
    )
    def test_not_found_message_is_an_exact_match(self, message, expected):
        """Only the exact text matches, in cause.message or error.message."""
        import json

        from backups.minio_client import _is_object_not_found_error

        in_cause = {"status": "error", "error": {"message": "x", "cause": {"message": message, "error": {}}}}
        in_error = {"status": "error", "error": {"message": message, "cause": {}}}
        assert _is_object_not_found_error(json.dumps(in_cause)) is expected
        assert _is_object_not_found_error(json.dumps(in_error)) is expected


def _old_run(target, key, days=10, bucket="backups"):
    run = BackupRun.objects.create(
        target=target,
        status=BackupRunStatus.SUCCESS,
        storage_bucket=bucket,
        storage_key=key,
    )
    run.finished_at = timezone.now() - timedelta(days=days)
    run.save()
    return run


class TestCleanupMissingAndSharedObjects:
    """Command-level tests for missing objects and keys shared by several runs."""

    @patch("backups.minio_client.subprocess.run")
    def test_real_not_found_output_deletes_row_and_exits_zero(
        self, mock_run, target_with_retention, capsys
    ):
        """A row whose object is already gone is removed and the run exits 0."""
        backup = _old_run(target_with_retention, "retention-test/2026-01-01T02-00-00.tar.gz")
        mock_run.return_value = MagicMock(
            returncode=1, stdout=TestMinioClient.REAL_MC_NOT_FOUND, stderr=""
        )

        with pytest.raises(SystemExit) as exc_info:
            Command().handle(dry_run=False, target=None)

        assert exc_info.value.code == 0
        assert not BackupRun.objects.filter(pk=backup.pk).exists()
        assert "Complete: 1 deleted" in capsys.readouterr().out

    @patch("backups.management.commands.cleanup_old_backups.delete_object")
    def test_shared_key_kept_while_a_kept_run_uses_it(
        self, mock_delete, target_with_retention, capsys
    ):
        """Old rows sharing the newest run's key are removed without deleting the object."""
        old = [_old_run(target_with_retention, ".tar.gz", days=d) for d in (12, 11, 10)]
        newest = _old_run(target_with_retention, ".tar.gz", days=0)

        with pytest.raises(SystemExit) as exc_info:
            Command().handle(dry_run=False, target=None)

        assert exc_info.value.code == 0
        mock_delete.assert_not_called()
        assert not BackupRun.objects.filter(pk__in=[r.pk for r in old]).exists()
        assert BackupRun.objects.filter(pk=newest.pk).exists()
        out = capsys.readouterr().out
        assert f"Keeping MinIO object backups/.tar.gz: still used by backup(s) {newest.pk}" in out

    @patch("backups.management.commands.cleanup_old_backups.delete_object")
    def test_shared_key_deleted_with_the_last_row(
        self, mock_delete, target_with_retention
    ):
        """When every run sharing a key expires, the object goes with the last row."""
        mock_delete.return_value = True
        first = _old_run(target_with_retention, ".tar.gz", days=12)
        last = _old_run(target_with_retention, ".tar.gz", days=10)

        with pytest.raises(SystemExit) as exc_info:
            Command().handle(dry_run=False, target=None)

        assert exc_info.value.code == 0
        mock_delete.assert_called_once_with("backups", ".tar.gz")
        assert not BackupRun.objects.filter(pk__in=[first.pk, last.pk]).exists()

    @patch("backups.management.commands.cleanup_old_backups.delete_object")
    def test_same_key_in_another_bucket_is_not_shared(
        self, mock_delete, target_with_retention
    ):
        """Only bucket and key together identify the object."""
        mock_delete.return_value = True
        old = _old_run(target_with_retention, "k.tar.gz", days=10, bucket="backups")
        _old_run(target_with_retention, "k.tar.gz", days=0, bucket="other")

        with pytest.raises(SystemExit):
            Command().handle(dry_run=False, target=None)

        mock_delete.assert_called_once_with("backups", "k.tar.gz")
        assert not BackupRun.objects.filter(pk=old.pk).exists()

    @patch("backups.management.commands.cleanup_old_backups.delete_object")
    def test_dry_run_reports_kept_shared_object(
        self, mock_delete, target_with_retention, capsys
    ):
        """Dry run says which object is kept, and that the last expired row would delete it."""
        _old_run(target_with_retention, "shared.tar.gz", days=12)
        _old_run(target_with_retention, "shared.tar.gz", days=10)

        with pytest.raises(SystemExit) as exc_info:
            Command().handle(dry_run=True, target=None)

        assert exc_info.value.code == 0
        mock_delete.assert_not_called()
        lines = [line for line in capsys.readouterr().out.splitlines() if "Would delete" in line]
        assert len(lines) == 2
        assert "MinIO object kept" in lines[0]
        assert "MinIO object kept" not in lines[1]


MANIFEST_LINE = (
    '{"status":"success","type":"file","lastModified":"2026-10-13T01:45:30Z","size":2,'
    '"key":"manifest.json","url":"http://127.0.0.1:9001/backups/python-podcast-media/'
    '2026-10-13T01-45-02/","versionOrdinal":1,"storageClass":"STANDARD"}'
)


class TestMinioPrefix:
    """mc calls for media targets, which store a prefix as storage_key."""

    @patch("backups.minio_client.subprocess.run")
    def test_list_prefix_lists_below_trailing_slash(self, mock_run):
        from backups.minio_client import list_prefix

        mock_run.return_value = MagicMock(returncode=0, stdout=MANIFEST_LINE + "\n", stderr="")

        assert list_prefix("backups", "python-podcast-media/2026-10-13T01-45-02") == ["manifest.json"]
        assert mock_run.call_args[0][0][1:] == [
            "ls",
            "--recursive",
            "--json",
            "minio/backups/python-podcast-media/2026-10-13T01-45-02/",
        ]

    @patch("backups.minio_client.subprocess.run")
    def test_list_prefix_empty_and_errors(self, mock_run):
        from backups.minio_client import list_prefix

        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        assert list_prefix("backups", "a/b") == []

        no_bucket = (
            '{"status":"error","error":{"message":"Unable to list folder.","cause":'
            '{"message":"The specified bucket does not exist","error":{"Code":"NoSuchBucket"}},'
            '"type":"error"}}'
        )
        mock_run.return_value = MagicMock(returncode=1, stdout=no_bucket, stderr="")
        assert list_prefix("backups", "a/b") is None

        mock_run.return_value = MagicMock(returncode=0, stdout="not json", stderr="")
        assert list_prefix("backups", "a/b") is None

    @pytest.mark.parametrize("prefix", ["", "/", "//", "a/b/", "/a", "a//b", "a/.."])
    @patch("backups.minio_client.subprocess.run")
    def test_list_prefix_refuses_root_and_odd_keys(self, mock_run, prefix):
        from backups.minio_client import list_prefix

        assert list_prefix("backups", prefix) is None
        mock_run.assert_not_called()

    @patch("backups.minio_client.subprocess.run")
    def test_delete_prefix_recursive_with_trailing_slash_then_verifies(self, mock_run):
        from backups.minio_client import delete_prefix

        mock_run.side_effect = [
            MagicMock(returncode=0, stdout='{"status":"success"}', stderr=""),
            MagicMock(returncode=0, stdout="", stderr=""),
        ]

        assert delete_prefix("backups", "python-podcast-media/2026-10-13T01-45-02") is True
        rm_args = mock_run.call_args_list[0][0][0]
        assert rm_args[1:] == [
            "rm",
            "--recursive",
            "--force",
            "--json",
            "minio/backups/python-podcast-media/2026-10-13T01-45-02/",
        ]
        assert mock_run.call_args_list[1][0][0][1] == "ls"

    @patch("backups.minio_client.subprocess.run")
    def test_delete_prefix_fails_when_objects_remain(self, mock_run):
        from backups.minio_client import delete_prefix

        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="", stderr=""),
            MagicMock(returncode=0, stdout=MANIFEST_LINE, stderr=""),
        ]

        assert delete_prefix("backups", "python-podcast-media/2026-10-13T01-45-02") is False

    @patch("backups.minio_client.subprocess.run")
    def test_delete_prefix_fails_on_mc_error(self, mock_run):
        from backups.minio_client import delete_prefix

        mock_run.return_value = MagicMock(returncode=1, stdout='{"status":"error"}', stderr="")

        assert delete_prefix("backups", "python-podcast-media/2026-10-13T01-45-02") is False
        assert mock_run.call_count == 1

    @pytest.mark.parametrize(
        "prefix",
        ["", "/", "python-podcast-media", "python-podcast-media/", "a//b", "a/../b", "./a", "a/b/", "/a/b"],
    )
    @patch("backups.minio_client.subprocess.run")
    def test_delete_prefix_refuses_root_and_target_level_prefixes(self, mock_run, prefix):
        from backups.minio_client import delete_prefix

        assert delete_prefix("backups", prefix) is False
        mock_run.assert_not_called()


def _completed(stdout="", returncode=0):
    return MagicMock(returncode=returncode, stdout=stdout, stderr="")


@pytest.mark.real_list_prefix
class TestCleanupPrefixTargets:
    """Command-level tests for media runs whose storage_key is a prefix."""

    @patch("backups.minio_client.subprocess.run")
    def test_prefix_is_deleted_recursively_then_row(self, mock_run, target_with_retention, capsys):
        backup = _old_run(target_with_retention, "python-podcast-media/2026-02-27T01-45-02")
        mock_run.side_effect = [
            _completed(MANIFEST_LINE),  # ls: something below the prefix
            _completed('{"status":"success"}'),  # rm --recursive --force
            _completed(""),  # ls: prefix empty
        ]

        with pytest.raises(SystemExit) as exc_info:
            Command().handle(dry_run=False, target=None)

        assert exc_info.value.code == 0
        assert not BackupRun.objects.filter(pk=backup.pk).exists()
        commands = [c[0][0][1:] for c in mock_run.call_args_list]
        assert commands[1] == [
            "rm",
            "--recursive",
            "--force",
            "--json",
            "minio/backups/python-podcast-media/2026-02-27T01-45-02/",
        ]

    @patch("backups.minio_client.subprocess.run")
    def test_row_kept_when_prefix_not_empty_after_delete(self, mock_run, target_with_retention, capsys):
        backup = _old_run(target_with_retention, "python-podcast-media/2026-02-27T01-45-02")
        mock_run.side_effect = [_completed(MANIFEST_LINE), _completed(""), _completed(MANIFEST_LINE)]

        with pytest.raises(SystemExit) as exc_info:
            Command().handle(dry_run=False, target=None)

        assert exc_info.value.code == 1
        assert BackupRun.objects.filter(pk=backup.pk).exists()
        assert "Failed to delete from MinIO" in capsys.readouterr().err

    @patch("backups.minio_client.subprocess.run")
    def test_row_kept_for_target_level_prefix(self, mock_run, target_with_retention, capsys):
        """A one-segment key with objects below it is never deleted recursively."""
        backup = _old_run(target_with_retention, "python-podcast-media")
        mock_run.return_value = _completed(MANIFEST_LINE)

        with pytest.raises(SystemExit) as exc_info:
            Command().handle(dry_run=False, target=None)

        assert exc_info.value.code == 1
        assert BackupRun.objects.filter(pk=backup.pk).exists()
        assert all(c[0][0][1] == "ls" for c in mock_run.call_args_list)

    @patch("backups.minio_client.subprocess.run")
    def test_empty_prefix_falls_back_to_object_delete(self, mock_run, target_with_retention):
        """An already empty prefix (nothing below it) goes through mc rm; not-found counts as deleted."""
        backup = _old_run(target_with_retention, "python-podcast-media/2026-02-24T21-55-16")
        mock_run.side_effect = [_completed(""), _completed(TestMinioClient.REAL_MC_NOT_FOUND, returncode=1)]

        with pytest.raises(SystemExit) as exc_info:
            Command().handle(dry_run=False, target=None)

        assert exc_info.value.code == 0
        assert not BackupRun.objects.filter(pk=backup.pk).exists()
        assert mock_run.call_args_list[1][0][0][1:] == [
            "rm",
            "--json",
            "minio/backups/python-podcast-media/2026-02-24T21-55-16",
        ]

    @patch("backups.minio_client.subprocess.run")
    def test_list_failure_keeps_row(self, mock_run, target_with_retention):
        backup = _old_run(target_with_retention, "python-podcast-media/2026-02-27T01-45-02")
        mock_run.return_value = _completed('{"status":"error"}', returncode=1)

        with pytest.raises(SystemExit) as exc_info:
            Command().handle(dry_run=False, target=None)

        assert exc_info.value.code == 1
        assert BackupRun.objects.filter(pk=backup.pk).exists()
        assert mock_run.call_count == 1

    @patch("backups.minio_client.subprocess.run")
    def test_run_and_prefix_kept_while_a_run_is_stored_below_it(
        self, mock_run, target_with_retention, capsys
    ):
        """Neither the prefix nor its row goes while another run lives below it."""
        parent = _old_run(target_with_retention, "media/2026-02-27")
        _old_run(target_with_retention, "media/2026-02-27/nested.tar.gz", days=0)

        with pytest.raises(SystemExit) as exc_info:
            Command().handle(dry_run=False, target=None)

        assert exc_info.value.code == 0
        mock_run.assert_not_called()
        assert BackupRun.objects.filter(pk=parent.pk).exists()
        assert "1 skipped" in capsys.readouterr().out

    @patch("backups.minio_client.subprocess.run")
    def test_parent_prefix_deleted_after_the_nested_run(self, mock_run, target_with_retention):
        """Oldest first: once the nested run is gone, the parent prefix is deleted in a later pass."""
        parent = _old_run(target_with_retention, "media/2026-02-27", days=12)
        nested = _old_run(target_with_retention, "media/2026-02-27/nested.tar.gz", days=10)
        mock_run.side_effect = [
            _completed(""),  # nested: nothing below it
            _completed('{"status":"success"}'),  # nested: mc rm object
        ]

        with pytest.raises(SystemExit):
            Command().handle(dry_run=False, target=None)

        assert BackupRun.objects.filter(pk=parent.pk).exists()
        assert not BackupRun.objects.filter(pk=nested.pk).exists()

        mock_run.side_effect = [_completed(MANIFEST_LINE), _completed(""), _completed("")]
        with pytest.raises(SystemExit) as exc_info:
            Command().handle(dry_run=False, target=None)

        assert exc_info.value.code == 0
        assert not BackupRun.objects.filter(pk=parent.pk).exists()
        assert mock_run.call_args_list[-2][0][0][-1] == "minio/backups/media/2026-02-27/"

    @patch("backups.minio_client.subprocess.run")
    def test_trailing_slash_key_is_refused_not_normalized(self, mock_run, target_with_retention):
        """A key spelled "a/b/" never reaches mc, so it cannot delete the storage of a run keyed "a/b"."""
        odd = _old_run(target_with_retention, "media/2026-02-27/", days=12)
        kept = _old_run(target_with_retention, "media/2026-02-27", days=0)

        with pytest.raises(SystemExit) as exc_info:
            Command().handle(dry_run=False, target=None)

        assert exc_info.value.code == 1
        mock_run.assert_not_called()
        assert BackupRun.objects.filter(pk__in=[odd.pk, kept.pk]).count() == 2
