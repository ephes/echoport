"""
Management command to clean up old backups based on retention policy.

This command is designed to be run from cron (e.g., daily at 3am).
It removes backups older than the target's retention_days setting,
deleting from MinIO first then from the database.

Usage:
    python manage.py cleanup_old_backups
    python manage.py cleanup_old_backups --dry-run
    python manage.py cleanup_old_backups --target nyxmon

The command uses a file lock to prevent overlapping instances.
"""

import errno
import fcntl
import logging
import os
import sys
from datetime import timedelta
from enum import Enum
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import OperationalError, connection, transaction
from django.db.models import Exists, OuterRef
from django.utils import timezone

from backups.minio_client import delete_object
from backups.models import BackupRun, BackupRunStatus, BackupStatus, BackupTarget, RestoreRun

logger = logging.getLogger(__name__)


class DeleteResult(Enum):
    """Result of a backup deletion attempt."""

    DELETED = "deleted"  # Successfully deleted from MinIO and DB
    SKIPPED = "skipped"  # Skipped (lock contention, RestoreRun exists, already deleted)
    ERROR = "error"  # Failed to delete


def _get_lock_file_path() -> Path:
    """
    Get the lock file path, preferring a controlled directory.

    Uses cache dir if available (production), otherwise /tmp.
    """
    cache_dir = getattr(settings, "ECHOPORT_CACHE_DIR", None)
    if cache_dir and Path(cache_dir).is_dir():
        return Path(cache_dir) / "cleanup.lock"

    return Path("/tmp/echoport-cleanup.lock")


def get_backups_to_delete(target: BackupTarget, now=None) -> list[BackupRun]:
    """
    Get list of backups eligible for deletion based on retention policy.

    Criteria:
    - status is SUCCESS (don't delete failed backups - they're already empty in MinIO)
    - finished_at is older than retention_days
    - no RestoreRun references this backup (PROTECT FK would block deletion anyway)

    Args:
        target: The backup target to check
        now: Current time (for testing). Defaults to timezone.now()

    Returns:
        List of BackupRun objects that should be deleted
    """
    if now is None:
        now = timezone.now()

    cutoff = now - timedelta(days=target.retention_days)

    # Subquery to check if a backup has any restore runs
    has_restores = RestoreRun.objects.filter(backup_run=OuterRef("pk"))

    backups = (
        BackupRun.objects.filter(
            target=target,
            status=BackupRunStatus.SUCCESS,
            finished_at__lt=cutoff,
        )
        .annotate(has_restore_runs=Exists(has_restores))
        .filter(has_restore_runs=False)
        .order_by("finished_at")
    )

    return list(backups)


class Command(BaseCommand):
    help = "Clean up old backups based on retention policy"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be deleted without actually deleting",
        )
        parser.add_argument(
            "--target",
            type=str,
            help="Only cleanup backups for a specific target (by name)",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        target_name = options.get("target")

        # Acquire lock to prevent overlapping instances
        if not dry_run:
            try:
                lock_file = self._acquire_lock()
            except OSError as e:
                self.stderr.write(
                    self.style.ERROR(f"Failed to acquire lock file {_get_lock_file_path()}: {e}")
                )
                sys.exit(1)

            if lock_file is None:
                self.stderr.write(
                    self.style.WARNING("Another cleanup instance is running, exiting")
                )
                sys.exit(0)
        else:
            lock_file = None

        try:
            exit_code = self._run_cleanup(dry_run, target_name)
            sys.exit(exit_code)
        finally:
            if lock_file:
                self._release_lock(lock_file)

    def _acquire_lock(self):
        """
        Acquire an exclusive lock to prevent overlapping instances.

        Returns:
            - file object if lock acquired successfully
            - None if another instance is already running

        Raises:
            OSError for permission errors or other filesystem issues
        """
        lock_path = _get_lock_file_path()
        lock_file = None
        try:
            fd = os.open(
                lock_path,
                os.O_CREAT | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            lock_file = os.fdopen(fd, "w")
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return lock_file
        except OSError as e:
            if lock_file is not None:
                lock_file.close()
            if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                return None
            if e.errno == errno.ELOOP:
                raise OSError(errno.ELOOP, f"Lock file is a symlink (possible attack): {lock_path}")
            raise

    def _release_lock(self, lock_file):
        """Release the lock file."""
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            lock_file.close()
        except (OSError, IOError):
            pass

    def _run_cleanup(self, dry_run: bool, target_name: str | None) -> int:
        """
        Run the cleanup logic.

        Returns exit code: 0 for success, 1 if any errors occurred.
        """
        now = timezone.now()

        # Get targets to process
        if target_name:
            try:
                targets = [BackupTarget.objects.get(name=target_name)]
            except BackupTarget.DoesNotExist:
                self.stderr.write(
                    self.style.ERROR(f"Target not found: {target_name}")
                )
                return 1
        else:
            # Only process active targets
            targets = list(BackupTarget.objects.filter(status=BackupStatus.ACTIVE))

        self.stdout.write(f"Checking {len(targets)} target(s) for old backups...")

        total_deleted = 0
        total_skipped = 0
        total_errors = 0

        for target in targets:
            deleted, skipped, errors = self._cleanup_target(target, now, dry_run)
            total_deleted += deleted
            total_skipped += skipped
            total_errors += errors

        # Build summary message (used for both dry-run and actual runs)
        if dry_run:
            parts = []
            if total_deleted > 0:
                parts.append(f"{total_deleted} would be deleted")
            if total_errors > 0:
                parts.append(f"{total_errors} would error")
            summary = ", ".join(parts) if parts else "no backups to clean up"

            if total_errors > 0:
                # Warn about data integrity issues, but keep exit 0 (dry-run is informational)
                self.stderr.write(
                    self.style.WARNING(f"Dry run complete (with issues): {summary}")
                )
            else:
                self.stdout.write(
                    self.style.SUCCESS(f"Dry run complete: {summary}")
                )
            return 0

        # Actual run summary
        parts = []
        if total_deleted > 0:
            parts.append(f"{total_deleted} deleted")
        if total_skipped > 0:
            parts.append(f"{total_skipped} skipped")
        if total_errors > 0:
            parts.append(f"{total_errors} failed")

        summary = ", ".join(parts) if parts else "no backups to clean up"

        if total_errors > 0:
            self.stderr.write(
                self.style.ERROR(f"Complete with errors: {summary}")
            )
            return 1
        else:
            self.stdout.write(
                self.style.SUCCESS(f"Complete: {summary}")
            )
            return 0

    def _cleanup_target(
        self, target: BackupTarget, now, dry_run: bool
    ) -> tuple[int, int, int]:
        """
        Clean up old backups for a single target.

        Returns (deleted_count, skipped_count, error_count)
        """
        backups = get_backups_to_delete(target, now)

        if not backups:
            logger.debug(f"No old backups to delete for target '{target.name}'")
            return 0, 0, 0

        deleted = 0
        skipped = 0
        errors = 0

        for backup in backups:
            # finished_at is guaranteed non-null by the query filter
            finished_str = backup.finished_at.strftime('%Y-%m-%d %H:%M') if backup.finished_at else "unknown"
            if dry_run:
                # Validate storage info in dry-run to reflect actual outcome
                if not backup.storage_key or not backup.storage_bucket:
                    self.stderr.write(
                        self.style.ERROR(
                            f"  [DRY RUN] Would ERROR: backup {backup.id} has missing storage info"
                        )
                    )
                    errors += 1
                else:
                    self.stdout.write(
                        f"  [DRY RUN] Would delete '{target.name}' backup from "
                        f"{finished_str}: {backup.storage_key}"
                    )
                    deleted += 1
            else:
                result = self._delete_backup(backup)
                if result == DeleteResult.DELETED:
                    deleted += 1
                elif result == DeleteResult.SKIPPED:
                    skipped += 1
                else:  # DeleteResult.ERROR
                    errors += 1

        return deleted, skipped, errors

    def _delete_backup(self, backup: BackupRun) -> DeleteResult:
        """
        Delete a single backup from MinIO and database.

        Uses a transaction with select_for_update on BackupTarget to serialize
        with restore operations (which also lock BackupTarget). This prevents
        race conditions where a RestoreRun is created while we're deleting.

        Note: The BackupTarget lock is held while executing mc rm (up to 60s timeout).
        This blocks new backup/restore starts for that target during cleanup.
        Acceptable for scheduled 3am runs; be aware if running ad-hoc.

        Deletes from MinIO first, then from database.
        If MinIO deletion fails, database record is preserved.

        Returns DeleteResult enum indicating outcome.
        """
        target = backup.target
        target_name = target.name
        storage_key = backup.storage_key
        storage_bucket = backup.storage_bucket

        # Require storage info - if missing, this is a data integrity issue
        # that should be investigated, not silently cleaned up
        if not storage_key or not storage_bucket:
            self.stderr.write(
                self.style.ERROR(
                    f"  Backup {backup.id} for '{target_name}' has missing storage info "
                    f"(bucket={storage_bucket!r}, key={storage_key!r}) - skipping to avoid orphans"
                )
            )
            return DeleteResult.ERROR

        # SQLite doesn't support select_for_update - fall back to simple re-check
        if not connection.features.has_select_for_update:
            return self._delete_backup_simple(backup, target_name, storage_bucket, storage_key)

        try:
            with transaction.atomic():
                # Lock BackupTarget to serialize with restore operations
                # (restore_engine also locks BackupTarget, so this prevents races)
                try:
                    BackupTarget.objects.select_for_update(nowait=True).get(pk=target.pk)
                except OperationalError:
                    # Lock contention - another backup/restore operation is in progress
                    # Skip this backup, will retry on next cleanup run
                    self.stdout.write(
                        self.style.WARNING(
                            f"  Skipping backup {backup.id} - target '{target_name}' is locked "
                            f"(backup/restore in progress)"
                        )
                    )
                    return DeleteResult.SKIPPED

                # Re-check the backup still exists
                locked_backup = BackupRun.objects.filter(pk=backup.pk).first()
                if locked_backup is None:
                    logger.info(f"Backup {backup.id} already deleted")
                    return DeleteResult.SKIPPED

                # Re-check for RestoreRuns (defense in depth)
                if RestoreRun.objects.filter(backup_run=locked_backup).exists():
                    self.stdout.write(
                        self.style.WARNING(
                            f"  Skipping backup {backup.id} - RestoreRun was created after initial query"
                        )
                    )
                    return DeleteResult.SKIPPED

                # Delete from MinIO first
                if not delete_object(storage_bucket, storage_key):
                    self.stderr.write(
                        self.style.ERROR(
                            f"  Failed to delete from MinIO: {storage_bucket}/{storage_key}"
                        )
                    )
                    return DeleteResult.ERROR

                # Delete from database (inside transaction, lock held)
                finished_str = locked_backup.finished_at.strftime('%Y-%m-%d %H:%M') if locked_backup.finished_at else "unknown"
                locked_backup.delete()
                self.stdout.write(
                    f"  Deleted '{target_name}' backup from "
                    f"{finished_str}: {storage_key}"
                )
                return DeleteResult.DELETED

        except Exception as e:
            # PROTECT FK violation or other DB error
            self.stderr.write(
                self.style.ERROR(
                    f"  Failed to delete backup {backup.id}: {e}"
                )
            )
            return DeleteResult.ERROR

    def _delete_backup_simple(
        self, backup: BackupRun, target_name: str, storage_bucket: str, storage_key: str
    ) -> DeleteResult:
        """
        Simplified deletion for databases without select_for_update (SQLite).

        Still does the re-check for RestoreRuns, but can't prevent the race
        condition entirely. The PROTECT FK will catch it if a RestoreRun
        is created between check and delete.
        """
        backup_id = backup.pk

        # Re-check the backup still exists
        current_backup = BackupRun.objects.filter(pk=backup_id).first()
        if current_backup is None:
            logger.info(f"Backup {backup_id} already deleted")
            return DeleteResult.SKIPPED

        # Re-check for RestoreRuns
        if RestoreRun.objects.filter(backup_run=current_backup).exists():
            self.stdout.write(
                self.style.WARNING(
                    f"  Skipping backup {backup_id} - RestoreRun exists"
                )
            )
            return DeleteResult.SKIPPED

        # Delete from MinIO first
        if not delete_object(storage_bucket, storage_key):
            self.stderr.write(
                self.style.ERROR(
                    f"  Failed to delete from MinIO: {storage_bucket}/{storage_key}"
                )
            )
            return DeleteResult.ERROR

        # Delete from database
        try:
            finished_str = current_backup.finished_at.strftime('%Y-%m-%d %H:%M') if current_backup.finished_at else "unknown"
            current_backup.delete()
            self.stdout.write(
                f"  Deleted '{target_name}' backup from "
                f"{finished_str}: {storage_key}"
            )
            return DeleteResult.DELETED
        except Exception as e:
            # PROTECT FK violation - MinIO object is now orphaned
            self.stderr.write(
                self.style.ERROR(
                    f"  MinIO object deleted but DB delete failed for backup {backup_id}: {e}"
                )
            )
            return DeleteResult.ERROR
