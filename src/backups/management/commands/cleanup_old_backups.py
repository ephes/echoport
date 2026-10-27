"""
Management command to clean up old backups based on retention policy.

This command is designed to be run from cron (e.g., daily at 3am).
It removes backups older than the target's retention_days setting,
deleting from MinIO first then from the database. A MinIO object that
another backup run still references is kept; only the expired row goes.
Prefix keys (media targets) are deleted recursively and must be empty
before the row is removed.

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
from django.db import OperationalError, transaction
from django.db.models import Exists, OuterRef, Q
from django.utils import timezone

from backups.minio_client import delete_object, delete_prefix, list_prefix
from backups.models import (
    BackupRun,
    BackupRunStatus,
    BackupStatus,
    BackupTarget,
    RestoreRun,
    exact_name_hint,
)

logger = logging.getLogger(__name__)


class DeleteResult(Enum):
    """Result of a backup deletion attempt."""

    DELETED = "deleted"  # Successfully deleted from MinIO and DB
    SKIPPED = "skipped"  # Skipped (lock contention, RestoreRun exists, already deleted)
    ERROR = "error"  # Failed to delete


def _format_ids(ids: list[int], limit: int = 5) -> str:
    shown = ", ".join(str(i) for i in ids[:limit])
    return f"{shown} and {len(ids) - limit} more" if len(ids) > limit else shown


def _get_lock_file_path() -> Path:
    """
    Get the lock file path, preferring a controlled directory.

    Uses cache dir if available (production), otherwise /tmp.
    """
    cache_dir = getattr(settings, "ECHOPORT_CACHE_DIR", None)
    if cache_dir and Path(cache_dir).is_dir():
        return Path(cache_dir) / "cleanup.lock"

    return Path("/tmp/echoport-cleanup.lock")


def protected_success_ids(target: BackupTarget) -> list[int]:
    """Newest completed, unmarked recorded successes; failures never displace them."""
    floor = max(0, settings.ECHOPORT_RETENTION_MIN_SUCCESS)
    return list(BackupRun.objects.filter(
        target=target, status=BackupRunStatus.SUCCESS, deletion_pending_at__isnull=True,
        finished_at__isnull=False,
    )
        .order_by("-finished_at", "-pk").values_list("pk", flat=True)[:floor])


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
        BackupRun.objects.filter(target=target, status=BackupRunStatus.SUCCESS)
        .filter(Q(deletion_pending_at__isnull=False) | Q(finished_at__lt=cutoff))
        .exclude(pk__in=protected_success_ids(target))
        .annotate(has_restore_runs=Exists(has_restores))
        .filter(has_restore_runs=False)
        .order_by("finished_at", "pk")
    )

    return list(backups)


def other_runs_sharing_object(
    backup: BackupRun, bucket: str, key: str, ignore_ids: set[int] | None = None
) -> list[int]:
    """
    Return ids of other backup runs that still point to the same MinIO object.

    Retention must not delete an object another kept run still uses. This
    guards against shared keys such as the FastDeploy runs that were all
    written to ``backups/.tar.gz``: deleting the oldest row would remove the
    newest archive. The object is deleted only together with the last row
    that references it.
    """
    exclude_ids = {backup.pk} | (ignore_ids or set())
    return list(
        BackupRun.objects.filter(storage_bucket=bucket, storage_key=key)
        .exclude(pk__in=exclude_ids)
        .order_by("pk")
        .values_list("pk", flat=True)
    )


def runs_stored_below(
    backup: BackupRun, bucket: str, key: str, ignore_ids: set[int] | None = None
) -> list[int]:
    """
    Return ids of other backup runs whose key lies below ``<key>/``.

    A prefix is deleted recursively, which would take their data with it, so
    the expired run is left alone (row and storage) until they are gone.
    """
    exclude_ids = {backup.pk} | (ignore_ids or set())
    return list(
        BackupRun.objects.filter(storage_bucket=bucket, storage_key__startswith=f"{key}/")
        .exclude(pk__in=exclude_ids)
        .order_by("pk")
        .values_list("pk", flat=True)
    )


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
                    self.style.ERROR(
                        f"Target not found: {target_name}.{exact_name_hint(target_name)}"
                    )
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
            if total_skipped > 0:
                parts.append(f"{total_skipped} would be skipped")
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
        dry_run_deleted_ids: set[int] = set()

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
                elif below := runs_stored_below(
                    backup, backup.storage_bucket, backup.storage_key, dry_run_deleted_ids
                ):
                    self.stdout.write(
                        f"  [DRY RUN] Would skip '{target.name}' backup from "
                        f"{finished_str}: {backup.storage_key} (backup(s) "
                        f"{_format_ids(below)} stored below it)"
                    )
                    skipped += 1
                else:
                    sharing = other_runs_sharing_object(
                        backup, backup.storage_bucket, backup.storage_key, dry_run_deleted_ids
                    )
                    note = (
                        f" (MinIO object kept: still used by backup(s) {_format_ids(sharing)})"
                        if sharing
                        else ""
                    )
                    self.stdout.write(
                        f"  [DRY RUN] Would delete '{target.name}' backup from "
                        f"{finished_str}: {backup.storage_key}{note}"
                    )
                    dry_run_deleted_ids.add(backup.pk)
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
        """Commit intent, delete storage outside a transaction, then finalize metadata."""
        from backups.reservations import lock_target

        intent_committed = False
        try:
            # durable disallows an enclosing application transaction: intent must
            # be committed before touching storage, including on interruption.
            with transaction.atomic(durable=True):
                lock_target(backup.target_id)
                current = BackupRun.objects.select_related("target").filter(pk=backup.pk).first()
                if current is None:
                    return DeleteResult.SKIPPED
                if current.target_id != backup.target_id:
                    return DeleteResult.ERROR
                if RestoreRun.objects.filter(backup_run=current).exists():
                    self.stdout.write(f"  Skipping backup {backup.pk} - RestoreRun exists")
                    return DeleteResult.SKIPPED
                if not current.storage_bucket or not current.storage_key:
                    self.stderr.write(f"  Backup {backup.pk} has missing storage info")
                    return DeleteResult.ERROR
                if current.status != BackupRunStatus.SUCCESS:
                    return DeleteResult.SKIPPED
                if current.deletion_pending_at is None:
                    cutoff = timezone.now() - timedelta(days=current.target.retention_days)
                    if (current.finished_at is None or current.finished_at >= cutoff
                            or current.pk in protected_success_ids(current.target)):
                        return DeleteResult.SKIPPED
                    if runs_stored_below(current, current.storage_bucket, current.storage_key):
                        self.stdout.write(f"  Skipping backup {backup.pk} - other runs stored below it")
                        return DeleteResult.SKIPPED
                    current.deletion_pending_at = timezone.now()
                    current.save(update_fields=["deletion_pending_at"])
                identity = (current.storage_bucket, current.storage_key, current.deletion_pending_at)
            intent_committed = True
            outcome = self._delete_storage(current, identity[0], identity[1])
            if outcome is not DeleteResult.DELETED:
                return outcome
            with transaction.atomic(durable=True):
                lock_target(current.target_id)
                final = BackupRun.objects.get(pk=current.pk)
                if ((final.storage_bucket, final.storage_key, final.deletion_pending_at) != identity
                        or RestoreRun.objects.filter(backup_run=final).exists()):
                    raise ValueError("Deletion intent drift or unexpected restore reference; reconcile manually")
                final.delete()
            self.stdout.write(f"  Deleted '{current.target.name}' backup: {identity[1]}")
            return DeleteResult.DELETED
        except OperationalError as error:
            if intent_committed:
                self.stderr.write(f"  Failed to finalize deletion for backup {backup.pk}: {error}")
                return DeleteResult.ERROR
            self.stdout.write(f"  Skipping backup {backup.pk} - locked: {error}")
            return DeleteResult.SKIPPED
        except Exception as error:
            self.stderr.write(f"  Failed to delete backup {backup.pk}: {error}")
            return DeleteResult.ERROR

    def _delete_storage(
        self, backup: BackupRun, storage_bucket: str, storage_key: str
    ) -> DeleteResult:
        """
        Delete the backup's MinIO object or prefix unless another run needs it.

        Returns DELETED when the DB row may be deleted (storage deleted,
        already gone, or kept for a run with the same key), SKIPPED when other
        runs are stored below the key (row and storage stay), and ERROR on a
        MinIO failure or a prefix that is not empty after the delete.
        """
        below = runs_stored_below(backup, storage_bucket, storage_key)
        if below:
            self.stdout.write(
                self.style.WARNING(
                    f"  Skipping backup {backup.pk}: backup(s) {_format_ids(below)} "
                    f"are stored below {storage_bucket}/{storage_key}/"
                )
            )
            return DeleteResult.SKIPPED

        sharing = other_runs_sharing_object(backup, storage_bucket, storage_key)
        if sharing:
            self.stdout.write(
                self.style.WARNING(
                    f"  Keeping MinIO object {storage_bucket}/{storage_key}: "
                    f"still used by backup(s) {_format_ids(sharing)}"
                )
            )
            return DeleteResult.DELETED

        # Media targets store a prefix (<target>/<timestamp>) instead of an
        # object key. Anything stored below "<key>/" marks such a prefix; it
        # is deleted recursively and must be empty afterwards.
        stored = list_prefix(storage_bucket, storage_key)
        if stored is None:
            deleted = False
        elif stored:
            deleted = delete_prefix(storage_bucket, storage_key)
        else:
            deleted = delete_object(storage_bucket, storage_key)

        if not deleted:
            self.stderr.write(
                self.style.ERROR(
                    f"  Failed to delete from MinIO: {storage_bucket}/{storage_key}"
                )
            )
            return DeleteResult.ERROR
        return DeleteResult.DELETED
