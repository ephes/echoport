"""
Management command to run scheduled backups.

This command is designed to be run from cron every few minutes.
It checks all active targets with schedules and triggers backups
for any that are due.

Usage:
    python manage.py run_scheduled_backups

The command uses a file lock to prevent overlapping instances.
"""

import errno
import fcntl
import logging
import os
import sys
from pathlib import Path

from croniter import CroniterBadCronError, CroniterBadDateError, croniter
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from backups.backup_engine import (
    BackupError,
    ConcurrentBackupError,
    get_active_run,
    start_backup,
)
from backups.models import BackupRunStatus, BackupStatus, BackupTarget, BackupTrigger

logger = logging.getLogger(__name__)


def _get_lock_file_path() -> Path:
    """
    Get the lock file path, preferring a controlled directory.

    Uses cache dir if available (production), otherwise /tmp.
    """
    # Try to use the cache directory from settings (controlled, not world-writable)
    cache_dir = getattr(settings, "ECHOPORT_CACHE_DIR", None)
    if cache_dir and Path(cache_dir).is_dir():
        return Path(cache_dir) / "scheduler.lock"

    # Fall back to /tmp
    return Path("/tmp/echoport-scheduler.lock")


class Command(BaseCommand):
    help = "Run scheduled backups that are due"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be run without actually running backups",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]

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
                    self.style.WARNING("Another scheduler instance is running, exiting")
                )
                sys.exit(0)  # Exit cleanly - this is expected behavior
        else:
            lock_file = None

        try:
            exit_code = self._run_scheduler(dry_run)
            sys.exit(exit_code)
        finally:
            if lock_file:
                self._release_lock(lock_file)

    def _acquire_lock(self):
        """
        Acquire an exclusive lock to prevent overlapping instances.

        Returns:
            - file object if lock acquired successfully
            - None if another instance is already running (EAGAIN/EWOULDBLOCK)

        Raises:
            OSError for permission errors or other filesystem issues
        """
        lock_path = _get_lock_file_path()
        lock_file = None
        try:
            # Use O_NOFOLLOW to prevent symlink attacks in world-writable dirs
            # O_CREAT | O_WRONLY creates the file if it doesn't exist
            fd = os.open(
                lock_path,
                os.O_CREAT | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            lock_file = os.fdopen(fd, "w")
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return lock_file
        except OSError as e:
            # Clean up file handle on any error
            if lock_file is not None:
                lock_file.close()
            # EAGAIN/EWOULDBLOCK means another instance holds the lock
            if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                return None
            # ELOOP means it's a symlink (O_NOFOLLOW)
            if e.errno == errno.ELOOP:
                raise OSError(errno.ELOOP, f"Lock file is a symlink (possible attack): {lock_path}")
            # Other errors (permission denied, etc.) should propagate
            raise

    def _release_lock(self, lock_file):
        """Release the lock file."""
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            lock_file.close()
        except (OSError, IOError):
            pass

    def _run_scheduler(self, dry_run: bool) -> int:
        """
        Run the scheduler logic.

        Returns exit code: 0 for success, 1 if any errors occurred.
        """
        now = timezone.now()

        # Get all active targets with a schedule
        targets = BackupTarget.objects.filter(
            status=BackupStatus.ACTIVE,
        ).exclude(schedule="")

        self.stdout.write(f"Checking {targets.count()} scheduled targets...")

        triggered = 0
        skipped = 0
        errors = 0

        for target in targets:
            if self._is_due_for_backup(target, now):
                if dry_run:
                    self.stdout.write(f"  [DRY RUN] Would trigger backup for '{target.name}'")
                    triggered += 1
                else:
                    result = self._trigger_backup(target)
                    if result == "success":
                        triggered += 1
                    elif result == "skipped":
                        skipped += 1
                    else:  # "error"
                        errors += 1
            else:
                skipped += 1

        if dry_run:
            self.stdout.write(
                self.style.SUCCESS(f"Dry run complete: {triggered} would run, {skipped} not due")
            )
            return 0

        # Report results
        if errors > 0:
            self.stderr.write(
                self.style.ERROR(
                    f"Complete with errors: {triggered} succeeded, {errors} failed, {skipped} skipped"
                )
            )
            return 1
        else:
            self.stdout.write(
                self.style.SUCCESS(f"Complete: {triggered} triggered, {skipped} skipped")
            )
            return 0

    def _is_due_for_backup(self, target: BackupTarget, now) -> bool:
        """
        Determine if a target is due for a scheduled backup.

        Logic:
        1. Use croniter to find the most recent scheduled time before 'now'
        2. If the last scheduled run (trigger=scheduled) started before that time,
           the backup is due
        3. If there's no previous scheduled run, the backup is due (immediate first run)
        """
        if not target.schedule:
            return False

        try:
            # Find the most recent scheduled time before now
            cron = croniter(target.schedule, now)
            last_scheduled_time = cron.get_prev(type(now))

            # Get the most recent scheduled run
            last_run = target.get_last_scheduled_run()

            if last_run is None:
                # Never had a scheduled run - it's due
                logger.debug(f"Target '{target.name}' has no previous scheduled runs - due")
                return True

            # Compare: if last run started before the last scheduled time, we're due
            is_due = last_run.started_at < last_scheduled_time

            if is_due:
                logger.debug(
                    f"Target '{target.name}' is due: last run at {last_run.started_at}, "
                    f"scheduled time was {last_scheduled_time}"
                )
            else:
                logger.debug(
                    f"Target '{target.name}' not due: last run at {last_run.started_at}, "
                    f"scheduled time was {last_scheduled_time}"
                )

            return is_due

        except (KeyError, ValueError, CroniterBadCronError, CroniterBadDateError) as e:
            logger.warning(f"Invalid cron schedule for target '{target.name}': {e}")
            self.stderr.write(
                self.style.WARNING(f"  Skipping '{target.name}': invalid schedule '{target.schedule}'")
            )
            return False

    def _trigger_backup(self, target: BackupTarget) -> str:
        """
        Trigger a backup for the given target.

        Returns:
            "success" - backup completed successfully
            "skipped" - backup was skipped (already running)
            "error" - backup failed
        """
        # Check if there's already an active backup
        if get_active_run(target):
            self.stdout.write(
                self.style.WARNING(f"  Skipping '{target.name}': backup already in progress")
            )
            return "skipped"

        self.stdout.write(f"  Triggering backup for '{target.name}'...")

        try:
            run = start_backup(
                target,
                trigger=BackupTrigger.SCHEDULED,
                triggered_by="scheduler",
            )

            if run.status == BackupRunStatus.SUCCESS:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"    Success: {run.storage_key} ({run.size_bytes:,} bytes)"
                    )
                )
                return "success"
            else:
                self.stderr.write(
                    self.style.ERROR(f"    Failed: {run.error_message}")
                )
                return "error"

        except ConcurrentBackupError:
            self.stdout.write(
                self.style.WARNING(f"  Skipping '{target.name}': concurrent backup detected")
            )
            return "skipped"

        except BackupError as e:
            self.stderr.write(
                self.style.ERROR(f"  Error backing up '{target.name}': {e}")
            )
            return "error"
