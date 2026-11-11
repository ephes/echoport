"""
Management command to run a restore from the command line.

Usage:
    python manage.py restore <target_name> <backup_run_id> [--triggered-by=...]
        [--destination live|scratch] [--keep-scratch]

This command runs the restore synchronously, making it safe for self-restore
scenarios where the web process cannot be used (e.g., restoring echoport itself).

--destination scratch restores into a scratch directory under the target's
root instead of the live paths, without stopping services, verifies the copy
against the archive and removes it again (--keep-scratch keeps it). The target
needs scratch_restore_enabled and a newest successful backup by a runner that
reports the restore-scratch capability.
"""

from django.core.management.base import BaseCommand, CommandError

from backups.models import (
    BackupRun,
    BackupRunStatus,
    BackupTarget,
    RestoreDestination,
    RestoreRunStatus,
    exact_name_hint,
)
from backups.restore_engine import (
    ConcurrentBackupError,
    ConcurrentRestoreError,
    MissingChecksumError,
    RestoreError,
    RestoreTimeoutError,
    restore_blocked_reason,
    scratch_restore_blocked_reason,
    start_restore,
)


class Command(BaseCommand):
    help = "Run a restore for the specified target from a backup run"

    def add_arguments(self, parser):
        parser.add_argument(
            "target_name",
            type=str,
            help="Name of the backup target to restore",
        )
        parser.add_argument(
            "backup_run_id",
            type=int,
            help="ID of the backup run to restore from",
        )
        parser.add_argument(
            "--triggered-by",
            type=str,
            default="cli",
            help="Who/what triggered this restore",
        )
        parser.add_argument(
            "--destination",
            choices=RestoreDestination.values,
            default=RestoreDestination.LIVE,
            help=(
                "live (default): restore in place; scratch: restore into "
                "<root>/.restore-scratch-<stamp> under the target's root, verify it "
                "and remove it again, with no service stop and no live path touched"
            ),
        )
        parser.add_argument(
            "--keep-scratch",
            action="store_true",
            help="With --destination scratch: keep the verified copy for inspection",
        )

    def handle(self, *args, **options):
        target_name = options["target_name"]
        backup_run_id = options["backup_run_id"]
        triggered_by = options["triggered_by"]
        destination = options["destination"]
        keep_scratch = options["keep_scratch"]
        if keep_scratch and destination != RestoreDestination.SCRATCH:
            raise CommandError("--keep-scratch needs --destination scratch")

        try:
            target = BackupTarget.objects.get(name=target_name)
        except BackupTarget.DoesNotExist:
            raise CommandError(
                f"Backup target '{target_name}' not found.{exact_name_hint(target_name)}"
            )

        blocked_reason = restore_blocked_reason(target)
        if blocked_reason:
            raise CommandError(blocked_reason)
        if destination == RestoreDestination.SCRATCH:
            blocked_reason = scratch_restore_blocked_reason(target)
            if blocked_reason:
                raise CommandError(blocked_reason)

        try:
            backup_run = BackupRun.objects.get(id=backup_run_id, target=target)
        except BackupRun.DoesNotExist:
            raise CommandError(
                f"Backup run {backup_run_id} not found for target '{target_name}'"
            )

        if backup_run.status != BackupRunStatus.SUCCESS:
            raise CommandError(
                f"Backup run {backup_run_id} has status '{backup_run.status}', "
                f"expected '{BackupRunStatus.SUCCESS}'"
            )

        if not backup_run.checksum_sha256:
            raise CommandError(
                f"Backup run {backup_run_id} has no checksum — cannot safely restore"
            )

        self.stdout.write(
            f"Starting {destination} restore for target '{target_name}' "
            f"from backup run {backup_run_id}..."
        )

        try:
            run = start_restore(
                backup_run,
                triggered_by=triggered_by,
                destination=destination,
                keep_scratch=keep_scratch,
            )

            if run.status == RestoreRunStatus.SUCCESS:
                duration_text = (
                    f"{run.duration_seconds:.1f}s" if run.duration_seconds is not None else "unknown"
                )
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Restore completed successfully!\n"
                        f"  Restore ID: {run.id}\n"
                        f"  Backup run: {backup_run_id}\n"
                        f"  Files restored: {run.files_restored or 0}\n"
                        f"  Duration: {duration_text}"
                    )
                )
                if run.destination == RestoreDestination.SCRATCH:
                    verification = run.verification or {}
                    state = "kept" if run.keep_scratch else "removed again"
                    self.stdout.write(
                        f"  Scratch dir: {run.scratch_path} ({state})\n"
                        f"  Verification: archive sha256 {verification.get('archive_sha256')}, "
                        f"database sha256 {verification.get('db_sha256')}, "
                        f"integrity {verification.get('integrity')}, "
                        f"{verification.get('tables')} tables, {verification.get('entries')} entries"
                    )
            else:
                self.stdout.write(
                    self.style.ERROR(
                        f"Restore failed with status: {run.status}\n"
                        f"  Error: {run.error_message}"
                    )
                )
                raise CommandError("Restore failed")

        except ConcurrentRestoreError as e:
            raise CommandError(str(e))
        except ConcurrentBackupError as e:
            raise CommandError(str(e))
        except MissingChecksumError as e:
            raise CommandError(str(e))
        except RestoreTimeoutError as e:
            raise CommandError(str(e))
        except RestoreError as e:
            raise CommandError(f"Restore error: {e}")
