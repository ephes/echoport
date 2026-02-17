"""
Management command to run a restore from the command line.

Usage:
    python manage.py restore <target_name> <backup_run_id> [--triggered-by=...]

This command runs the restore synchronously, making it safe for self-restore
scenarios where the web process cannot be used (e.g., restoring echoport itself).
"""

from django.core.management.base import BaseCommand, CommandError

from backups.models import BackupRun, BackupRunStatus, BackupTarget, RestoreRunStatus
from backups.restore_engine import (
    ConcurrentBackupError,
    ConcurrentRestoreError,
    MissingChecksumError,
    RestoreError,
    RestoreTimeoutError,
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

    def handle(self, *args, **options):
        target_name = options["target_name"]
        backup_run_id = options["backup_run_id"]
        triggered_by = options["triggered_by"]

        try:
            target = BackupTarget.objects.get(name=target_name)
        except BackupTarget.DoesNotExist:
            raise CommandError(f"Backup target '{target_name}' not found")

        if target.status != "active":
            raise CommandError(
                f"Backup target '{target_name}' is not active (status: {target.status})"
            )

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
            f"Starting restore for target '{target_name}' "
            f"from backup run {backup_run_id}..."
        )

        try:
            run = start_restore(
                backup_run,
                triggered_by=triggered_by,
            )

            if run.status == RestoreRunStatus.SUCCESS:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Restore completed successfully!\n"
                        f"  Restore ID: {run.id}\n"
                        f"  Backup run: {backup_run_id}\n"
                        f"  Files restored: {run.files_restored or 0}\n"
                        f"  Duration: {run.duration_seconds:.1f}s"
                    )
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
