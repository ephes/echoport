"""
Management command to run a backup from the command line.

Usage:
    python manage.py backup <target_name>
"""

from django.core.management.base import BaseCommand, CommandError

from backups.backup_engine import (
    BackupError,
    BackupTimeoutError,
    ConcurrentBackupError,
    start_backup,
)
from backups.models import BackupRunStatus, BackupTarget, BackupTrigger


class Command(BaseCommand):
    help = "Run a backup for the specified target"

    def add_arguments(self, parser):
        parser.add_argument(
            "target_name",
            type=str,
            help="Name of the backup target to run",
        )
        parser.add_argument(
            "--triggered-by",
            type=str,
            default="cli",
            help="Who/what triggered this backup",
        )

    def handle(self, *args, **options):
        target_name = options["target_name"]
        triggered_by = options["triggered_by"]

        try:
            target = BackupTarget.objects.get(name=target_name)
        except BackupTarget.DoesNotExist:
            raise CommandError(f"Backup target '{target_name}' not found")

        if target.status != "active":
            raise CommandError(
                f"Backup target '{target_name}' is not active (status: {target.status})"
            )

        self.stdout.write(f"Starting backup for target '{target_name}'...")

        try:
            # start_backup is now synchronous
            run = start_backup(
                target,
                trigger=BackupTrigger.MANUAL,
                triggered_by=triggered_by,
            )

            if run.status == BackupRunStatus.SUCCESS:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Backup completed successfully!\n"
                        f"  Run ID: {run.id}\n"
                        f"  Storage: {run.storage_bucket}/{run.storage_key}\n"
                        f"  Size: {run.size_bytes:,} bytes\n"
                        f"  Duration: {run.duration_seconds:.1f}s"
                    )
                )
            else:
                self.stdout.write(
                    self.style.ERROR(
                        f"Backup failed with status: {run.status}\n"
                        f"  Error: {run.error_message}"
                    )
                )
                raise CommandError("Backup failed")

        except ConcurrentBackupError as e:
            raise CommandError(str(e))
        except BackupTimeoutError as e:
            raise CommandError(str(e))
        except BackupError as e:
            raise CommandError(f"Backup error: {e}")
