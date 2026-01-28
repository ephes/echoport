"""
Management command to create development/test data.

Usage:
    python manage.py create_devdata
"""

from django.core.management.base import BaseCommand

from backups.models import BackupTarget


class Command(BaseCommand):
    help = "Create development data including the NyxMon backup target"

    def handle(self, *args, **options):
        self.stdout.write("Creating development data...")

        # Create NyxMon backup target
        # Note: .env is NOT backed up (contains secrets managed by ops-control)
        # On restore, run `just deploy-one nyxmon` from ops-control to regenerate .env
        nyxmon_target, created = BackupTarget.objects.update_or_create(
            name="nyxmon",
            defaults={
                "description": "NyxMon monitoring service database and configuration",
                "fastdeploy_service": "echoport-backup",
                "db_path": "/home/nyxmon/site/db.sqlite3",
                "backup_files": [
                    # Only non-secret config files; .env excluded per PRD
                    "/home/nyxmon/site/pyproject.toml",
                    "/home/nyxmon/site/uv.lock",
                ],
                "schedule": "0 2 * * *",  # 2am daily
                "status": "active",
                "retention_days": 30,
                "timeout_seconds": 600,
                "storage_bucket": "backups",
            },
        )

        if created:
            self.stdout.write(
                self.style.SUCCESS(f"Created backup target: {nyxmon_target.name}")
            )
        else:
            self.stdout.write(
                self.style.WARNING(f"Updated backup target: {nyxmon_target.name}")
            )

        # Create FastDeploy backup target (for completeness)
        # Note: .env excluded; secrets managed by ops-control
        fastdeploy_target, created = BackupTarget.objects.update_or_create(
            name="fastdeploy",
            defaults={
                "description": "FastDeploy deployment service (PostgreSQL + services)",
                "fastdeploy_service": "echoport-backup",
                "db_path": "",  # PostgreSQL, handled differently
                "backup_files": [
                    # Only non-secret files; .env excluded per PRD
                    "/home/fastdeploy/site/services",
                ],
                "schedule": "0 3 * * *",  # 3am daily
                "status": "paused",  # Not active yet
                "retention_days": 30,
                "timeout_seconds": 900,
                "storage_bucket": "backups",
            },
        )

        if created:
            self.stdout.write(
                self.style.SUCCESS(f"Created backup target: {fastdeploy_target.name}")
            )
        else:
            self.stdout.write(
                self.style.WARNING(f"Updated backup target: {fastdeploy_target.name}")
            )

        self.stdout.write(self.style.SUCCESS("Development data created successfully!"))
