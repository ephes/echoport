"""
Management command to create development/test data.

Usage:
    python manage.py create_devdata

Note: This command is disabled in production (DEBUG=False).
Use Django admin to manage backup targets in production.
"""

from django.conf import settings
from django.core.management.base import BaseCommand

from backups.models import BackupTarget


class Command(BaseCommand):
    help = "Create development data including backup targets"

    def handle(self, *args, **options):
        if not settings.DEBUG:
            self.stderr.write(
                self.style.ERROR(
                    "create_devdata is disabled in production. "
                    "Use Django admin to manage backup targets."
                )
            )
            return

        self.stdout.write("Creating development data...")

        targets = [
            {
                "name": "nyxmon",
                "defaults": {
                    "description": "NyxMon monitoring service database and configuration",
                    "icon": "📊",
                    "fastdeploy_service": "echoport-backup",
                    "service_name": "nyxmon.service",
                    "db_path": "/home/nyxmon/site/db.sqlite3",
                    "backup_files": [
                        "/home/nyxmon/site/pyproject.toml",
                        "/home/nyxmon/site/uv.lock",
                    ],
                    "schedule": "0 2 * * *",  # 2am daily
                    "status": "active",
                    "retention_days": 30,
                    "timeout_seconds": 600,
                    "storage_bucket": "backups",
                },
            },
            {
                "name": "homelab",
                "defaults": {
                    "description": "Homelab Django app - home infrastructure dashboard",
                    "icon": "🏠",
                    "fastdeploy_service": "echoport-backup",
                    "service_name": "homelab.service",
                    "db_path": "/home/homelab/site/db.sqlite3",
                    "backup_files": [
                        "/home/homelab/site/pyproject.toml",
                        "/home/homelab/site/uv.lock",
                    ],
                    "schedule": "0 2 * * *",  # 2am daily
                    "status": "active",
                    "retention_days": 30,
                    "timeout_seconds": 600,
                    "storage_bucket": "backups",
                },
            },
            {
                "name": "fastdeploy",
                "defaults": {
                    "description": (
                        "FastDeploy PostgreSQL + services + runners backup/restore "
                        "via service-owned script."
                    ),
                    "icon": "🚀",
                    "fastdeploy_service": "fastdeploy-backup",
                    "service_name": "fastdeploy",
                    # Placeholder required by current BackupTarget validation.
                    # The service-owned script handles PostgreSQL backup/restore.
                    "db_path": "/home/fastdeploy/site/db.sqlite3",
                    "backup_files": [],
                    "schedule": "0 3 * * *",  # 3am daily
                    "status": "active",
                    "retention_days": 30,
                    "timeout_seconds": 1200,
                    "storage_bucket": "backups",
                },
            },
            {
                "name": "echoport",
                "defaults": {
                    "description": "Echoport backup service database",
                    "icon": "🔄",
                    "fastdeploy_service": "echoport-backup",
                    "service_name": "echoport.service",
                    "db_path": "/home/echoport/site/db.sqlite3",
                    "backup_files": [
                        "/home/echoport/site/pyproject.toml",
                        "/home/echoport/site/uv.lock",
                    ],
                    "schedule": "0 4 * * *",  # 4am daily
                    "status": "active",
                    "retention_days": 30,
                    "timeout_seconds": 600,
                    "storage_bucket": "backups",
                },
            },
        ]

        for target_data in targets:
            target, created = BackupTarget.objects.update_or_create(
                name=target_data["name"],
                defaults=target_data["defaults"],
            )

            if created:
                self.stdout.write(
                    self.style.SUCCESS(f"Created backup target: {target.name}")
                )
            else:
                self.stdout.write(
                    self.style.WARNING(f"Updated backup target: {target.name}")
                )

        self.stdout.write(self.style.SUCCESS("Development data created successfully!"))
