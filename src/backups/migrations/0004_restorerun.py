# Generated manually for restore functionality

from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ("backups", "0003_add_icon_field"),
    ]

    operations = [
        # Add service_name field to BackupTarget
        migrations.AddField(
            model_name="backuptarget",
            name="service_name",
            field=models.CharField(
                blank=True,
                help_text="Systemd service to stop during restore (e.g., 'nyxmon.service')",
                max_length=100,
            ),
        ),
        # Create RestoreRun model
        migrations.CreateModel(
            name="RestoreRun",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("pending", "Pending"),
                            ("running", "Running"),
                            ("success", "Success"),
                            ("failed", "Failed"),
                            ("timeout", "Timeout"),
                        ],
                        default="pending",
                        max_length=20,
                    ),
                ),
                (
                    "trigger",
                    models.CharField(
                        choices=[("manual", "Manual"), ("api", "API")],
                        default="manual",
                        max_length=20,
                    ),
                ),
                (
                    "triggered_by",
                    models.CharField(
                        blank=True,
                        help_text="User or system that triggered this restore",
                        max_length=100,
                    ),
                ),
                (
                    "fastdeploy_deployment_id",
                    models.PositiveIntegerField(
                        blank=True,
                        help_text="FastDeploy deployment ID for this restore",
                        null=True,
                    ),
                ),
                (
                    "files_restored",
                    models.PositiveIntegerField(
                        blank=True,
                        help_text="Number of files restored",
                        null=True,
                    ),
                ),
                ("error_message", models.TextField(blank=True)),
                (
                    "logs",
                    models.TextField(
                        blank=True,
                        help_text="Captured output from the restore process",
                    ),
                ),
                (
                    "started_at",
                    models.DateTimeField(default=django.utils.timezone.now),
                ),
                ("finished_at", models.DateTimeField(blank=True, null=True)),
                (
                    "backup_run",
                    models.ForeignKey(
                        help_text="The backup run being restored from",
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="restores",
                        to="backups.backuprun",
                    ),
                ),
                (
                    "target",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="restore_runs",
                        to="backups.backuptarget",
                    ),
                ),
            ],
            options={
                "db_table": "restore_run",
                "ordering": ["-started_at"],
            },
        ),
        # Add constraint for unique active restore per target
        migrations.AddConstraint(
            model_name="restorerun",
            constraint=models.UniqueConstraint(
                condition=models.Q(status__in=["pending", "running"]),
                fields=("target",),
                name="unique_active_restore_per_target",
            ),
        ),
    ]
