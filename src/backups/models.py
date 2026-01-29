from django.db import models
from django.utils import timezone


class BackupStatus(models.TextChoices):
    ACTIVE = "active", "Active"
    PAUSED = "paused", "Paused"
    DISABLED = "disabled", "Disabled"


class BackupRunStatus(models.TextChoices):
    PENDING = "pending", "Pending"
    RUNNING = "running", "Running"
    SUCCESS = "success", "Success"
    FAILED = "failed", "Failed"
    TIMEOUT = "timeout", "Timeout"


class BackupTrigger(models.TextChoices):
    MANUAL = "manual", "Manual"
    SCHEDULED = "scheduled", "Scheduled"
    API = "api", "API"


class BackupTarget(models.Model):
    """
    Source of truth for backup configuration.
    Defines what to back up and how.
    """

    name = models.CharField(
        max_length=100,
        unique=True,
        help_text="Unique identifier for this backup target (e.g., 'nyxmon')",
    )
    description = models.TextField(
        blank=True,
        help_text="Human-readable description of what this backup covers",
    )
    icon = models.CharField(
        max_length=50,
        blank=True,
        help_text="Emoji or icon identifier for this target (e.g., '🏠' or '📊')",
    )

    # FastDeploy service configuration
    fastdeploy_service = models.CharField(
        max_length=100,
        help_text="Name of the FastDeploy service to use for backups",
    )

    # Backup source configuration - passed to FastDeploy as context
    db_path = models.CharField(
        max_length=500,
        blank=True,
        help_text="Path to SQLite database to back up",
    )
    backup_files = models.JSONField(
        default=list,
        blank=True,
        help_text="List of additional files/directories to include in backup",
    )

    # Schedule (cron format)
    schedule = models.CharField(
        max_length=100,
        blank=True,
        help_text="Cron schedule expression (e.g., '0 2 * * *' for 2am daily)",
    )

    # Status and settings
    status = models.CharField(
        max_length=20,
        choices=BackupStatus.choices,
        default=BackupStatus.ACTIVE,
    )
    retention_days = models.PositiveIntegerField(
        default=30,
        help_text="Number of days to retain backups",
    )
    timeout_seconds = models.PositiveIntegerField(
        default=600,
        help_text="Maximum time to wait for backup to complete",
    )

    # MinIO storage configuration
    storage_bucket = models.CharField(
        max_length=100,
        default="backups",
        help_text="MinIO bucket for storing backups",
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "backup_target"
        ordering = ["name"]

    def __str__(self):
        return self.name

    def get_last_successful_run(self):
        """Get the most recent successful backup run for this target."""
        return self.runs.filter(status=BackupRunStatus.SUCCESS).order_by("-started_at").first()

    def get_last_run(self):
        """Get the most recent backup run for this target."""
        return self.runs.order_by("-started_at").first()

    def get_last_scheduled_run(self):
        """Get the most recent scheduled backup run for this target (any status)."""
        return self.runs.filter(trigger=BackupTrigger.SCHEDULED).order_by("-started_at").first()


class BackupRun(models.Model):
    """
    Individual backup execution record.
    """

    target = models.ForeignKey(
        BackupTarget,
        on_delete=models.CASCADE,
        related_name="runs",
    )
    status = models.CharField(
        max_length=20,
        choices=BackupRunStatus.choices,
        default=BackupRunStatus.PENDING,
    )
    trigger = models.CharField(
        max_length=20,
        choices=BackupTrigger.choices,
        default=BackupTrigger.MANUAL,
    )
    triggered_by = models.CharField(
        max_length=100,
        blank=True,
        help_text="User or system that triggered this backup",
    )

    # FastDeploy tracking
    fastdeploy_deployment_id = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="FastDeploy deployment ID for this backup",
    )

    # Backup result data
    storage_bucket = models.CharField(
        max_length=100,
        blank=True,
    )
    storage_key = models.CharField(
        max_length=500,
        blank=True,
        help_text="Object key in MinIO (e.g., 'nyxmon/2024-01-15T02-00-00.tar.gz')",
    )
    size_bytes = models.BigIntegerField(
        null=True,
        blank=True,
    )
    checksum_sha256 = models.CharField(
        max_length=64,
        blank=True,
    )
    file_count = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Number of files included in the backup",
    )

    # Error handling
    error_message = models.TextField(
        blank=True,
    )
    logs = models.TextField(
        blank=True,
        help_text="Captured output from the backup process",
    )

    # Timestamps
    started_at = models.DateTimeField(
        default=timezone.now,
    )
    finished_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    class Meta:
        db_table = "backup_run"
        ordering = ["-started_at"]
        constraints = [
            # Prevent concurrent backups for the same target
            models.UniqueConstraint(
                fields=["target"],
                condition=models.Q(status__in=["pending", "running"]),
                name="unique_active_backup_per_target",
            ),
        ]

    def __str__(self):
        return f"{self.target.name} - {self.started_at.strftime('%Y-%m-%d %H:%M')}"

    @property
    def duration_seconds(self):
        """Calculate backup duration in seconds."""
        if self.finished_at and self.started_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None

    @property
    def is_active(self):
        """Check if this backup run is still in progress."""
        return self.status in [BackupRunStatus.PENDING, BackupRunStatus.RUNNING]
