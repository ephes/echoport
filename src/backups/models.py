from django.db import models
from django.utils import timezone


class BackupStatus(models.TextChoices):
    ACTIVE = "active", "Active"
    PAUSED = "paused", "Paused"
    DISABLED = "disabled", "Disabled"

    @classmethod
    def schedule_contract_observed_values(cls):
        """Statuses in which a required target must remain health-visible."""
        return tuple(value for value in cls.values if value != cls.DISABLED)


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


class BackupTargetMode(models.TextChoices):
    GENERIC_PATHS = "generic_paths", "Generic paths"
    SERVICE_OWNED = "service_owned", "Service-owned"


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
    fastdeploy_endpoint_key = models.CharField(
        max_length=50,
        blank=True,
        default="",
        help_text="Key to select FastDeploy endpoint from settings (e.g., 'staging'). "
                  "If blank, uses default FASTDEPLOY_BASE_URL/TOKEN.",
    )
    service_token = models.TextField(
        blank=True,
        default="",
        help_text="JWT token for FastDeploy service. If set, overrides token from "
                  "FASTDEPLOY_ENDPOINTS. Leave blank to use endpoint/default token.",
    )
    service_name = models.CharField(
        max_length=100,
        blank=True,
        help_text="Systemd service to stop during restore (e.g., 'nyxmon.service')",
    )
    restore_owner = models.CharField(
        max_length=100,
        blank=True,
        default="",
        help_text="User:group to chown restored files (e.g., 'marina:marina'). "
                  "If blank, ownership is not changed after restore.",
    )
    target_mode = models.CharField(
        max_length=20,
        choices=BackupTargetMode.choices,
        default=BackupTargetMode.GENERIC_PATHS,
        help_text=(
            "Target contract mode. 'Generic paths' requires service_name and at least one "
            "of db_path/backup_files. 'Service-owned' allows empty source fields."
        ),
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
    schedule_required = models.BooleanField(
        default=False,
        help_text=(
            "Require this active target to have a schedule. Missing schedules are "
            "rejected on save and reported as unhealthy if configuration drift "
            "bypasses validation."
        ),
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

    def clean(self):
        """
        Model-level validation, called by full_clean().

        This duplicates form validation to catch issues from non-admin save paths
        (management commands, Django shell). Uses centralized validation module.

        Aggregates errors into a dict for proper Django model validation.
        """
        from django.core.exceptions import ValidationError

        from .validation import (
            validate_backup_files,
            validate_backup_source,
            validate_endpoint_key,
            validate_path,
            validate_schedule,
        )

        errors = {}

        # Once enabled, the schedule contract may only be cleared as part of
        # explicit retirement. This prevents an active/paused Tier 1 target
        # from becoming silently manual-only through ordinary Admin saves.
        if self.pk and not self.schedule_required:
            persisted_contract = (
                type(self).objects.filter(pk=self.pk)
                .values_list("schedule_required", "status")
                .first()
            )
            if (
                persisted_contract
                and persisted_contract[0]
                and (
                    persisted_contract[1]
                    in BackupStatus.schedule_contract_observed_values()
                    or self.status in BackupStatus.schedule_contract_observed_values()
                )
            ):
                errors["schedule_required"] = (
                    "To remove the contract, save the target as disabled first, "
                    "then clear this flag in a separate save."
                )

        # Normalize service_token (strip whitespace)
        if self.service_token:
            self.service_token = self.service_token.strip()

        # Normalize service fields
        if self.service_name:
            self.service_name = self.service_name.strip()
        if self.restore_owner:
            self.restore_owner = self.restore_owner.strip()

        # Normalize and validate db_path (persists the trimmed value)
        if self.db_path:
            result = validate_path(self.db_path)
            if not result.is_valid:
                errors["db_path"] = result.error
            else:
                self.db_path = result.value

        # Always validate backup_files (catches invalid falsy values like "", 0, {})
        result = validate_backup_files(self.backup_files)
        if not result.is_valid:
            errors["backup_files"] = result.error
        else:
            self.backup_files = result.value

        # Validate schedule contract and cron expression
        if (
            self.status == BackupStatus.ACTIVE
            and self.schedule_required
            and not self.schedule
        ):
            errors["schedule"] = "A schedule is required for this backup target."
        if self.schedule:
            result = validate_schedule(self.schedule)
            if not result.is_valid:
                errors["schedule"] = result.error

        # Validate fastdeploy_endpoint_key
        if self.fastdeploy_endpoint_key:
            result = validate_endpoint_key(
                self.fastdeploy_endpoint_key,
                has_token_override=bool(self.service_token),
                service_name=self.fastdeploy_service,
            )
            if not result.is_valid:
                errors["fastdeploy_endpoint_key"] = result.error

        # Require at least one backup source
        source_error = validate_backup_source(
            self.db_path,
            self.backup_files,
            target_mode=self.target_mode,
            service_name=self.service_name,
        )
        if source_error:
            errors["__all__"] = source_error

        if errors:
            raise ValidationError(errors)

    def save(self, *args, **kwargs):
        """Override save to enforce validation."""
        self.full_clean()
        super().save(*args, **kwargs)

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


class RestoreRunStatus(models.TextChoices):
    PENDING = "pending", "Pending"
    RUNNING = "running", "Running"
    SUCCESS = "success", "Success"
    FAILED = "failed", "Failed"
    TIMEOUT = "timeout", "Timeout"


class RestoreTrigger(models.TextChoices):
    MANUAL = "manual", "Manual"
    API = "api", "API"


class RestoreRun(models.Model):
    """
    Individual restore execution record.
    """

    backup_run = models.ForeignKey(
        BackupRun,
        on_delete=models.PROTECT,
        related_name="restores",
        help_text="The backup run being restored from",
    )
    target = models.ForeignKey(
        BackupTarget,
        on_delete=models.CASCADE,
        related_name="restore_runs",
    )
    status = models.CharField(
        max_length=20,
        choices=RestoreRunStatus.choices,
        default=RestoreRunStatus.PENDING,
    )
    trigger = models.CharField(
        max_length=20,
        choices=RestoreTrigger.choices,
        default=RestoreTrigger.MANUAL,
    )
    triggered_by = models.CharField(
        max_length=100,
        blank=True,
        help_text="User or system that triggered this restore",
    )

    # FastDeploy tracking
    fastdeploy_deployment_id = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="FastDeploy deployment ID for this restore",
    )

    # Restore result data
    files_restored = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Number of files restored",
    )

    # Error handling
    error_message = models.TextField(
        blank=True,
    )
    logs = models.TextField(
        blank=True,
        help_text="Captured output from the restore process",
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
        db_table = "restore_run"
        ordering = ["-started_at"]
        constraints = [
            # Prevent concurrent restores for the same target
            models.UniqueConstraint(
                fields=["target"],
                condition=models.Q(status__in=["pending", "running"]),
                name="unique_active_restore_per_target",
            ),
        ]

    def __str__(self):
        return f"Restore #{self.id} from backup #{self.backup_run_id}"

    @property
    def duration_seconds(self):
        """Calculate restore duration in seconds."""
        if self.finished_at and self.started_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None

    @property
    def is_active(self):
        """Check if this restore run is still in progress."""
        return self.status in [RestoreRunStatus.PENDING, RestoreRunStatus.RUNNING]
