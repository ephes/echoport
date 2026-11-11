from django.conf import settings
from django.db import models
from django.db.models.functions import Lower
from django.utils import timezone

# Identity of Echoport's own backup target. A restore of this target stops the
# echoport systemd unit, which kills the web process running the restore, so it
# must run from the CLI. Matching is case-insensitive and whitespace-tolerant.
SELF_TARGET_NAME = "echoport"
SELF_FASTDEPLOY_SERVICE = "echoport-self-backup"
SELF_SYSTEMD_UNIT = "echoport.service"


def normalize_identity(value: str | None) -> str:
    """Normalize a target/service identifier for identity comparisons."""
    return (value or "").strip().casefold()


def _normalize_systemd_unit(value: str | None) -> str:
    unit = normalize_identity(value)
    if unit and "." not in unit:
        # systemd treats a bare unit name as "<name>.service"
        unit = f"{unit}.service"
    return unit


# BackupTarget.storage_alias under which retention leaves the target alone:
# while its archives move between stores, neither store holds all of them.
STORAGE_ALIAS_PAUSED = ""


def default_storage_alias() -> str:
    """The store of a new target (and of every target that existed before the field)."""
    return settings.MINIO_ALIAS


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
        help_text=(
            "Unique identifier for this backup target (e.g., 'nyxmon'). "
            "Names are unique regardless of case."
        ),
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
    scratch_restore_enabled = models.BooleanField(
        default=False,
        help_text=(
            "Allow restores into a scratch destination under the target's root "
            "(manage.py restore --destination scratch). Only for targets whose "
            "runner supports it; a scratch restore also needs the newest "
            "successful backup to report the runner capability. Clear it "
            "before rolling a runner back."
        ),
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
        help_text="Cron expression in UTC (e.g. '0 2 * * *' = 02:00 UTC daily)",
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
    # Changed only by `manage.py set_storage_alias`, which holds the cleanup
    # lock and refuses while a deletion of the target is pending.
    storage_alias = models.CharField(
        max_length=64,
        blank=True,
        default=default_storage_alias,
        editable=False,
        help_text=(
            "mc alias of the object store retention deletes this target's "
            "archives from (one of MINIO_ALIASES); empty pauses retention "
            "for the target. Set with manage.py set_storage_alias."
        ),
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "backup_target"
        ordering = ["name"]
        constraints = [
            models.UniqueConstraint(
                Lower("name"),
                name="unique_backup_target_name_ci",
                violation_error_message=(
                    "A backup target with this name already exists "
                    "(names are compared case-insensitively)."
                ),
            ),
        ]

    def __str__(self):
        return self.name

    @property
    def is_self_target(self) -> bool:
        """
        True when this target backs up Echoport itself.

        Matches the normalized target name, the dedicated FastDeploy service,
        or the systemd unit a restore would stop, so a renamed or differently
        cased self target is still recognized.
        """
        return (
            normalize_identity(self.name) == SELF_TARGET_NAME
            or normalize_identity(self.fastdeploy_service) == SELF_FASTDEPLOY_SERVICE
            or _normalize_systemd_unit(self.service_name) == SELF_SYSTEMD_UNIT
        )

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
            validate_storage_alias,
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
        non_field = [source_error] if source_error else []
        # Not a form field (set by command only), so reported for the whole target.
        result = validate_storage_alias(self.storage_alias)
        if not result.is_valid:
            non_field.append(result.error)
        if non_field:
            errors["__all__"] = non_field

        if errors:
            raise ValidationError(errors)

    def save(self, *args, **kwargs):
        """
        Override save to enforce validation.

        An existing target's storage_alias is never written here: only
        set_storage_alias changes it, under its barrier, so a save of an
        instance loaded before that change cannot write the old store back.
        """
        self.full_clean()
        if not self._state.adding:
            fields = kwargs.get("update_fields")
            if fields is None:
                kwargs["update_fields"] = [
                    field.name for field in self._meta.concrete_fields
                    if not field.primary_key and field.name != "storage_alias"
                ]
            elif "storage_alias" in fields:
                raise ValueError("storage_alias changes only with manage.py set_storage_alias")
        super().save(*args, **kwargs)

    def get_last_successful_run(self):
        """Get the most recent successful backup run for this target."""
        return self.runs.filter(status=BackupRunStatus.SUCCESS, deletion_pending_at__isnull=True).order_by("-started_at").first()

    def get_last_run(self):
        """Get the most recent backup run for this target."""
        return self.runs.order_by("-started_at").first()

    def get_last_scheduled_run(self):
        """Get the most recent scheduled backup run for this target (any status)."""
        return self.runs.filter(trigger=BackupTrigger.SCHEDULED).order_by("-started_at").first()


def exact_name_hint(name: str) -> str:
    """
    Explain a failed exact target-name lookup.

    CLI lookups stay exact so scripts and cron entries never act on a
    different target than they name. When a target matches only after
    ignoring case or surrounding whitespace, name it so the operator can
    correct the command.
    """
    candidates = list(
        BackupTarget.objects.filter(name__iexact=(name or "").strip())
        .exclude(name=name)
        .values_list("name", flat=True)
    )
    if not candidates:
        return ""
    suggestions = ", ".join(f"'{candidate}'" for candidate in candidates)
    return f" Target names are matched exactly; did you mean {suggestions}?"


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
    deletion_pending_at = models.DateTimeField(null=True, blank=True, editable=False)

    checksum_sha256 = models.CharField(
        max_length=64,
        blank=True,
    )
    file_count = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Number of files included in the backup",
    )
    # What the runner that made this backup reported it can do
    # (ECHOPORT_RESULT "capabilities", e.g. ["restore-scratch"]).
    runner_capabilities = models.JSONField(
        default=list,
        blank=True,
        editable=False,
        help_text="Capabilities the runner reported with this backup",
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


class RestoreDestination(models.TextChoices):
    LIVE = "live", "Live (in place)"
    SCRATCH = "scratch", "Scratch (under the target root, removed again)"


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

    destination = models.CharField(
        max_length=20,
        choices=RestoreDestination.choices,
        default=RestoreDestination.LIVE,
        help_text="Where the restore wrote: the live paths or a scratch directory",
    )
    keep_scratch = models.BooleanField(
        default=False,
        help_text="Scratch restores only: keep the verified copy for inspection",
    )
    scratch_path = models.CharField(
        max_length=512,
        blank=True,
        default="",
        help_text="Scratch restores: the directory the runner restored into",
    )
    verification = models.JSONField(
        null=True,
        blank=True,
        help_text="Scratch restores: the runner's verification of the restored copy",
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
