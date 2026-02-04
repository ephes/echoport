from django import forms
from django.contrib import admin
from django.core.exceptions import ValidationError

from .models import BackupRun, BackupTarget, RestoreRun
from .validation import (
    get_allowed_path_prefixes,
    validate_backup_source,
    validate_endpoint_key,
    validate_path,
    validate_schedule,
)


class BackupTargetAdminForm(forms.ModelForm):
    """Custom form with enhanced validation for BackupTarget."""

    # Use textarea for backup_files, one path per line (user-friendly)
    backup_files_text = forms.CharField(
        widget=forms.Textarea(attrs={"rows": 4, "cols": 60}),
        required=False,
        label="Backup files",
        help_text="One file/directory path per line.",
    )

    class Meta:
        model = BackupTarget
        fields = "__all__"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Update help text with current allowed prefixes
        allowed = ", ".join(get_allowed_path_prefixes())
        self.fields["backup_files_text"].help_text = (
            f"One file/directory path per line. Must be under: {allowed}"
        )
        # Convert JSON list to newline-separated text for editing.
        # Guard against invalid legacy data (non-list or non-string items) by
        # coercing to strings. This allows the form to load for remediation.
        # Note: Saving will still validate paths, so invalid data must be
        # corrected before any changes can be saved (intentional).
        if self.instance and self.instance.pk:
            files = self.instance.backup_files
            if isinstance(files, list):
                # Coerce non-string items to strings to allow remediation
                self.fields["backup_files_text"].initial = "\n".join(
                    str(f) if not isinstance(f, str) else f for f in files
                )
            elif files:
                # Non-list truthy value - show as string for remediation
                self.fields["backup_files_text"].initial = str(files)
            # else: None/empty - leave initial blank
        # Hide the raw JSON field
        if "backup_files" in self.fields:
            self.fields["backup_files"].widget = forms.HiddenInput()
            self.fields["backup_files"].required = False

    def clean_db_path(self):
        """Validate and normalize db_path."""
        db_path = self.cleaned_data.get("db_path", "")
        result = validate_path(db_path)
        if not result.is_valid:
            raise ValidationError(result.error)  # Plain string for field-level
        return result.value

    def clean_backup_files_text(self):
        """Convert textarea input to list of strings, validate paths."""
        text = self.cleaned_data.get("backup_files_text", "")
        if not text.strip():
            return []

        paths = []
        for line in text.strip().split("\n"):
            path = line.strip()
            if path:
                result = validate_path(path)
                if not result.is_valid:
                    raise ValidationError(result.error)  # Plain string for field-level
                paths.append(result.value)
        return paths

    def clean_schedule(self):
        """Validate cron expression."""
        schedule = self.cleaned_data.get("schedule", "")
        result = validate_schedule(schedule)
        if not result.is_valid:
            raise ValidationError(result.error)  # Plain string for field-level
        return result.value

    def clean_fastdeploy_endpoint_key(self):
        """Validate endpoint key exists and is complete in settings."""
        key = self.cleaned_data.get("fastdeploy_endpoint_key", "")
        result = validate_endpoint_key(key)
        if not result.is_valid:
            raise ValidationError(result.error)  # Plain string for field-level
        return result.value

    def clean(self):
        """Cross-field validation."""
        cleaned_data = super().clean()
        db_path = cleaned_data.get("db_path", "")
        backup_files = cleaned_data.get("backup_files_text", [])

        # Require at least one of db_path or backup_files
        error = validate_backup_source(db_path, backup_files)
        if error:
            raise ValidationError(error)

        # Transfer validated backup_files to the actual field
        cleaned_data["backup_files"] = backup_files
        return cleaned_data


@admin.register(BackupTarget)
class BackupTargetAdmin(admin.ModelAdmin):
    form = BackupTargetAdminForm
    list_display = ["name", "status", "schedule", "fastdeploy_service", "updated_at"]
    list_filter = ["status"]
    search_fields = ["name", "description"]
    readonly_fields = ["created_at", "updated_at"]

    fieldsets = [
        (None, {
            "fields": ["name", "description", "icon", "status"],
        }),
        ("FastDeploy Configuration", {
            "fields": ["fastdeploy_service", "fastdeploy_endpoint_key",
                       "service_name", "restore_owner"],
            "description": "FastDeploy endpoint key must match a key in FASTDEPLOY_ENDPOINTS setting (leave blank for default).",
        }),
        ("Backup Source", {
            "fields": ["db_path", "backup_files_text", "backup_files"],
        }),
        ("Schedule & Retention", {
            "fields": ["schedule", "retention_days", "timeout_seconds", "storage_bucket"],
            "description": "Schedule uses cron syntax (e.g., '0 2 * * *' for 2am daily). "
                          "Note: Paused/disabled targets are excluded from retention cleanup.",
        }),
        ("Timestamps", {
            "fields": ["created_at", "updated_at"],
            "classes": ["collapse"],
        }),
    ]

    def has_delete_permission(self, request, obj=None):
        # Block deletion to preserve audit history
        # Use status=disabled to retire targets instead
        return False


@admin.register(BackupRun)
class BackupRunAdmin(admin.ModelAdmin):
    """Read-only admin for viewing backup run history."""

    list_display = ["id", "target", "status", "trigger", "started_at", "finished_at"]
    list_filter = ["status", "trigger", "target"]
    search_fields = ["target__name", "storage_key"]
    date_hierarchy = "started_at"
    readonly_fields = [
        "target", "status", "trigger", "triggered_by",
        "fastdeploy_deployment_id", "storage_bucket", "storage_key",
        "size_bytes", "checksum_sha256", "file_count",
        "error_message", "logs", "started_at", "finished_at",
    ]

    def has_add_permission(self, request):
        return False  # Runs are created by the system

    def has_change_permission(self, request, obj=None):
        return False  # Runs are immutable

    def has_delete_permission(self, request, obj=None):
        return False  # Preserve audit trail


@admin.register(RestoreRun)
class RestoreRunAdmin(admin.ModelAdmin):
    """Read-only admin for viewing restore run history."""

    list_display = ["id", "target", "backup_run", "status", "trigger", "started_at", "finished_at"]
    list_filter = ["status", "trigger", "target"]
    search_fields = ["target__name"]
    date_hierarchy = "started_at"
    readonly_fields = [
        "backup_run", "target", "status", "trigger", "triggered_by",
        "fastdeploy_deployment_id", "files_restored",
        "error_message", "logs", "started_at", "finished_at",
    ]

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False  # Preserve audit trail
