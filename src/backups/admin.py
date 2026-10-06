from django import forms
from django.contrib import admin
from django.core.exceptions import ValidationError
from django.utils import timezone
from django.utils.html import format_html

from .models import BackupRun, BackupTarget, BackupTargetMode, RestoreRun
from .service_tokens import STATUS_LEGACY, STATUS_OK, assess_target
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
        widgets = {
            "service_token": forms.PasswordInput(render_value=True),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Update help text with current allowed prefixes
        allowed = ", ".join(get_allowed_path_prefixes())
        self.fields["backup_files_text"].help_text = (
            f"One file/directory path per line. Must be under: {allowed}. "
            f"Required for '{BackupTargetMode.GENERIC_PATHS}' when db_path is empty."
        )
        self.fields["db_path"].help_text = (
            f"{self.fields['db_path'].help_text} "
            f"Required for '{BackupTargetMode.GENERIC_PATHS}' when backup_files is empty."
        )
        self.fields["service_name"].help_text = (
            f"{self.fields['service_name'].help_text} "
            f"Required for '{BackupTargetMode.GENERIC_PATHS}'."
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

    def clean(self):
        """Cross-field validation."""
        cleaned_data = super().clean()
        target_mode = cleaned_data.get("target_mode", BackupTargetMode.GENERIC_PATHS)
        db_path = cleaned_data.get("db_path", "")
        backup_files = cleaned_data.get("backup_files_text", [])
        service_name = cleaned_data.get("service_name", "")

        # Mode-aware source validation
        error = validate_backup_source(
            db_path,
            backup_files,
            target_mode=target_mode,
            service_name=service_name,
        )
        if error:
            raise ValidationError(error)

        # Validate endpoint key (done here so all fields are available)
        key = cleaned_data.get("fastdeploy_endpoint_key", "")
        if key:
            has_token_override = bool(cleaned_data.get("service_token", "").strip())
            service_name = cleaned_data.get("fastdeploy_service", "")
            result = validate_endpoint_key(
                key,
                has_token_override=has_token_override,
                service_name=service_name,
            )
            if not result.is_valid:
                self.add_error("fastdeploy_endpoint_key", result.error)

        # Transfer validated backup_files to the actual field
        cleaned_data["backup_files"] = backup_files
        return cleaned_data


@admin.register(BackupTarget)
class BackupTargetAdmin(admin.ModelAdmin):
    form = BackupTargetAdminForm
    list_display = [
        "name",
        "target_mode",
        "status",
        "schedule",
        "schedule_required",
        "fastdeploy_service",
        "has_service_token",
        "token_expires",
        "updated_at",
    ]
    list_filter = ["target_mode", "status", "schedule_required"]
    search_fields = ["name", "description"]
    readonly_fields = ["created_at", "updated_at"]

    fieldsets = [
        (None, {
            "fields": ["name", "description", "icon", "status", "target_mode"],
        }),
        ("FastDeploy Configuration", {
            "fields": ["fastdeploy_service", "fastdeploy_endpoint_key",
                       "service_token", "service_name", "restore_owner"],
            "description": "FastDeploy endpoint key must match a key in FASTDEPLOY_ENDPOINTS setting (leave blank for default).",
        }),
        ("Backup Source", {
            "fields": ["db_path", "backup_files_text", "backup_files"],
            "description": (
                "Mode rules: 'generic_paths' requires service_name and at least one of "
                "db_path/backup_files. 'service_owned' allows both source fields empty."
            ),
        }),
        ("Schedule & Retention", {
            "fields": [
                "schedule",
                "schedule_required",
                "retention_days",
                "timeout_seconds",
                "storage_bucket",
            ],
            "description": "Schedule uses cron syntax (e.g., '0 2 * * *' for 2am daily). "
                          "Enable schedule required for targets that must never become manual-only. "
                          "Note: Paused/disabled targets are excluded from retention cleanup.",
        }),
        ("Timestamps", {
            "fields": ["created_at", "updated_at"],
            "classes": ["collapse"],
        }),
    ]

    @admin.display(boolean=True, description="Token")
    def has_service_token(self, obj):
        return bool(obj.service_token)

    @admin.display(description="Token expires")
    def token_expires(self, obj):
        """Expiry of the token a backup would use, with a status badge.

        Decodes only non-secret claims (see backups.service_tokens); the token
        value is never rendered.
        """
        report = assess_target(obj, now=timezone.now())
        expires = report.expires_at.strftime("%Y-%m-%d") if report.expires_at else "-"
        badges = []
        if report.status != STATUS_OK:
            badges.append(report.status)
        if report.is_legacy and report.status != STATUS_LEGACY:
            badges.append("legacy")
        if not badges:
            return expires
        badge_html = format_html(
            '<strong style="color: #ba2121;">{}</strong>', ", ".join(badges)
        )
        return format_html("{} {}", expires, badge_html)

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
