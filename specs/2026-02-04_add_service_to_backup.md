# BackupTarget Configuration Improvement PRD

**Status**: Implemented
**Date**: 2026-02-04
**Implemented**: 2026-02-04

> **Historical snapshot**: Sections 4-8 (code snippets *and* prose descriptions
> like "Current State" and "Runtime Validation Gaps") reflect the state at
> initial implementation (commit `c30695c`). Subsequent work (per-target endpoint
> support, `service_token` override, tighter validation) has changed signatures,
> logic, and addressed several of those gaps. See **Section 14** for the current
> implementation state.

---

## 1. Problem Statement

Currently, adding a new service to be backed up requires:

1. Modifying `src/backups/management/commands/create_devdata.py` to add the new target
2. Redeploying echoport via `just deploy-one echoport`
3. Running `python manage.py create_devdata` on the server

This workflow has several problems:

- **Requires code changes**: Adding a backup target is a runtime configuration concern, not a code change
- **Requires redeployment**: Even a simple target addition triggers a full deployment cycle
- **Developer-only operation**: Only those with code access can add targets
- **No immediate feedback**: Changes require deployment to take effect
- **Violates separation of concerns**: Target configuration is mixed with development data seeding

For a backup service, the ability to quickly add new services to the backup rotation is operationally critical. The current approach creates unnecessary friction.

---

## 2. Goals

1. **Runtime configuration**: Add/edit/remove BackupTargets without code changes or redeployment
2. **Operator-friendly**: Staff users should be able to manage targets through a web interface
3. **Immediate effect**: Changes take effect on next scheduler run (cron cadence defined in ops-library Ansible role, default `*/5 * * * *`)
4. **Validation**: Prevent invalid configurations at save time, not at backup runtime
5. **Audit trail**: Track who created/modified targets and when (via Django admin history)

---

## 3. Non-Goals

1. **Public API**: No need for external REST API access (internal tool only)
2. **Bulk import/export**: Not needed for 4-10 targets
3. **Target templates**: No need for "clone target" functionality
4. **Role-based permissions**: Staff access is sufficient (no need for granular permissions)
5. **Frontend UI overhaul**: The dashboard is for monitoring, not configuration

---

## 4. Current State

### Data Model

`BackupTarget` model exists with all necessary fields:

```python
class BackupTarget(models.Model):
    name = models.CharField(max_length=100, unique=True)
    description = models.TextField(blank=True)
    icon = models.CharField(max_length=50, blank=True)  # Emoji

    # FastDeploy configuration
    fastdeploy_service = models.CharField(max_length=100)
    fastdeploy_endpoint_key = models.CharField(max_length=50, blank=True)
    service_name = models.CharField(max_length=100, blank=True)  # For restore
    restore_owner = models.CharField(max_length=100, blank=True)

    # What to backup
    db_path = models.CharField(max_length=500, blank=True)
    backup_files = models.JSONField(default=list, blank=True)

    # Schedule and settings
    schedule = models.CharField(max_length=100, blank=True)  # Cron
    status = models.CharField(max_length=20, choices=BackupStatus.choices)
    retention_days = models.PositiveIntegerField(default=30)
    timeout_seconds = models.PositiveIntegerField(default=600)
    storage_bucket = models.CharField(max_length=100, default="backups")
```

### Current Creation Method

Targets are created only via `create_devdata.py`:

```python
targets = [
    {
        "name": "nyxmon",
        "defaults": {
            "description": "NyxMon monitoring service...",
            "fastdeploy_service": "echoport-backup",
            # ... all other fields
        },
    },
    # ... more targets
]

for target_data in targets:
    BackupTarget.objects.update_or_create(
        name=target_data["name"],
        defaults=target_data["defaults"],
    )
```

### What Was Missing (at time of writing)

- **No Django admin registration**: `admin.py` did not exist *(now implemented)*
- **No REST API**: Only HTML views exist
- **No CLI for individual targets**: Only bulk `create_devdata` command

### Runtime Validation Gaps

The following fields can cause runtime errors if misconfigured:

1. **`backup_files`**: Used as `",".join(target.backup_files)` in `backup_engine.py:238`. If not a list of strings, this crashes at backup time.

2. **`fastdeploy_endpoint_key`**: Validated at runtime in `fastdeploy_client.py:60-64`. Invalid keys raise `EndpointConfigError` when backup starts.

3. **`schedule`**: Invalid cron expressions cause `CroniterBadCronError` or `CroniterBadDateError` during scheduler runs.

### Retention Cleanup Behavior

The `cleanup_old_backups` command only processes **ACTIVE** targets (line 198). This means:
- **Paused** targets: Backups accumulate indefinitely (storage grows silently)
- **Disabled** targets: Same behavior

This may be intentional (preserve paused service backups) but needs explicit documentation.

### Cascade Deletion Risk

The current model uses `on_delete=models.CASCADE` for BackupRun → BackupTarget. Deleting a BackupTarget would:
- Delete all associated BackupRun records
- Delete all associated RestoreRun records (via BackupRun)
- Lose audit history permanently

This is unacceptable for operational integrity.

---

## 5. Proposed Solution: Django Admin

**Recommendation**: Register BackupTarget with Django Admin, with enhanced validation and deletion protection.

### Rationale

1. **Already available**: Django admin is installed, configured, and accessible
2. **Zero new dependencies**: No additional packages needed
3. **Built-in features**:
   - CRUD operations with form validation
   - Change history (audit trail)
   - Staff authentication (already required for restore operations)
   - Search and filtering
   - Inline help text from model field definitions
4. **Minimal implementation**: ~150 lines of code (including validation)
5. **Proven pattern**: Standard Django approach for internal configuration

### Why Django Admin is Sufficient

- **User base**: 1-2 operators (Jochen + potentially others)
- **Frequency**: Targets added rarely (new service deployed every few months)
- **Complexity**: Simple form fields, no complex workflows
- **Security**: Staff-only access matches existing restore permission model

---

## 6. Alternative Options Considered

### Option A: REST API with Django REST Framework

**Pros**:
- Enables automation (scripts can create targets)
- Could power a custom frontend later

**Cons**:
- Adds dependency (djangorestframework)
- More code to write and maintain
- Overkill for 1-2 users creating targets occasionally
- Would still need some UI for human operators

**Verdict**: Not needed. If automation is required later, Django admin can export/import via fixtures.

### Option B: Management Commands (`add_target`, `edit_target`)

**Pros**:
- CLI-friendly
- Can be scripted

**Cons**:
- Awkward for complex fields (JSON for backup_files)
- No validation feedback loop
- Still requires SSH access to server

**Verdict**: Could supplement Django admin for automation, but not replace it.

### Option C: Configuration File (YAML/JSON)

**Pros**:
- Version-controlled configuration
- GitOps-friendly

**Cons**:
- Requires restart or reload mechanism
- Sync logic between file and database
- Two sources of truth problem
- Still requires redeployment to update

**Verdict**: Creates more problems than it solves. Database is already the source of truth.

### Option D: Custom Frontend in Dashboard

**Pros**:
- Unified UI experience
- Could match existing HTMX patterns

**Cons**:
- Significant development effort
- Duplicates Django admin functionality
- More code to maintain
- Dashboard is for monitoring, not configuration

**Verdict**: Overengineered for the use case.

---

## 7. User Stories

### US1: Add a New Backup Target

**As** an operator,
**I want** to add a new service to the backup rotation,
**So that** its data is protected without requiring a code change.

**Acceptance Criteria**:
1. Navigate to Django admin (configured via `settings.ADMIN_URL`)
2. Click "Backup targets" → "Add backup target"
3. Fill in required fields (name, fastdeploy_service)
4. Configure optional fields (schedule, db_path, backup_files, etc.)
5. Validation errors shown immediately if configuration is invalid
6. Save and see target on dashboard within next cron cycle

### US2: Modify Backup Schedule

**As** an operator,
**I want** to change a target's backup schedule,
**So that** I can adjust timing based on operational needs.

**Acceptance Criteria**:
1. Navigate to target in admin
2. Edit schedule field (e.g., change from `0 2 * * *` to `0 3 * * *`)
3. Invalid cron expressions rejected with clear error message
4. Save and scheduler uses new schedule on next invocation

### US3: Pause/Disable a Target

**As** an operator,
**I want** to temporarily disable backups for a service,
**So that** I can perform maintenance without failed backup alerts.

**Acceptance Criteria**:
1. Edit target in admin
2. Change status from "Active" to "Paused"
3. Save and target is skipped in scheduler
4. Note: Retention cleanup also skips paused targets (backups preserved)

### US4: View Change History

**As** an operator,
**I want** to see who changed a target's configuration and when,
**So that** I can audit configuration changes.

**Acceptance Criteria**:
1. View target in admin
2. Click "History" button
3. See list of changes with timestamps and usernames

**Limitation**: Only changes made via admin are logged. Direct database edits or management commands are not audited. Enforce admin-only edits for production.

### US5: View Backup Run History (Read-Only)

**As** an operator,
**I want** to view backup and restore run history in admin,
**So that** I can investigate issues without using the dashboard.

**Acceptance Criteria**:
1. Navigate to "Backup runs" or "Restore runs" in admin
2. View list with filtering by target, status, trigger
3. Cannot add, edit, or delete runs (system-managed)

### US6: Cannot Delete Targets

**As** an operator,
**I want** deletion of targets to be blocked,
**So that** I don't accidentally lose backup history and audit records.

**Acceptance Criteria**:
1. Delete action is not available in admin (hidden from UI)
2. Direct delete attempts return 403 Forbidden
3. Must change status to "Disabled" instead to retire a target

**Note**: Django admin's `has_delete_permission = False` hides the delete action and blocks delete requests. No custom message is shown; operators learn the "Disabled" pattern from documentation.

---

## 8. Technical Design

### Settings Configuration

Add to `settings/base.py`:

```python
# Path allowlist for backup targets
# Only paths under these prefixes can be configured for backup
# Configurable to support different deployment layouts
ECHOPORT_ALLOWED_PATH_PREFIXES = env.list(
    "ECHOPORT_ALLOWED_PATH_PREFIXES",
    default=["/home/", "/opt/", "/var/lib/"]
)
```

This allows ops to adjust the allowlist without code changes if services live under `/srv/`, `/data/`, etc.

### Validation Module

Create `src/backups/validation.py` to centralize validation logic:

```python
"""
Centralized validation for BackupTarget fields.

This module is the single source of truth for validation rules,
imported by both admin forms and model clean().

Validation functions return error messages (strings) on failure, or the
validated/normalized value on success. Callers are responsible for raising
ValidationError with appropriate structure (plain string for form field
cleaners, dict for model clean()).
"""

from django.conf import settings


class ValidationResult:
    """Result of a validation check."""

    def __init__(self, value=None, error: str | None = None):
        self.value = value
        self.error = error

    @property
    def is_valid(self) -> bool:
        return self.error is None


def get_allowed_path_prefixes() -> list[str]:
    """
    Get the configured allowed path prefixes.

    Sanitizes the list to:
    - Remove empty strings
    - Ensure leading /
    - Ensure trailing / (prevents /home matching /homebrew)

    Prefixes must end with / to ensure path segment boundaries.
    """
    raw = getattr(settings, "ECHOPORT_ALLOWED_PATH_PREFIXES", ["/home/", "/opt/", "/var/lib/"])

    # Sanitize: strip whitespace, drop empty strings, ensure leading and trailing /
    sanitized = []
    for prefix in raw:
        prefix = prefix.strip() if isinstance(prefix, str) else ""
        if prefix and prefix.startswith("/"):
            # Ensure trailing slash for path segment boundary
            if not prefix.endswith("/"):
                prefix = prefix + "/"
            sanitized.append(prefix)

    # Fallback if all prefixes were invalid
    if not sanitized:
        return ["/home/", "/opt/", "/var/lib/"]

    return sanitized


def validate_path(path: str) -> ValidationResult:
    """
    Validate and normalize a path.

    Normalizes paths to prevent traversal attacks (e.g., /home/../etc/passwd).

    Args:
        path: The path to validate

    Returns:
        ValidationResult with normalized path or error message
    """
    import os

    path = path.strip() if path else ""
    if not path:
        return ValidationResult(value="")

    if not path.startswith("/"):
        return ValidationResult(error=f"Path must be absolute: {path}")

    # Normalize to collapse .. and . segments, preventing traversal attacks
    # e.g., /home/../etc/passwd -> /etc/passwd
    normalized = os.path.normpath(path)

    # normpath removes trailing slashes; re-check it's still absolute
    if not normalized.startswith("/"):
        return ValidationResult(error=f"Path must be absolute: {path}")

    allowed = get_allowed_path_prefixes()
    # Prefixes have trailing slash (enforced by get_allowed_path_prefixes),
    # so startswith() correctly enforces path segment boundaries:
    # "/home/user" starts with "/home/" ✓
    # "/homebrew" does NOT start with "/home/" ✓
    if not any(normalized.startswith(prefix) for prefix in allowed):
        return ValidationResult(
            error=f"Path must be under one of: {', '.join(allowed)}. Got: {path} (normalized: {normalized})"
        )

    return ValidationResult(value=normalized)


def validate_backup_files(backup_files) -> ValidationResult:
    """
    Validate backup_files is a list of valid paths.

    Handles all possible values including falsy non-list types that could
    land in the JSONField (e.g., "", 0, None, {}).

    Returns:
        ValidationResult with list of normalized paths or error message
    """
    # Handle None and empty list - valid cases
    if backup_files is None:
        return ValidationResult(value=[])

    # Reject non-list types (including falsy ones like "", 0, {})
    if not isinstance(backup_files, list):
        return ValidationResult(
            error=f"Must be a list, got {type(backup_files).__name__}"
        )

    # Empty list is valid
    if not backup_files:
        return ValidationResult(value=[])

    validated = []
    for i, path in enumerate(backup_files):
        if not isinstance(path, str):
            return ValidationResult(
                error=f"Item {i} must be a string, got {type(path).__name__}"
            )
        result = validate_path(path)
        if not result.is_valid:
            return ValidationResult(error=result.error)
        if result.value:  # Skip empty strings
            validated.append(result.value)

    return ValidationResult(value=validated)


def validate_schedule(schedule: str) -> ValidationResult:
    """
    Validate a cron expression.

    Returns:
        ValidationResult with schedule string or error message
    """
    if not schedule:
        return ValidationResult(value="")

    from croniter import croniter, CroniterBadCronError, CroniterBadDateError

    try:
        croniter(schedule)
    except (CroniterBadCronError, CroniterBadDateError, KeyError, ValueError) as e:
        return ValidationResult(error=f"Invalid cron expression: {e}")

    return ValidationResult(value=schedule)


def validate_endpoint_key(endpoint_key: str) -> ValidationResult:
    """
    Validate fastdeploy_endpoint_key exists and is complete in settings.

    Returns:
        ValidationResult with endpoint_key or error message
    """
    # Normalize whitespace before validation
    endpoint_key = endpoint_key.strip() if endpoint_key else ""
    if not endpoint_key:
        return ValidationResult(value="")

    endpoints = getattr(settings, "FASTDEPLOY_ENDPOINTS", {})
    if endpoint_key not in endpoints:
        available = list(endpoints.keys()) or "(none configured)"
        return ValidationResult(
            error=f"Unknown endpoint key '{endpoint_key}'. Available: {available}"
        )

    # Validate completeness
    endpoint = endpoints[endpoint_key]
    missing = []
    if not endpoint.get("base_url"):
        missing.append("base_url")
    if not endpoint.get("token"):
        missing.append("token")
    if missing:
        return ValidationResult(
            error=f"Endpoint '{endpoint_key}' is incomplete: missing {', '.join(missing)}"
        )

    return ValidationResult(value=endpoint_key)


def validate_backup_source(db_path: str, backup_files: list) -> str | None:
    """
    Validate that at least one backup source is specified.

    Returns:
        Error message if invalid, None if valid
    """
    if not db_path and not backup_files:
        return "At least one of 'Database path' or 'Backup files' must be specified."
    return None
```

### Admin Implementation

Create `src/backups/admin.py`:

```python
from django import forms
from django.contrib import admin
from django.core.exceptions import ValidationError

from .models import BackupTarget, BackupRun, RestoreRun
from .validation import (
    get_allowed_path_prefixes,
    validate_path,
    validate_schedule,
    validate_endpoint_key,
    validate_backup_source,
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
        # Convert JSON list to newline-separated text for editing
        if self.instance and self.instance.pk:
            files = self.instance.backup_files or []
            self.fields["backup_files_text"].initial = "\n".join(files)
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
```

### Model-Level Validation (Defense-in-Depth)

Add to `models.py` to catch validation issues from `save()` paths:

```python
# In models.py, add to BackupTarget class

def clean(self):
    """
    Model-level validation, called by full_clean().

    This duplicates form validation to catch issues from non-admin save paths
    (management commands, Django shell). Uses centralized validation module.

    Aggregates errors into a dict for proper Django model validation.
    """
    from django.core.exceptions import ValidationError
    from .validation import (
        validate_path,
        validate_backup_files,
        validate_schedule,
        validate_endpoint_key,
        validate_backup_source,
    )

    errors = {}

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

    # Validate schedule
    if self.schedule:
        result = validate_schedule(self.schedule)
        if not result.is_valid:
            errors["schedule"] = result.error

    # Validate fastdeploy_endpoint_key
    if self.fastdeploy_endpoint_key:
        result = validate_endpoint_key(self.fastdeploy_endpoint_key)
        if not result.is_valid:
            errors["fastdeploy_endpoint_key"] = result.error

    # Require at least one backup source
    source_error = validate_backup_source(self.db_path, self.backup_files)
    if source_error:
        errors["__all__"] = source_error

    if errors:
        raise ValidationError(errors)

def save(self, *args, **kwargs):
    """Override save to enforce validation."""
    self.full_clean()
    super().save(*args, **kwargs)
```

### Validation Coverage and Limitations

| Save Method | Validation Enforced | Notes |
|-------------|---------------------|-------|
| Django Admin | Yes | Form validation + model clean() |
| `Model.save()` | Yes | save() calls full_clean() |
| `Model.objects.create()` | Yes | Calls save() internally |
| `QuerySet.update()` | **No** | Bypasses save() entirely |
| `QuerySet.bulk_update()` | **No** | Bypasses save() entirely |
| `QuerySet.bulk_create()` | **No** | Bypasses save() entirely |

**Important**: `QuerySet.update()`, `bulk_update()`, and `bulk_create()` bypass the `save()` method and therefore bypass validation. These methods **must not be used** for BackupTarget modifications. All BackupTarget changes should go through:
- Django admin (recommended)
- Individual `save()` calls
- Management commands that use `save()`

### Validation Summary

| Field | Validation | Error Behavior |
|-------|------------|----------------|
| `backup_files` | Must be list (or None) of absolute paths under allowed prefixes; paths normalized to prevent traversal | Form/model error on save |
| `db_path` | Must be absolute path under allowed prefixes; normalized (trimmed, `..` collapsed) | Form/model error on save |
| `fastdeploy_endpoint_key` | Must exist in `FASTDEPLOY_ENDPOINTS` with complete config; whitespace stripped | Form/model error on save |
| `schedule` | Must be valid cron (handles `CroniterBadCronError`, `CroniterBadDateError`) | Form/model error on save |
| `db_path` + `backup_files` | At least one must be non-empty | Form/model error on save |

**Note on `db_path`**: This field should contain the full path to a SQLite database file (e.g., `/home/myapp/site/db.sqlite3`). The backup script validates that the file exists at backup time. If a directory path is entered, the backup will fail with a clear error from the backup script. Adding file existence checks to admin validation was considered but rejected because:
1. Admin runs on the echoport server, not the target server
2. The file may not exist yet when configuring a new service
3. The backup script already provides clear error messages

### Allowed Path Prefixes

To prevent accidental configuration of sensitive system paths (e.g., `/etc/shadow`), paths are restricted by default to:

- `/home/` - User home directories (where services live)
- `/opt/` - Optional application software
- `/var/lib/` - Variable state data

**Configurable**: Set `ECHOPORT_ALLOWED_PATH_PREFIXES` environment variable to customize:
```bash
# In .env, to add /srv/ and /data/:
ECHOPORT_ALLOWED_PATH_PREFIXES=/home/,/opt/,/var/lib/,/srv/,/data/
```

**Sanitization**: The validation module sanitizes the prefix list:
- Strips whitespace from each entry
- Removes empty strings (prevents `startswith("")` bypass)
- Requires leading `/` (absolute paths only)
- Enforces trailing `/` (prevents `/home` matching `/homebrew`)
- Falls back to default if all entries are invalid

**Format**: Prefixes should be directory paths ending with `/`. If you omit the trailing slash, it will be added automatically (e.g., `/srv` becomes `/srv/`).

**Note**: Prefix roots themselves (e.g., `/home/`) are intentionally disallowed as backup paths. Paths must be *under* a prefix, not equal to it. This prevents accidentally backing up entire top-level directories.

Current deployed services all use `/home/*/site/` paths, so the default is appropriate.

### Migration Path

1. Existing targets in database remain unchanged
2. `create_devdata.py` behavior change (see below)
3. Production targets managed via admin going forward

### Safeguarding `create_devdata`

The current `update_or_create` behavior will overwrite admin changes if run in production:

```python
def handle(self, *args, **options):
    if not settings.DEBUG:
        self.stderr.write(
            self.style.ERROR(
                "create_devdata is disabled in production. "
                "Use Django admin to manage backup targets."
            )
        )
        return

    # ... rest of command
```

**Note**: With model-level `save()` validation, `create_devdata` will also be validated. If any target data violates the new validation rules, the command will fail with a clear error.

---

## 9. Security Considerations

### Access Control

- Django admin requires staff status (`is_staff=True`)
- Matches existing restore permission model (`@staff_member_required`)
- Superuser created during deployment via `ensure_superuser` command

### Permission Plan

**Approach**: Operators are superusers.

For this homelab deployment with 1-2 operators:
- Superuser accounts provide full admin access
- No need for granular model permissions
- Simpler to manage than group-based permissions

If granular permissions are needed later:
1. Create an "Operators" group
2. Assign `backups.view_backuptarget`, `backups.add_backuptarget`, `backups.change_backuptarget` permissions
3. Assign `backups.view_backuprun`, `backups.view_restorerun` permissions
4. Add operators to the group

### Input Validation

- Form-level validation prevents invalid configurations at save time
- Model-level `save()` override provides defense-in-depth for `save()` paths
- Path allowlist prevents configuration of sensitive system paths
- Allowlist is configurable via environment variable for operational flexibility
- Allowlist is sanitized to prevent empty-string bypass attacks
- Path traversal (`..` segments) is normalized before validation

**Symlink limitation**: Path validation does not resolve symlinks. A path under an allowed prefix that is a symlink to a sensitive location would pass validation. This is acceptable given:
1. Only trusted operators can configure targets
2. Symlink creation on the target server requires separate access
3. Backup scripts run with limited permissions on the target

### Audit Trail

- Django admin logs all changes to `django_admin_log` table
- Includes: user, timestamp, action, changed fields
- **Limitation**: Only admin changes are logged; enforce admin-only edits
- Deletion blocked in admin to preserve history

### No New Attack Surface

- Admin already exists and is accessible
- No new endpoints exposed
- No API tokens or external access

---

## 10. Operational Considerations

### Scheduler Cadence

The scheduler is a cron job defined in the **ops-library** Ansible role `echoport_deploy`:

```yaml
# In ops-library/roles/echoport_deploy/defaults/main.yml
echoport_scheduler_enabled: true
echoport_scheduler_interval: "*/5 * * * *"  # Every 5 minutes
```

Changes to admin take effect on the **next cron invocation**. There is no in-app Django setting for scheduler interval.

**Reference**: See `ops-library/roles/echoport_deploy/` for cron configuration.

### Retention Cleanup Behavior

Current behavior: `cleanup_old_backups` only processes **ACTIVE** targets.

| Status | Scheduled Backups | Retention Cleanup | Storage Impact |
|--------|-------------------|-------------------|----------------|
| Active | Yes | Yes | Normal |
| Paused | No | No | Backups accumulate |
| Disabled | No | No | Backups accumulate |

**Recommendation**: Document this behavior in admin help text. If storage growth is a concern for paused targets, consider:
1. Running cleanup for all statuses (may surprise operators who expect paused backups preserved)
2. Adding a separate `cleanup_paused_targets` flag
3. Keeping current behavior but documenting it clearly

For now, keeping current behavior and documenting it is sufficient.

### Target Deletion Policy

Targets cannot be deleted via admin to preserve:
- Backup run history
- Restore run history
- Audit trail

**To retire a target**: Change status to "Disabled". This:
- Stops scheduled backups
- Stops retention cleanup (preserves existing backups)
- Preserves all history

If permanent deletion is truly needed, use Django shell with explicit confirmation:
```python
# Only if absolutely necessary - loses all history!
BackupTarget.objects.get(name="old-target").delete()
```

---

## 11. Migration / Rollout Plan

### Phase 1: Implementation

1. Add `ECHOPORT_ALLOWED_PATH_PREFIXES` to `settings/base.py`
2. Create `src/backups/validation.py` with centralized validation
3. Create `src/backups/admin.py` with BackupTargetAdmin and form validation
4. Add model-level `clean()` and `save()` override to BackupTarget
5. Gate `create_devdata` to DEBUG mode
6. Test locally: create, edit targets via admin
7. Verify validation rejects invalid configurations
8. Verify scheduler picks up admin-created targets
9. Verify deletion is blocked

### Phase 2: Deployment

1. Deploy updated echoport
2. Verify admin accessible at configured path
3. Verify existing targets visible and editable in admin
4. Test creating a new target via admin
5. Verify existing `create_devdata` targets pass new validation

### Phase 3: Documentation

1. Update README with admin usage
2. Document standard target configuration fields
3. Document retention cleanup behavior for paused targets
4. Document target retirement process (status=Disabled)
5. Document `ECHOPORT_ALLOWED_PATH_PREFIXES` configuration

### No Data Migration Required

- Existing BackupTarget records work as-is (all use `/home/` paths)
- Django admin reads from same database table
- `create_devdata.py` remains available for development (DEBUG only)

---

## 12. Future Considerations

If more sophisticated management is needed later:

1. **CLI commands**: Add `manage.py add_target --name foo --schedule "0 2 * * *"` for automation
2. **API endpoint**: Add simple REST endpoint if external tools need to create targets
3. **Import/export**: Django fixtures (`dumpdata`/`loaddata`) work out of the box
4. **Granular permissions**: Create Operators group with specific model permissions

These can be added incrementally without changing the core approach.

---

## 13. Summary

| Aspect | Decision |
|--------|----------|
| **Approach** | Django Admin with form + model validation |
| **Effort** | ~150 lines of code (validation module + admin + model changes) |
| **Dependencies** | None (already installed) |
| **Validation** | Form-level + model-level (centralized in validation.py) |
| **Path safety** | Configurable allowlist via `ECHOPORT_ALLOWED_PATH_PREFIXES`, sanitized |
| **Deletion** | Blocked in admin (use status=Disabled) |
| **Permissions** | Operators are superusers |
| **User experience** | Standard Django admin with improved UX |
| **Audit** | Built-in admin history (admin changes only) |
| **Migration** | None required |
| **create_devdata** | Gated to DEBUG mode |

Django Admin with proper validation is the right tool for this job: it's already available, well-understood, and with the added validation prevents runtime errors from misconfiguration.

---

## Appendix: Review Response

### v7 → v8 Changes

| Concern | Resolution |
|---------|------------|
| `prefix.rstrip("/")` reintroduced `/home` vs `/homebrew` boundary bug | Removed rstrip; prefixes keep trailing slash, startswith() enforces boundaries correctly |
| Symlinks not resolved by `normpath()` | Documented as acceptable limitation in Security Considerations given operator trust model |

### v6 → v7 Changes

| Concern | Resolution |
|---------|------------|
| Path traversal via `..` segments (e.g., `/home/../etc/passwd`) | Added `os.path.normpath()` to collapse `..` and `.` segments before prefix check; error message shows both original and normalized path |
| `fastdeploy_endpoint_key` whitespace not stripped | Added `.strip()` before validation to avoid confusing "unknown endpoint" errors |

### v4 → v5 Changes

| Concern | Resolution |
|---------|------------|
| ValidationError dict payload breaks form field cleaners | Changed to `ValidationResult` class; form cleaners use plain string errors, model clean() aggregates into dict |
| `fastdeploy_endpoint_key` validation missing from model clean() | Added `validate_endpoint_key()` to validation module, called from model `clean()` |
| Empty prefix entries could bypass path validation | Added sanitization in `get_allowed_path_prefixes()`: strips whitespace, drops empty strings, requires leading `/`, falls back to default |
| US6 claims delete shows message but it doesn't | Clarified: delete action is hidden/blocked, no custom message; operators learn from documentation |

### v3 → v4 Changes

| Concern | Resolution |
|---------|------------|
| Model clean() missing cross-field validation | Added `validate_backup_source()` call to model `clean()` |
| `update()`/`bulk_update()` bypass validation | Documented in "Validation Coverage and Limitations" table with explicit warning |
| Path allowlist may not match deployed paths | Made configurable via `ECHOPORT_ALLOWED_PATH_PREFIXES` env var |
| `ALLOWED_PATH_PREFIXES` duplicated | Centralized in `validation.py` module, imported by both admin and model |
| Model clean() doesn't persist trimmed db_path | Now assigns `self.db_path = result.value` to persist normalization |

### v2 → v3 Changes

| Concern | Resolution |
|---------|------------|
| Scheduler cadence factual error | Fixed: now references ops-library Ansible role, not nonexistent Django setting |
| Target deletion cascades | Added `has_delete_permission = False` on BackupTargetAdmin |
| Model clean() not enforced | Added `save()` override to call `full_clean()` |
| Permission plan needed | Added: "Operators are superusers" with notes on future granular permissions |
| Endpoint key completeness check | Added validation for base_url and token presence |
| Path allowlist | Added ALLOWED_PATH_PREFIXES validation |
| db_path whitespace | Added `.strip()` normalization |

### v1 → v2 Changes

| Concern | Resolution |
|---------|------------|
| `backup_files` accepts any JSON | Form validates as list of absolute path strings |
| `fastdeploy_endpoint_key` free-form | Form validates against `FASTDEPLOY_ENDPOINTS` |
| Paused targets excluded from cleanup | Documented in admin help text and section 10 |
| `create_devdata` overwrites admin changes | Gated to DEBUG mode |
| Admin history only logs admin changes | Documented as limitation in US4 |
| Missing view/delete permissions on RunAdmin | Explicit `has_delete_permission = False` added |
| Schedule validation missing `CroniterBadDateError` | Added to validation |
| Hardcoded `/admin/` paths | Changed to "Django admin (configured via settings)" |
| UX for backup_files JSON | Textarea widget, one path per line |
| Require db_path or backup_files | Cross-field validation added |

---

## 14. Implementation Notes

**Implemented**: 2026-02-04

### Commits

1. `c30695c` - Add Django Admin for BackupTarget management
2. `a59cccc` - Add per-target FastDeploy endpoint support and restore_owner

### Files Created

| File | Description |
|------|-------------|
| `src/backups/validation.py` | Centralized validation module (219 lines) |
| `src/backups/admin.py` | Django admin configuration (197 lines) |
| `tests/test_validation.py` | Validation tests (416 lines) |
| `tests/test_admin.py` | Admin form and permission tests (243 lines) |
| `src/backups/migrations/0005_add_endpoint_key_and_restore_owner.py` | Migration for new fields |
| `tests/test_fastdeploy_config.py` | Endpoint config tests (103 lines) |

### Files Modified

| File | Changes |
|------|---------|
| `src/backups/models.py` | Added `clean()` and `save()` methods to BackupTarget |
| `src/django/config/settings/base.py` | Added `ECHOPORT_ALLOWED_PATH_PREFIXES` setting |
| `src/django/config/settings/test.py` | Added `/tmp/` to allowed prefixes for tests |
| `src/backups/management/commands/create_devdata.py` | Added DEBUG mode gate |
| `src/backups/backup_engine.py` | Integrated `get_fastdeploy_config()` for per-target endpoints |
| `src/backups/restore_engine.py` | Integrated `get_fastdeploy_config()` and `restore_owner` support |
| `src/backups/fastdeploy_client.py` | Added `get_fastdeploy_config()` and `EndpointConfigError` |

### Enhancements Beyond PRD

During code review, the following enhancements were added:

1. **Admin legacy data handling**: Form coerces invalid `backup_files` data (non-list, non-string items) to strings, allowing admins to view and remediate corrupt data instead of crashing.

2. **POSIX `//` quirk handling**: `get_allowed_path_prefixes()` collapses leading `//` to `/` to prevent confusing misconfiguration where all normal paths would fail the `startswith` check.

3. **Prefix normalization**: Prefixes are normalized with `os.path.normpath()` to collapse `..`, `//`, etc.

4. **Root prefix rejection**: `/` is explicitly rejected as a prefix (would allow all paths).

### Test Coverage

- **all tests passing** (74 new tests for validation/admin)
- Key validation paths tested: path traversal, boundary cases, invalid types
- Admin form round-trip conversion tested
- Permission blocking verified for all admin classes

### Deployment Checklist

- [x] Code implemented and reviewed
- [x] Tests passing (all tests)
- [x] Pre-commit hooks passing (ruff)
- [ ] Deploy to production
- [ ] Run migration: `python manage.py migrate backups`
- [ ] Verify admin accessible
- [ ] Verify existing targets editable
- [ ] Test creating new target via admin

### Post-Implementation Changes (commit `453d46b` and subsequent)

The following changes were made after the initial implementation. The inline code
snippets in sections 4-8 above are **stale** — refer to the source files directly.

#### Per-target service_token (migration 0006)

`BackupTarget` gained a `service_token` TextField. When set, it overrides all
other token resolution in `get_fastdeploy_config()`. This allows operators to
paste a FastDeploy JWT per-target in Django Admin without modifying settings.

Key changes vs the snippets above:
- **`models.py`**: Added `service_token` field; `clean()` strips whitespace and
  passes `has_token_override` and `service_name` to `validate_endpoint_key()`.
- **`fastdeploy_client.py`**: `get_fastdeploy_config()` gained a `token_override`
  parameter. When truthy, it short-circuits token resolution.
- **`backup_engine.py` / `restore_engine.py`**: Both pass
  `token_override=target.service_token`.
- **`admin.py`**: `service_token` uses `PasswordInput(render_value=True)`;
  `has_service_token` boolean in list_display; endpoint validation moved from
  `clean_fastdeploy_endpoint_key()` into `clean()` (all fields available).
- **`validation.py`**: `validate_endpoint_key()` now accepts `has_token_override`
  and `service_name` parameters. Validates `service_tokens` type and requires
  either a default token, a matching `service_tokens[service_name]` entry, or
  `has_token_override=True`. Empty/falsy token values in `service_tokens` are
  rejected.

#### Tests: all passing as of 2026-02-06
