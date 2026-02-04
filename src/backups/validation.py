"""
Centralized validation for BackupTarget fields.

This module is the single source of truth for validation rules,
imported by both admin forms and model clean().

Validation functions return error messages (strings) on failure, or the
validated/normalized value on success. Callers are responsible for raising
ValidationError with appropriate structure (plain string for form field
cleaners, dict for model clean()).
"""

import os

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
    - Normalize paths (collapse .., //, etc.)
    - Reject "/" (would allow all paths)
    - Ensure leading /
    - Ensure trailing / (prevents /home matching /homebrew)

    Prefixes must end with / to ensure path segment boundaries.
    """
    raw = getattr(settings, "ECHOPORT_ALLOWED_PATH_PREFIXES", ["/home/", "/opt/", "/var/lib/"])

    # Sanitize: strip whitespace, normalize, drop invalid entries
    sanitized = []
    for prefix in raw:
        prefix = prefix.strip() if isinstance(prefix, str) else ""
        if not prefix or not prefix.startswith("/"):
            continue

        # Normalize to collapse .. and // segments
        prefix = os.path.normpath(prefix)

        # POSIX quirk: normpath preserves leading // (implementation-defined).
        # Collapse // to / to avoid confusing misconfiguration where all
        # normal paths would fail the startswith check.
        if prefix.startswith("//"):
            prefix = prefix[1:]

        # Reject "/" as it would allow all paths
        if prefix == "/":
            continue

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

    from croniter import CroniterBadCronError, CroniterBadDateError, croniter

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
