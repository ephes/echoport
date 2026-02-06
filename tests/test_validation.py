"""
Tests for the validation module.
"""

import pytest
from django.test import override_settings

from backups.validation import (
    ValidationResult,
    get_allowed_path_prefixes,
    validate_backup_files,
    validate_backup_source,
    validate_endpoint_key,
    validate_path,
    validate_schedule,
)


class TestValidationResult:
    def test_valid_result(self):
        result = ValidationResult(value="test")
        assert result.is_valid is True
        assert result.value == "test"
        assert result.error is None

    def test_invalid_result(self):
        result = ValidationResult(error="error message")
        assert result.is_valid is False
        assert result.error == "error message"
        assert result.value is None


class TestGetAllowedPathPrefixes:
    def test_default_prefixes(self):
        prefixes = get_allowed_path_prefixes()
        assert "/home/" in prefixes
        assert "/opt/" in prefixes
        assert "/var/lib/" in prefixes

    @override_settings(ECHOPORT_ALLOWED_PATH_PREFIXES=["/srv/", "/data"])
    def test_custom_prefixes(self):
        prefixes = get_allowed_path_prefixes()
        assert "/srv/" in prefixes
        assert "/data/" in prefixes  # Trailing slash added

    @override_settings(ECHOPORT_ALLOWED_PATH_PREFIXES=["/home/", "  /opt/  ", "/var/lib/"])
    def test_strips_whitespace(self):
        prefixes = get_allowed_path_prefixes()
        assert "/opt/" in prefixes
        # No whitespace in result
        assert all(p == p.strip() for p in prefixes)

    @override_settings(ECHOPORT_ALLOWED_PATH_PREFIXES=["", "  ", None, 123])
    def test_filters_invalid_entries(self):
        # All entries invalid, should fall back to defaults
        prefixes = get_allowed_path_prefixes()
        assert prefixes == ["/home/", "/opt/", "/var/lib/"]

    @override_settings(ECHOPORT_ALLOWED_PATH_PREFIXES=["/"])
    def test_rejects_root_prefix(self):
        """Root prefix would allow all paths - should be rejected."""
        prefixes = get_allowed_path_prefixes()
        assert "/" not in prefixes
        # Falls back to defaults
        assert prefixes == ["/home/", "/opt/", "/var/lib/"]

    @override_settings(ECHOPORT_ALLOWED_PATH_PREFIXES=["relative/path", "no-slash"])
    def test_rejects_relative_paths(self):
        """Relative paths should be rejected."""
        prefixes = get_allowed_path_prefixes()
        # Falls back to defaults
        assert prefixes == ["/home/", "/opt/", "/var/lib/"]

    @override_settings(ECHOPORT_ALLOWED_PATH_PREFIXES=["/home/../etc/"])
    def test_normalizes_traversal_in_prefix(self):
        """Traversal in prefix should be normalized."""
        prefixes = get_allowed_path_prefixes()
        assert "/etc/" in prefixes
        assert "/home/../etc/" not in prefixes

    @override_settings(ECHOPORT_ALLOWED_PATH_PREFIXES=["/home//user//"])
    def test_normalizes_double_slashes(self):
        """Double slashes should be normalized."""
        prefixes = get_allowed_path_prefixes()
        assert "/home/user/" in prefixes

    @override_settings(ECHOPORT_ALLOWED_PATH_PREFIXES=["/./home/./"])
    def test_normalizes_dot_segments(self):
        """Dot segments should be normalized."""
        prefixes = get_allowed_path_prefixes()
        assert "/home/" in prefixes

    @override_settings(ECHOPORT_ALLOWED_PATH_PREFIXES=["//home/"])
    def test_collapses_posix_double_slash(self):
        """Leading // should be collapsed to / (POSIX quirk)."""
        prefixes = get_allowed_path_prefixes()
        assert "/home/" in prefixes
        assert "//home/" not in prefixes

    @override_settings(ECHOPORT_ALLOWED_PATH_PREFIXES=["//"])
    def test_rejects_double_slash_root(self):
        """// alone should be rejected (collapses to /)."""
        prefixes = get_allowed_path_prefixes()
        # Falls back to defaults since // -> / which is rejected
        assert prefixes == ["/home/", "/opt/", "/var/lib/"]


class TestValidatePath:
    def test_valid_path(self):
        result = validate_path("/home/user/file.db")
        assert result.is_valid is True
        assert result.value == "/home/user/file.db"

    def test_empty_path(self):
        result = validate_path("")
        assert result.is_valid is True
        assert result.value == ""

    def test_none_path(self):
        result = validate_path(None)
        assert result.is_valid is True
        assert result.value == ""

    def test_whitespace_only(self):
        result = validate_path("   ")
        assert result.is_valid is True
        assert result.value == ""

    def test_strips_whitespace(self):
        result = validate_path("  /home/user/file.db  ")
        assert result.is_valid is True
        assert result.value == "/home/user/file.db"

    def test_relative_path_rejected(self):
        result = validate_path("relative/path")
        assert result.is_valid is False
        assert "absolute" in result.error.lower()

    def test_traversal_attack_prevented(self):
        """Path traversal via .. should be caught."""
        result = validate_path("/home/../etc/passwd")
        assert result.is_valid is False
        # Error should mention the normalized path
        assert "/etc/passwd" in result.error

    def test_traversal_within_allowed_prefix(self):
        """Traversal that stays within allowed prefix should work."""
        result = validate_path("/home/user/../otheruser/file.db")
        assert result.is_valid is True
        assert result.value == "/home/otheruser/file.db"

    def test_homebrew_not_matched_by_home(self):
        """/homebrew should not match /home/ prefix."""
        result = validate_path("/homebrew/app")
        assert result.is_valid is False
        assert "must be under" in result.error.lower()

    def test_path_outside_allowlist(self):
        result = validate_path("/etc/passwd")
        assert result.is_valid is False
        assert "must be under" in result.error.lower()

    def test_normalizes_double_slashes(self):
        result = validate_path("/home//user//file.db")
        assert result.is_valid is True
        assert result.value == "/home/user/file.db"


class TestValidateBackupFiles:
    def test_none_is_valid(self):
        result = validate_backup_files(None)
        assert result.is_valid is True
        assert result.value == []

    def test_empty_list_is_valid(self):
        result = validate_backup_files([])
        assert result.is_valid is True
        assert result.value == []

    def test_valid_list_of_paths(self):
        result = validate_backup_files(["/home/user/a.txt", "/home/user/b.txt"])
        assert result.is_valid is True
        assert result.value == ["/home/user/a.txt", "/home/user/b.txt"]

    def test_empty_string_rejected(self):
        """Empty string is not a valid type for backup_files."""
        result = validate_backup_files("")
        assert result.is_valid is False
        assert "list" in result.error.lower()

    def test_zero_rejected(self):
        """0 is not a valid type for backup_files."""
        result = validate_backup_files(0)
        assert result.is_valid is False
        assert "list" in result.error.lower()

    def test_empty_dict_rejected(self):
        """Empty dict is not a valid type for backup_files."""
        result = validate_backup_files({})
        assert result.is_valid is False
        assert "list" in result.error.lower()

    def test_string_rejected(self):
        """String is not a valid type for backup_files."""
        result = validate_backup_files("/home/user/file.txt")
        assert result.is_valid is False
        assert "list" in result.error.lower()

    def test_list_with_non_string_item(self):
        """List containing non-string items should be rejected."""
        result = validate_backup_files(["/home/user/a.txt", 123, "/home/user/b.txt"])
        assert result.is_valid is False
        assert "string" in result.error.lower()
        assert "item 1" in result.error.lower()

    def test_list_with_none_item(self):
        """List containing None should be rejected."""
        result = validate_backup_files(["/home/user/a.txt", None])
        assert result.is_valid is False
        assert "string" in result.error.lower()

    def test_list_with_invalid_path(self):
        """List containing path outside allowlist should be rejected."""
        result = validate_backup_files(["/home/user/a.txt", "/etc/passwd"])
        assert result.is_valid is False
        assert "must be under" in result.error.lower()

    def test_empty_strings_in_list_filtered(self):
        """Empty strings in list should be filtered out."""
        result = validate_backup_files(["/home/user/a.txt", "", "/home/user/b.txt"])
        assert result.is_valid is True
        assert result.value == ["/home/user/a.txt", "/home/user/b.txt"]


class TestValidateSchedule:
    def test_empty_schedule_valid(self):
        result = validate_schedule("")
        assert result.is_valid is True
        assert result.value == ""

    def test_none_schedule_valid(self):
        result = validate_schedule(None)
        assert result.is_valid is True
        assert result.value == ""

    def test_valid_cron_daily(self):
        result = validate_schedule("0 2 * * *")
        assert result.is_valid is True
        assert result.value == "0 2 * * *"

    def test_valid_cron_hourly(self):
        result = validate_schedule("0 * * * *")
        assert result.is_valid is True

    def test_valid_cron_every_5_minutes(self):
        result = validate_schedule("*/5 * * * *")
        assert result.is_valid is True

    def test_invalid_cron_expression(self):
        result = validate_schedule("not a valid cron")
        assert result.is_valid is False
        assert "invalid cron" in result.error.lower()

    def test_invalid_cron_too_few_fields(self):
        result = validate_schedule("0 2 * *")
        assert result.is_valid is False

    def test_invalid_cron_out_of_range(self):
        result = validate_schedule("0 25 * * *")  # Hour 25 doesn't exist
        assert result.is_valid is False


class TestValidateEndpointKey:
    def test_empty_key_valid(self):
        result = validate_endpoint_key("")
        assert result.is_valid is True
        assert result.value == ""

    def test_none_key_valid(self):
        result = validate_endpoint_key(None)
        assert result.is_valid is True
        assert result.value == ""

    def test_whitespace_only_valid(self):
        result = validate_endpoint_key("   ")
        assert result.is_valid is True
        assert result.value == ""

    @override_settings(FASTDEPLOY_ENDPOINTS={
        "staging": {"base_url": "http://staging.example.com", "token": "secret"}
    })
    def test_valid_endpoint_key(self):
        result = validate_endpoint_key("staging")
        assert result.is_valid is True
        assert result.value == "staging"

    @override_settings(FASTDEPLOY_ENDPOINTS={
        "staging": {"base_url": "http://staging.example.com", "token": "secret"}
    })
    def test_strips_whitespace(self):
        result = validate_endpoint_key("  staging  ")
        assert result.is_valid is True
        assert result.value == "staging"

    @override_settings(FASTDEPLOY_ENDPOINTS={})
    def test_unknown_key_rejected(self):
        result = validate_endpoint_key("nonexistent")
        assert result.is_valid is False
        assert "unknown endpoint" in result.error.lower()

    @override_settings(FASTDEPLOY_ENDPOINTS={
        "staging": {"base_url": "", "token": "secret"}
    })
    def test_incomplete_endpoint_missing_url(self):
        result = validate_endpoint_key("staging")
        assert result.is_valid is False
        assert "incomplete" in result.error.lower()
        assert "base_url" in result.error

    @override_settings(FASTDEPLOY_ENDPOINTS={
        "staging": {"base_url": "http://example.com", "token": ""}
    })
    def test_incomplete_endpoint_missing_token(self):
        result = validate_endpoint_key("staging")
        assert result.is_valid is False
        assert "incomplete" in result.error.lower()
        assert "token" in result.error

    @override_settings(FASTDEPLOY_ENDPOINTS={
        "staging": {"base_url": "http://example.com"}
    })
    def test_has_token_override_skips_token_requirement(self):
        """Endpoint with no token is valid when model provides its own token."""
        result = validate_endpoint_key("staging", has_token_override=True)
        assert result.is_valid is True
        assert result.value == "staging"

    @override_settings(FASTDEPLOY_ENDPOINTS={
        "staging": {"base_url": "http://example.com", "service_tokens": {"svc": "tok"}}
    })
    def test_service_tokens_satisfies_token_requirement_for_matching_service(self):
        """Endpoint with matching service_tokens entry is valid."""
        result = validate_endpoint_key("staging", service_name="svc")
        assert result.is_valid is True
        assert result.value == "staging"

    @override_settings(FASTDEPLOY_ENDPOINTS={
        "staging": {"base_url": "http://example.com", "service_tokens": {"svc": "tok"}}
    })
    def test_service_tokens_rejects_unmatched_service(self):
        """Endpoint rejects when service is not in service_tokens and no default token."""
        result = validate_endpoint_key("staging", service_name="other-svc")
        assert result.is_valid is False
        assert "token" in result.error
        assert "other-svc" in result.error

    @override_settings(FASTDEPLOY_ENDPOINTS={
        "staging": {"base_url": "http://example.com", "service_tokens": {"svc": "tok"}}
    })
    def test_service_tokens_without_service_name_rejects(self):
        """Endpoint with only service_tokens and no service_name rejects (no default token)."""
        result = validate_endpoint_key("staging")
        assert result.is_valid is False
        assert "token" in result.error

    @override_settings(FASTDEPLOY_ENDPOINTS={
        "staging": {"base_url": "http://example.com", "service_tokens": {"svc": ""}}
    })
    def test_service_tokens_empty_value_rejected(self):
        """Endpoint with matching service_tokens key but empty value is rejected."""
        result = validate_endpoint_key("staging", service_name="svc")
        assert result.is_valid is False
        assert "token" in result.error

    @override_settings(FASTDEPLOY_ENDPOINTS={
        "staging": {"base_url": "http://example.com", "service_tokens": "not-a-dict"}
    })
    def test_invalid_service_tokens_type_rejected(self):
        """Endpoint with non-dict service_tokens is rejected."""
        result = validate_endpoint_key("staging", service_name="svc")
        assert result.is_valid is False
        assert "invalid service_tokens" in result.error.lower()

    @override_settings(FASTDEPLOY_ENDPOINTS={
        "staging": {"base_url": ""}
    })
    def test_has_token_override_still_requires_base_url(self):
        """Even with token override, base_url is still required."""
        result = validate_endpoint_key("staging", has_token_override=True)
        assert result.is_valid is False
        assert "base_url" in result.error


class TestValidateBackupSource:
    def test_db_path_only(self):
        error = validate_backup_source("/home/app/db.sqlite3", [])
        assert error is None

    def test_backup_files_only(self):
        error = validate_backup_source("", ["/home/app/data"])
        assert error is None

    def test_both_provided(self):
        error = validate_backup_source("/home/app/db.sqlite3", ["/home/app/data"])
        assert error is None

    def test_neither_provided(self):
        error = validate_backup_source("", [])
        assert error is not None
        assert "at least one" in error.lower()

    def test_none_values(self):
        error = validate_backup_source(None, None)
        assert error is not None


@pytest.mark.django_db
class TestModelValidation:
    """Tests for model-level validation via save()."""

    def test_invalid_schedule_rejected_on_save(self):
        """Model save() should reject invalid schedule."""
        from django.core.exceptions import ValidationError
        from backups.models import BackupTarget

        target = BackupTarget(
            name="test",
            fastdeploy_service="test-service",
            db_path="/home/test/db.sqlite3",
            schedule="invalid cron",
        )
        with pytest.raises(ValidationError) as exc_info:
            target.save()
        assert "schedule" in exc_info.value.message_dict

    @override_settings(FASTDEPLOY_ENDPOINTS={})
    def test_invalid_endpoint_key_rejected_on_save(self):
        """Model save() should reject invalid endpoint key."""
        from django.core.exceptions import ValidationError
        from backups.models import BackupTarget

        target = BackupTarget(
            name="test",
            fastdeploy_service="test-service",
            db_path="/home/test/db.sqlite3",
            fastdeploy_endpoint_key="nonexistent",
        )
        with pytest.raises(ValidationError) as exc_info:
            target.save()
        assert "fastdeploy_endpoint_key" in exc_info.value.message_dict

    def test_path_traversal_rejected_on_save(self):
        """Model save() should reject path traversal."""
        from django.core.exceptions import ValidationError
        from backups.models import BackupTarget

        target = BackupTarget(
            name="test",
            fastdeploy_service="test-service",
            db_path="/home/../etc/passwd",
        )
        with pytest.raises(ValidationError) as exc_info:
            target.save()
        assert "db_path" in exc_info.value.message_dict

    def test_invalid_backup_files_type_rejected_on_save(self):
        """Model save() should reject invalid backup_files type."""
        from django.core.exceptions import ValidationError
        from backups.models import BackupTarget

        target = BackupTarget(
            name="test",
            fastdeploy_service="test-service",
            db_path="/home/test/db.sqlite3",
        )
        # Bypass Python to set invalid type
        target.backup_files = "not a list"
        with pytest.raises(ValidationError) as exc_info:
            target.save()
        assert "backup_files" in exc_info.value.message_dict

    @override_settings(FASTDEPLOY_ENDPOINTS={
        "staging": {"base_url": "https://staging.example.com"},
    })
    def test_service_token_allows_endpoint_without_token_on_save(self):
        """Model save() should accept endpoint without token when service_token is set."""
        from backups.models import BackupTarget

        target = BackupTarget(
            name="test-token-override",
            fastdeploy_service="test-service",
            db_path="/home/test/db.sqlite3",
            fastdeploy_endpoint_key="staging",
            service_token="my-jwt-token",
        )
        target.save()  # Should not raise
        assert target.pk is not None

    def test_whitespace_service_token_stripped_on_save(self):
        """Whitespace-only service_token should be stripped to empty."""
        from backups.models import BackupTarget

        target = BackupTarget(
            name="test-whitespace-token",
            fastdeploy_service="test-service",
            db_path="/home/test/db.sqlite3",
            service_token="   ",
        )
        target.save()
        assert target.service_token == ""

    def test_service_token_trimmed_on_save(self):
        """Leading/trailing whitespace in service_token should be trimmed."""
        from backups.models import BackupTarget

        target = BackupTarget(
            name="test-trimmed-token",
            fastdeploy_service="test-service",
            db_path="/home/test/db.sqlite3",
            service_token="  my-token  ",
        )
        target.save()
        assert target.service_token == "my-token"
