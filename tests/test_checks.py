"""System check that MINIO_ALIAS is one of MINIO_ALIASES, and MINIO_ALIASES parsing."""

import os
import subprocess
import sys
from pathlib import Path

import environ
import pytest
from django.core.management import call_command
from django.core.management.base import SystemCheckError

from backups import minio_client
from backups.checks import check_minio_alias
from backups.validation import get_storage_aliases, validate_storage_alias


def ids(errors):
    return [error.id for error in errors]


class TestMinioAliasCheck:
    def test_defaults_pass(self, settings):
        settings.MINIO_ALIAS = "minio"
        settings.MINIO_ALIASES = ["minio", "rustfs"]
        assert check_minio_alias(None) == []
        call_command("check")

    def test_another_allowlisted_alias_passes(self, settings):
        settings.MINIO_ALIAS = "rustfs"
        settings.MINIO_ALIASES = ["minio", "rustfs"]
        assert check_minio_alias(None) == []

    def test_alias_outside_the_allowlist_is_an_error(self, settings):
        settings.MINIO_ALIAS = "myminio"
        settings.MINIO_ALIASES = ["minio", "rustfs"]
        errors = check_minio_alias(None)
        assert ids(errors) == ["backups.E002"]
        assert errors[0].is_serious()
        assert "'myminio' is not one of MINIO_ALIASES (minio, rustfs)" in errors[0].msg

    def test_empty_allowlist_is_an_error(self, settings):
        settings.MINIO_ALIAS = "minio"
        settings.MINIO_ALIASES = []
        errors = check_minio_alias(None)
        assert ids(errors) == ["backups.E002"]
        assert "none configured" in errors[0].msg

    @pytest.mark.parametrize("alias", ["", "   ", None])
    def test_empty_alias_is_an_error_not_a_pause(self, settings, alias):
        settings.MINIO_ALIAS = alias
        settings.MINIO_ALIASES = ["minio", "rustfs", ""]
        errors = check_minio_alias(None)
        assert ids(errors) == ["backups.E001"]
        assert errors[0].is_serious()
        assert "set_storage_alias <target> paused" in errors[0].hint

    def test_alias_with_whitespace_is_an_error(self, settings):
        # MINIO_ALIAS is not stripped: " minio" is not the alias mc has.
        settings.MINIO_ALIAS = " minio"
        settings.MINIO_ALIASES = ["minio"]
        assert ids(check_minio_alias(None)) == ["backups.E002"]

    def test_registered_by_the_app(self):
        # A fresh process, so BackupsConfig.ready() registers it, not this file's import.
        manage = Path(__file__).resolve().parents[1] / "src" / "django" / "manage.py"
        env = {
            **os.environ,
            "DJANGO_SETTINGS_MODULE": "config.settings.test",
            "MINIO_ALIAS": "myminio",
        }
        env.pop("MINIO_ALIASES", None)
        result = subprocess.run(
            [sys.executable, str(manage), "check"],
            cwd=manage.parent,
            env=env,
            timeout=60,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1
        assert "backups.E002" in result.stderr

    def test_manage_py_check_fails(self, settings):
        settings.MINIO_ALIAS = "myminio"
        with pytest.raises(SystemCheckError, match="backups.E002"):
            call_command("check")

    def test_management_commands_refuse_to_run(self, settings):
        settings.MINIO_ALIAS = "myminio"
        with pytest.raises(SystemCheckError, match="backups.E002"):
            call_command("cleanup_old_backups", "--dry-run", skip_checks=False)


class TestMinioAliasesWhitespace:
    def test_entries_are_stripped_and_empty_ones_dropped(self, settings):
        settings.MINIO_ALIASES = [" minio ", "", "  ", "rustfs\t"]
        assert get_storage_aliases() == ["minio", "rustfs"]

    def test_parsed_from_the_environment(self, settings, monkeypatch):
        # The same parsing as config.settings.base.
        monkeypatch.setenv("MINIO_ALIASES", "minio, rustfs,,")
        settings.MINIO_ALIASES = environ.Env().list(
            "MINIO_ALIASES", default=["minio", "rustfs"]
        )
        assert get_storage_aliases() == ["minio", "rustfs"]
        settings.MINIO_ALIAS = "rustfs"
        assert check_minio_alias(None) == []

    def test_stripped_entries_validate_and_reach_the_client(self, settings):
        settings.MINIO_ALIASES = ["minio", " rustfs"]
        assert validate_storage_alias("rustfs").is_valid
        assert not validate_storage_alias(" rustfs").is_valid
        assert minio_client._allowed_alias("rustfs")
        assert not minio_client._allowed_alias(" rustfs")

    def test_blank_entry_allowlists_nothing(self, settings):
        settings.MINIO_ALIASES = ["", " "]
        assert get_storage_aliases() == []
        assert not minio_client._allowed_alias("")
        assert not minio_client._allowed_alias(" ")
