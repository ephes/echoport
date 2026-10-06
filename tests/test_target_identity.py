"""
Tests for case-insensitive backup target identity and the self-restore guard.
"""

import importlib

import pytest
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import IntegrityError, connection, transaction
from django.db.migrations.executor import MigrationExecutor

from backups.admin import BackupTargetAdminForm
from backups.models import (
    BackupRun,
    BackupRunStatus,
    BackupTarget,
    RestoreRun,
    exact_name_hint,
)

MIGRATION = "0009_backuptarget_name_case_insensitive_unique"
PREVIOUS = "0008_backuptarget_schedule_required"
CONSTRAINT = "unique_backup_target_name_ci"


def _target(name, **overrides):
    fields = {
        "name": name,
        "fastdeploy_service": "echoport-backup",
        "service_name": f"{name.strip().lower()}.service",
        "db_path": "/tmp/identity.db",
        "status": "active",
    }
    fields.update(overrides)
    return BackupTarget.objects.create(**fields)


# ---------------------------------------------------------------------------
# Case-insensitive uniqueness
# ---------------------------------------------------------------------------


@pytest.mark.django_db
class TestCaseInsensitiveUniqueness:
    def test_model_validation_rejects_case_variant(self):
        _target("nyxmon")

        with pytest.raises(ValidationError) as excinfo:
            _target("NyxMon", service_name="other.service")

        assert "compared case-insensitively" in str(excinfo.value)
        assert BackupTarget.objects.count() == 1

    def test_database_rejects_case_variant_that_bypasses_validation(self):
        _target("nyxmon")

        with pytest.raises(IntegrityError), transaction.atomic():
            BackupTarget.objects.bulk_create(
                [BackupTarget(name="NYXMON", fastdeploy_service="x", db_path="/tmp/x.db")]
            )

        assert list(BackupTarget.objects.values_list("name", flat=True)) == ["nyxmon"]

    def test_admin_form_reports_case_variant(self):
        _target("homelab")

        form = BackupTargetAdminForm(
            data={
                "name": "HomeLab",
                "fastdeploy_service": "echoport-backup",
                "target_mode": "generic_paths",
                "service_name": "homelab2.service",
                "db_path": "/home/homelab/db.sqlite3",
                "backup_files_text": "",
                "backup_files": "[]",
                "status": "active",
                "retention_days": 30,
                "timeout_seconds": 600,
                "storage_bucket": "backups",
            }
        )

        assert not form.is_valid()
        assert "compared case-insensitively" in str(form.errors)

    def test_renaming_own_case_is_allowed(self):
        target = _target("homelab")
        target.name = "HomeLab"
        target.save()

        target.refresh_from_db()
        assert target.name == "HomeLab"

    def test_distinct_names_are_allowed(self):
        _target("homelab")
        _target("homelab-staging")

        assert BackupTarget.objects.count() == 2


# ---------------------------------------------------------------------------
# Migration pre-check
# ---------------------------------------------------------------------------


def _migrate(target):
    executor = MigrationExecutor(connection)
    executor.loader.build_graph()
    executor.migrate([("backups", target)])
    return executor


def _constraint_present():
    with connection.cursor() as cursor:
        constraints = connection.introspection.get_constraints(cursor, "backup_target")
    return CONSTRAINT in constraints


@pytest.mark.django_db(transaction=True)
class TestMigrationPrecheck:
    @pytest.fixture(autouse=True)
    def back_to_previous(self):
        executor = _migrate(PREVIOUS)
        self.old_target = executor.loader.project_state(("backups", PREVIOUS)).apps.get_model(
            "backups", "BackupTarget"
        )
        yield
        # Leave the schema at the latest migration for the rest of the suite.
        self.old_target.objects.all().delete()
        leaf = MigrationExecutor(connection).loader.graph.leaf_nodes("backups")
        MigrationExecutor(connection).migrate(leaf)

    def _create(self, name):
        return self.old_target.objects.create(
            name=name, fastdeploy_service="echoport-backup", db_path="/tmp/m.db"
        )

    def test_collisions_fail_clearly_without_changes(self):
        first = self._create("echoport")
        second = self._create("Echoport")
        self._create("nyxmon")
        module = importlib.import_module(f"backups.migrations.{MIGRATION}")

        with pytest.raises(module.CaseInsensitiveNameCollisionError) as excinfo:
            _migrate(MIGRATION)

        message = str(excinfo.value)
        assert f"'echoport' (id {first.id}), 'Echoport' (id {second.id})" in message
        assert "nyxmon" not in message
        assert "Rename or delete the duplicates" in message
        assert not _constraint_present()
        assert sorted(self.old_target.objects.values_list("name", flat=True)) == [
            "Echoport",
            "echoport",
            "nyxmon",
        ]

    def test_clean_data_adds_constraint(self):
        self._create("echoport")
        self._create("nyxmon")

        _migrate(MIGRATION)

        assert _constraint_present()


# ---------------------------------------------------------------------------
# Self-target identity and the UI self-restore guard
# ---------------------------------------------------------------------------


@pytest.mark.django_db
class TestIsSelfTarget:
    @pytest.mark.parametrize("name", ["echoport", "Echoport", "ECHOPORT", " echoport "])
    def test_name_variants(self, name):
        target = BackupTarget(name=name, fastdeploy_service="echoport-backup", service_name="")
        assert target.is_self_target

    @pytest.mark.parametrize("service", ["echoport-self-backup", "Echoport-Self-Backup "])
    def test_fastdeploy_service(self, service):
        target = BackupTarget(name="control-plane", fastdeploy_service=service, service_name="")
        assert target.is_self_target

    @pytest.mark.parametrize("unit", ["echoport.service", "Echoport.Service", "echoport"])
    def test_systemd_unit(self, unit):
        target = BackupTarget(name="control-plane", fastdeploy_service="x", service_name=unit)
        assert target.is_self_target

    @pytest.mark.parametrize(
        "name,service,unit",
        [
            ("nyxmon", "echoport-backup", "nyxmon.service"),
            ("echoport-staging", "echoport-backup", "echoport-staging.service"),
            ("other", "echoport-self-backup-test", "echoport.timer"),
        ],
    )
    def test_other_targets(self, name, service, unit):
        target = BackupTarget(name=name, fastdeploy_service=service, service_name=unit)
        assert not target.is_self_target


@pytest.mark.django_db
class TestSelfRestoreGuard:
    @pytest.fixture
    def staff_client(self, client):
        client.force_login(User.objects.create_user("op", "op@test.com", "pw", is_staff=True))
        return client

    def _backup(self, target):
        return BackupRun.objects.create(
            target=target,
            status=BackupRunStatus.SUCCESS,
            checksum_sha256="f" * 64,
            storage_key="x.tar.gz",
        )

    @pytest.mark.parametrize(
        "name,overrides",
        [
            ("Echoport", {"service_name": "control.service"}),
            ("control-plane", {"fastdeploy_service": "echoport-self-backup"}),
            ("control-plane", {"service_name": "echoport.service"}),
        ],
    )
    def test_self_target_variants_are_blocked(self, staff_client, name, overrides):
        backup = self._backup(_target(name, **overrides))

        response = staff_client.post(f"/runs/{backup.id}/restore/", follow=True)

        assert response.status_code == 200
        assert RestoreRun.objects.count() == 0
        messages = [str(m) for m in response.context["messages"]]
        assert messages == [
            f"Self-restore cannot run from the UI. Use CLI: manage.py restore {name} {backup.id}"
        ]

    def test_cli_hint_quotes_names_with_spaces(self, staff_client):
        backup = self._backup(_target("echoport self", service_name="echoport.service"))

        response = staff_client.post(f"/runs/{backup.id}/restore/", follow=True)

        messages = [str(m) for m in response.context["messages"]]
        assert messages[0].endswith(f"manage.py restore 'echoport self' {backup.id}")


# ---------------------------------------------------------------------------
# CLI lookups stay exact but point at the intended target
# ---------------------------------------------------------------------------


@pytest.mark.django_db
class TestExactCliLookup:
    def test_hint_names_case_variant(self):
        _target("Echoport")

        assert exact_name_hint("echoport") == (
            " Target names are matched exactly; did you mean 'Echoport'?"
        )

    def test_hint_ignores_surrounding_whitespace(self):
        _target("nyxmon")

        assert "did you mean 'nyxmon'" in exact_name_hint(" nyxmon ")

    def test_no_hint_without_candidates(self):
        _target("nyxmon")

        assert exact_name_hint("homelab") == ""

    def test_restore_command_is_exact_with_hint(self):
        _target("Echoport")

        with pytest.raises(CommandError, match="did you mean 'Echoport'"):
            call_command("restore", "echoport", "1")

    def test_backup_command_is_exact_with_hint(self):
        _target("Nyxmon")

        with pytest.raises(CommandError, match="not found. Target names are matched exactly"):
            call_command("backup", "nyxmon")

    def test_cleanup_command_is_exact_with_hint(self, capsys):
        _target("Nyxmon")

        with pytest.raises(SystemExit):
            call_command("cleanup_old_backups", "--target", "nyxmon")

        assert "did you mean 'Nyxmon'" in capsys.readouterr().err
