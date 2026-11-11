"""
Restores into a scratch destination (``--destination scratch``).

- the gates: the target flag and a newest successful backup whose runner
  reported the ``restore-scratch`` capability;
- the context: action ``restore-scratch``, the live database path in its own
  variable, the live fields a runner without the action would read poisoned;
- the result: success only with a confirmed, verified (and removed) scratch copy;
- the CLI options, and capabilities stored from backup results.
"""

from datetime import timedelta
from unittest.mock import patch

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.utils import timezone

from backups.backup_engine import start_backup
from backups.fastdeploy_client import FastDeployClient
from backups.models import (
    BackupRun,
    BackupRunStatus,
    BackupTarget,
    RestoreDestination,
    RestoreRun,
    RestoreRunStatus,
)
from backups.restore_engine import (
    SCRATCH_POISON_DB_PATH,
    SCRATCH_POISON_SERVICE,
    RestoreError,
    scratch_restore_blocked_reason,
    start_restore,
)
from tests.conftest import finished_status, result_step

CHECKSUM = "b" * 64
SCRATCH_DIR = "/home/marina/site/.restore-scratch-20261111T120000Z"
VERIFIED = {"ok": True, "archive_sha256": CHECKSUM, "db_sha256": "c" * 64, "integrity": "ok", "tables": 65,
            "entries": 12, "inventory_sha256": "d" * 64, "differences": []}


@pytest.fixture
def target(db):
    return BackupTarget.objects.create(
        name="marina-staging",
        fastdeploy_service="marina-staging-backup",
        service_name="marina.service",
        db_path="/home/marina/site/db.sqlite3",
        backup_files=["/home/marina/site/media"],
        restore_owner="marina:marina",
        timeout_seconds=3,
        status="active",
        scratch_restore_enabled=True,
    )


def make_backup(target, capabilities=("restore-scratch",), status=BackupRunStatus.SUCCESS, age_minutes=0):
    run = BackupRun.objects.create(
        target=target,
        status=status,
        storage_bucket="backups",
        storage_key=f"marina-staging/run-{BackupRun.objects.count()}.tar.gz",
        checksum_sha256=CHECKSUM,
        size_bytes=100,
        file_count=2,
        runner_capabilities=list(capabilities),
    )
    BackupRun.objects.filter(pk=run.pk).update(started_at=timezone.now() - timedelta(minutes=age_minutes))
    run.refresh_from_db()
    return run


def scratch_result(**overrides):
    payload = {
        "success": True, "destination": "scratch", "scratch_path": "<run>", "kept": False, "cleaned": True,
        "file_count": 2, "verification": VERIFIED,
    }
    payload.update(overrides)
    return [result_step(payload)]


def answer_scratch(fake, **overrides):
    """The deployment finishes with a scratch result; scratch_path "<run>" is
    the scratch dir of the restore run being polled."""

    def status(deployment_id):
        fake.polled.append(deployment_id)
        run = RestoreRun.objects.latest("pk")
        path = overrides.get("scratch_path", "<run>")
        path = path.replace("<run>", f"{SCRATCH_DIR}-{run.pk}").replace("<id>", str(run.pk))
        return finished_status(scratch_result(**{**overrides, "scratch_path": path}))

    fake.get_deployment_status = status


@pytest.mark.django_db
class TestGates:
    def test_ready_target(self, target):
        make_backup(target)
        assert scratch_restore_blocked_reason(target) is None

    def test_flag_off(self, target):
        make_backup(target)
        target.scratch_restore_enabled = False
        assert "not enabled" in scratch_restore_blocked_reason(target)

    def test_no_successful_backup(self, target):
        make_backup(target, status=BackupRunStatus.FAILED)
        assert "no successful backup" in scratch_restore_blocked_reason(target)

    def test_newest_backup_decides(self, target):
        make_backup(target, age_minutes=60)
        newest = make_backup(target, capabilities=(), age_minutes=1)
        reason = scratch_restore_blocked_reason(target)
        assert f"run {newest.pk}" in reason and "restore-scratch" in reason
        # An older failed run does not matter, a newer failed one neither.
        make_backup(target, capabilities=(), status=BackupRunStatus.FAILED)
        make_backup(target, age_minutes=0)
        assert scratch_restore_blocked_reason(target) is None

    def test_start_restore_refuses_and_starts_nothing(self, target, fake_fastdeploy):
        backup = make_backup(target, capabilities=())
        with pytest.raises(RestoreError, match="without the restore-scratch capability"):
            start_restore(backup, destination=RestoreDestination.SCRATCH)
        assert fake_fastdeploy.started == []
        assert RestoreRun.objects.count() == 0

    def test_keep_scratch_only_with_scratch(self, target, fake_fastdeploy):
        backup = make_backup(target)
        with pytest.raises(RestoreError, match="only for scratch"):
            start_restore(backup, keep_scratch=True)
        assert fake_fastdeploy.started == []

    def test_existing_run_must_have_the_same_destination(self, target, fake_fastdeploy):
        backup = make_backup(target)
        pending = RestoreRun.objects.create(backup_run=backup, target=target, status=RestoreRunStatus.PENDING)
        with pytest.raises(RestoreError, match="is a live restore"):
            start_restore(backup, existing_run=pending, destination=RestoreDestination.SCRATCH)
        assert fake_fastdeploy.started == []


@pytest.mark.django_db
class TestScratchRestore:
    def test_context_and_success(self, target, fake_fastdeploy):
        backup = make_backup(target)
        answer_scratch(fake_fastdeploy)

        run = start_restore(backup, triggered_by="drill", destination=RestoreDestination.SCRATCH)

        run.refresh_from_db()
        assert run.status == RestoreRunStatus.SUCCESS, run.error_message
        assert (run.destination, run.keep_scratch, run.scratch_path) == ("scratch", False, f"{SCRATCH_DIR}-{run.pk}")
        assert run.verification == {**VERIFIED, "kept": False, "cleaned": True}
        assert run.files_restored == 2
        service, context = fake_fastdeploy.started[0]
        assert service == "marina-staging-backup"
        assert context == {
            "ECHOPORT_ACTION": "restore-scratch",
            "ECHOPORT_TARGET": "marina-staging",
            "ECHOPORT_RESTORE_ID": str(run.id),
            "ECHOPORT_RESTORE_DESTINATION": "scratch",
            "ECHOPORT_RESTORE_KEEP_SCRATCH": "0",
            "ECHOPORT_RESTORE_SCRATCH_DB_PATH": "/home/marina/site/db.sqlite3",
            "ECHOPORT_DB_PATH": SCRATCH_POISON_DB_PATH,
            "ECHOPORT_SERVICE_NAME": SCRATCH_POISON_SERVICE,
            "ECHOPORT_BACKUP_FILES": "",
            "ECHOPORT_BUCKET": "backups",
            "ECHOPORT_KEY": backup.storage_key,
            "ECHOPORT_CHECKSUM": CHECKSUM,
            "ECHOPORT_RESTORE_OWNER": "marina:marina",
        }

    def test_keep_scratch(self, target, fake_fastdeploy):
        backup = make_backup(target)
        answer_scratch(fake_fastdeploy, kept=True, cleaned=False)
        run = start_restore(backup, destination=RestoreDestination.SCRATCH, keep_scratch=True)
        run.refresh_from_db()
        assert run.status == RestoreRunStatus.SUCCESS, run.error_message
        assert fake_fastdeploy.started[0][1]["ECHOPORT_RESTORE_KEEP_SCRATCH"] == "1"

    @pytest.mark.parametrize(
        "overrides, reason",
        [
            ({"destination": None}, "did not confirm a scratch restore"),
            ({"destination": "live"}, "did not confirm a scratch restore"),
            ({"scratch_path": "/home/marina/site"}, "no valid scratch path"),
            ({"scratch_path": "/home/marina/site/.restore-scratch-x"}, "no valid scratch path"),
            ({"scratch_path": SCRATCH_DIR}, "no valid scratch path"),
            ({"scratch_path": f"{SCRATCH_DIR}-999999"}, "of another restore"),
            ({"scratch_path": "/etc/.restore-scratch-20261111T120000Z-<id>"}, "outside the target's root"),
            ({"scratch_path": "/home/marina/site/../../../etc/.restore-scratch-20261111T120000Z-<id>"},
             "no valid scratch path"),
            ({"scratch_path": "/home/marina/site/media/.restore-scratch-20261111T120000Z-<id>"},
             "outside the target's root"),
            ({"scratch_path": "/home/marina/.restore-scratch-20261111T120000Z-<id>"},
             "outside the target's root"),
            ({"scratch_path": "/home/.restore-scratch-20261111T120000Z-<id>"},
             "outside the target's root"),
            ({"verification": {"ok": False}}, "passed verification"),
            ({"verification": None}, "passed verification"),
            ({"verification": {"ok": "true"}}, "passed verification"),
            ({"cleaned": False}, "was removed"),
        ],
    )
    def test_success_needs_a_confirmed_verified_copy(self, target, fake_fastdeploy, overrides, reason):
        backup = make_backup(target)
        answer_scratch(fake_fastdeploy, **overrides)
        run = start_restore(backup, destination=RestoreDestination.SCRATCH)
        run.refresh_from_db()
        assert run.status == RestoreRunStatus.FAILED
        assert reason in run.error_message

    def test_an_old_runner_answering_with_a_plain_success_fails(self, target, fake_fastdeploy):
        """A runner without the action ran its backup and reported success."""
        backup = make_backup(target)
        fake_fastdeploy.statuses = [finished_status([result_step({"success": True, "key": "x", "file_count": 2})])]
        run = start_restore(backup, destination=RestoreDestination.SCRATCH)
        run.refresh_from_db()
        assert run.status == RestoreRunStatus.FAILED
        assert "did not confirm a scratch restore" in run.error_message

    def test_reported_failure_keeps_the_runner_details(self, target, fake_fastdeploy):
        backup = make_backup(target)
        failed = {"ok": False, "differences": ["media/x"]}
        answer_scratch(fake_fastdeploy, success=False, error="verification failed", verification=failed,
                       cleaned=False)
        run = start_restore(backup, destination=RestoreDestination.SCRATCH)
        run.refresh_from_db()
        assert run.status == RestoreRunStatus.FAILED
        assert run.error_message == "verification failed"
        assert run.scratch_path == f"{SCRATCH_DIR}-{run.pk}"
        # The operator can see that the copy is still on the host.
        assert run.verification == {**failed, "kept": False, "cleaned": False}

    def test_live_restore_is_unchanged(self, target, fake_fastdeploy):
        backup = make_backup(target)
        fake_fastdeploy.statuses = [finished_status([result_step({"success": True, "file_count": 2})])]
        run = start_restore(backup)
        run.refresh_from_db()
        assert run.status == RestoreRunStatus.SUCCESS
        assert run.destination == "live" and run.scratch_path == "" and run.verification is None
        context = fake_fastdeploy.started[0][1]
        assert context["ECHOPORT_ACTION"] == "restore"
        assert context["ECHOPORT_DB_PATH"] == "/home/marina/site/db.sqlite3"
        assert not any(key.startswith("ECHOPORT_RESTORE_") and key != "ECHOPORT_RESTORE_ID"
                       and key != "ECHOPORT_RESTORE_OWNER" for key in context)


@pytest.mark.django_db
class TestCapabilitiesFromBackups:
    def test_parser_keeps_only_short_strings(self):
        steps = [result_step({"success": True, "capabilities": ["restore-scratch", 3, "", "x" * 65]})]
        assert FastDeployClient.parse_echoport_result(steps).capabilities == ["restore-scratch"]
        steps = [result_step({"success": True, "capabilities": "restore-scratch"})]
        assert FastDeployClient.parse_echoport_result(steps).capabilities == []

    def test_backup_stores_them(self, target, fake_fastdeploy):
        fake_fastdeploy.statuses = [finished_status([result_step({
            "success": True, "key": "marina-staging/a.tar.gz", "size_bytes": 5,
            "checksum_sha256": CHECKSUM, "file_count": 2, "capabilities": ["restore-scratch"],
        })])]
        run = start_backup(target)
        run.refresh_from_db()
        assert run.status == BackupRunStatus.SUCCESS
        assert run.runner_capabilities == ["restore-scratch"]
        assert scratch_restore_blocked_reason(target) is None


@pytest.mark.django_db
class TestCommand:
    def test_scratch_destination_is_passed(self, target, capsys):
        backup = make_backup(target)
        done = RestoreRun(
            id=9, backup_run=backup, target=target, status=RestoreRunStatus.SUCCESS,
            destination="scratch", scratch_path=f"{SCRATCH_DIR}-9", verification=VERIFIED, files_restored=2,
        )
        with patch("backups.management.commands.restore.start_restore", return_value=done) as start:
            call_command("restore", "marina-staging", str(backup.id), "--destination", "scratch", "--keep-scratch")
        assert start.call_args.kwargs == {"triggered_by": "cli", "destination": "scratch", "keep_scratch": True}
        out = capsys.readouterr().out
        assert f"Scratch dir: {SCRATCH_DIR}-9" in out and "integrity ok" in out

    def test_keep_scratch_needs_scratch(self, target):
        backup = make_backup(target)
        with pytest.raises(CommandError, match="--keep-scratch needs --destination scratch"):
            call_command("restore", "marina-staging", str(backup.id), "--keep-scratch")

    def test_gates_refuse_before_starting(self, target):
        backup = make_backup(target, capabilities=())
        with patch("backups.management.commands.restore.start_restore") as start:
            with pytest.raises(CommandError, match="restore-scratch capability"):
                call_command("restore", "marina-staging", str(backup.id), "--destination", "scratch")
        start.assert_not_called()

    def test_default_is_live(self, target):
        backup = make_backup(target)
        done = RestoreRun(id=9, backup_run=backup, target=target, status=RestoreRunStatus.SUCCESS)
        with patch("backups.management.commands.restore.start_restore", return_value=done) as start:
            call_command("restore", "marina-staging", str(backup.id))
        assert start.call_args.kwargs["destination"] == "live"


@pytest.mark.django_db(transaction=True)
class TestMigration:
    def test_existing_rows_get_the_safe_defaults(self):
        from django.db import connection
        from django.db.migrations.executor import MigrationExecutor

        before = ("backups", "0012_backuptarget_storage_alias")
        executor = MigrationExecutor(connection)
        executor.migrate([before])
        old = executor.loader.project_state(before).apps
        old_target = old.get_model("backups", "BackupTarget").objects.create(
            name="legacy", fastdeploy_service="echoport-backup", db_path="/tmp/legacy.db"
        )
        old_backup = old.get_model("backups", "BackupRun").objects.create(
            target=old_target, status="success", checksum_sha256=CHECKSUM
        )
        old.get_model("backups", "RestoreRun").objects.create(
            backup_run=old_backup, target=old_target, status="success"
        )
        try:
            leaf = MigrationExecutor(connection).loader.graph.leaf_nodes("backups")
            MigrationExecutor(connection).migrate(leaf)
            target = BackupTarget.objects.get(name="legacy")
            assert target.scratch_restore_enabled is False
            assert BackupRun.objects.get(target=target).runner_capabilities == []
            restore = RestoreRun.objects.get(target=target)
            assert (restore.destination, restore.keep_scratch, restore.scratch_path, restore.verification) == (
                "live", False, "", None)
            assert "not enabled" in scratch_restore_blocked_reason(target)
            target.scratch_restore_enabled = True
            assert "without the restore-scratch capability" in scratch_restore_blocked_reason(target)
        finally:
            RestoreRun.objects.all().delete()
            BackupRun.objects.all().delete()
            BackupTarget.objects.all().delete()
