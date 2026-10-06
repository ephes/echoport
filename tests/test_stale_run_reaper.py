"""
Tests for reaping backup/restore runs left active by a killed process.

A run that stays pending/running longer than its target's timeout plus
ECHOPORT_STALE_RUN_GRACE_SECONDS is marked TIMEOUT so it stops blocking
its target. A reaped run must never flip back to success or failure.
"""

from datetime import timedelta
from unittest.mock import patch

import pytest
from django.contrib.auth.models import User
from django.urls import reverse
from django.utils import timezone

from backups import backup_engine, restore_engine
from backups.management.commands.run_scheduled_backups import Command
from backups.models import (
    BackupRun,
    BackupRunStatus,
    BackupTarget,
    BackupTrigger,
    RestoreRun,
    RestoreRunStatus,
)
from backups.run_reaper import (
    DEFAULT_STALE_RUN_GRACE_SECONDS,
    get_stale_run_grace_seconds,
    reap_all_stale_runs,
    save_if_still_active,
)
from tests.conftest import finished_status, result_step

CHECKSUM = "d" * 64
TIMEOUT = 600
GRACE = 900
BACKUP_RESULT = {
    "success": True,
    "key": "reaper-target/2026-01-02T02-00-00.tar.gz",
    "size_bytes": 10,
    "checksum_sha256": "c" * 64,
    "file_count": 1,
}


@pytest.fixture(autouse=True)
def grace(settings):
    settings.ECHOPORT_STALE_RUN_GRACE_SECONDS = GRACE
    return GRACE


@pytest.fixture
def target(db):
    return BackupTarget.objects.create(
        name="reaper-target",
        fastdeploy_service="echoport-backup",
        service_name="reaper-target.service",
        db_path="/tmp/reaper.db",
        schedule="0 2 * * *",
        timeout_seconds=TIMEOUT,
        status="active",
    )


@pytest.fixture
def other_target(db):
    return BackupTarget.objects.create(
        name="other-target",
        fastdeploy_service="echoport-backup",
        service_name="other-target.service",
        db_path="/tmp/other.db",
        timeout_seconds=TIMEOUT,
        status="active",
    )


@pytest.fixture
def good_backup(target):
    return BackupRun.objects.create(
        target=target,
        status=BackupRunStatus.SUCCESS,
        storage_bucket="backups",
        storage_key="reaper-target/2026-01-01T00-00-00.tar.gz",
        checksum_sha256=CHECKSUM,
        started_at=timezone.now() - timedelta(days=2),
    )


def _ago(seconds):
    return timezone.now() - timedelta(seconds=seconds)


STALE_AGE = TIMEOUT + GRACE + 60
FRESH_AGE = TIMEOUT + GRACE - 60


def _backup(target, status=BackupRunStatus.RUNNING, age=STALE_AGE, **kwargs):
    return BackupRun.objects.create(
        target=target,
        status=status,
        storage_bucket=target.storage_bucket,
        started_at=_ago(age),
        **kwargs,
    )


def _restore(backup_run, status=RestoreRunStatus.RUNNING, age=STALE_AGE):
    return RestoreRun.objects.create(
        backup_run=backup_run,
        target=backup_run.target,
        status=status,
        started_at=_ago(age),
    )


@pytest.mark.django_db
class TestReapBackups:
    @pytest.mark.parametrize("status", [BackupRunStatus.PENDING, BackupRunStatus.RUNNING])
    def test_stale_active_run_is_marked_timeout(self, target, status):
        run = _backup(target, status=status)

        reaped = backup_engine.reap_stale_runs()

        assert reaped == [run.id]
        run.refresh_from_db()
        assert run.status == BackupRunStatus.TIMEOUT
        assert run.finished_at is not None
        assert "Reaped as stale" in run.error_message
        assert f"still {status}" in run.error_message
        assert backup_engine.get_active_run(target) is None

    def test_fresh_run_is_untouched(self, target):
        run = _backup(target, age=FRESH_AGE)

        assert backup_engine.reap_stale_runs() == []
        run.refresh_from_db()
        assert run.status == BackupRunStatus.RUNNING
        assert run.error_message == ""
        assert run.finished_at is None

    def test_finished_runs_are_untouched(self, target):
        old = _backup(target, status=BackupRunStatus.FAILED, error_message="boom")

        assert backup_engine.reap_stale_runs() == []
        old.refresh_from_db()
        assert old.status == BackupRunStatus.FAILED
        assert old.error_message == "boom"

    def test_deadline_uses_each_targets_timeout(self, target, other_target):
        BackupTarget.objects.filter(pk=other_target.pk).update(timeout_seconds=TIMEOUT * 10)
        stale = _backup(target)
        long_running = _backup(other_target)

        assert backup_engine.reap_stale_runs() == [stale.id]
        long_running.refresh_from_db()
        assert long_running.status == BackupRunStatus.RUNNING

    def test_target_filter(self, target, other_target):
        mine = _backup(target)
        theirs = _backup(other_target)

        assert backup_engine.reap_stale_runs(target=target) == [mine.id]
        theirs.refresh_from_db()
        assert theirs.status == BackupRunStatus.RUNNING

    def test_grace_setting_is_respected(self, target, settings):
        run = _backup(target, age=TIMEOUT + 30)
        assert backup_engine.reap_stale_runs() == []

        settings.ECHOPORT_STALE_RUN_GRACE_SECONDS = 0
        assert backup_engine.reap_stale_runs() == [run.id]

    def test_explicit_now(self, target):
        run = _backup(target, age=0)

        assert backup_engine.reap_stale_runs(now=timezone.now() + timedelta(seconds=STALE_AGE)) == [
            run.id
        ]


@pytest.mark.django_db
class TestGraceSetting:
    def test_default(self, settings):
        del settings.ECHOPORT_STALE_RUN_GRACE_SECONDS
        assert get_stale_run_grace_seconds() == DEFAULT_STALE_RUN_GRACE_SECONDS == 900

    def test_invalid_value_falls_back_to_default(self, settings):
        settings.ECHOPORT_STALE_RUN_GRACE_SECONDS = "soon"
        assert get_stale_run_grace_seconds() == DEFAULT_STALE_RUN_GRACE_SECONDS

    def test_negative_value_is_clamped(self, settings):
        settings.ECHOPORT_STALE_RUN_GRACE_SECONDS = -5
        assert get_stale_run_grace_seconds() == 0


@pytest.mark.django_db
class TestReapRestores:
    @pytest.mark.parametrize("status", [RestoreRunStatus.PENDING, RestoreRunStatus.RUNNING])
    def test_stale_restore_is_marked_timeout(self, good_backup, status):
        run = _restore(good_backup, status=status)

        assert restore_engine.reap_stale_runs() == [run.id]
        run.refresh_from_db()
        assert run.status == RestoreRunStatus.TIMEOUT
        assert run.finished_at is not None
        assert "Reaped as stale" in run.error_message
        assert restore_engine.get_active_restore(good_backup.target) is None

    def test_fresh_restore_is_untouched(self, good_backup):
        run = _restore(good_backup, age=FRESH_AGE)

        assert restore_engine.reap_stale_runs() == []
        run.refresh_from_db()
        assert run.status == RestoreRunStatus.RUNNING

    def test_reap_all(self, target, good_backup):
        backup = _backup(target)
        restore = _restore(good_backup)

        assert reap_all_stale_runs(target=target) == ([backup.id], [restore.id])


@pytest.mark.django_db
class TestEnginesReapBeforeStarting:
    def test_stale_backup_no_longer_blocks_start_backup(self, target, fake_fastdeploy):
        stale = _backup(target)
        fake_fastdeploy.statuses = [finished_status([result_step(BACKUP_RESULT)])]

        run = backup_engine.start_backup(target)

        stale.refresh_from_db()
        assert stale.status == BackupRunStatus.TIMEOUT
        run.refresh_from_db()
        assert run.id != stale.id
        assert run.status == BackupRunStatus.SUCCESS

    def test_stale_restore_no_longer_blocks_start_backup(self, target, good_backup, fake_fastdeploy):
        stale = _restore(good_backup)
        fake_fastdeploy.statuses = [finished_status([result_step(BACKUP_RESULT)])]

        run = backup_engine.start_backup(target)

        stale.refresh_from_db()
        assert stale.status == RestoreRunStatus.TIMEOUT
        assert run.status == BackupRunStatus.SUCCESS

    def test_existing_run_is_never_reaped_by_its_own_start(self, target, fake_fastdeploy):
        existing = _backup(target, status=BackupRunStatus.PENDING)
        fake_fastdeploy.statuses = [finished_status([result_step(BACKUP_RESULT)])]

        run = backup_engine.start_backup(target, existing_run=existing)

        assert run.id == existing.id
        run.refresh_from_db()
        assert run.status == BackupRunStatus.SUCCESS

    def test_fresh_backup_still_blocks(self, target, fake_fastdeploy):
        _backup(target, age=FRESH_AGE)

        with pytest.raises(backup_engine.ConcurrentBackupError):
            backup_engine.start_backup(target)
        assert fake_fastdeploy.started == []

    def test_stale_restore_no_longer_blocks_start_restore(self, good_backup, fake_fastdeploy):
        stale = _restore(good_backup)
        fake_fastdeploy.statuses = [
            finished_status([result_step({"success": True, "file_count": 1})])
        ]

        run = restore_engine.start_restore(good_backup)

        stale.refresh_from_db()
        assert stale.status == RestoreRunStatus.TIMEOUT
        run.refresh_from_db()
        assert run.status == RestoreRunStatus.SUCCESS

    def test_stale_backup_no_longer_blocks_start_restore(self, target, good_backup, fake_fastdeploy):
        stale = _backup(target)
        fake_fastdeploy.statuses = [
            finished_status([result_step({"success": True, "file_count": 1})])
        ]

        run = restore_engine.start_restore(good_backup)

        stale.refresh_from_db()
        assert stale.status == BackupRunStatus.TIMEOUT
        assert run.status == RestoreRunStatus.SUCCESS


@pytest.mark.django_db
class TestReapedRunNeverFlipsBack:
    def _reap(self, run):
        type(run).objects.filter(pk=run.pk).update(
            status="timeout", error_message="Reaped as stale", finished_at=timezone.now()
        )

    def test_backup_success_after_reap_is_ignored(self, target):
        run = _backup(target, age=0)
        self._reap(run)
        status = finished_status([result_step(BACKUP_RESULT)])

        backup_engine._handle_deployment_finished(run, status, _Client())

        run.refresh_from_db()
        assert run.status == BackupRunStatus.TIMEOUT
        assert run.error_message == "Reaped as stale"
        assert run.storage_key == ""

    def test_backup_failure_after_reap_is_ignored(self, target):
        run = _backup(target, age=0)
        self._reap(run)

        backup_engine._mark_run_failed(run, "late failure")
        backup_engine._mark_run_timeout(run)

        run.refresh_from_db()
        assert run.status == BackupRunStatus.TIMEOUT
        assert run.error_message == "Reaped as stale"

    def test_restore_success_after_reap_is_ignored(self, good_backup):
        run = _restore(good_backup, age=0)
        self._reap(run)
        status = finished_status([result_step({"success": True, "file_count": 1})])

        restore_engine._handle_deployment_finished(run, status, _Client())

        run.refresh_from_db()
        assert run.status == RestoreRunStatus.TIMEOUT
        assert run.files_restored is None

    def test_restore_failure_after_reap_is_ignored(self, good_backup):
        run = _restore(good_backup, age=0)
        self._reap(run)

        restore_engine._mark_run_failed(run, "late failure")

        run.refresh_from_db()
        assert run.status == RestoreRunStatus.TIMEOUT

    def test_reaped_before_running_transition_stops_tracking(self, target, fake_fastdeploy, monkeypatch):
        """If the run is finalized between creation and deployment start, do not resurrect it."""
        original_start = fake_fastdeploy.start_deployment

        def start_and_reap(service_name, context=None):
            BackupRun.objects.filter(pk=int(context["ECHOPORT_RUN_ID"])).update(
                status=BackupRunStatus.TIMEOUT, error_message="Reaped as stale"
            )
            return original_start(service_name, context)

        monkeypatch.setattr(fake_fastdeploy, "start_deployment", start_and_reap)

        with pytest.raises(backup_engine.BackupError, match="not tracking"):
            backup_engine.start_backup(target)

        run = BackupRun.objects.get(target=target)
        assert run.status == BackupRunStatus.TIMEOUT
        assert run.error_message == "Reaped as stale"
        assert fake_fastdeploy.polled == []

    def test_reaped_existing_backup_run_never_deploys(self, target, fake_fastdeploy):
        """A paused UI worker whose run was reaped must not deploy alongside its replacement."""
        existing = _backup(target, status=BackupRunStatus.PENDING)
        self._reap(existing)  # reaped while the worker was paused; `existing` is stale in memory
        replacement = _backup(target, age=0)

        with pytest.raises(backup_engine.BackupError, match="no longer pending"):
            backup_engine.start_backup(target, existing_run=existing)

        assert fake_fastdeploy.started == []
        existing.refresh_from_db()
        replacement.refresh_from_db()
        assert existing.status == BackupRunStatus.TIMEOUT
        assert existing.error_message == "Reaped as stale"
        assert replacement.status == BackupRunStatus.RUNNING

    def test_reaped_existing_restore_run_never_deploys(self, good_backup, fake_fastdeploy):
        """A paused UI worker whose restore was reaped must not start a second restore."""
        existing = _restore(good_backup, status=RestoreRunStatus.PENDING)
        self._reap(existing)
        replacement = _restore(good_backup, age=0)

        with pytest.raises(restore_engine.RestoreError, match="no longer pending"):
            restore_engine.start_restore(good_backup, existing_run=existing)

        assert fake_fastdeploy.started == []
        existing.refresh_from_db()
        replacement.refresh_from_db()
        assert existing.status == RestoreRunStatus.TIMEOUT
        assert replacement.status == RestoreRunStatus.RUNNING

    def test_deleted_run_is_not_claimed(self, target, fake_fastdeploy):
        existing = _backup(target, status=BackupRunStatus.PENDING, age=0)
        BackupRun.objects.filter(pk=existing.pk).delete()

        with pytest.raises(backup_engine.BackupError, match="no longer pending"):
            backup_engine.start_backup(target, existing_run=existing)

        assert fake_fastdeploy.started == []
        assert BackupRun.objects.count() == 0

    def test_active_runs_are_written_normally(self, target):
        run = _backup(target, age=0)
        run.status = BackupRunStatus.SUCCESS
        run.storage_key = "k"

        assert save_if_still_active(run, backup_engine.ACTIVE_BACKUP_STATUSES, ["status", "storage_key"])
        run.refresh_from_db()
        assert run.status == BackupRunStatus.SUCCESS
        assert run.storage_key == "k"

    def test_missing_row_is_saved_like_before(self, good_backup):
        """A self-restore may replace the DB so the run row is gone; it is re-inserted."""
        run = _restore(good_backup, age=0)
        run_id = run.id
        RestoreRun.objects.filter(pk=run_id).delete()
        run.status = RestoreRunStatus.SUCCESS

        assert save_if_still_active(run, restore_engine.ACTIVE_RESTORE_STATUSES, ["status"])
        assert RestoreRun.objects.get(pk=run_id).status == RestoreRunStatus.SUCCESS


class _Client:
    @staticmethod
    def parse_echoport_result(steps):
        from backups.fastdeploy_client import FastDeployClient

        return FastDeployClient.parse_echoport_result(steps)


@pytest.mark.django_db
class TestScheduler:
    def test_stale_scheduled_run_is_reaped_and_next_backup_starts(
        self, target, fake_fastdeploy, capsys
    ):
        # Yesterday's scheduled run was orphaned by a killed cron process.
        stale = _backup(
            target,
            age=26 * 3600,
            trigger=BackupTrigger.SCHEDULED,
            triggered_by="scheduler",
        )
        fake_fastdeploy.statuses = [finished_status([result_step(BACKUP_RESULT)])]

        with pytest.raises(SystemExit) as exc_info:
            Command().handle(dry_run=False)
        assert exc_info.value.code == 0

        stale.refresh_from_db()
        assert stale.status == BackupRunStatus.TIMEOUT
        new_run = BackupRun.objects.exclude(pk=stale.pk).get(target=target)
        assert new_run.status == BackupRunStatus.SUCCESS
        assert new_run.trigger == BackupTrigger.SCHEDULED
        out = capsys.readouterr().out
        assert "Reaped stale runs: 1 backup(s)" in out
        assert "backup already in progress" not in out

    @patch("backups.management.commands.run_scheduled_backups.start_backup")
    def test_reap_only(self, mock_start_backup, target, good_backup, capsys):
        stale_backup = _backup(target, trigger=BackupTrigger.SCHEDULED)
        stale_restore = _restore(good_backup)

        with pytest.raises(SystemExit) as exc_info:
            Command().handle(dry_run=False, reap_only=True)
        assert exc_info.value.code == 0

        mock_start_backup.assert_not_called()
        stale_backup.refresh_from_db()
        stale_restore.refresh_from_db()
        assert stale_backup.status == BackupRunStatus.TIMEOUT
        assert stale_restore.status == RestoreRunStatus.TIMEOUT
        assert "1 backup(s)" in capsys.readouterr().out

    def test_dry_run_does_not_reap(self, target):
        stale = _backup(target)

        with pytest.raises(SystemExit):
            Command().handle(dry_run=True)

        stale.refresh_from_db()
        assert stale.status == BackupRunStatus.RUNNING

    def test_dry_run_and_reap_only_conflict(self, target):
        stale = _backup(target)

        with pytest.raises(SystemExit) as exc_info:
            Command().handle(dry_run=True, reap_only=True)
        assert exc_info.value.code == 2

        stale.refresh_from_db()
        assert stale.status == BackupRunStatus.RUNNING


@pytest.mark.django_db
class TestViews:
    @pytest.fixture
    def staff_client(self, client):
        client.force_login(User.objects.create_user("op", "op@test.com", "pw", is_staff=True))
        return client

    @pytest.fixture
    def fake_thread(self):
        with patch("backups.views.threading.Thread") as thread_cls:
            yield thread_cls

    def test_trigger_backup_reaps_stale_run(self, staff_client, target, fake_thread):
        stale = _backup(target)

        staff_client.post(reverse("backups:trigger_backup", args=[target.id]))

        stale.refresh_from_db()
        assert stale.status == BackupRunStatus.TIMEOUT
        assert BackupRun.objects.filter(target=target, status=BackupRunStatus.PENDING).count() == 1

    def test_trigger_restore_reaps_stale_restore(self, staff_client, good_backup, fake_thread):
        stale = _restore(good_backup)

        staff_client.post(reverse("backups:trigger_restore", args=[good_backup.id]))

        stale.refresh_from_db()
        assert stale.status == RestoreRunStatus.TIMEOUT
        assert RestoreRun.objects.filter(status=RestoreRunStatus.PENDING).count() == 1

    def test_trigger_restore_reaps_stale_backup(self, staff_client, target, good_backup, fake_thread):
        stale = _backup(target)

        staff_client.post(reverse("backups:trigger_restore", args=[good_backup.id]))

        stale.refresh_from_db()
        assert stale.status == BackupRunStatus.TIMEOUT
        assert RestoreRun.objects.filter(status=RestoreRunStatus.PENDING).count() == 1
