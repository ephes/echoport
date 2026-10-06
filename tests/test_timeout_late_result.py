"""
Tests for collecting the late result of a backup run that timed out.

FastDeploy cannot cancel a deployment, so a deployment can still finish and
upload an archive after its run was marked TIMEOUT. The scheduler records
that archive on the run (which stays TIMEOUT) so it is not orphaned.
"""

from datetime import timedelta
from unittest.mock import patch

import pytest
from django.utils import timezone

from backups import backup_engine
from backups.backup_engine import (
    DEFAULT_LATE_RESULT_WINDOW_SECONDS,
    get_late_result_window_seconds,
    reconcile_timed_out_runs,
)
from backups.fastdeploy_client import DeploymentNotFoundError, FastDeployError
from backups.management.commands.run_scheduled_backups import Command
from backups.models import BackupRun, BackupRunStatus, BackupTarget
from tests.conftest import finished_status, result_step

TIMEOUT = 600
WINDOW = 3600
LATE_RESULT = {
    "success": True,
    "key": "late-target/2026-01-02T02-00-00.tar.gz",
    "size_bytes": 42,
    "checksum_sha256": "e" * 64,
    "file_count": 3,
}


@pytest.fixture(autouse=True)
def window(settings):
    settings.ECHOPORT_LATE_RESULT_WINDOW_SECONDS = WINDOW
    return WINDOW


@pytest.fixture
def target(db):
    return BackupTarget.objects.create(
        name="late-target",
        fastdeploy_service="echoport-backup",
        service_name="late-target.service",
        db_path="/tmp/late.db",
        schedule="0 2 * * *",
        timeout_seconds=TIMEOUT,
        status="active",
    )


def make_timed_out_run(target, deployment_id=7, age=TIMEOUT + 60, **fields):
    started = timezone.now() - timedelta(seconds=age)
    values = {
        "target": target,
        "status": BackupRunStatus.TIMEOUT,
        "fastdeploy_deployment_id": deployment_id,
        "error_message": f"Backup timed out after {TIMEOUT} seconds",
        "started_at": started,
        "finished_at": started + timedelta(seconds=TIMEOUT),
    }
    values.update(fields)
    return BackupRun.objects.create(**values)


class TestReconcileTimedOutRuns:
    def test_records_late_archive_and_keeps_timeout(self, fake_fastdeploy, target):
        run = make_timed_out_run(target)
        fake_fastdeploy.statuses = [finished_status([result_step(LATE_RESULT)])]

        assert reconcile_timed_out_runs() == [run.pk]

        run.refresh_from_db()
        assert run.status == BackupRunStatus.TIMEOUT
        assert run.storage_key == LATE_RESULT["key"]
        assert run.size_bytes == 42
        assert run.checksum_sha256 == "e" * 64
        assert run.file_count == 3
        assert run.error_message.startswith(f"Backup timed out after {TIMEOUT} seconds")
        assert "finished later and uploaded" in run.error_message
        assert run.logs.startswith("[late result] deployment 7 finished after the run timed out")
        assert "ECHOPORT_RESULT" in run.logs
        assert fake_fastdeploy.polled == [7]

    def test_reconciled_run_is_not_checked_again(self, fake_fastdeploy, target):
        make_timed_out_run(target)
        fake_fastdeploy.statuses = [finished_status([result_step(LATE_RESULT)])]

        reconcile_timed_out_runs()
        assert reconcile_timed_out_runs() == []
        assert fake_fastdeploy.polled == [7]

    def test_still_running_deployment_is_retried(self, fake_fastdeploy, target):
        run = make_timed_out_run(target)
        fake_fastdeploy.statuses = []  # still running

        assert reconcile_timed_out_runs() == []
        run.refresh_from_db()
        assert run.logs == ""
        assert run.storage_key == ""

        fake_fastdeploy.statuses = [finished_status([result_step(LATE_RESULT)])]
        assert reconcile_timed_out_runs() == [run.pk]
        assert fake_fastdeploy.polled == [7, 7]

    def test_failed_deployment_stores_logs_only(self, fake_fastdeploy, target):
        run = make_timed_out_run(target)
        fake_fastdeploy.statuses = [
            finished_status([{"name": "upload", "state": "failure", "message": "mc: denied"}])
        ]

        assert reconcile_timed_out_runs() == []
        run.refresh_from_db()
        assert run.status == BackupRunStatus.TIMEOUT
        assert run.storage_key == ""
        assert run.size_bytes is None
        assert run.error_message == f"Backup timed out after {TIMEOUT} seconds"
        assert "mc: denied" in run.logs

        assert reconcile_timed_out_runs() == []
        assert fake_fastdeploy.polled == [7]

    def test_upload_followed_by_failed_step_is_recorded(self, fake_fastdeploy, target):
        run = make_timed_out_run(target)
        fake_fastdeploy.statuses = [
            finished_status(
                [
                    result_step(LATE_RESULT),
                    {"name": "cleanup", "state": "failure", "message": "rm: busy"},
                ]
            )
        ]

        assert reconcile_timed_out_runs() == [run.pk]
        run.refresh_from_db()
        assert run.storage_key == LATE_RESULT["key"]
        assert "rm: busy" in run.logs

    def test_reported_failure_is_not_recorded_as_archive(self, fake_fastdeploy, target):
        run = make_timed_out_run(target)
        failure = {**LATE_RESULT, "success": False, "error": "disk full"}
        fake_fastdeploy.statuses = [finished_status([result_step(failure)])]

        assert reconcile_timed_out_runs() == []
        run.refresh_from_db()
        assert run.storage_key == ""
        assert run.logs.startswith("[late result]")

    def test_missing_result_is_not_recorded_as_archive(self, fake_fastdeploy, target):
        run = make_timed_out_run(target)
        fake_fastdeploy.statuses = [
            finished_status([{"name": "backup", "state": "success", "message": "done"}])
        ]

        assert reconcile_timed_out_runs() == []
        run.refresh_from_db()
        assert run.storage_key == ""
        assert run.logs.startswith("[late result]")

    def test_vanished_deployment_stops_checking(self, fake_fastdeploy, target):
        run = make_timed_out_run(target)
        fake_fastdeploy.statuses = [DeploymentNotFoundError("gone")]

        assert reconcile_timed_out_runs() == []
        run.refresh_from_db()
        assert run.logs == "[late result] deployment 7 no longer exists"
        assert run.storage_key == ""

        assert reconcile_timed_out_runs() == []
        assert fake_fastdeploy.polled == [7]

    def test_poll_error_is_retried_and_does_not_block_other_runs(self, fake_fastdeploy, target):
        other = BackupTarget.objects.create(
            name="other-late-target",
            fastdeploy_service="echoport-backup",
            service_name="other.service",
            db_path="/tmp/other.db",
            timeout_seconds=TIMEOUT,
            status="active",
        )
        flaky = make_timed_out_run(target, deployment_id=7, age=TIMEOUT + 120)
        good = make_timed_out_run(other, deployment_id=8)
        fake_fastdeploy.statuses = [
            FastDeployError("connection refused"),
            finished_status([result_step(LATE_RESULT)], deployment_id=8),
        ]

        assert reconcile_timed_out_runs() == [good.pk]
        flaky.refresh_from_db()
        assert flaky.logs == ""
        assert fake_fastdeploy.polled == [7, 8]

    def test_only_recent_unreconciled_timeout_runs_with_deployment(self, fake_fastdeploy, target):
        too_old = make_timed_out_run(target, age=WINDOW + 60)
        no_deployment = make_timed_out_run(target, deployment_id=None)
        already_has_key = make_timed_out_run(target, storage_key="late-target/x.tar.gz")
        failed = make_timed_out_run(target, status=BackupRunStatus.FAILED)
        succeeded = make_timed_out_run(target, status=BackupRunStatus.SUCCESS)
        fake_fastdeploy.statuses = [finished_status([result_step(LATE_RESULT)])]

        assert reconcile_timed_out_runs() == []
        assert fake_fastdeploy.polled == []
        for run in (too_old, no_deployment, already_has_key, failed, succeeded):
            run.refresh_from_db()
            assert run.logs == ""

    def test_does_not_overwrite_run_changed_meanwhile(self, fake_fastdeploy, target):
        run = make_timed_out_run(target)
        fake_fastdeploy.statuses = [finished_status([result_step(LATE_RESULT)])]

        def change_then_poll(deployment_id):
            BackupRun.objects.filter(pk=run.pk).update(logs="collected elsewhere")
            return finished_status([result_step(LATE_RESULT)])

        with patch.object(fake_fastdeploy, "get_deployment_status", side_effect=change_then_poll):
            assert reconcile_timed_out_runs() == []
        run.refresh_from_db()
        assert run.logs == "collected elsewhere"
        assert run.storage_key == ""

    def test_reaped_run_gets_late_archive(self, fake_fastdeploy, target, settings):
        """A run reaped as stale (killed poller) is reconciled the same way."""
        settings.ECHOPORT_STALE_RUN_GRACE_SECONDS = 0
        run = make_timed_out_run(
            target, status=BackupRunStatus.RUNNING, finished_at=None, error_message=""
        )
        assert backup_engine.reap_stale_runs() == [run.pk]
        fake_fastdeploy.statuses = [finished_status([result_step(LATE_RESULT)])]

        assert reconcile_timed_out_runs() == [run.pk]
        run.refresh_from_db()
        assert run.status == BackupRunStatus.TIMEOUT
        assert run.storage_key == LATE_RESULT["key"]
        assert run.error_message.startswith("Reaped as stale")


class TestWindowSetting:
    def test_invalid_value_uses_default(self, settings):
        settings.ECHOPORT_LATE_RESULT_WINDOW_SECONDS = "soon"
        assert get_late_result_window_seconds() == DEFAULT_LATE_RESULT_WINDOW_SECONDS

    def test_negative_value_is_clamped(self, settings):
        settings.ECHOPORT_LATE_RESULT_WINDOW_SECONDS = -5
        assert get_late_result_window_seconds() == 0


@pytest.mark.django_db
class TestSchedulerIntegration:
    @patch("backups.management.commands.run_scheduled_backups.reconcile_timed_out_runs")
    def test_normal_pass_reconciles(self, mock_reconcile, target, capsys):
        mock_reconcile.return_value = [5]
        target.schedule = ""
        target.save()
        with pytest.raises(SystemExit):
            Command().handle(dry_run=False)
        mock_reconcile.assert_called_once()
        assert "Recorded late archives for 1 timed-out backup run(s) [5]" in capsys.readouterr().out

    @patch("backups.management.commands.run_scheduled_backups.reconcile_timed_out_runs")
    def test_dry_run_and_reap_only_do_not_reconcile(self, mock_reconcile, target):
        with pytest.raises(SystemExit):
            Command().handle(dry_run=True)
        with pytest.raises(SystemExit):
            Command().handle(dry_run=False, reap_only=True)
        mock_reconcile.assert_not_called()
