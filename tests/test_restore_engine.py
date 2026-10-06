"""
Tests for the restore engine (start_restore) with a scripted FastDeploy client.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from django.db import OperationalError

from backups.fastdeploy_client import (
    DeploymentNotFoundError,
    DeploymentStartError,
    FastDeployError,
)
from backups.models import (
    BackupRun,
    BackupRunStatus,
    BackupTarget,
    RestoreRun,
    RestoreRunStatus,
)
from backups.restore_engine import (
    ConcurrentBackupError,
    ConcurrentRestoreError,
    MissingChecksumError,
    RestoreError,
    RestoreTimeoutError,
    _handle_deployment_finished,
    start_restore,
)
from tests.conftest import finished_status, result_step

CHECKSUM = "a" * 64


@pytest.fixture
def target(db):
    return BackupTarget.objects.create(
        name="restore-engine",
        fastdeploy_service="echoport-backup",
        service_name="restore-engine.service",
        db_path="/tmp/restore-engine.db",
        backup_files=["/tmp/one.txt", "/tmp/two.txt"],
        restore_owner="svc:svc",
        timeout_seconds=3,
        status="active",
    )


@pytest.fixture
def backup_run(target):
    return BackupRun.objects.create(
        target=target,
        status=BackupRunStatus.SUCCESS,
        storage_bucket="backups",
        storage_key="restore-engine/2026-01-01T00-00-00.tar.gz",
        checksum_sha256=CHECKSUM,
        size_bytes=100,
        file_count=2,
    )


def _pending_restore(backup_run, **kwargs):
    return RestoreRun.objects.create(
        backup_run=backup_run,
        target=kwargs.pop("target", backup_run.target),
        status=kwargs.pop("status", RestoreRunStatus.PENDING),
        **kwargs,
    )


SUCCESS_STEPS = [
    {"name": "stop", "state": "success", "message": "stopped"},
    result_step({"success": True, "file_count": 4}),
]


@pytest.mark.django_db
class TestStartRestoreSuccess:
    def test_creates_run_starts_deployment_and_records_success(self, backup_run, fake_fastdeploy):
        fake_fastdeploy.statuses = [finished_status(SUCCESS_STEPS)]

        run = start_restore(backup_run, triggered_by="alice")

        run.refresh_from_db()
        assert run.status == RestoreRunStatus.SUCCESS
        assert run.files_restored == 4
        assert run.triggered_by == "alice"
        assert run.fastdeploy_deployment_id == 7
        assert run.finished_at is not None
        assert "[stop] (success)" in run.logs
        assert RestoreRun.objects.count() == 1

        service, context = fake_fastdeploy.started[0]
        assert service == "echoport-backup"
        assert context == {
            "ECHOPORT_ACTION": "restore",
            "ECHOPORT_TARGET": "restore-engine",
            "ECHOPORT_RESTORE_ID": str(run.id),
            "ECHOPORT_DB_PATH": "/tmp/restore-engine.db",
            "ECHOPORT_BACKUP_FILES": "/tmp/one.txt,/tmp/two.txt",
            "ECHOPORT_BUCKET": "backups",
            "ECHOPORT_KEY": "restore-engine/2026-01-01T00-00-00.tar.gz",
            "ECHOPORT_CHECKSUM": CHECKSUM,
            "ECHOPORT_SERVICE_NAME": "restore-engine.service",
            "ECHOPORT_RESTORE_OWNER": "svc:svc",
        }
        assert fake_fastdeploy.polled == [7]
        assert fake_fastdeploy.sleeps == [1]
        assert fake_fastdeploy.exited == 1

    def test_context_omits_restore_owner_when_blank(self, target, backup_run, fake_fastdeploy):
        BackupTarget.objects.filter(pk=target.pk).update(restore_owner="", backup_files=[])
        backup_run.refresh_from_db()
        backup_run.target.refresh_from_db()
        fake_fastdeploy.statuses = [finished_status(SUCCESS_STEPS)]

        start_restore(backup_run)

        _, context = fake_fastdeploy.started[0]
        assert "ECHOPORT_RESTORE_OWNER" not in context
        assert context["ECHOPORT_BACKUP_FILES"] == ""

    def test_uses_target_token_override(self, target, backup_run, fake_fastdeploy):
        BackupTarget.objects.filter(pk=target.pk).update(service_token="target-token")
        backup_run.target.refresh_from_db()
        fake_fastdeploy.statuses = [finished_status(SUCCESS_STEPS)]

        start_restore(backup_run)

        assert fake_fastdeploy.client_kwargs == [
            {"base_url": None, "service_token": "target-token"}
        ]

    def test_continues_existing_pending_run(self, backup_run, fake_fastdeploy):
        existing = _pending_restore(backup_run, triggered_by="ui-user")
        fake_fastdeploy.statuses = [finished_status(SUCCESS_STEPS)]

        run = start_restore(backup_run, existing_run=existing)

        assert run.pk == existing.pk
        assert RestoreRun.objects.count() == 1
        existing.refresh_from_db()
        assert existing.status == RestoreRunStatus.SUCCESS
        assert existing.triggered_by == "ui-user"

    def test_transient_poll_errors_are_retried(self, backup_run, fake_fastdeploy):
        fake_fastdeploy.statuses = [
            FastDeployError("HTTP 502"),
            finished_status(SUCCESS_STEPS),
        ]

        run = start_restore(backup_run)

        assert run.status == RestoreRunStatus.SUCCESS
        assert len(fake_fastdeploy.polled) == 2


@pytest.mark.django_db
class TestStartRestorePreconditions:
    def test_rejects_unsuccessful_backup(self, backup_run, fake_fastdeploy):
        BackupRun.objects.filter(pk=backup_run.pk).update(status=BackupRunStatus.FAILED)
        backup_run.refresh_from_db()

        with pytest.raises(RestoreError, match="status 'failed'"):
            start_restore(backup_run)

        assert RestoreRun.objects.count() == 0
        assert fake_fastdeploy.started == []

    def test_rejects_unsuccessful_backup_and_fails_existing_run(self, backup_run, fake_fastdeploy):
        existing = _pending_restore(backup_run)
        BackupRun.objects.filter(pk=backup_run.pk).update(status=BackupRunStatus.FAILED)
        backup_run.refresh_from_db()

        with pytest.raises(RestoreError):
            start_restore(backup_run, existing_run=existing)

        existing.refresh_from_db()
        assert existing.status == RestoreRunStatus.FAILED
        assert "status 'failed'" in existing.error_message
        assert existing.finished_at is not None
        assert fake_fastdeploy.started == []

    def test_rejects_missing_checksum(self, backup_run, fake_fastdeploy):
        existing = _pending_restore(backup_run)
        BackupRun.objects.filter(pk=backup_run.pk).update(checksum_sha256="")
        backup_run.refresh_from_db()

        with pytest.raises(MissingChecksumError):
            start_restore(backup_run, existing_run=existing)

        existing.refresh_from_db()
        assert existing.status == RestoreRunStatus.FAILED
        assert "missing checksum" in existing.error_message
        assert fake_fastdeploy.started == []

    def test_blocks_while_backup_is_running(self, target, backup_run, fake_fastdeploy):
        active = BackupRun.objects.create(target=target, status=BackupRunStatus.RUNNING)
        existing = _pending_restore(backup_run)

        with pytest.raises(ConcurrentBackupError, match=f"backup {active.id} is running"):
            start_restore(backup_run, existing_run=existing)

        existing.refresh_from_db()
        assert existing.status == RestoreRunStatus.FAILED
        assert fake_fastdeploy.started == []

    @pytest.mark.django_db(transaction=True)
    def test_concurrent_restore_hits_unique_constraint(self, backup_run, fake_fastdeploy):
        # transaction=True: the IntegrityError would otherwise poison the
        # per-test transaction (production runs in autocommit).
        _pending_restore(backup_run, status=RestoreRunStatus.RUNNING)

        with pytest.raises(ConcurrentRestoreError, match="already running"):
            start_restore(backup_run)

        assert RestoreRun.objects.count() == 1
        assert fake_fastdeploy.started == []

    def test_existing_run_for_other_backup_is_rejected(self, target, backup_run, fake_fastdeploy):
        other_backup = BackupRun.objects.create(
            target=target,
            status=BackupRunStatus.SUCCESS,
            checksum_sha256="b" * 64,
        )
        existing = _pending_restore(other_backup)

        with pytest.raises(RestoreError, match=f"is for backup {other_backup.id}"):
            start_restore(backup_run, existing_run=existing)

        assert fake_fastdeploy.started == []

    def test_existing_run_for_other_target_is_rejected(self, backup_run, fake_fastdeploy):
        other_target = BackupTarget.objects.create(
            name="other",
            fastdeploy_service="echoport-backup",
            service_name="other.service",
            db_path="/tmp/other.db",
        )
        existing = _pending_restore(backup_run, target=other_target)

        with pytest.raises(RestoreError, match="target mismatch"):
            start_restore(backup_run, existing_run=existing)

        assert fake_fastdeploy.started == []

    def test_existing_run_must_be_pending(self, backup_run, fake_fastdeploy):
        existing = _pending_restore(backup_run, status=RestoreRunStatus.FAILED)

        with pytest.raises(RestoreError, match="expected 'pending'"):
            start_restore(backup_run, existing_run=existing)

        assert fake_fastdeploy.started == []


@pytest.mark.django_db
class TestStartRestoreRowLock:
    """
    The select_for_update branch only runs on backends with row locking.
    SQLite has none, so these tests force the branch and fake the lock query.
    """

    @pytest.fixture
    def locking_backend(self, monkeypatch):
        fake_connection = SimpleNamespace(
            features=SimpleNamespace(has_select_for_update=True),
            close=lambda: None,
        )
        monkeypatch.setattr("backups.restore_engine.connection", fake_connection)
        lock_qs = MagicMock()
        monkeypatch.setattr(
            BackupTarget.objects, "select_for_update", MagicMock(return_value=lock_qs)
        )
        return lock_qs

    def test_lock_contention_raises_without_starting(self, backup_run, fake_fastdeploy, locking_backend):
        locking_backend.get.side_effect = OperationalError("database is locked")

        with pytest.raises(ConcurrentBackupError, match="Cannot acquire lock"):
            start_restore(backup_run)

        BackupTarget.objects.select_for_update.assert_called_once_with(nowait=True)
        assert RestoreRun.objects.count() == 0
        assert fake_fastdeploy.started == []

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "Known gap: on row-locking backends the FAILED mark is written inside "
            "transaction.atomic() and rolled back with the raised error, leaving the "
            "UI-created run PENDING. SQLite (the default database) skips this branch."
        ),
    )
    def test_lock_contention_fails_existing_run(self, backup_run, fake_fastdeploy, locking_backend):
        locking_backend.get.side_effect = OperationalError("database is locked")
        existing = _pending_restore(backup_run)

        with pytest.raises(ConcurrentBackupError, match="Cannot acquire lock"):
            start_restore(backup_run, existing_run=existing)

        BackupTarget.objects.select_for_update.assert_called_once_with(nowait=True)
        existing.refresh_from_db()
        assert existing.status == RestoreRunStatus.FAILED
        assert fake_fastdeploy.started == []

    def test_lock_acquired_creates_run(self, backup_run, fake_fastdeploy, locking_backend):
        fake_fastdeploy.statuses = [finished_status(SUCCESS_STEPS)]

        run = start_restore(backup_run)

        locking_backend.get.assert_called_once_with(id=backup_run.target_id)
        assert run.status == RestoreRunStatus.SUCCESS


@pytest.mark.django_db
class TestStartRestoreFailures:
    def test_deployment_start_error_marks_failed(self, backup_run, fake_fastdeploy):
        fake_fastdeploy.start_error = DeploymentStartError("HTTP 403: forbidden")

        with pytest.raises(RestoreError, match="Failed to start restore deployment"):
            start_restore(backup_run)

        run = RestoreRun.objects.get()
        assert run.status == RestoreRunStatus.FAILED
        assert "HTTP 403" in run.error_message
        assert run.finished_at is not None
        assert fake_fastdeploy.polled == []

    def test_deployment_disappearing_marks_failed(self, backup_run, fake_fastdeploy):
        fake_fastdeploy.statuses = [DeploymentNotFoundError("gone")]

        with pytest.raises(RestoreError, match="disappeared"):
            start_restore(backup_run)

        run = RestoreRun.objects.get()
        assert run.status == RestoreRunStatus.FAILED
        assert run.error_message == "Deployment not found"
        assert run.fastdeploy_deployment_id == 7

    def test_timeout_marks_run_timed_out(self, backup_run, fake_fastdeploy):
        # No statuses scripted: the deployment never finishes.
        with pytest.raises(RestoreTimeoutError, match="timed out after 3 seconds"):
            start_restore(backup_run)

        run = RestoreRun.objects.get()
        assert run.status == RestoreRunStatus.TIMEOUT
        assert run.error_message == "Restore timed out after 3 seconds"
        assert run.finished_at is not None
        assert fake_fastdeploy.sleeps == [1, 1, 1]
        assert len(fake_fastdeploy.polled) == 3

    def test_endpoint_config_error_marks_failed(self, target, backup_run, fake_fastdeploy):
        # Bypass model validation to simulate settings drift after the target was saved.
        BackupTarget.objects.filter(pk=target.pk).update(fastdeploy_endpoint_key="gone")
        backup_run.target.refresh_from_db()

        with pytest.raises(RestoreError, match="FastDeploy configuration error"):
            start_restore(backup_run)

        run = RestoreRun.objects.get()
        assert run.status == RestoreRunStatus.FAILED
        assert "'gone' not found" in run.error_message
        assert fake_fastdeploy.started == []

    def test_unexpected_error_marks_failed(self, backup_run, fake_fastdeploy):
        fake_fastdeploy.statuses = [ValueError("boom")]

        with pytest.raises(RestoreError, match="Unexpected error: boom"):
            start_restore(backup_run)

        run = RestoreRun.objects.get()
        assert run.status == RestoreRunStatus.FAILED
        assert run.error_message == "boom"

    def test_failed_step_marks_failed(self, backup_run, fake_fastdeploy):
        fake_fastdeploy.statuses = [
            finished_status(
                [
                    {"name": "stop", "state": "success", "message": ""},
                    {"name": "extract", "state": "failure", "message": "checksum mismatch"},
                ]
            )
        ]

        run = start_restore(backup_run)

        assert run.status == RestoreRunStatus.FAILED
        assert run.error_message == "checksum mismatch"
        assert "[extract] (failure)" in run.logs


@pytest.mark.django_db
class TestHandleDeploymentFinished:
    @pytest.fixture
    def running_restore(self, backup_run):
        return _pending_restore(backup_run, status=RestoreRunStatus.RUNNING)

    def _client(self):
        from tests.conftest import FakeFastDeploy

        return FakeFastDeploy()

    def test_reported_success(self, running_restore):
        status = finished_status([result_step({"success": True, "file_count": 2})])

        run = _handle_deployment_finished(running_restore, status, self._client())

        run.refresh_from_db()
        assert run.status == RestoreRunStatus.SUCCESS
        assert run.files_restored == 2
        assert run.error_message == ""
        assert run.finished_at is not None

    def test_reported_failure(self, running_restore):
        status = finished_status([result_step({"success": False, "error": "disk full"})])

        run = _handle_deployment_finished(running_restore, status, self._client())

        run.refresh_from_db()
        assert run.status == RestoreRunStatus.FAILED
        assert run.error_message == "disk full"

    def test_reported_failure_without_error_text(self, running_restore):
        status = finished_status([result_step({"success": False})])

        run = _handle_deployment_finished(running_restore, status, self._client())

        assert run.status == RestoreRunStatus.FAILED
        assert run.error_message == "Restore reported failure"

    def test_missing_result_is_failure(self, running_restore):
        status = finished_status([{"name": "restore", "state": "success", "message": "ok"}])

        run = _handle_deployment_finished(running_restore, status, self._client())

        run.refresh_from_db()
        assert run.status == RestoreRunStatus.FAILED
        assert "no result was reported" in run.error_message

    def test_failed_deployment_without_failed_step(self, running_restore):
        status = finished_status([{"name": "restore", "state": "cancelled"}])

        run = _handle_deployment_finished(running_restore, status, self._client())

        assert run.status == RestoreRunStatus.FAILED
        assert run.error_message == "Deployment failed"

    def test_failed_step_without_message(self, running_restore):
        status = finished_status([{"name": "restore", "state": "failure"}])

        run = _handle_deployment_finished(running_restore, status, self._client())

        assert run.status == RestoreRunStatus.FAILED
        assert run.error_message == "Unknown error"

    def test_reconnects_before_final_save(self, running_restore, monkeypatch):
        """Self-restore may replace the SQLite file; the connection is closed first."""
        calls = []

        class RecordingConnection:
            def close(self):
                calls.append("close")
                raise RuntimeError("close failures are ignored")

        monkeypatch.setattr("backups.restore_engine.connection", RecordingConnection())
        original_save = running_restore.save

        def recording_save(*args, **kwargs):
            calls.append("save")
            return original_save(*args, **kwargs)

        monkeypatch.setattr(running_restore, "save", recording_save)
        status = finished_status([result_step({"success": True, "file_count": 1})])

        run = _handle_deployment_finished(running_restore, status, self._client())

        assert calls == ["close", "save"]
        run.refresh_from_db()
        assert run.status == RestoreRunStatus.SUCCESS
