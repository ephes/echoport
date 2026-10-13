"""
Tests for backup engine behavior.
"""

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from django.db import OperationalError, connection, transaction

from backups.backup_engine import (
    BackupError,
    BackupTimeoutError,
    ConcurrentBackupError,
    ConcurrentRestoreError,
    _handle_deployment_finished,
    start_backup,
)
from backups.fastdeploy_client import (
    DeploymentNotFoundError,
    DeploymentStartError,
    FastDeployError,
)
from backups.models import (
    BackupRun,
    BackupRunStatus,
    BackupTarget,
    BackupTrigger,
    RestoreRun,
    RestoreRunStatus,
)
from tests.conftest import finished_status, result_step


@pytest.fixture
def backup_target(db):
    return BackupTarget.objects.create(
        name="engine-test",
        description="Backup engine test target",
        fastdeploy_service="echoport-backup",
        service_name="engine-test.service",
        db_path="/tmp/test.db",
        backup_files=["/tmp/test.txt"],
        status="active",
    )


@pytest.mark.django_db
def test_handle_deployment_finished_missing_result_marks_failed(backup_target):
    run = BackupRun.objects.create(
        target=backup_target,
        status=BackupRunStatus.RUNNING,
        storage_bucket="backups",
    )
    status = SimpleNamespace(
        is_successful=True,
        steps=[{"name": "backup", "state": "success", "message": "done"}],
        failed_step=None,
    )
    client = MagicMock()
    client.parse_echoport_result.return_value = None

    updated_run = _handle_deployment_finished(run, status, client)

    assert updated_run.status == BackupRunStatus.FAILED
    assert "no result was reported" in updated_run.error_message
    assert updated_run.finished_at is not None


# ---------------------------------------------------------------------------
# start_backup with a scripted FastDeploy client
# ---------------------------------------------------------------------------

BACKUP_RESULT = {
    "success": True,
    "bucket": "backups",
    "key": "engine-test/2026-01-01T00-00-00.tar.gz",
    "size_bytes": 2048,
    "checksum_sha256": "c" * 64,
    "file_count": 3,
}


@pytest.fixture
def short_target(backup_target):
    BackupTarget.objects.filter(pk=backup_target.pk).update(timeout_seconds=3)
    backup_target.refresh_from_db()
    return backup_target


def _pending_backup(target, **kwargs):
    return BackupRun.objects.create(
        target=target,
        status=kwargs.pop("status", BackupRunStatus.PENDING),
        storage_bucket=target.storage_bucket,
        **kwargs,
    )


@pytest.mark.django_db
class TestStartBackupSuccess:
    def test_creates_run_and_records_result(self, short_target, fake_fastdeploy):
        fake_fastdeploy.statuses = [finished_status([result_step(BACKUP_RESULT)])]

        run = start_backup(
            short_target, trigger=BackupTrigger.SCHEDULED, triggered_by="scheduler"
        )

        run.refresh_from_db()
        assert run.status == BackupRunStatus.SUCCESS
        assert run.trigger == BackupTrigger.SCHEDULED
        assert run.triggered_by == "scheduler"
        assert run.fastdeploy_deployment_id == 7
        assert run.storage_key == BACKUP_RESULT["key"]
        assert run.size_bytes == 2048
        assert run.checksum_sha256 == "c" * 64
        assert run.file_count == 3
        assert run.finished_at is not None

        service, context = fake_fastdeploy.started[0]
        assert service == "echoport-backup"
        assert context["ECHOPORT_ACTION"] == "backup"
        assert context["ECHOPORT_TARGET"] == "engine-test"
        assert context["ECHOPORT_RUN_ID"] == str(run.id)
        assert context["ECHOPORT_DB_PATH"] == "/tmp/test.db"
        assert context["ECHOPORT_BACKUP_FILES"] == "/tmp/test.txt"
        assert context["ECHOPORT_BUCKET"] == short_target.storage_bucket
        assert context["ECHOPORT_KEY_PREFIX"] == f"engine-test/{context['ECHOPORT_TIMESTAMP']}"
        assert fake_fastdeploy.exited == 1

    def test_continues_existing_pending_run(self, short_target, fake_fastdeploy):
        existing = _pending_backup(short_target, triggered_by="ui-user")
        fake_fastdeploy.statuses = [finished_status([result_step(BACKUP_RESULT)])]

        run = start_backup(short_target, existing_run=existing)

        assert run.pk == existing.pk
        assert BackupRun.objects.count() == 1
        existing.refresh_from_db()
        assert existing.status == BackupRunStatus.SUCCESS

    def test_transient_poll_errors_are_retried(self, short_target, fake_fastdeploy):
        fake_fastdeploy.statuses = [
            FastDeployError("HTTP 502"),
            finished_status([result_step(BACKUP_RESULT)]),
        ]

        run = start_backup(short_target)

        assert run.status == BackupRunStatus.SUCCESS
        assert len(fake_fastdeploy.polled) == 2


@pytest.mark.django_db
class TestStartBackupGuards:
    def test_blocks_while_restore_is_running(self, short_target, fake_fastdeploy):
        source = BackupRun.objects.create(
            target=short_target, status=BackupRunStatus.SUCCESS, checksum_sha256="d" * 64
        )
        restore = RestoreRun.objects.create(
            backup_run=source, target=short_target, status=RestoreRunStatus.RUNNING
        )
        existing = _pending_backup(short_target)

        with pytest.raises(ConcurrentRestoreError, match=f"restore {restore.id} is running"):
            start_backup(short_target, existing_run=existing)

        existing.refresh_from_db()
        assert existing.status == BackupRunStatus.FAILED
        assert existing.finished_at is not None
        assert fake_fastdeploy.started == []

    @pytest.mark.django_db(transaction=True)
    def test_concurrent_backup_hits_unique_constraint(self, short_target, fake_fastdeploy):
        # transaction=True: the IntegrityError would otherwise poison the
        # per-test transaction (production runs in autocommit).
        _pending_backup(short_target, status=BackupRunStatus.RUNNING)

        with pytest.raises(ConcurrentBackupError, match="already running"):
            start_backup(short_target)

        assert BackupRun.objects.count() == 1
        assert fake_fastdeploy.started == []

    def test_existing_run_for_other_target_is_rejected(self, short_target, fake_fastdeploy):
        other = BackupTarget.objects.create(
            name="other",
            fastdeploy_service="echoport-backup",
            service_name="other.service",
            db_path="/tmp/other.db",
        )
        existing = _pending_backup(other)

        with pytest.raises(BackupError, match="belongs to target 'other'"):
            start_backup(short_target, existing_run=existing)

        assert fake_fastdeploy.started == []

    def test_existing_run_must_be_pending(self, short_target, fake_fastdeploy):
        existing = _pending_backup(short_target, status=BackupRunStatus.FAILED)

        with pytest.raises(BackupError, match="expected 'pending'"):
            start_backup(short_target, existing_run=existing)

        assert fake_fastdeploy.started == []

    def test_lock_contention_raises_without_starting(self, short_target, fake_fastdeploy, monkeypatch):
        monkeypatch.setattr(
            "backups.backup_engine.connection",
            SimpleNamespace(features=SimpleNamespace(has_select_for_update=True)),
        )
        lock_qs = MagicMock()
        lock_qs.get.side_effect = OperationalError("database is locked")
        select = MagicMock(return_value=lock_qs)
        monkeypatch.setattr(BackupTarget.objects, "select_for_update", select)

        with pytest.raises(ConcurrentRestoreError, match="Cannot acquire lock"):
            start_backup(short_target)

        select.assert_called_once_with(nowait=True)
        assert BackupRun.objects.count() == 0
        assert fake_fastdeploy.started == []

    def test_lock_contention_fails_existing_run(self, short_target, fake_fastdeploy, monkeypatch):
        monkeypatch.setattr(
            "backups.backup_engine.connection",
            SimpleNamespace(features=SimpleNamespace(has_select_for_update=True)),
        )
        lock_qs = MagicMock()
        lock_qs.get.side_effect = OperationalError("database is locked")
        monkeypatch.setattr(
            BackupTarget.objects, "select_for_update", MagicMock(return_value=lock_qs)
        )
        existing = _pending_backup(short_target)

        with pytest.raises(ConcurrentRestoreError, match="Cannot acquire lock"):
            start_backup(short_target, existing_run=existing)

        existing.refresh_from_db()
        assert existing.status == BackupRunStatus.FAILED
        assert "Cannot acquire lock" in existing.error_message
        assert fake_fastdeploy.started == []


@pytest.mark.django_db
class TestStartBackupFailures:
    def test_deployment_start_error_marks_failed(self, short_target, fake_fastdeploy):
        fake_fastdeploy.start_error = DeploymentStartError("HTTP 500: oops")

        with pytest.raises(BackupError, match="Failed to start backup deployment"):
            start_backup(short_target)

        run = BackupRun.objects.get()
        assert run.status == BackupRunStatus.FAILED
        assert "HTTP 500" in run.error_message
        assert fake_fastdeploy.polled == []

    def test_deployment_disappearing_marks_failed(self, short_target, fake_fastdeploy):
        fake_fastdeploy.statuses = [DeploymentNotFoundError("gone")]

        with pytest.raises(BackupError, match="disappeared"):
            start_backup(short_target)

        run = BackupRun.objects.get()
        assert run.status == BackupRunStatus.FAILED
        assert run.error_message == "Deployment not found"

    def test_timeout_marks_run_timed_out(self, short_target, fake_fastdeploy):
        with pytest.raises(BackupTimeoutError, match="timed out after 3 seconds"):
            start_backup(short_target)

        run = BackupRun.objects.get()
        assert run.status == BackupRunStatus.TIMEOUT
        assert run.error_message == "Backup timed out after 3 seconds"
        assert run.finished_at is not None
        assert fake_fastdeploy.sleeps == [1, 1, 1]

    def test_endpoint_config_error_marks_failed(self, short_target, fake_fastdeploy):
        BackupTarget.objects.filter(pk=short_target.pk).update(fastdeploy_endpoint_key="gone")
        short_target.refresh_from_db()

        with pytest.raises(BackupError, match="FastDeploy configuration error"):
            start_backup(short_target)

        run = BackupRun.objects.get()
        assert run.status == BackupRunStatus.FAILED
        assert fake_fastdeploy.started == []

    def test_unexpected_error_marks_failed(self, short_target, fake_fastdeploy):
        fake_fastdeploy.statuses = [ValueError("boom")]

        with pytest.raises(BackupError, match="Unexpected error: boom"):
            start_backup(short_target)

        assert BackupRun.objects.get().status == BackupRunStatus.FAILED

    def test_reported_failure(self, short_target, fake_fastdeploy):
        fake_fastdeploy.statuses = [
            finished_status([result_step({"success": False, "error": "upload failed"})])
        ]

        run = start_backup(short_target)

        assert run.status == BackupRunStatus.FAILED
        assert run.error_message == "upload failed"
        assert run.storage_key == ""

    def test_failed_step(self, short_target, fake_fastdeploy):
        fake_fastdeploy.statuses = [
            finished_status([{"name": "dump", "state": "failure", "message": "sqlite busy"}])
        ]

        run = start_backup(short_target)

        assert run.status == BackupRunStatus.FAILED
        assert run.error_message == "sqlite busy"


def _status_seen_by_another_connection(backup_run):
    """Read the committed status of a run through a second database connection."""
    seen = []

    def read():
        try:
            seen.append(BackupRun.objects.values_list("status", "error_message").get(pk=backup_run.pk))
        finally:
            connection.close()

    thread = threading.Thread(target=read)
    thread.start()
    thread.join()
    return seen[0]


@pytest.mark.django_db(transaction=True)
class TestFailedMarkIsCommitted:
    """
    A pre-created (UI) run whose backup is refused must end FAILED in the
    database, visible to other connections - not rolled back to PENDING.
    These tests use real transactions and no mocks of the database layer.
    """

    def test_running_restore_fails_existing_run(self, short_target, fake_fastdeploy):
        source = BackupRun.objects.create(
            target=short_target, status=BackupRunStatus.SUCCESS, checksum_sha256="d" * 64
        )
        RestoreRun.objects.create(
            backup_run=source, target=short_target, status=RestoreRunStatus.RUNNING
        )
        existing = _pending_backup(short_target)

        with pytest.raises(ConcurrentRestoreError, match="Cannot backup while restore"):
            start_backup(short_target, existing_run=existing)

        status, error_message = _status_seen_by_another_connection(existing)
        assert status == BackupRunStatus.FAILED
        assert "Cannot backup while restore" in error_message
        assert fake_fastdeploy.started == []

    @pytest.mark.skipif(
        not connection.features.has_select_for_update_nowait,
        reason="needs a database with row locks (PostgreSQL); see README, Tests",
    )
    def test_real_lock_contention_fails_existing_run(self, short_target, fake_fastdeploy):
        existing = _pending_backup(short_target)
        locked = threading.Event()
        release = threading.Event()

        def hold_target_lock():
            try:
                with transaction.atomic():
                    BackupTarget.objects.select_for_update().get(pk=short_target.pk)
                    locked.set()
                    release.wait(timeout=30)
            finally:
                locked.set()
                connection.close()

        holder = threading.Thread(target=hold_target_lock)
        holder.start()
        try:
            assert locked.wait(timeout=30)
            with pytest.raises(ConcurrentRestoreError, match="Cannot acquire lock"):
                start_backup(short_target, existing_run=existing)
        finally:
            release.set()
            holder.join()

        status, error_message = _status_seen_by_another_connection(existing)
        assert status == BackupRunStatus.FAILED
        assert "Cannot acquire lock" in error_message
        assert fake_fastdeploy.started == []
