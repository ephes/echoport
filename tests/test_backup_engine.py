"""
Tests for backup engine behavior.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from backups.backup_engine import _handle_deployment_finished
from backups.models import BackupRun, BackupRunStatus, BackupTarget


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
