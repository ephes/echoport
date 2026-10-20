"""Durable cleanup admission, including actual file-backed SQLite connections."""
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from unittest.mock import patch

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection, connections, transaction
from django.test import Client
from django.contrib.auth.models import User
from django.utils import timezone

from backups.management.commands.cleanup_old_backups import (
    Command, DeleteResult, get_backups_to_delete,
)
from backups.models import BackupRun, BackupTarget, RestoreRun
from backups.restore_engine import reserve_restore, start_restore, RestoreError

pytestmark = pytest.mark.django_db(transaction=True)
STORAGE = "backups.management.commands.cleanup_old_backups"


@pytest.fixture
def point():
    target = BackupTarget.objects.create(
        name="safety", service_name="safety.service", db_path="/tmp/safety.db",
        fastdeploy_service="echoport-backup", retention_days=7,
    )
    return BackupRun.objects.create(
        target=target, status="success", finished_at=timezone.now()-timedelta(days=10),
        storage_bucket="backups", storage_key="safety/archive", checksum_sha256="abc",
    )


@pytest.fixture
def age_only(settings):
    settings.ECHOPORT_RETENTION_MIN_SUCCESS = 0


@pytest.fixture
def file_sqlite(tmp_path):
    if connection.vendor != "sqlite":
        yield
        return
    from copy import deepcopy
    from django.db.backends.sqlite3.base import DatabaseWrapper
    original = connections["default"]
    original_config = connections.databases["default"]
    config = deepcopy(original.settings_dict)
    config["NAME"] = str(tmp_path / "coordination.sqlite3")
    connections.databases["default"] = config
    connections["default"] = DatabaseWrapper(config, "default")
    call_command("migrate", verbosity=0)
    try:
        yield
    finally:
        connections["default"].close()
        connections["default"] = original
        connections.databases["default"] = original_config


def other_connection(function):
    def run():
        try:
            return function()
        finally:
            connections.close_all()
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(run).result(timeout=10)


def test_floor_newest_three_with_ties_failures_and_incomplete(point):
    points = [point]
    for i in range(4):
        points.append(BackupRun.objects.create(
            target=point.target, status="success", finished_at=point.finished_at,
            storage_bucket="backups", storage_key=f"safety/{i}", checksum_sha256="abc",
        ))
    BackupRun.objects.create(target=point.target, status="failed", finished_at=timezone.now())
    BackupRun.objects.create(
        target=point.target, status="success", finished_at=point.finished_at,
    )
    assert {b.pk for b in get_backups_to_delete(point.target)} == {
        points[0].pk, points[1].pk, points[2].pk,
    }
    assert Command()._delete_backup(points[-1]) == DeleteResult.SKIPPED


def test_claim_rechecks_floor_after_listing(point, settings):
    settings.ECHOPORT_RETENTION_MIN_SUCCESS = 0
    assert get_backups_to_delete(point.target) == [point]
    settings.ECHOPORT_RETENTION_MIN_SUCCESS = 3
    with patch(f"{STORAGE}.delete_object") as delete:
        assert Command()._delete_backup(point) == DeleteResult.SKIPPED
        delete.assert_not_called()


@pytest.mark.parametrize("admission", ["ui", "cli", "engine"])
def test_dangerous_interval(file_sqlite, point, age_only, admission, fake_fastdeploy):
    """At storage entry the final check is over; another connection tries restore."""
    user = User.objects.create_user("staff", is_staff=True)
    stale = BackupRun.objects.select_related("target").get(pk=point.pk)
    attempted = []

    def attempt():
        if admission == "ui":
            client = Client()
            client.force_login(user)
            with patch("backups.views.threading.Thread") as thread:
                assert client.post(f"/runs/{point.pk}/restore/").status_code == 302
                thread.assert_not_called()
        elif admission == "cli":
            with pytest.raises(CommandError, match="pending deletion"):
                call_command("restore", point.target.name, str(point.pk))
        else:
            with pytest.raises(RestoreError, match="pending deletion"):
                start_restore(stale)
        assert not RestoreRun.objects.exists()
        attempted.append(True)

    def delete(bucket, key):
        assert not connection.in_atomic_block
        other_connection(attempt)
        return True

    with patch(f"{STORAGE}.list_prefix", return_value=[]), patch(f"{STORAGE}.delete_object", side_effect=delete):
        assert Command()._delete_backup(point) == DeleteResult.DELETED
    assert attempted == [True]
    assert not BackupRun.objects.filter(pk=point.pk).exists()
    assert fake_fastdeploy.started == []


@pytest.mark.parametrize("phase", ["before_storage", "after_storage", "ambiguous", "finalize"])
def test_intent_survives_interruption_and_retry(point, age_only, phase):
    class Crash(BaseException):
        pass

    def delete(bucket, key):
        assert (bucket, key) == (point.storage_bucket, point.storage_key)
        if phase == "after_storage":
            raise Crash()
        return phase != "ambiguous"

    with patch(f"{STORAGE}.list_prefix", return_value=[]), patch(f"{STORAGE}.delete_object", side_effect=delete):
        if phase == "before_storage":
            with patch.object(Command, "_delete_storage", side_effect=Crash()), pytest.raises(Crash):
                Command()._delete_backup(point)
        elif phase == "after_storage":
            with pytest.raises(Crash):
                Command()._delete_backup(point)
        elif phase == "finalize":
            with patch.object(BackupRun, "delete", side_effect=RuntimeError("DB failed")):
                assert Command()._delete_backup(point) == DeleteResult.ERROR
        else:
            assert Command()._delete_backup(point) == DeleteResult.ERROR
    point.refresh_from_db()
    intent = point.deletion_pending_at
    assert intent
    with pytest.raises(RestoreError, match="pending deletion"):
        reserve_restore(point.pk)
    assert point.target.get_last_successful_run() is None
    # Retry despite changed age/floor: never clear intent or resurrect availability.
    point.finished_at = timezone.now()
    point.save(update_fields=["finished_at"])
    assert get_backups_to_delete(point.target) == [point]
    with patch(f"{STORAGE}.list_prefix", return_value=[]), patch(f"{STORAGE}.delete_object", return_value=True) as retry:
        assert Command()._delete_backup(point) == DeleteResult.DELETED
        retry.assert_called_once_with(point.storage_bucket, point.storage_key)


def test_restore_first_blocks_cleanup(file_sqlite, point, age_only):
    reserved = other_connection(lambda: reserve_restore(point.pk))
    with patch(f"{STORAGE}.delete_object") as delete:
        assert Command()._delete_backup(point) == DeleteResult.SKIPPED
        delete.assert_not_called()
    assert reserved.status == "pending"


def test_sqlite_contention_has_no_storage_effect(file_sqlite, point, age_only):
    if connection.vendor != "sqlite":
        pytest.skip("SQLite writer reservation proof")
    from backups.reservations import lock_target
    def contend():
        connections["default"].settings_dict["OPTIONS"] = {"timeout": 0}
        with patch(f"{STORAGE}.delete_object") as delete:
            result = Command()._delete_backup(point)
            delete.assert_not_called()
        with pytest.raises(RestoreError, match="lock"):
            reserve_restore(point.pk)
        return result
    with transaction.atomic():
        lock_target(point.target_id)
        assert other_connection(contend) == DeleteResult.SKIPPED
    point.refresh_from_db()
    assert point.deletion_pending_at is None


def test_sqlite_stale_snapshot_fails_closed(file_sqlite, point, age_only):
    if connection.vendor != "sqlite":
        pytest.skip("SQLite snapshot upgrade proof")
    # WAL permits another writer to commit while this connection holds an old read.
    with connection.cursor() as cursor:
        cursor.execute("PRAGMA journal_mode=WAL")
    with transaction.atomic():
        BackupRun.objects.get(pk=point.pk)
        def mark():
            with transaction.atomic():
                from backups.reservations import lock_target
                lock_target(point.target_id)
                BackupRun.objects.filter(pk=point.pk).update(deletion_pending_at=timezone.now())
        other_connection(mark)
        with pytest.raises(RestoreError, match="lock"):
            reserve_restore(point.pk)
    assert not RestoreRun.objects.exists()


def test_marked_ui_hides_restore(point, client):
    User.objects.create_user("operator", password="pw", is_staff=True)
    client.login(username="operator", password="pw")
    point.deletion_pending_at = timezone.now()
    point.save(update_fields=["deletion_pending_at"])
    response = client.get(f"/runs/{point.pk}/")
    assert b"Deletion pending" in response.content
    assert b'id="restore-dialog"' not in response.content


@pytest.mark.parametrize("after_object", [False, True])
def test_process_crash_leaves_durable_intent(file_sqlite, point, age_only, tmp_path, after_object):
    if connection.vendor != "sqlite":
        pytest.skip("Disposable SQLite process-crash durability proof")
    import os
    from pathlib import Path
    import subprocess
    import sys

    object_file = tmp_path / "fake-object"
    object_file.write_text("archive")
    script = '''
import os, sys, django
from pathlib import Path
from unittest.mock import patch
os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings.test"
django.setup()
from django.conf import settings
from django.db import connections
settings.ECHOPORT_RETENTION_MIN_SUCCESS = 0
connections["default"].settings_dict["NAME"] = sys.argv[1]
from backups.models import BackupRun
from backups.management.commands.cleanup_old_backups import Command
point = BackupRun.objects.get(pk=int(sys.argv[2]))
def crash(*args):
    if sys.argv[4] == "True":
        Path(sys.argv[3]).unlink()
    os._exit(23)
with patch.object(Command, "_delete_storage", side_effect=crash):
    Command()._delete_backup(point)
'''
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([
        str(Path("src").resolve()), str(Path("src/django").resolve()),
    ]))
    result = subprocess.run([
        sys.executable, "-c", script, connection.settings_dict["NAME"], str(point.pk),
        str(object_file), str(after_object),
    ], env=env, timeout=15, capture_output=True, text=True)
    assert result.returncode == 23, result.stderr
    assert object_file.exists() is not after_object
    point.refresh_from_db()
    assert point.deletion_pending_at
    with pytest.raises(RestoreError, match="pending deletion"):
        reserve_restore(point.pk)
    with patch(f"{STORAGE}.list_prefix", return_value=[]), patch(f"{STORAGE}.delete_object", return_value=True):
        assert Command()._delete_backup(point) == DeleteResult.DELETED


def test_stale_coordinates_are_not_deleted(point, age_only):
    BackupRun.objects.filter(pk=point.pk).update(storage_key="safety/fresh-key")
    with patch(f"{STORAGE}.list_prefix", return_value=[]), patch(f"{STORAGE}.delete_object", return_value=True) as delete:
        assert Command()._delete_backup(point) == DeleteResult.DELETED
        delete.assert_called_once_with("backups", "safety/fresh-key")


def test_claim_rollback_never_calls_storage(point, age_only):
    with patch.object(BackupRun, "save", side_effect=RuntimeError("claim failed")), patch(f"{STORAGE}.delete_object") as delete:
        assert Command()._delete_backup(point) == DeleteResult.ERROR
        delete.assert_not_called()
    point.refresh_from_db()
    assert point.deletion_pending_at is None


def test_unexpected_reference_after_claim_keeps_intent(point, age_only):
    # Direct non-participating writer is outside admission guarantees, but PROTECT
    # and the final guard must preserve metadata for explicit reconciliation.
    def delete(*args):
        RestoreRun.objects.create(target=point.target, backup_run=point, status="failed")
        return True
    with patch(f"{STORAGE}.list_prefix", return_value=[]), patch(f"{STORAGE}.delete_object", side_effect=delete):
        assert Command()._delete_backup(point) == DeleteResult.ERROR
    point.refresh_from_db()
    assert point.deletion_pending_at


def test_finalization_lock_failure_is_error(point, age_only):
    from django.db import OperationalError
    with patch(f"{STORAGE}.list_prefix", return_value=[]), patch(f"{STORAGE}.delete_object", return_value=True), patch.object(BackupRun, "delete", side_effect=OperationalError("DB failed")):
        assert Command()._delete_backup(point) == DeleteResult.ERROR
    point.refresh_from_db()
    assert point.deletion_pending_at
