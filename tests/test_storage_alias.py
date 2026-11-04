"""A target's object store for retention: BackupTarget.storage_alias and set_storage_alias."""

import json
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.utils import timezone

from backups import minio_client
from backups.management.commands.cleanup_old_backups import (
    Command,
    DeleteResult,
    acquire_cleanup_lock,
    release_cleanup_lock,
)
from backups.models import STORAGE_ALIAS_PAUSED, BackupRun, BackupTarget

STORAGE = "backups.management.commands.cleanup_old_backups"
NOT_FOUND = json.dumps(
    {
        "status": "error",
        "error": {
            "message": "Failed to remove `rustfs/backups/x`.",
            "cause": {"message": "Object does not exist", "error": {}},
            "type": "error",
        },
    }
)


@pytest.fixture(autouse=True)
def isolated_lock(settings, tmp_path):
    # The cleanup lock file lives in the cache dir, never the shared /tmp one.
    settings.ECHOPORT_CACHE_DIR = str(tmp_path)
    settings.ECHOPORT_RETENTION_MIN_SUCCESS = 0


def make_target(name, **fields):
    return BackupTarget.objects.create(
        name=name,
        service_name=f"{name}.service",
        db_path=f"/tmp/{name}.db",
        fastdeploy_service="echoport-backup",
        retention_days=7,
        **fields,
    )


def expired_run(target, key=None):
    return BackupRun.objects.create(
        target=target,
        status="success",
        finished_at=timezone.now() - timedelta(days=10),
        storage_bucket="backups",
        storage_key=key or f"{target.name}/archive.tar.gz",
    )


def set_alias(target, alias):
    """Bypass the barrier and validation, as a stray writer would."""
    BackupTarget.objects.filter(pk=target.pk).update(storage_alias=alias)
    target.refresh_from_db()


def cleanup(*args):
    with pytest.raises(SystemExit) as exit_info:
        call_command("cleanup_old_backups", *args)
    return exit_info.value.code


# ---------------------------------------------------------------------------
# Field, default and validation
# ---------------------------------------------------------------------------


@pytest.mark.django_db
class TestField:
    def test_new_targets_use_minio_alias(self, settings):
        assert make_target("a").storage_alias == "minio"
        settings.MINIO_ALIAS = "rustfs"
        assert make_target("b").storage_alias == "rustfs"

    def test_only_allowlisted_or_empty_aliases_save(self, settings):
        target = make_target("a")
        for alias in ("rustfs", STORAGE_ALIAS_PAUSED, "minio"):
            target.storage_alias = alias
            target.save()
        target.storage_alias = "backup-a"
        with pytest.raises(ValidationError, match="not one of MINIO_ALIASES"):
            target.save()
        settings.MINIO_ALIASES = ["minio"]
        target.storage_alias = "rustfs"
        with pytest.raises(ValidationError, match="not one of MINIO_ALIASES"):
            target.save()

    def test_an_ordinary_save_never_writes_it_back(self, rf):
        from django.contrib import admin
        from django.contrib.auth.models import User

        target = make_target("a")
        stale = BackupTarget.objects.get(pk=target.pk)
        call_command("set_storage_alias", "a", "paused")
        stale.description = "edited meanwhile"
        stale.save()
        model_admin = admin.site._registry[BackupTarget]
        request = rf.post("/")
        request.user = User(is_staff=True, is_superuser=True)
        stale.icon = "x"
        model_admin.save_model(request, stale, form=None, change=True)
        target.refresh_from_db()
        assert (target.storage_alias, target.description, target.icon) == (STORAGE_ALIAS_PAUSED, "edited meanwhile", "x")
        with pytest.raises(ValueError, match="set_storage_alias"):
            stale.save(update_fields=["storage_alias"])
        # A new target is created with its alias.
        assert make_target("b", storage_alias="rustfs").storage_alias == "rustfs"
        assert BackupTarget.objects.get(name="b").storage_alias == "rustfs"

    def test_admin_shows_it_read_only(self):
        from django.contrib import admin

        model_admin = admin.site._registry[BackupTarget]
        assert "storage_alias" in model_admin.readonly_fields
        assert "storage_alias" not in model_admin.get_form(None)().fields


@pytest.mark.django_db(transaction=True)
class TestMigration:
    def test_existing_targets_get_minio_alias(self, settings):
        executor = MigrationExecutor(connection)
        executor.migrate([("backups", "0011_backuprun_deletion_pending_at")])
        old = executor.loader.project_state(("backups", "0011_backuprun_deletion_pending_at")).apps
        old.get_model("backups", "BackupTarget").objects.create(
            name="legacy", fastdeploy_service="echoport-backup", db_path="/tmp/legacy.db"
        )
        try:
            executor = MigrationExecutor(connection)
            executor.migrate([("backups", "0012_backuptarget_storage_alias")])
            assert BackupTarget.objects.get(name="legacy").storage_alias == "minio"
        finally:
            BackupTarget.objects.all().delete()
            leaf = MigrationExecutor(connection).loader.graph.leaf_nodes("backups")
            MigrationExecutor(connection).migrate(leaf)


# ---------------------------------------------------------------------------
# minio_client: the alias names the store
# ---------------------------------------------------------------------------


class TestClientAlias:
    @pytest.mark.parametrize("alias", ["minio", "rustfs"])
    @patch("backups.minio_client.subprocess.run")
    def test_each_call_goes_through_the_given_alias(self, run, alias):
        run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        assert minio_client.delete_object("backups", "t/k.tar.gz", alias=alias)
        assert run.call_args[0][0][-1] == f"{alias}/backups/t/k.tar.gz"
        assert minio_client.list_prefix("backups", "t/run", alias=alias) == []
        assert run.call_args[0][0][-1] == f"{alias}/backups/t/run/"
        assert minio_client.delete_prefix("backups", "t/run", alias=alias)
        commands = [call[0][0] for call in run.call_args_list[-2:]]
        assert commands[0][1:4] == ["rm", "--recursive", "--force"]
        assert [command[-1] for command in commands] == [f"{alias}/backups/t/run/"] * 2

    @pytest.mark.parametrize("alias", ["backup-t", "", "local", "minio/backups"])
    @patch("backups.minio_client.subprocess.run")
    def test_an_alias_outside_the_allowlist_never_reaches_mc(self, run, alias):
        with patch.object(minio_client, "logger") as logger:
            assert minio_client.delete_object("backups", "t/k.tar.gz", alias=alias) is False
            assert minio_client.list_prefix("backups", "t/run", alias=alias) is None
            assert minio_client.delete_prefix("backups", "t/run", alias=alias) is False
        run.assert_not_called()
        assert all("not one of MINIO_ALIASES" in call.args[0] for call in logger.error.call_args_list)
        assert len(logger.error.call_args_list) == 3

    @patch("backups.minio_client.subprocess.run")
    def test_not_found_on_the_named_store_counts_as_deleted(self, run):
        run.return_value = MagicMock(returncode=1, stdout=NOT_FOUND, stderr="")
        assert minio_client.delete_object("backups", "x", alias="rustfs") is True


# ---------------------------------------------------------------------------
# cleanup_old_backups per target store
# ---------------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
class TestCleanupPerStore:
    def test_each_target_deletes_through_its_own_alias(self):
        on_minio = expired_run(make_target("old"))
        moved = make_target("moved")
        set_alias(moved, "rustfs")
        on_rustfs = expired_run(moved)
        with patch(f"{STORAGE}.list_prefix", return_value=[]) as listing, patch(
            f"{STORAGE}.delete_object", return_value=True
        ) as delete:
            assert cleanup() == 0
        assert sorted(call.kwargs["alias"] for call in listing.call_args_list) == ["minio", "rustfs"]
        assert sorted((call.args[1], call.kwargs["alias"]) for call in delete.call_args_list) == [
            (on_rustfs.storage_key, "rustfs"),
            (on_minio.storage_key, "minio"),
        ]
        assert not BackupRun.objects.exists()

    def test_a_prefix_is_listed_and_deleted_on_the_targets_store(self):
        target = make_target("media")
        set_alias(target, "rustfs")
        run = expired_run(target, key="media/2026-10-01T02-00-00")
        with patch(f"{STORAGE}.list_prefix", return_value=["manifest.json"]) as listing, patch(
            f"{STORAGE}.delete_prefix", return_value=True
        ) as delete:
            assert cleanup() == 0
        listing.assert_called_once_with("backups", run.storage_key, alias="rustfs")
        delete.assert_called_once_with("backups", run.storage_key, alias="rustfs")

    @patch("backups.minio_client.subprocess.run")
    def test_object_missing_on_the_targets_store_drops_the_row(self, run):
        target = make_target("moved")
        set_alias(target, "rustfs")
        expired_run(target)
        listed = MagicMock(returncode=0, stdout="", stderr="")
        missing = MagicMock(returncode=1, stdout=NOT_FOUND, stderr="")
        run.side_effect = [listed, missing]
        assert cleanup() == 0
        assert [call[0][0][-1] for call in run.call_args_list] == [
            "rustfs/backups/moved/archive.tar.gz/",
            "rustfs/backups/moved/archive.tar.gz",
        ]
        assert not BackupRun.objects.exists()

    @pytest.mark.parametrize("dry_run", [False, True])
    def test_a_paused_target_is_skipped_without_intent(self, dry_run, capsys):
        target = make_target("moving")
        set_alias(target, STORAGE_ALIAS_PAUSED)
        run = expired_run(target)
        other = expired_run(make_target("other"))
        with patch(f"{STORAGE}.list_prefix", return_value=[]) as listing, patch(
            f"{STORAGE}.delete_object", return_value=True
        ) as delete:
            assert cleanup(*(["--dry-run"] if dry_run else [])) == 0
        out = capsys.readouterr().out
        assert "1 expired backup(s) of 'moving'" in out and "paused" in out
        run.refresh_from_db()
        assert run.deletion_pending_at is None
        if dry_run:
            delete.assert_not_called()
        else:
            assert [call.args[1] for call in delete.call_args_list] == [other.storage_key]
            assert all(call.kwargs["alias"] == "minio" for call in listing.call_args_list)
            assert not BackupRun.objects.filter(pk=other.pk).exists()

    def test_a_paused_target_named_with_target_is_skipped(self, capsys):
        target = make_target("moving")
        set_alias(target, STORAGE_ALIAS_PAUSED)
        expired_run(target)
        with patch(f"{STORAGE}.delete_object") as delete:
            assert cleanup("--target", "moving") == 0
        delete.assert_not_called()
        assert BackupRun.objects.filter(target=target).exists()

    def test_an_unknown_alias_fails_closed(self, capsys):
        target = make_target("odd")
        set_alias(target, "backup-odd")
        run = expired_run(target)
        with patch("backups.minio_client.subprocess.run") as mc:
            assert cleanup() == 1
        mc.assert_not_called()
        assert "not one of MINIO_ALIASES" in capsys.readouterr().err
        run.refresh_from_db()
        assert run.deletion_pending_at is None

    def test_the_locked_row_decides_not_the_listing(self):
        # Paused after the target was listed: no intent, no storage call.
        target = make_target("moving")
        run = expired_run(target)
        set_alias(target, STORAGE_ALIAS_PAUSED)
        stale = BackupRun.objects.get(pk=run.pk)
        with patch(f"{STORAGE}.delete_object") as delete:
            assert Command()._delete_backup(stale) == DeleteResult.SKIPPED
        delete.assert_not_called()
        run.refresh_from_db()
        assert run.deletion_pending_at is None
        set_alias(target, "backup-moving")
        with patch(f"{STORAGE}.delete_object") as delete:
            assert Command()._delete_backup(stale) == DeleteResult.ERROR
        delete.assert_not_called()

    def test_an_alias_change_during_the_storage_delete_keeps_the_row(self):
        target = make_target("flip")
        run = expired_run(target)

        def delete(bucket, key, *, alias):
            assert alias == "minio"
            set_alias(target, "rustfs")  # a writer that ignored the barrier
            return True

        with patch(f"{STORAGE}.list_prefix", return_value=[]), patch(f"{STORAGE}.delete_object", side_effect=delete):
            assert Command()._delete_backup(run) == DeleteResult.ERROR
        run.refresh_from_db()
        assert run.deletion_pending_at is not None


# ---------------------------------------------------------------------------
# set_storage_alias: the barrier
# ---------------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
class TestSetStorageAlias:
    def test_switch_pause_and_back(self, capsys):
        target = make_target("nyxmon")
        call_command("set_storage_alias", "nyxmon", "paused")
        target.refresh_from_db()
        assert target.storage_alias == STORAGE_ALIAS_PAUSED
        assert "minio -> paused" in capsys.readouterr().out
        call_command("set_storage_alias", "nyxmon", "rustfs")
        target.refresh_from_db()
        assert target.storage_alias == "rustfs"
        assert "paused -> rustfs" in capsys.readouterr().out
        call_command("set_storage_alias", "nyxmon", "rustfs")
        assert "already rustfs" in capsys.readouterr().out

    @pytest.mark.parametrize("alias", ["backup-nyxmon", "", "PAUSED", "local"])
    def test_unknown_values_are_refused(self, alias):
        target = make_target("nyxmon")
        with pytest.raises(CommandError):
            call_command("set_storage_alias", "nyxmon", alias)
        target.refresh_from_db()
        assert target.storage_alias == "minio"

    def test_unknown_target_is_refused(self):
        make_target("nyxmon")
        with pytest.raises(CommandError, match="Target not found: Nyxmon"):
            call_command("set_storage_alias", "Nyxmon", "rustfs")

    def test_a_pending_deletion_blocks_the_change(self):
        target = make_target("nyxmon")
        run = expired_run(target)
        BackupRun.objects.filter(pk=run.pk).update(deletion_pending_at=timezone.now())
        with pytest.raises(CommandError, match=f"Deletion of backup\\(s\\) {run.pk} of 'nyxmon' is pending"):
            call_command("set_storage_alias", "nyxmon", "paused")
        target.refresh_from_db()
        assert target.storage_alias == "minio"
        # The next cleanup finishes it through the old alias; then the change goes through.
        with patch(f"{STORAGE}.list_prefix", return_value=[]), patch(
            f"{STORAGE}.delete_object", return_value=True
        ) as delete:
            assert cleanup("--target", "nyxmon") == 0
        assert delete.call_args.kwargs["alias"] == "minio"
        call_command("set_storage_alias", "nyxmon", "paused")
        target.refresh_from_db()
        assert target.storage_alias == STORAGE_ALIAS_PAUSED

    def test_a_running_cleanup_blocks_the_change(self):
        target = make_target("nyxmon")
        held = acquire_cleanup_lock()
        assert held is not None
        try:
            with pytest.raises(CommandError, match="cleanup_old_backups is running"):
                call_command("set_storage_alias", "nyxmon", "rustfs")
        finally:
            release_cleanup_lock(held)
        target.refresh_from_db()
        assert target.storage_alias == "minio"
        call_command("set_storage_alias", "nyxmon", "rustfs")

    def test_cleanup_cannot_start_while_the_change_holds_the_lock(self):
        make_target("nyxmon")
        seen = []

        def update(*args, **kwargs):
            seen.append(acquire_cleanup_lock())
            return 1

        with patch("django.db.models.query.QuerySet.update", side_effect=update):
            call_command("set_storage_alias", "nyxmon", "rustfs")
        assert seen and all(lock is None for lock in seen)
