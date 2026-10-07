"""
Tests for the dashboard backup/restore views: access control, method
restrictions, preconditions and the background-thread hand-off.

Background threads are never started: threading.Thread in backups.views is
replaced, and on_commit callbacks are captured explicitly.
"""

from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from django.contrib.auth.models import User
from django.urls import reverse
from django.utils import timezone

from backups import views
from backups.models import (
    BackupRun,
    BackupRunStatus,
    BackupTarget,
    RestoreRun,
    RestoreRunStatus,
)

CHECKSUM = "e" * 64
LOGIN_URL = "/accounts/login/"
ADMIN_LOGIN_URL = "/admin/login/"


@pytest.fixture
def target(db):
    return BackupTarget.objects.create(
        name="view-target",
        fastdeploy_service="echoport-backup",
        service_name="view-target.service",
        db_path="/tmp/view.db",
        status="active",
    )


@pytest.fixture
def backup_run(target):
    return BackupRun.objects.create(
        target=target,
        status=BackupRunStatus.SUCCESS,
        storage_bucket="backups",
        storage_key="view-target/2026-01-01T00-00-00.tar.gz",
        checksum_sha256=CHECKSUM,
    )


@pytest.fixture
def user(db):
    return User.objects.create_user("viewer", "viewer@test.com", "pw")


@pytest.fixture
def staff(db):
    return User.objects.create_user("operator", "operator@test.com", "pw", is_staff=True)


@pytest.fixture
def user_client(client, user):
    client.force_login(user)
    return client


@pytest.fixture
def staff_client(client, staff):
    client.force_login(staff)
    return client


@pytest.fixture
def fake_thread():
    with patch("backups.views.threading.Thread") as thread_cls:
        yield thread_cls


def _restore(backup_run, status=RestoreRunStatus.PENDING):
    return RestoreRun.objects.create(
        backup_run=backup_run, target=backup_run.target, status=status
    )


# ---------------------------------------------------------------------------
# Access control and method restrictions
# ---------------------------------------------------------------------------


@pytest.mark.django_db
class TestAnonymousAccess:
    @pytest.mark.parametrize(
        "method,name,kwarg",
        [
            ("post", "trigger_backup", "target_id"),
            ("get", "backup_status", "target_id"),
            ("get", "restore_detail", "restore_id"),
            ("get", "restore_status", "restore_id"),
        ],
    )
    def test_login_required_views_redirect_to_login(self, client, backup_run, method, name, kwarg):
        obj_id = backup_run.target_id if kwarg == "target_id" else _restore(backup_run).id
        url = reverse(f"backups:{name}", kwargs={kwarg: obj_id})

        response = getattr(client, method)(url)

        assert response.status_code == 302
        assert response["Location"].startswith(f"{LOGIN_URL}?next=")
        assert BackupRun.objects.filter(status=BackupRunStatus.PENDING).count() == 0

    def test_trigger_restore_redirects_to_admin_login(self, client, backup_run, fake_thread):
        url = reverse("backups:trigger_restore", kwargs={"run_id": backup_run.id})

        response = client.post(url)

        assert response.status_code == 302
        assert response["Location"].startswith(f"{ADMIN_LOGIN_URL}?next=")
        assert RestoreRun.objects.count() == 0
        fake_thread.assert_not_called()


@pytest.mark.django_db
class TestMethodRestrictions:
    def test_trigger_backup_rejects_get(self, user_client, target):
        response = user_client.get(reverse("backups:trigger_backup", args=[target.id]))

        assert response.status_code == 405
        assert BackupRun.objects.count() == 0

    def test_trigger_restore_rejects_get(self, staff_client, backup_run):
        response = staff_client.get(reverse("backups:trigger_restore", args=[backup_run.id]))

        assert response.status_code == 405
        assert RestoreRun.objects.count() == 0


@pytest.mark.django_db
class TestNotFound:
    def test_trigger_backup_unknown_target(self, user_client):
        assert user_client.post(reverse("backups:trigger_backup", args=[999])).status_code == 404

    def test_backup_status_unknown_target(self, user_client):
        assert user_client.get(reverse("backups:backup_status", args=[999])).status_code == 404

    def test_trigger_restore_unknown_run(self, staff_client):
        assert staff_client.post(reverse("backups:trigger_restore", args=[999])).status_code == 404

    @pytest.mark.parametrize(
        "status", [BackupRunStatus.FAILED, BackupRunStatus.RUNNING, BackupRunStatus.TIMEOUT]
    )
    def test_trigger_restore_requires_successful_backup(self, staff_client, target, status):
        run = BackupRun.objects.create(target=target, status=status, checksum_sha256=CHECKSUM)

        response = staff_client.post(reverse("backups:trigger_restore", args=[run.id]))

        assert response.status_code == 404
        assert RestoreRun.objects.count() == 0

    def test_restore_detail_unknown(self, user_client):
        assert user_client.get(reverse("backups:restore_detail", args=[999])).status_code == 404

    def test_restore_status_unknown(self, user_client):
        assert user_client.get(reverse("backups:restore_status", args=[999])).status_code == 404


# ---------------------------------------------------------------------------
# trigger_backup / backup_status
# ---------------------------------------------------------------------------


@pytest.mark.django_db
class TestTriggerBackup:
    def test_creates_pending_run_and_starts_thread_on_commit(
        self, user_client, user, target, fake_thread, django_capture_on_commit_callbacks
    ):
        with django_capture_on_commit_callbacks(execute=True) as callbacks:
            response = user_client.post(reverse("backups:trigger_backup", args=[target.id]))

        assert response.status_code == 302
        assert response["Location"] == reverse("backups:dashboard")
        run = BackupRun.objects.get()
        assert run.status == BackupRunStatus.PENDING
        assert run.triggered_by == user.username
        assert run.trigger == "manual"
        assert run.storage_bucket == target.storage_bucket
        assert len(callbacks) == 1
        fake_thread.assert_called_once_with(
            target=views._run_backup_in_thread, args=(run.id,), daemon=True
        )
        fake_thread.return_value.start.assert_called_once_with()

    def test_htmx_request_renders_target_card(self, user_client, target, fake_thread):
        response = user_client.post(
            reverse("backups:trigger_backup", args=[target.id]), HTTP_HX_REQUEST="true"
        )

        assert response.status_code == 200
        assert response.templates[0].name == "backups/partials/target_card.html"
        assert response.context["target"].active_run == BackupRun.objects.get()

    @pytest.mark.parametrize("status", ["paused", "disabled"])
    def test_inactive_target_creates_no_run(self, user_client, target, fake_thread, status):
        BackupTarget.objects.filter(pk=target.pk).update(status=status)

        response = user_client.post(reverse("backups:trigger_backup", args=[target.id]))

        assert response.status_code == 302
        assert BackupRun.objects.count() == 0
        fake_thread.assert_not_called()

    def test_running_backup_blocks_second_backup(self, user_client, target, fake_thread):
        active = BackupRun.objects.create(target=target, status=BackupRunStatus.RUNNING)

        user_client.post(reverse("backups:trigger_backup", args=[target.id]))

        assert list(BackupRun.objects.all()) == [active]
        fake_thread.assert_not_called()

    def test_running_restore_blocks_backup(self, user_client, backup_run, fake_thread):
        _restore(backup_run, status=RestoreRunStatus.RUNNING)

        user_client.post(reverse("backups:trigger_backup", args=[backup_run.target_id]))

        assert list(BackupRun.objects.all()) == [backup_run]
        fake_thread.assert_not_called()

    def test_scheduling_failure_marks_run_failed(self, user_client, target):
        with patch("backups.views.transaction.on_commit", side_effect=RuntimeError("no hook")):
            response = user_client.post(reverse("backups:trigger_backup", args=[target.id]))

        assert response.status_code == 302
        run = BackupRun.objects.get()
        assert run.status == BackupRunStatus.FAILED
        assert "Failed to start backup thread: no hook" in run.error_message


@pytest.mark.django_db
class TestBackupStatus:
    def test_active_run_keeps_polling(self, user_client, target):
        BackupRun.objects.create(target=target, status=BackupRunStatus.RUNNING)

        response = user_client.get(reverse("backups:backup_status", args=[target.id]))

        assert response.status_code == 200
        assert response["HX-Trigger-After-Swap"] == "continuePolling"

    def test_idle_target_stops_polling(self, user_client, backup_run):
        response = user_client.get(reverse("backups:backup_status", args=[backup_run.target_id]))

        assert response.status_code == 200
        assert "HX-Trigger-After-Swap" not in response
        assert response.context["target"].last_success == backup_run


# ---------------------------------------------------------------------------
# trigger_restore / restore_detail / restore_status
# ---------------------------------------------------------------------------


@pytest.mark.django_db
class TestTriggerRestore:
    def test_non_staff_user_is_refused(self, user_client, backup_run, fake_thread):
        response = user_client.post(reverse("backups:trigger_restore", args=[backup_run.id]))

        assert response.status_code == 302
        assert response["Location"].startswith(f"{ADMIN_LOGIN_URL}?next=")
        assert RestoreRun.objects.count() == 0
        fake_thread.assert_not_called()

    def test_staff_creates_pending_restore_and_starts_thread_on_commit(
        self, staff_client, staff, backup_run, fake_thread, django_capture_on_commit_callbacks
    ):
        with django_capture_on_commit_callbacks(execute=True) as callbacks:
            response = staff_client.post(reverse("backups:trigger_restore", args=[backup_run.id]))

        restore = RestoreRun.objects.get()
        assert response.status_code == 302
        assert response["Location"] == reverse("backups:restore_detail", args=[restore.id])
        assert restore.status == RestoreRunStatus.PENDING
        assert restore.backup_run == backup_run
        assert restore.target == backup_run.target
        assert restore.triggered_by == staff.username
        assert len(callbacks) == 1
        fake_thread.assert_called_once_with(
            target=views._run_restore_in_thread, args=(restore.id,), daemon=True
        )
        fake_thread.return_value.start.assert_called_once_with()

    def test_missing_checksum_creates_no_restore(self, staff_client, backup_run, fake_thread):
        BackupRun.objects.filter(pk=backup_run.pk).update(checksum_sha256="")

        response = staff_client.post(reverse("backups:trigger_restore", args=[backup_run.id]))

        assert response["Location"] == reverse("backups:run_detail", args=[backup_run.id])
        assert RestoreRun.objects.count() == 0
        fake_thread.assert_not_called()

    def test_running_backup_blocks_restore(self, staff_client, backup_run, fake_thread):
        BackupRun.objects.create(target=backup_run.target, status=BackupRunStatus.RUNNING)

        response = staff_client.post(reverse("backups:trigger_restore", args=[backup_run.id]))

        assert response["Location"] == reverse("backups:run_detail", args=[backup_run.id])
        assert RestoreRun.objects.count() == 0
        fake_thread.assert_not_called()

    def test_running_restore_blocks_second_restore(self, staff_client, backup_run, fake_thread):
        active = _restore(backup_run, status=RestoreRunStatus.RUNNING)

        response = staff_client.post(reverse("backups:trigger_restore", args=[backup_run.id]))

        assert response["Location"] == reverse("backups:run_detail", args=[backup_run.id])
        assert list(RestoreRun.objects.all()) == [active]
        fake_thread.assert_not_called()

    def test_scheduling_failure_marks_restore_failed(self, staff_client, backup_run):
        with patch("backups.views.transaction.on_commit", side_effect=RuntimeError("no hook")):
            response = staff_client.post(reverse("backups:trigger_restore", args=[backup_run.id]))

        assert response["Location"] == reverse("backups:run_detail", args=[backup_run.id])
        restore = RestoreRun.objects.get()
        assert restore.status == RestoreRunStatus.FAILED
        assert "Failed to start restore thread: no hook" in restore.error_message


@pytest.mark.django_db
class TestRestoreDetailAndStatus:
    def test_restore_detail_renders(self, user_client, backup_run):
        restore = _restore(backup_run)

        response = user_client.get(reverse("backups:restore_detail", args=[restore.id]))

        assert response.status_code == 200
        assert response.context["restore"] == restore
        assert response.context["backup_run"] == backup_run
        assert response.context["target"] == backup_run.target

    def test_active_restore_keeps_polling(self, user_client, backup_run):
        restore = _restore(backup_run, status=RestoreRunStatus.RUNNING)

        response = user_client.get(reverse("backups:restore_status", args=[restore.id]))

        assert response.status_code == 200
        assert response["HX-Trigger-After-Swap"] == "continuePolling"

    @pytest.mark.parametrize(
        "status", [RestoreRunStatus.SUCCESS, RestoreRunStatus.FAILED, RestoreRunStatus.TIMEOUT]
    )
    def test_finished_restore_stops_polling(self, user_client, backup_run, status):
        restore = _restore(backup_run, status=status)

        response = user_client.get(reverse("backups:restore_status", args=[restore.id]))

        assert response.status_code == 200
        assert "HX-Trigger-After-Swap" not in response


# ---------------------------------------------------------------------------
# Read-only restore history on target and backup run pages
# ---------------------------------------------------------------------------


def _restore_at(backup_run, started_at, status=RestoreRunStatus.SUCCESS, **fields):
    return RestoreRun.objects.create(
        backup_run=backup_run,
        target=backup_run.target,
        status=status,
        started_at=started_at,
        **fields,
    )


def _successful_backup(target, key):
    return BackupRun.objects.create(
        target=target,
        status=BackupRunStatus.SUCCESS,
        storage_bucket="backups",
        storage_key=key,
        checksum_sha256=CHECKSUM,
    )


@pytest.mark.django_db
class TestRestoreHistory:
    @pytest.fixture
    def other_target(self, db):
        return BackupTarget.objects.create(
            name="other-target",
            fastdeploy_service="echoport-backup",
            service_name="other-target.service",
            db_path="/tmp/other.db",
            status="active",
        )

    def test_target_detail_lists_own_restores_newest_first(
        self, user_client, target, backup_run, other_target
    ):
        now = timezone.now()
        second_backup = _successful_backup(target, "view-target/second.tar.gz")
        older = _restore_at(
            backup_run,
            now - timedelta(days=2),
            status=RestoreRunStatus.FAILED,
            triggered_by="operator",
        )
        newer = _restore_at(
            second_backup,
            now - timedelta(hours=1),
            finished_at=now - timedelta(minutes=58),
        )
        foreign = _restore_at(
            _successful_backup(other_target, "other-target/x.tar.gz"),
            now,
        )

        response = user_client.get(reverse("backups:target_detail", args=[target.id]))

        assert response.status_code == 200
        assert list(response.context["restores"]) == [newer, older]
        html = response.content.decode()
        assert "Restore History" in html
        newer_link = reverse("backups:restore_detail", args=[newer.id])
        older_link = reverse("backups:restore_detail", args=[older.id])
        assert newer_link in html and older_link in html
        assert html.index(newer_link) < html.index(older_link)
        assert reverse("backups:restore_detail", args=[foreign.id]) not in html
        # Source backup links, status, trigger user and duration are shown.
        assert reverse("backups:run_detail", args=[second_backup.id]) in html
        assert "Failed" in html
        assert "(operator)" in html
        assert "120.0s" in html

    def test_target_detail_without_restores_shows_empty_state(self, user_client, target):
        response = user_client.get(reverse("backups:target_detail", args=[target.id]))

        assert response.status_code == 200
        assert "No restores yet." in response.content.decode()

    def test_target_detail_offers_no_restore_action(self, staff_client, backup_run):
        _restore_at(backup_run, timezone.now())

        response = staff_client.get(reverse("backups:target_detail", args=[backup_run.target.id]))

        html = response.content.decode()
        assert reverse("backups:trigger_restore", args=[backup_run.id]) not in html

    def test_target_detail_caps_restore_list(self, user_client, backup_run):
        now = timezone.now()
        limit = views.RESTORE_HISTORY_LIMIT
        for i in range(limit + 3):
            _restore_at(backup_run, now - timedelta(minutes=i))

        response = user_client.get(reverse("backups:target_detail", args=[backup_run.target.id]))

        assert len(response.context["restores"]) == limit
        assert f"Showing the {limit} most recent restores." in response.content.decode()

    def test_target_detail_query_count_is_bounded(
        self, user_client, backup_run, django_assert_max_num_queries
    ):
        now = timezone.now()
        for i in range(10):
            backup = _successful_backup(backup_run.target, f"view-target/{i}.tar.gz")
            _restore_at(backup, now - timedelta(minutes=i))
        url = reverse("backups:target_detail", args=[backup_run.target.id])

        with django_assert_max_num_queries(10):
            response = user_client.get(url)

        assert len(response.context["restores"]) == 10

    def test_run_detail_lists_only_restores_of_that_backup(
        self, user_client, target, backup_run
    ):
        now = timezone.now()
        other_backup = _successful_backup(target, "view-target/other.tar.gz")
        older = _restore_at(backup_run, now - timedelta(days=1))
        newer = _restore_at(backup_run, now, status=RestoreRunStatus.RUNNING)
        unrelated = _restore_at(other_backup, now - timedelta(hours=2))

        response = user_client.get(reverse("backups:run_detail", args=[backup_run.id]))

        assert response.status_code == 200
        assert list(response.context["restores"]) == [newer, older]
        html = response.content.decode()
        assert "Restores from this Backup" in html
        assert reverse("backups:restore_detail", args=[newer.id]) in html
        assert reverse("backups:restore_detail", args=[unrelated.id]) not in html

    def test_run_detail_without_restores_shows_empty_state(self, user_client, backup_run):
        response = user_client.get(reverse("backups:run_detail", args=[backup_run.id]))

        assert "No restores from this backup." in response.content.decode()

    def test_failed_backup_run_hides_empty_restore_section(self, user_client, target):
        failed = BackupRun.objects.create(target=target, status=BackupRunStatus.FAILED)

        response = user_client.get(reverse("backups:run_detail", args=[failed.id]))

        assert response.status_code == 200
        assert "Restores from this Backup" not in response.content.decode()

    def test_run_detail_query_count_is_bounded(
        self, user_client, backup_run, django_assert_max_num_queries
    ):
        now = timezone.now()
        for i in range(10):
            _restore_at(backup_run, now - timedelta(minutes=i))
        url = reverse("backups:run_detail", args=[backup_run.id])

        with django_assert_max_num_queries(10):
            response = user_client.get(url)

        assert len(response.context["restores"]) == 10

    def test_history_pages_require_login(self, client, backup_run):
        for url in (
            reverse("backups:target_detail", args=[backup_run.target.id]),
            reverse("backups:run_detail", args=[backup_run.id]),
        ):
            response = client.get(url)
            assert response.status_code == 302
            assert response["Location"].startswith(LOGIN_URL)


# ---------------------------------------------------------------------------
# Background thread bodies
# ---------------------------------------------------------------------------


@pytest.mark.django_db
class TestThreadBodies:
    def test_backup_thread_continues_existing_run(self, target):
        run = BackupRun.objects.create(target=target, status=BackupRunStatus.PENDING)

        with patch("backups.views.start_backup") as start:
            views._run_backup_in_thread(run.id)

        start.assert_called_once()
        assert start.call_args.args == (target,)
        assert start.call_args.kwargs == {"existing_run": run}

    def test_backup_thread_missing_run_is_a_noop(self, db):
        with patch("backups.views.start_backup") as start:
            views._run_backup_in_thread(999)

        start.assert_not_called()

    def test_backup_thread_swallows_engine_errors(self, target):
        run = BackupRun.objects.create(target=target, status=BackupRunStatus.PENDING)

        with patch("backups.views.start_backup", side_effect=RuntimeError("engine down")):
            views._run_backup_in_thread(run.id)  # must not raise

    def test_restore_thread_continues_existing_run(self, backup_run):
        restore = _restore(backup_run)

        with patch("backups.views.start_restore") as start:
            views._run_restore_in_thread(restore.id)

        start.assert_called_once()
        assert start.call_args.args == (backup_run,)
        assert start.call_args.kwargs == {"existing_run": restore}

    def test_restore_thread_missing_run_is_a_noop(self, db):
        with patch("backups.views.start_restore") as start:
            views._run_restore_in_thread(999)

        start.assert_not_called()

    def test_restore_thread_swallows_engine_errors(self, backup_run):
        restore = _restore(backup_run)
        failing = MagicMock(side_effect=RuntimeError("engine down"))

        with patch("backups.views.start_restore", failing):
            views._run_restore_in_thread(restore.id)  # must not raise

        failing.assert_called_once()
