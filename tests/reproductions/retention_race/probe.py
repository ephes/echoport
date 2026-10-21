"""Actual baseline-only SQLite race; no live settings/storage/deployment."""

import argparse
import hashlib
import json
import socket
import sys
import tempfile
import threading
from datetime import timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def probe(pathway, ordering):
    pins = json.loads(Path(__file__).with_name("baseline.json").read_text())
    for name, checksum in pins["sha256"].items():
        require(
            hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == checksum,
            "actual baseline source drift",
        )
    with tempfile.TemporaryDirectory(
        prefix="echoport-retention-counterexample-"
    ) as owned:
        directory = Path(owned)
        db_path = directory / "disposable.sqlite"
        from django.conf import settings

        # Configure without importing project settings: no .env or live DB path.
        settings.configure(
            SECRET_KEY="invented-reproduction-only",
            INSTALLED_APPS=[
                "django.contrib.auth",
                "django.contrib.contenttypes",
                "backups",
            ],
            DATABASES={
                "default": {
                    "ENGINE": "django.db.backends.sqlite3",
                    "NAME": str(db_path),
                    "OPTIONS": {"timeout": 2},
                }
            },
            USE_TZ=True,
            ROOT_URLCONF="retention_fixture_urls",
            DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
            ECHOPORT_ALLOWED_PATH_PREFIXES=["/tmp/"],
        )
        import django

        django.setup()
        from django.core.management import call_command
        from django.db import connections
        from django.http import Http404
        from django.test import RequestFactory
        from django.urls import include, path
        from django.utils import timezone

        from backups import restore_engine, views
        from backups.management.commands import cleanup_old_backups as cleanup
        from backups.models import BackupRun, BackupRunStatus, BackupTarget, RestoreRun

        for module in (views, restore_engine, cleanup):
            require(
                Path(module.__file__).resolve().is_relative_to(ROOT / "src"),
                "wrong source checkout imported",
            )
        urls = ModuleType("retention_fixture_urls")
        urls.urlpatterns = [path("", include("backups.urls", namespace="backups"))]
        sys.modules[urls.__name__] = urls
        call_command("migrate", verbosity=0, interactive=False)
        require(db_path.is_file(), "file-backed SQLite required")
        require(
            connections["default"].get_autocommit(), "outer test transaction forbidden"
        )
        target = BackupTarget.objects.create(
            name="synthetic-race",
            fastdeploy_service="synthetic-offline",
            service_name="synthetic.service",
            db_path="/tmp/synthetic-unused.sqlite",
            retention_days=7,
        )
        backup = BackupRun.objects.create(
            target=target,
            status=BackupRunStatus.SUCCESS,
            storage_bucket="invented-bucket",
            storage_key="invented-object",
            checksum_sha256="a" * 64,
            size_bytes=10,
            finished_at=timezone.now() - timedelta(days=10),
        )
        backup_id = backup.pk
        require(
            [row.pk for row in cleanup.get_backups_to_delete(target)] == [backup_id],
            "actual initial retention eligibility required",
        )
        entered_delete = threading.Event()
        permit_delete = threading.Event()
        reserved = threading.Event()
        finish_direct = threading.Event()
        trace = []
        results = {}
        native_connections = {}
        connection_lock = threading.Lock()
        fake_storage = {("invented-bucket", "invented-object"): b"fictional"}
        threads = []
        external_attempts = []

        def record_connection(name):
            connection = connections["default"]
            connection.ensure_connection()
            with connection_lock:
                native_connections[name] = (
                    connection.connection
                )  # Keep objects alive; no ID reuse.
            require(
                connection.get_autocommit(), "thread transaction must be autocommit"
            )

        def fake_delete(bucket, key):
            require(
                (bucket, key) == ("invented-bucket", "invented-object"),
                "unexpected fake object",
            )
            trace.append("cleanup-at-delete-entry-after-reference-check")
            entered_delete.set()
            require(permit_delete.wait(5), "explicit cleanup barrier timed out")
            fake_storage.pop((bucket, key), None)
            trace.append("fake-object-deleted")
            return True

        def forbidden(*_args, **_kwargs):
            external_attempts.append("blocked")
            raise AssertionError("external network/deployment forbidden")

        class SuppressedThread:
            def __init__(self, target, args, daemon):
                require(
                    target is views._run_restore_in_thread and daemon,
                    "unexpected UI worker",
                )
                self.args = args

            def start(self):
                # UI's real on_commit callback executes; only worker is suppressed.
                require(
                    RestoreRun.objects.filter(
                        pk=self.args[0], status="pending"
                    ).exists(),
                    "UI reservation not committed",
                )
                trace.append("ui-worker-suppressed-after-committed-pending")

        def fake_config(*_args, **_kwargs):
            # This actual direct path boundary follows PENDING reservation commit.
            require(
                RestoreRun.objects.filter(
                    backup_run_id=backup_id, status="pending"
                ).exists(),
                "direct reservation not committed",
            )
            trace.append("direct-committed-pending-before-fastdeploy")
            reserved.set()
            require(finish_direct.wait(5), "direct completion barrier timed out")
            raise restore_engine.EndpointConfigError("synthetic stop before deployment")

        def cleanup_worker():
            try:
                record_connection("cleanup")
                row = BackupRun.objects.select_related("target").get(pk=backup_id)
                results["cleanup"] = cleanup.Command()._delete_backup(row).value
            except BaseException as error:  # noqa: BLE001 - parent asserts recorded worker failures
                results["cleanup_error"] = type(error).__name__
            finally:
                connections.close_all()

        def direct_worker():
            try:
                record_connection("restore")
                # Intentionally use stale, previously loaded actual baseline object.
                restore_engine.start_restore(backup, triggered_by="invented-staff")
                results["direct"] = "unexpected-success"
            except restore_engine.RestoreError as error:
                results["direct"] = type(error).__name__
            except BaseException as error:  # noqa: BLE001 - parent asserts recorded worker failures
                results["direct_error"] = type(error).__name__
            finally:
                connections.close_all()

        def start_cleanup():
            thread = threading.Thread(target=cleanup_worker)
            threads.append(thread)
            thread.start()

        def start_restore():
            if pathway == "direct":
                thread = threading.Thread(target=direct_worker)
                threads.append(thread)
                thread.start()
            else:
                record_connection("restore")
                request = RequestFactory().post(f"/runs/{backup_id}/restore/")
                request.user = SimpleNamespace(
                    is_active=True,
                    is_staff=True,
                    is_authenticated=True,
                    username="invented-staff",
                )
                try:
                    response = views.trigger_restore(request, backup_id)
                    results["ui_http"] = response.status_code
                    if RestoreRun.objects.filter(
                        backup_run_id=backup_id, status="pending"
                    ).exists():
                        trace.append("ui-committed-pending")
                        reserved.set()
                except Http404:
                    results["ui_http"] = 404

        def join_cleanup():
            threads[0 if ordering != "restore-first" or pathway == "ui" else 1].join(5)
            require("cleanup" in results, "cleanup did not finish normally")

        with (
            patch.object(cleanup, "delete_object", fake_delete),
            patch.object(views, "threading", SimpleNamespace(Thread=SuppressedThread)),
            patch.object(restore_engine, "get_fastdeploy_config", fake_config),
            patch.object(restore_engine, "FastDeployClient", forbidden),
            patch.object(socket, "create_connection", forbidden),
        ):
            try:
                if ordering == "restore-first":
                    start_restore()
                    require(reserved.wait(5), "reservation barrier timed out")
                    start_cleanup()
                    join_cleanup()
                    require(
                        not entered_delete.is_set(), "reference should prevent deletion"
                    )
                else:
                    start_cleanup()
                    require(entered_delete.wait(5), "cleanup entry barrier timed out")
                    if ordering == "race":
                        start_restore()
                        require(reserved.wait(5), "reservation barrier timed out")
                        require(
                            RestoreRun.objects.filter(
                                backup_run_id=backup_id, status="pending"
                            ).exists(),
                            "pending reference not visible on observer connection",
                        )
                        trace.append("observer-sees-committed-pending-before-delete")
                    permit_delete.set()
                    join_cleanup()
                    if ordering == "cleanup-first":
                        start_restore()
                finish_direct.set()
            finally:
                permit_delete.set()
                finish_direct.set()
                for thread in threads:
                    thread.join(5)
                require(
                    not any(thread.is_alive() for thread in threads),
                    "owned thread survived",
                )

        require(not external_attempts, "unexpected external attempt")
        require(
            "cleanup_error" not in results and "direct_error" not in results,
            "unexpected worker failure",
        )
        require(
            len(set(native_connections.values())) == 2,
            "separate SQLite connections required",
        )
        row = BackupRun.objects.filter(pk=backup_id).first()
        refs = list(
            RestoreRun.objects.filter(backup_run_id=backup_id).values_list(
                "status", flat=True
            )
        )
        present = ("invented-bucket", "invented-object") in fake_storage
        summary = {
            "baseline_revision": pins["revision"],
            "pathway": pathway,
            "ordering": ordering,
            "file_backed_sqlite": True,
            "separate_connections": True,
            "events": trace,
            "cleanup_result": results["cleanup"],
            "backup_retained": row is not None,
            "backup_status": row.status if row else None,
            "restore_statuses": refs,
            "fake_object_present": present,
            "dangling_metadata": row is not None and bool(refs) and not present,
            "external_calls": len(external_attempts),
            "owned_thread_survivors": 0,
        }
        if ordering == "race":
            require(
                summary["dangling_metadata"] and results["cleanup"] == "error",
                "race falsified or not reproduced",
            )
        elif ordering == "restore-first":
            require(
                present
                and row is not None
                and bool(refs)
                and results["cleanup"] == "skipped",
                "restore-first control failed",
            )
        else:
            require(
                not present
                and row is None
                and not refs
                and results["cleanup"] == "deleted",
                "cleanup-first control failed",
            )
        connections.close_all()
        return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pathway", choices=["ui", "direct"], required=True)
    parser.add_argument(
        "--ordering", choices=["race", "restore-first", "cleanup-first"], required=True
    )
    args = parser.parse_args()
    print(json.dumps(probe(args.pathway, args.ordering), sort_keys=True))


if __name__ == "__main__":
    main()
