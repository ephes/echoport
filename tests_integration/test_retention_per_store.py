"""Retention with one mc alias per store: MinIO and RustFS side by side.

One MinIO and one RustFS container on an internal Docker network and one mc
container whose configuration holds both aliases, ``minio`` and ``rustfs``,
each with a retention account like production's ``echoport-retention``.
``cleanup_old_backups`` then runs against real mc for targets on either
store: it deletes through the target's alias only, skips a paused target,
counts an object missing on the target's store as deleted, and fails closed
for an allowlisted alias mc does not have.

Not part of ``just test`` (needs Docker): ``just test-s3-backends``.
Containers are named ``echoport-s3-backend-neutral-*`` and removed afterwards.
"""

from __future__ import annotations

import json
import shutil
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import pytest
from django.core.management import call_command
from django.utils import timezone

from backups.models import STORAGE_ALIAS_PAUSED, BackupRun, BackupTarget

from test_minio_client_backends import (
    BUCKET,
    NAME_PREFIX,
    RETENTION_POLICY,
    RETENTION_SECRET,
    RETENTION_USER,
    ROOT_PASSWORD,
    ROOT_USER,
    STORES,
    client_image,
    docker,
)


@dataclass
class Stores:
    client: str

    def mc(self, *args: str, stdin: str | None = None, check: bool = True):
        return docker("exec", "-i", self.client, "mc", *args, stdin=stdin, check=check)

    def put(self, store: str, key: str, content: str = "data") -> None:
        self.mc("pipe", f"admin-{store}/{BUCKET}/{key}", stdin=content)

    def exists(self, store: str, key: str) -> bool:
        return self.mc("stat", f"admin-{store}/{BUCKET}/{key}", check=False).returncode == 0


@pytest.fixture(scope="module")
def stores() -> Iterator[Stores]:
    assert shutil.which("docker"), "The S3 backend integration check requires Docker"
    run = f"{NAME_PREFIX}-per-store-{uuid.uuid4().hex[:8]}"
    network, client = f"{run}-net", f"{run}-client"
    docker("network", "create", "--internal", network)
    started: list[str] = []
    try:
        for name, (image, env, command) in sorted(STORES.items()):
            flags = [arg for key, value in env.items() for arg in ("-e", f"{key}={value}")]
            docker("run", "-d", "--name", f"{run}-{name}", "--network", network, *flags, image, *command)
            started.append(f"{run}-{name}")
        docker("run", "-d", "--name", client, "--network", network, "--entrypoint", "sleep", client_image(), "infinity")
        started.append(client)
        current = Stores(client)
        for name in sorted(STORES):
            url = f"http://{run}-{name}:9000"
            admin = f"admin-{name}"
            deadline = time.monotonic() + 90
            while current.mc("alias", "set", admin, url, ROOT_USER, ROOT_PASSWORD, check=False).returncode != 0 or (
                current.mc("ls", admin, check=False).returncode != 0
            ):
                assert time.monotonic() < deadline, docker("logs", f"{run}-{name}", check=False).stderr[-2000:]
                time.sleep(1)
            current.mc("mb", f"{admin}/{BUCKET}")
            current.mc("version", "enable", f"{admin}/{BUCKET}")
            current.mc("admin", "policy", "create", admin, "backup-echoport-retention", "/dev/stdin",
                       stdin=json.dumps(RETENTION_POLICY))
            current.mc("admin", "user", "add", admin, stdin=f"{RETENTION_USER}\n{RETENTION_SECRET}\n")
            current.mc("admin", "policy", "attach", admin, "backup-echoport-retention", "--user", RETENTION_USER)
            # The retention alias carries the store's own name, as on macmini.
            alias = {"url": url, "accessKey": RETENTION_USER, "secretKey": RETENTION_SECRET,
                     "api": "s3v4", "path": "auto"}
            current.mc("alias", "import", name, stdin=json.dumps(alias))
        yield current
    finally:
        for name in reversed(started):
            docker("rm", "--force", "--volumes", name, check=False)
        docker("network", "rm", network, check=False)


@pytest.fixture
def mc(stores: Stores, settings, tmp_path: Path) -> Stores:
    wrapper = tmp_path / "mc"
    wrapper.write_text(f'#!/bin/sh\nexec docker exec -i {stores.client} mc "$@"\n')
    wrapper.chmod(0o755)
    settings.MINIO_MC_PATH = str(wrapper)
    settings.MINIO_ALIASES = ["minio", "rustfs"]
    settings.ECHOPORT_CACHE_DIR = str(tmp_path)
    settings.ECHOPORT_RETENTION_MIN_SUCCESS = 0
    return stores


def target_with_expired_run(name: str, alias: str, key: str) -> BackupRun:
    target = BackupTarget.objects.create(
        name=name, service_name=f"{name}.service", db_path=f"/tmp/{name}.db",
        fastdeploy_service="echoport-backup", retention_days=7,
    )
    BackupTarget.objects.filter(pk=target.pk).update(storage_alias=alias)
    return BackupRun.objects.create(
        target=target, status="success", finished_at=timezone.now() - timedelta(days=10),
        storage_bucket=BUCKET, storage_key=key,
    )


def cleanup() -> int:
    with pytest.raises(SystemExit) as exit_info:
        call_command("cleanup_old_backups")
    return exit_info.value.code


@pytest.mark.django_db
def test_each_target_deletes_only_on_its_own_store(mc: Stores) -> None:
    suffix = uuid.uuid4().hex[:6]
    keys = {"minio": f"old-{suffix}/a.tar.gz", "rustfs": f"moved-{suffix}/a.tar.gz"}
    media = f"media-{suffix}/2026-10-01T02-00-00"
    for key in (*keys.values(), f"{media}/manifest.json"):
        # The data move copied every archive, so both stores hold it.
        mc.put("minio", key)
        mc.put("rustfs", key)
    target_with_expired_run(f"old-{suffix}", "minio", keys["minio"])
    target_with_expired_run(f"moved-{suffix}", "rustfs", keys["rustfs"])
    target_with_expired_run(f"media-{suffix}", "rustfs", media)

    assert cleanup() == 0
    assert not BackupRun.objects.exists()
    assert not mc.exists("minio", keys["minio"]) and mc.exists("rustfs", keys["minio"])
    assert not mc.exists("rustfs", keys["rustfs"]) and mc.exists("minio", keys["rustfs"])
    assert not mc.exists("rustfs", f"{media}/manifest.json") and mc.exists("minio", f"{media}/manifest.json")


@pytest.mark.django_db
def test_a_paused_target_keeps_rows_and_objects(mc: Stores) -> None:
    key = f"moving-{uuid.uuid4().hex[:6]}/a.tar.gz"
    mc.put("minio", key)
    mc.put("rustfs", key)
    run = target_with_expired_run(key.split("/")[0], STORAGE_ALIAS_PAUSED, key)

    assert cleanup() == 0
    run.refresh_from_db()
    assert run.deletion_pending_at is None
    assert mc.exists("minio", key) and mc.exists("rustfs", key)


@pytest.mark.django_db
def test_an_object_missing_on_the_targets_store_counts_as_deleted(mc: Stores) -> None:
    key = f"gone-{uuid.uuid4().hex[:6]}/a.tar.gz"
    mc.put("minio", key)  # only on the other store
    target_with_expired_run(key.split("/")[0], "rustfs", key)

    assert cleanup() == 0
    assert not BackupRun.objects.exists()
    assert mc.exists("minio", key)


@pytest.mark.django_db
def test_an_allowlisted_alias_mc_does_not_have_fails_closed(mc: Stores, settings) -> None:
    settings.MINIO_ALIASES = ["minio", "rustfs", "spare"]
    key = f"spare-{uuid.uuid4().hex[:6]}/a.tar.gz"
    mc.put("rustfs", key)
    run = target_with_expired_run(key.split("/")[0], "spare", key)
    # mc lists an unknown alias as empty (exit 0) and fails its delete with
    # "Requested path ... not found", which is not "Object does not exist".
    assert mc.mc("ls", "--recursive", "--json", f"spare/{BUCKET}/{key}/", check=False).returncode == 0

    assert cleanup() == 1
    run.refresh_from_db()
    assert run.deletion_pending_at is not None
    assert mc.exists("rustfs", key)
