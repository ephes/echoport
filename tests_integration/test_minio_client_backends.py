"""Echoport's object-store client (``backups.minio_client``) against MinIO and RustFS.

MinIO is being replaced by RustFS, and the cleanup job must work unchanged on
either. For each store this starts a pinned server container and a container
with the pinned ``mc`` on an internal Docker network, creates a retention
account like production's ``echoport-retention`` (list, read and delete the
whole bucket, no writes) and calls the real client functions with
``MINIO_MC_PATH`` pointing at a wrapper that runs ``mc`` in that container.
That covers mc's ``--json`` error output the client parses ("Object does not
exist" for an already deleted object) on both stores.

Not part of ``just test`` (needs Docker; the first run builds the mc image and
pulls the store images): ``just test-s3-backends``. Containers and the network
are named ``echoport-s3-backend-neutral-*`` and removed afterwards.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from backups import minio_client

CLIENT_CONTEXT = Path(__file__).resolve().parent / "s3_client"
NAME_PREFIX = "echoport-s3-backend-neutral"
ROOT_USER = "labroot"
ROOT_PASSWORD = "labroot-password-0123456789"  # pragma: allowlist secret
RETENTION_USER = "bk-retention-lab"
RETENTION_SECRET = "retention-lab-secret-0123456789abcd"  # pragma: allowlist secret
BUCKET = "backups"
# The policy minio_deploy renders for echoport-retention (whole bucket, delete, no put).
RETENTION_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow", "Action": ["s3:GetBucketLocation"], "Resource": [f"arn:aws:s3:::{BUCKET}"]},
        {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": [f"arn:aws:s3:::{BUCKET}"]},
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:GetObjectVersion", "s3:DeleteObject"],
            "Resource": [f"arn:aws:s3:::{BUCKET}/*"],
        },
    ],
}

STORES = {
    "minio": (
        "minio/minio:RELEASE.2025-01-20T14-49-07Z"
        "@sha256:ed9be66eb5f2636c18289c34c3b725ddf57815f2777c77b5938543b78a44f144",
        {"MINIO_ROOT_USER": ROOT_USER, "MINIO_ROOT_PASSWORD": ROOT_PASSWORD},
        ("server", "/data"),
    ),
    "rustfs": (
        "rustfs/rustfs:1.0.3@sha256:9ca9559533b537d10482fe923288b676ad08f92a7069d9a17327fbae3cab688b",
        {"RUSTFS_ACCESS_KEY": ROOT_USER, "RUSTFS_SECRET_KEY": ROOT_PASSWORD},
        (),
    ),
}


def docker(*args: str, stdin: str | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(["docker", *args], input=stdin, capture_output=True, text=True, timeout=900)
    if check and result.returncode != 0:
        raise AssertionError(f"docker {' '.join(args[:3])} failed ({result.returncode}): {result.stderr[-2000:]}")
    return result


def client_image() -> str:
    digest = hashlib.sha256((CLIENT_CONTEXT / "Dockerfile").read_bytes()).hexdigest()[:12]
    tag = f"{NAME_PREFIX}-mc:{digest}"
    if docker("image", "inspect", tag, check=False).returncode != 0:
        docker("build", "--quiet", "--tag", tag, str(CLIENT_CONTEXT))
    return tag


@dataclass
class Store:
    name: str
    client: str

    def mc(self, *args: str, stdin: str | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
        return docker("exec", "-i", self.client, "mc", *args, stdin=stdin, check=check)

    def put(self, key: str, content: str = "data") -> None:
        self.mc("pipe", f"admin/{BUCKET}/{key}", stdin=content)

    def exists(self, key: str) -> bool:
        return self.mc("stat", f"admin/{BUCKET}/{key}", check=False).returncode == 0


@pytest.fixture(scope="module", params=sorted(STORES))
def store(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory) -> Iterator[Store]:
    assert shutil.which("docker"), "The S3 backend integration check requires Docker"
    image, env, command = STORES[request.param]
    mc_image = client_image()
    run = f"{NAME_PREFIX}-echoport-{request.param}-{uuid.uuid4().hex[:8]}"
    network, server, client = f"{run}-net", f"{run}-server", f"{run}-client"
    docker("network", "create", "--internal", network)
    started: list[str] = []
    try:
        flags = [arg for key, value in env.items() for arg in ("-e", f"{key}={value}")]
        docker("run", "-d", "--name", server, "--network", network, *flags, image, *command)
        started.append(server)
        docker("run", "-d", "--name", client, "--network", network, "--entrypoint", "sleep", mc_image, "infinity")
        started.append(client)
        current = Store(request.param, client)
        url = f"http://{server}:9000"
        deadline = time.monotonic() + 90
        while current.mc("alias", "set", "admin", url, ROOT_USER, ROOT_PASSWORD, check=False).returncode != 0 or (
            current.mc("ls", "admin", check=False).returncode != 0
        ):
            assert time.monotonic() < deadline, docker("logs", server, check=False).stderr[-2000:]
            time.sleep(1)
        current.mc("mb", f"admin/{BUCKET}")
        current.mc("version", "enable", f"admin/{BUCKET}")
        current.mc("admin", "policy", "create", "admin", "backup-echoport-retention", "/dev/stdin",
                   stdin=json.dumps(RETENTION_POLICY))
        current.mc("admin", "user", "add", "admin", stdin=f"{RETENTION_USER}\n{RETENTION_SECRET}\n")
        current.mc("admin", "policy", "attach", "admin", "backup-echoport-retention", "--user", RETENTION_USER)
        alias = {"url": url, "accessKey": RETENTION_USER, "secretKey": RETENTION_SECRET, "api": "s3v4", "path": "auto"}
        current.mc("alias", "import", "retention", stdin=json.dumps(alias))
        yield current
    finally:
        for name in reversed(started):
            docker("rm", "--force", "--volumes", name, check=False)
        docker("network", "rm", network, check=False)


@pytest.fixture
def client(store: Store, settings, tmp_path: Path) -> Store:
    wrapper = tmp_path / "mc"
    wrapper.write_text(f'#!/bin/sh\nexec docker exec -i {store.client} mc "$@"\n')
    wrapper.chmod(0o755)
    settings.MINIO_MC_PATH = str(wrapper)
    settings.MINIO_ALIAS = "retention"
    settings.MINIO_ALIASES = ["retention"]
    return store


def test_delete_object_removes_the_object(client: Store) -> None:
    client.put("nyxmon/2026-11-01T02-00-00.tar.gz")
    assert minio_client.object_exists(BUCKET, "nyxmon/2026-11-01T02-00-00.tar.gz", alias="retention")
    assert minio_client.delete_object(BUCKET, "nyxmon/2026-11-01T02-00-00.tar.gz", alias="retention")
    assert not client.exists("nyxmon/2026-11-01T02-00-00.tar.gz")
    assert not minio_client.object_exists(BUCKET, "nyxmon/2026-11-01T02-00-00.tar.gz", alias="retention")


def test_delete_object_of_a_missing_object_is_idempotent(client: Store) -> None:
    result = client.mc("rm", "--json", f"retention/{BUCKET}/nyxmon/never-there.tar.gz", check=False)
    assert result.returncode != 0
    assert minio_client._is_object_not_found_error(result.stdout + result.stderr)
    assert minio_client.delete_object(BUCKET, "nyxmon/never-there.tar.gz", alias="retention")


def test_delete_object_fails_for_a_missing_bucket(client: Store) -> None:
    assert not minio_client.delete_object("no-such-bucket", "nyxmon/x.tar.gz", alias="retention")


def test_prefix_listing_and_recursive_delete(client: Store) -> None:
    for key in ("homepage-media/2026-11-01T02-00-00/manifest.json", "homepage-media/2026-11-01T02-00-00/sub/a.bin"):
        client.put(key)
    client.put("homepage-media/2026-11-02T02-00-00/manifest.json")
    assert sorted(minio_client.list_prefix(BUCKET, "homepage-media/2026-11-01T02-00-00", alias="retention") or []) == [
        "manifest.json",
        "sub/a.bin",
    ]
    assert minio_client.list_prefix(BUCKET, "homepage-media/nothing-here", alias="retention") == []
    assert minio_client.delete_prefix(BUCKET, "homepage-media/2026-11-01T02-00-00", alias="retention")
    assert minio_client.list_prefix(BUCKET, "homepage-media/2026-11-01T02-00-00", alias="retention") == []
    # The neighbouring run is untouched.
    assert client.exists("homepage-media/2026-11-02T02-00-00/manifest.json")
