"""
Tests for create_devdata management command.
"""

import pytest
from django.core.management import call_command
from django.test import override_settings

from backups.models import BackupTarget


EXPECTED_TARGETS = {
    "nyxmon": {
        "fastdeploy_service": "echoport-backup",
        "service_name": "nyxmon.service",
        "db_path": "/home/nyxmon/site/db.sqlite3",
        "backup_files": [
            "/home/nyxmon/site/pyproject.toml",
            "/home/nyxmon/site/uv.lock",
        ],
        "schedule": "0 2 * * *",
        "status": "active",
        "retention_days": 30,
        "timeout_seconds": 600,
        "storage_bucket": "backups",
    },
    "homelab": {
        "fastdeploy_service": "echoport-backup",
        "service_name": "homelab.service",
        "db_path": "/home/homelab/site/db.sqlite3",
        "backup_files": [
            "/home/homelab/site/pyproject.toml",
            "/home/homelab/site/uv.lock",
        ],
        "schedule": "0 2 * * *",
        "status": "active",
        "retention_days": 30,
        "timeout_seconds": 600,
        "storage_bucket": "backups",
    },
    "fastdeploy": {
        "fastdeploy_service": "fastdeploy-backup",
        "service_name": "fastdeploy",
        "db_path": "/home/fastdeploy/site/db.sqlite3",
        "backup_files": [],
        "schedule": "0 3 * * *",
        "status": "active",
        "retention_days": 30,
        "timeout_seconds": 1200,
        "storage_bucket": "backups",
    },
    "echoport": {
        "fastdeploy_service": "echoport-backup",
        "service_name": "echoport.service",
        "db_path": "/home/echoport/site/db.sqlite3",
        "backup_files": [
            "/home/echoport/site/pyproject.toml",
            "/home/echoport/site/uv.lock",
        ],
        "schedule": "0 4 * * *",
        "status": "active",
        "retention_days": 30,
        "timeout_seconds": 600,
        "storage_bucket": "backups",
    },
}


def _assert_target_matches_expected(name: str) -> None:
    target = BackupTarget.objects.get(name=name)
    expected = EXPECTED_TARGETS[name]
    assert target.fastdeploy_service == expected["fastdeploy_service"]
    assert target.service_name == expected["service_name"]
    assert target.db_path == expected["db_path"]
    assert target.backup_files == expected["backup_files"]
    assert target.schedule == expected["schedule"]
    assert target.status == expected["status"]
    assert target.retention_days == expected["retention_days"]
    assert target.timeout_seconds == expected["timeout_seconds"]
    assert target.storage_bucket == expected["storage_bucket"]


@pytest.mark.django_db
@override_settings(DEBUG=True)
def test_create_devdata_sets_fastdeploy_service_owned_defaults():
    """FastDeploy target should use dedicated service-owned backup defaults."""
    call_command("create_devdata")
    _assert_target_matches_expected("fastdeploy")


@pytest.mark.django_db
@override_settings(DEBUG=True)
def test_create_devdata_updates_existing_fastdeploy_target():
    """Existing fastdeploy target should be updated to new defaults."""
    BackupTarget.objects.create(
        name="fastdeploy",
        fastdeploy_service="echoport-backup",
        status="paused",
        service_name="fastdeploy.service",
        db_path="/home/fastdeploy/site/old.sqlite3",
        backup_files=["/home/fastdeploy/site/services"],
        timeout_seconds=900,
    )

    call_command("create_devdata")
    _assert_target_matches_expected("fastdeploy")


@pytest.mark.django_db
@override_settings(DEBUG=True)
def test_create_devdata_keeps_other_targets_at_expected_defaults():
    """Non-fastdeploy dev targets should remain on their expected defaults."""
    call_command("create_devdata")

    _assert_target_matches_expected("nyxmon")
    _assert_target_matches_expected("homelab")
    _assert_target_matches_expected("echoport")


@pytest.mark.django_db
@override_settings(DEBUG=True)
def test_create_devdata_is_idempotent_for_all_targets():
    """Running create_devdata multiple times should not create duplicates."""
    call_command("create_devdata")
    call_command("create_devdata")

    assert BackupTarget.objects.count() == len(EXPECTED_TARGETS)
    assert set(BackupTarget.objects.values_list("name", flat=True)) == set(EXPECTED_TARGETS.keys())
    for name in EXPECTED_TARGETS:
        _assert_target_matches_expected(name)


@pytest.mark.django_db
@override_settings(DEBUG=False)
def test_create_devdata_noops_when_debug_is_false():
    """Command should not create targets in production mode."""
    call_command("create_devdata")
    assert BackupTarget.objects.count() == 0
