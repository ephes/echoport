import pytest


@pytest.fixture
def backup_target(db):
    """Create a test backup target."""
    from backups.models import BackupTarget

    return BackupTarget.objects.create(
        name="test-target",
        description="Test backup target",
        fastdeploy_service="echoport-backup",
        db_path="/tmp/test.db",
        backup_files=["/tmp/test.txt"],
        schedule="0 2 * * *",
        status="active",
    )
