"""
Tests for the backup management command.
"""

from unittest.mock import MagicMock, patch

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from backups.models import BackupRunStatus, BackupTarget


@pytest.fixture
def backup_target(db):
    """Create a test backup target for backup command tests."""
    return BackupTarget.objects.create(
        name="test-backup",
        description="Test backup target",
        fastdeploy_service="echoport-backup",
        db_path="/tmp/test.db",
        backup_files=["/tmp/test.txt"],
        status="active",
    )


@pytest.mark.django_db
class TestBackupCommand:
    """Tests for manage.py backup command."""

    def test_target_not_found(self):
        with pytest.raises(CommandError, match="not found"):
            call_command("backup", "nonexistent")

    @patch("backups.management.commands.backup.start_backup")
    def test_successful_backup_with_unknown_size(self, mock_start, backup_target, capsys):
        mock_run = MagicMock()
        mock_run.status = BackupRunStatus.SUCCESS
        mock_run.id = 42
        mock_run.storage_bucket = "backups"
        mock_run.storage_key = "test-backup/2026-02-25.tar.gz"
        mock_run.size_bytes = None
        mock_run.duration_seconds = None
        mock_start.return_value = mock_run

        call_command("backup", backup_target.name)

        captured = capsys.readouterr()
        assert "Backup completed successfully" in captured.out
        assert "Size: unknown" in captured.out
        assert "Duration: unknown" in captured.out
