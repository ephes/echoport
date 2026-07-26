import pytest
from django.core.exceptions import ValidationError
from django.db import IntegrityError

from backups.models import BackupRun, BackupRunStatus, BackupStatus, BackupTarget


@pytest.mark.django_db
class TestBackupTarget:
    def test_every_non_disabled_status_keeps_required_contract_observable(self):
        observed = set(BackupStatus.schedule_contract_observed_values())

        assert observed == set(BackupStatus.values) - {BackupStatus.DISABLED}

    def test_create_target(self):
        target = BackupTarget.objects.create(
            name="nyxmon",
            fastdeploy_service="echoport-backup",
            service_name="nyxmon.service",
            db_path="/home/nyxmon/site/db.sqlite3",
        )
        assert target.id is not None
        assert target.name == "nyxmon"
        assert target.status == "active"

    def test_unique_name(self, backup_target):
        # Since save() now calls full_clean(), the unique constraint is caught
        # at validation time (ValidationError) before reaching the database
        with pytest.raises((ValidationError, IntegrityError)):
            BackupTarget.objects.create(
                name=backup_target.name,
                fastdeploy_service="echoport-backup",
                service_name="dup.service",
                db_path="/tmp/other.db",
            )


@pytest.mark.django_db
class TestBackupRun:
    def test_create_run(self, backup_target):
        run = BackupRun.objects.create(
            target=backup_target,
            status=BackupRunStatus.PENDING,
        )
        assert run.id is not None
        assert run.target == backup_target

    def test_concurrent_backup_constraint(self, backup_target):
        """Test that only one active backup can run per target."""
        # Create first pending run
        BackupRun.objects.create(
            target=backup_target,
            status=BackupRunStatus.PENDING,
        )

        # Second pending run should fail
        with pytest.raises(IntegrityError):
            BackupRun.objects.create(
                target=backup_target,
                status=BackupRunStatus.PENDING,
            )

    def test_concurrent_backup_allowed_after_completion(self, backup_target):
        """Test that new backup can start after previous one completes."""
        # Create and complete first run
        run1 = BackupRun.objects.create(
            target=backup_target,
            status=BackupRunStatus.SUCCESS,
        )

        # Second run should succeed
        run2 = BackupRun.objects.create(
            target=backup_target,
            status=BackupRunStatus.PENDING,
        )
        assert run2.id is not None
        assert run2.id != run1.id
