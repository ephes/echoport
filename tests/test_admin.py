"""
Tests for the Django admin configuration.
"""

import pytest
from django.contrib.admin.sites import AdminSite
from django.contrib.auth.models import User

from backups.admin import BackupTargetAdmin, BackupTargetAdminForm
from backups.models import BackupTarget


@pytest.fixture
def admin_site():
    return AdminSite()


@pytest.fixture
def admin_user(db):
    return User.objects.create_superuser("admin", "admin@example.com", "password")


@pytest.mark.django_db
class TestBackupTargetAdminForm:
    """Tests for the BackupTargetAdminForm."""

    def test_form_converts_list_to_textarea(self):
        """Valid list should be converted to newline-separated text."""
        target = BackupTarget.objects.create(
            name="test",
            fastdeploy_service="test-service",
            db_path="/home/test/db.sqlite3",
            backup_files=["/home/test/a.txt", "/home/test/b.txt"],
        )
        form = BackupTargetAdminForm(instance=target)
        assert form.fields["backup_files_text"].initial == "/home/test/a.txt\n/home/test/b.txt"

    def test_form_handles_empty_list(self):
        """Empty list should result in empty textarea."""
        target = BackupTarget.objects.create(
            name="test",
            fastdeploy_service="test-service",
            db_path="/home/test/db.sqlite3",
            backup_files=[],
        )
        form = BackupTargetAdminForm(instance=target)
        # Initial is not set for empty, so it's None or empty
        assert not form.fields["backup_files_text"].initial

    def test_form_handles_falsy_backup_files(self):
        """Falsy values (empty list) should result in empty textarea."""
        target = BackupTarget.objects.create(
            name="test",
            fastdeploy_service="test-service",
            db_path="/home/test/db.sqlite3",
            backup_files=[],  # Empty list is falsy
        )

        form = BackupTargetAdminForm(instance=target)
        # Empty list should result in no initial value
        assert not form.fields["backup_files_text"].initial

    def test_form_handles_non_list_legacy_data(self):
        """Non-list value (legacy/corrupt data) should be coerced to string."""
        target = BackupTarget.objects.create(
            name="test",
            fastdeploy_service="test-service",
            db_path="/home/test/db.sqlite3",
        )
        # Set invalid data via update to bypass validation
        BackupTarget.objects.filter(pk=target.pk).update(backup_files="a string value")
        target.refresh_from_db()

        # Form should not crash
        form = BackupTargetAdminForm(instance=target)
        # Should coerce to string for remediation
        assert form.fields["backup_files_text"].initial == "a string value"

    def test_form_handles_list_with_non_string_items(self):
        """List with non-string items should be coerced to strings."""
        target = BackupTarget.objects.create(
            name="test",
            fastdeploy_service="test-service",
            db_path="/home/test/db.sqlite3",
        )
        # Set invalid data via raw SQL or update
        # JSONField stores [1, 2, 3] as valid JSON
        BackupTarget.objects.filter(pk=target.pk).update(backup_files=[1, "/home/test/b.txt", None])
        target.refresh_from_db()

        # Form should not crash
        form = BackupTargetAdminForm(instance=target)
        # Non-strings coerced
        assert "1" in form.fields["backup_files_text"].initial
        assert "/home/test/b.txt" in form.fields["backup_files_text"].initial
        assert "None" in form.fields["backup_files_text"].initial

    def test_form_textarea_to_list_conversion(self):
        """Textarea input should be converted to list on clean."""
        form = BackupTargetAdminForm(data={
            "name": "test",
            "fastdeploy_service": "test-service",
            "db_path": "/home/test/db.sqlite3",
            "backup_files_text": "/home/test/a.txt\n/home/test/b.txt",
            "backup_files": "[]",  # Hidden field
            "status": "active",
            "retention_days": 30,
            "timeout_seconds": 600,
            "storage_bucket": "backups",
        })
        assert form.is_valid(), form.errors
        assert form.cleaned_data["backup_files"] == ["/home/test/a.txt", "/home/test/b.txt"]

    def test_form_textarea_strips_empty_lines(self):
        """Empty lines in textarea should be ignored."""
        form = BackupTargetAdminForm(data={
            "name": "test",
            "fastdeploy_service": "test-service",
            "db_path": "/home/test/db.sqlite3",
            "backup_files_text": "/home/test/a.txt\n\n/home/test/b.txt\n",
            "backup_files": "[]",
            "status": "active",
            "retention_days": 30,
            "timeout_seconds": 600,
            "storage_bucket": "backups",
        })
        assert form.is_valid(), form.errors
        assert form.cleaned_data["backup_files"] == ["/home/test/a.txt", "/home/test/b.txt"]

    def test_form_validates_paths_in_textarea(self):
        """Invalid paths in textarea should cause validation error."""
        form = BackupTargetAdminForm(data={
            "name": "test",
            "fastdeploy_service": "test-service",
            "db_path": "/home/test/db.sqlite3",
            "backup_files_text": "/home/test/a.txt\n/etc/passwd",  # /etc not allowed
            "backup_files": "[]",
            "status": "active",
            "retention_days": 30,
            "timeout_seconds": 600,
            "storage_bucket": "backups",
        })
        assert not form.is_valid()
        assert "backup_files_text" in form.errors

    def test_form_requires_backup_source(self):
        """Form should require at least one of db_path or backup_files."""
        form = BackupTargetAdminForm(data={
            "name": "test",
            "fastdeploy_service": "test-service",
            "db_path": "",
            "backup_files_text": "",
            "backup_files": "[]",
            "status": "active",
            "retention_days": 30,
            "timeout_seconds": 600,
            "storage_bucket": "backups",
        })
        assert not form.is_valid()
        assert "__all__" in form.errors or "at least one" in str(form.errors).lower()


@pytest.mark.django_db
class TestBackupTargetAdmin:
    """Tests for BackupTargetAdmin."""

    def test_delete_permission_blocked(self, admin_site, admin_user):
        """Delete permission should be blocked."""
        from django.test import RequestFactory

        admin = BackupTargetAdmin(BackupTarget, admin_site)
        request = RequestFactory().get("/")
        request.user = admin_user

        assert admin.has_delete_permission(request) is False

        # Also test with an object
        target = BackupTarget.objects.create(
            name="test",
            fastdeploy_service="test-service",
            db_path="/home/test/db.sqlite3",
        )
        assert admin.has_delete_permission(request, target) is False


@pytest.mark.django_db
class TestBackupRunAdmin:
    """Tests for BackupRunAdmin (read-only)."""

    def test_add_permission_blocked(self, admin_site, admin_user):
        """Add permission should be blocked."""
        from django.test import RequestFactory
        from backups.admin import BackupRunAdmin
        from backups.models import BackupRun

        admin = BackupRunAdmin(BackupRun, admin_site)
        request = RequestFactory().get("/")
        request.user = admin_user

        assert admin.has_add_permission(request) is False

    def test_change_permission_blocked(self, admin_site, admin_user):
        """Change permission should be blocked."""
        from django.test import RequestFactory
        from backups.admin import BackupRunAdmin
        from backups.models import BackupRun

        admin = BackupRunAdmin(BackupRun, admin_site)
        request = RequestFactory().get("/")
        request.user = admin_user

        assert admin.has_change_permission(request) is False

    def test_delete_permission_blocked(self, admin_site, admin_user):
        """Delete permission should be blocked."""
        from django.test import RequestFactory
        from backups.admin import BackupRunAdmin
        from backups.models import BackupRun

        admin = BackupRunAdmin(BackupRun, admin_site)
        request = RequestFactory().get("/")
        request.user = admin_user

        assert admin.has_delete_permission(request) is False


@pytest.mark.django_db
class TestRestoreRunAdmin:
    """Tests for RestoreRunAdmin (read-only)."""

    def test_all_permissions_blocked(self, admin_site, admin_user):
        """All modification permissions should be blocked."""
        from django.test import RequestFactory
        from backups.admin import RestoreRunAdmin
        from backups.models import RestoreRun

        admin = RestoreRunAdmin(RestoreRun, admin_site)
        request = RequestFactory().get("/")
        request.user = admin_user

        assert admin.has_add_permission(request) is False
        assert admin.has_change_permission(request) is False
        assert admin.has_delete_permission(request) is False
