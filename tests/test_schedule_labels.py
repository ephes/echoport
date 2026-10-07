"""Cron schedules run in UTC; the UI must say so and show the next run in local time."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from django.contrib.auth.models import User
from django.core.management import call_command
from django.urls import reverse

from backups.models import BackupTarget


@pytest.fixture
def target(db):
    return BackupTarget.objects.create(
        name="utc-target",
        fastdeploy_service="echoport-backup",
        service_name="utc-target.service",
        db_path="/tmp/utc.db",
        status="active",
        schedule="0 2 * * *",
    )


@pytest.fixture
def user_client(client, db):
    client.force_login(User.objects.create_user("viewer", "viewer@test.com", "pw"))
    return client


def frozen_now(moment):
    # Replace only the tag module's clock; patching django.utils.timezone.now would also age the session.
    return patch(
        "backups.templatetags.backup_tags.timezone", SimpleNamespace(now=lambda: moment)
    )


@pytest.mark.parametrize(
    "now,expected_next",
    [
        (datetime(2026, 10, 6, 12, 0, tzinfo=UTC), "(next: Oct 7, 04:00 CEST)"),
        (datetime(2026, 12, 6, 12, 0, tzinfo=UTC), "(next: Dec 7, 03:00 CET)"),
    ],
)
@pytest.mark.parametrize("page", ["dashboard", "detail"])
def test_schedule_labelled_utc_with_local_next_run(
    user_client, target, now, expected_next, page
):
    url = (
        reverse("backups:dashboard")
        if page == "dashboard"
        else reverse("backups:target_detail", args=[target.pk])
    )
    with frozen_now(now):
        html = " ".join(user_client.get(url).content.decode().split())
    assert "<code>0 2 * * *</code> UTC" in html
    assert expected_next in html


def test_detail_without_schedule(user_client, target):
    target.schedule = ""
    target.save()
    html = user_client.get(
        reverse("backups:target_detail", args=[target.pk])
    ).content.decode()
    assert "Not scheduled" in html and "UTC" not in html


def test_schedule_help_text_names_utc():
    assert "UTC" in BackupTarget._meta.get_field("schedule").help_text


@pytest.mark.django_db
def test_no_missing_migrations():
    call_command("makemigrations", "backups", "--check", "--dry-run", verbosity=0)
