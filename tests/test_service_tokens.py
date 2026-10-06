"""
Tests for the FastDeploy service token expiry report.

All tokens here are crafted, unsigned test fixtures; only their payload claims
matter because Echoport decodes them without verification.
"""

import base64
import json
from datetime import datetime, timedelta, timezone
from io import StringIO

import pytest
from django.contrib.auth.models import User
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import override_settings
from django.urls import reverse

from backups.models import BackupStatus, BackupTarget
from backups.service_tokens import (
    STATUS_EXPIRED,
    STATUS_EXPIRING,
    STATUS_LEGACY,
    STATUS_MISSING,
    STATUS_OK,
    STATUS_UNDECODABLE,
    STATUS_UNSET,
    assess_global_token,
    assess_target,
    assess_token,
    decode_token_claims,
)

NOW = datetime(2026, 10, 6, 12, 0, tzinfo=timezone.utc)


def _segment(data: object) -> str:
    raw = json.dumps(data).encode()
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def make_token(*, exp: datetime | None = None, jti: str | None = "abc123", service: str = "svc") -> str:
    payload: dict = {"type": "service", "service": service, "user": "u", "origin": "test"}
    if exp is not None:
        payload["exp"] = int(exp.timestamp())
    if jti is not None:
        payload["jti"] = jti
    # Recognizable signature segment so tests can assert it is never echoed.
    return f"{_segment({'alg': 'HS256', 'typ': 'JWT'})}.{_segment(payload)}.SECRETSIGNATUREFIXTURE"


def make_target(name: str, **kwargs) -> BackupTarget:
    defaults = {
        "fastdeploy_service": f"{name}-backup",
        "service_name": f"{name}.service",
        "db_path": "/tmp/test.db",
    }
    defaults.update(kwargs)
    return BackupTarget.objects.create(name=name, **defaults)


class TestDecodeTokenClaims:
    def test_with_jti(self):
        claims = decode_token_claims(make_token(exp=NOW + timedelta(days=60)))
        assert claims.decodable
        assert claims.has_jti
        assert claims.service == "svc"
        assert claims.expires_at == NOW + timedelta(days=60)

    def test_without_jti(self):
        claims = decode_token_claims(make_token(exp=NOW + timedelta(days=60), jti=None))
        assert claims.decodable
        assert not claims.has_jti

    @pytest.mark.parametrize(
        "garbage",
        [
            "",
            "not-a-jwt",
            "a.b",
            "a..c",
            "a.!!!.c",
            f"a.{base64.urlsafe_b64encode(b'not json').decode()}.c",
            f"a.{_segment([1, 2])}.c",
            f"a.{_segment({'exp': 'tomorrow'})}.c",
            f"a.{_segment({'exp': 10**20})}.c",
        ],
    )
    def test_garbage_is_undecodable(self, garbage):
        assert not decode_token_claims(garbage).decodable


class TestAssessToken:
    def test_ok(self):
        report = assess_token(make_token(exp=NOW + timedelta(days=60)), source="s", now=NOW)
        assert report.status == STATUS_OK
        assert not report.needs_attention
        assert not report.is_legacy

    def test_expiring_within_warn_days(self):
        report = assess_token(make_token(exp=NOW + timedelta(days=10)), source="s", now=NOW)
        assert report.status == STATUS_EXPIRING
        assert report.needs_attention

    def test_warn_days_is_configurable(self):
        token = make_token(exp=NOW + timedelta(days=10))
        assert assess_token(token, source="s", now=NOW, warn_days=5).status == STATUS_OK

    def test_expired(self):
        report = assess_token(make_token(exp=NOW - timedelta(seconds=1)), source="s", now=NOW)
        assert report.status == STATUS_EXPIRED

    def test_legacy(self):
        report = assess_token(make_token(exp=NOW + timedelta(days=170), jti=None), source="s", now=NOW)
        assert report.status == STATUS_LEGACY
        assert report.is_legacy
        assert report.needs_attention

    def test_expired_outranks_legacy_but_keeps_flag(self):
        report = assess_token(make_token(exp=NOW - timedelta(days=1), jti=None), source="s", now=NOW)
        assert report.status == STATUS_EXPIRED
        assert report.is_legacy

    def test_undecodable(self):
        report = assess_token("garbage", source="s", now=NOW)
        assert report.status == STATUS_UNDECODABLE
        assert report.needs_attention
        assert not report.is_legacy


@pytest.mark.django_db
class TestAssessTarget:
    def test_target_token_wins(self):
        target = make_target("own", service_token=make_token(exp=NOW + timedelta(days=60)))
        report = assess_target(target, now=NOW)
        assert report.source == "target"
        assert report.status == STATUS_OK

    @override_settings(FASTDEPLOY_SERVICE_TOKEN="")
    def test_missing_when_nothing_resolves(self):
        report = assess_target(make_target("bare"), now=NOW)
        assert report.status == STATUS_MISSING
        assert report.source == "global FASTDEPLOY_SERVICE_TOKEN"

    def test_global_fallback(self):
        with override_settings(FASTDEPLOY_SERVICE_TOKEN=make_token(exp=NOW + timedelta(days=5), jti=None)):
            report = assess_target(make_target("fallback"), now=NOW)
        assert report.source == "global FASTDEPLOY_SERVICE_TOKEN"
        assert report.status == STATUS_EXPIRING

    def test_endpoint_service_token(self):
        endpoints = {
            "staging": {
                "base_url": "https://staging.example.test",
                "token": make_token(exp=NOW - timedelta(days=1)),
                "service_tokens": {"svc-backup": make_token(exp=NOW + timedelta(days=60))},
            }
        }
        with override_settings(FASTDEPLOY_ENDPOINTS=endpoints):
            target = make_target("ep", fastdeploy_service="svc-backup", fastdeploy_endpoint_key="staging")
            report = assess_target(target, now=NOW)
            other = make_target("ep2", fastdeploy_service="other", fastdeploy_endpoint_key="staging")
            other_report = assess_target(other, now=NOW)
        assert report.source == "endpoint staging (service_tokens)"
        assert report.status == STATUS_OK
        assert other_report.source == "endpoint staging"
        assert other_report.status == STATUS_EXPIRED

    def test_unknown_endpoint_is_missing(self):
        target = make_target("ep")
        target.fastdeploy_endpoint_key = "gone"  # bypass model validation
        report = assess_target(target, now=NOW)
        assert report.status == STATUS_MISSING

    def test_invalid_endpoint_key_is_not_echoed(self):
        target = make_target("ep")
        # A token pasted into the endpoint key field (bypassing validation)
        BackupTarget.objects.filter(pk=target.pk).update(
            fastdeploy_endpoint_key="SECRETSIGNATUREFIXTURE"
        )
        target.refresh_from_db()
        report = assess_target(target, now=NOW)
        assert report.status == STATUS_MISSING
        assert "SECRETSIGNATUREFIXTURE" not in f"{report.source} {report.detail}"

    def test_non_string_endpoint_token_is_undecodable(self):
        endpoints = {"staging": {"base_url": "https://staging.example.test", "token": 123}}
        with override_settings(FASTDEPLOY_ENDPOINTS=endpoints):
            target = make_target("ep", fastdeploy_endpoint_key="staging")
            report = assess_target(target, now=NOW)
        assert report.status == STATUS_UNDECODABLE

    @override_settings(FASTDEPLOY_SERVICE_TOKEN="")
    def test_global_unset_is_not_attention(self):
        report = assess_global_token(now=NOW)
        assert report.status == STATUS_UNSET
        assert not report.needs_attention


def _run(*args):
    out, err = StringIO(), StringIO()
    code = 0
    try:
        call_command("check_service_tokens", *args, stdout=out, stderr=err)
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else 1
    return code, out.getvalue(), err.getvalue()


@pytest.mark.django_db
class TestCheckServiceTokensCommand:
    def test_all_ok_exits_zero(self):
        future = datetime.now(timezone.utc) + timedelta(days=80)
        with override_settings(FASTDEPLOY_SERVICE_TOKEN=make_token(exp=future)):
            make_target("good", service_token=make_token(exp=future))
            code, out, err = _run()
        assert code == 0
        assert "good: status=ok source=target" in out
        assert "(global): status=ok" in out
        assert "All service tokens ok" in out
        assert "SECRETSIGNATUREFIXTURE" not in out + err

    def test_attention_exits_non_zero_without_leaking_tokens(self):
        now = datetime.now(timezone.utc)
        tokens = {
            "legacy": make_token(exp=now + timedelta(days=170), jti=None),
            "expiring": make_token(exp=now + timedelta(days=3)),
            "expired": make_token(exp=now - timedelta(days=3)),
            "garbage": "SECRETSIGNATUREFIXTURE-not-a-jwt",
        }
        with override_settings(FASTDEPLOY_SERVICE_TOKEN=make_token(exp=now + timedelta(days=80))):
            for name, token in tokens.items():
                make_target(name, service_token=token)
            make_target("off", service_token="SECRETSIGNATUREFIXTURE", status=BackupStatus.DISABLED)
            badkey = make_target("badkey")
            BackupTarget.objects.filter(pk=badkey.pk).update(
                fastdeploy_endpoint_key="SECRETSIGNATUREFIXTURE"
            )
            code, out, err = _run()
        assert code == 1
        assert "legacy: status=legacy" in out
        assert "legacy=yes" in out
        assert "expiring: status=expiring" in out
        assert "expired: status=expired" in out
        assert "garbage: status=undecodable" in out
        assert "off:" not in out
        assert "badkey: status=missing source=endpoint (invalid configuration)" in out
        assert "5 service token(s) need attention" in err
        combined = out + err
        assert "SECRETSIGNATUREFIXTURE" not in combined
        for token in tokens.values():
            assert token not in combined
            if token.count(".") == 2:
                assert token.split(".")[1] not in combined

    def test_warn_days_option(self):
        soon = datetime.now(timezone.utc) + timedelta(days=10)
        with override_settings(FASTDEPLOY_SERVICE_TOKEN=make_token(exp=soon)):
            assert _run("--warn-days", "5")[0] == 0
            assert _run("--warn-days", "30")[0] == 1

    def test_negative_warn_days_rejected(self):
        with pytest.raises(CommandError):
            call_command("check_service_tokens", "--warn-days", "-1", stdout=StringIO())


@pytest.mark.django_db
class TestAdminTokenExpiresColumn:
    def test_changelist_shows_expiry_and_badges(self, client):
        User.objects.create_superuser("admin", "admin@example.com", "password")
        client.force_login(User.objects.get(username="admin"))
        future = datetime.now(timezone.utc) + timedelta(days=80)
        legacy_token = make_token(exp=future, jti=None)
        make_target("legacytarget", service_token=legacy_token)
        make_target("goodtarget", service_token=make_token(exp=future))
        make_target("badtarget", service_token="SECRETSIGNATUREFIXTURE-garbage")

        response = client.get(reverse("admin:backups_backuptarget_changelist"))

        assert response.status_code == 200
        html = response.content.decode()
        assert "Token expires" in html
        assert future.strftime("%Y-%m-%d") in html
        assert ">legacy<" in html
        assert ">undecodable<" in html
        assert "SECRETSIGNATUREFIXTURE" not in html
        assert legacy_token.split(".")[1] not in html
