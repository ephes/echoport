"""
Read-only inspection of FastDeploy service tokens.

FastDeploy service tokens are JWTs. Their payload carries ``exp`` and, for
tokens issued through FastDeploy's token registry, a ``jti``. Tokens without
a ``jti`` ("legacy" tokens) are rejected by FastDeploy once its legacy grace
(``LEGACY_SERVICE_TOKENS_ACCEPTED_UNTIL``) ends, and every token is rejected
after ``exp``. Either way all backups using the token fail with HTTP 401.

This module decodes the payload *without verifying the signature* (Echoport
does not have FastDeploy's signing key and never needs it) so that operators
can see which tokens need re-issuing before that happens. It only reports
non-secret claims and never returns, logs or prints the token itself.
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from django.conf import settings

from .fastdeploy_client import EndpointConfigError, get_fastdeploy_config

DEFAULT_WARN_DAYS = 21

STATUS_OK = "ok"
STATUS_EXPIRING = "expiring"
STATUS_EXPIRED = "expired"
STATUS_LEGACY = "legacy"
STATUS_UNDECODABLE = "undecodable"
STATUS_MISSING = "missing"
STATUS_UNSET = "unset"

# Statuses that mean backups using the token fail now or soon.
ATTENTION_STATUSES = frozenset(
    {STATUS_EXPIRING, STATUS_EXPIRED, STATUS_LEGACY, STATUS_UNDECODABLE, STATUS_MISSING}
)


@dataclass(frozen=True)
class TokenClaims:
    """Non-secret claims of a service token. Never holds the token itself."""

    decodable: bool
    expires_at: datetime | None = None
    has_jti: bool = False
    service: str | None = None


@dataclass(frozen=True)
class TokenReport:
    """Assessment of one token as used by a target (or the global setting)."""

    source: str
    status: str
    expires_at: datetime | None = None
    has_jti: bool = False
    service: str | None = None
    detail: str = ""

    @property
    def needs_attention(self) -> bool:
        return self.status in ATTENTION_STATUSES

    @property
    def is_legacy(self) -> bool:
        """A decodable token without ``jti``."""
        return self.status not in (STATUS_UNDECODABLE, STATUS_MISSING, STATUS_UNSET) and not self.has_jti


def _b64url_decode(segment: str) -> bytes:
    padding = "=" * (-len(segment) % 4)
    return base64.urlsafe_b64decode(segment + padding)


def decode_token_claims(token: object) -> TokenClaims:
    """
    Decode the payload of a JWT without verifying it.

    Returns only ``exp`` (as an aware UTC datetime), whether a ``jti`` is
    present and the ``service`` claim. Anything that is not a three-segment
    JWT with a JSON object payload is reported as undecodable.
    """
    if not isinstance(token, str):
        # e.g. a non-string token value in FASTDEPLOY_ENDPOINTS
        return TokenClaims(decodable=False)
    parts = token.strip().split(".")
    if len(parts) != 3 or not parts[1]:
        return TokenClaims(decodable=False)
    try:
        payload: Any = json.loads(_b64url_decode(parts[1]))
    except (binascii.Error, ValueError):
        return TokenClaims(decodable=False)
    if not isinstance(payload, dict):
        return TokenClaims(decodable=False)

    expires_at = None
    exp = payload.get("exp")
    if exp is not None:
        if isinstance(exp, bool) or not isinstance(exp, (int, float)):
            return TokenClaims(decodable=False)
        try:
            expires_at = datetime.fromtimestamp(exp, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return TokenClaims(decodable=False)

    service = payload.get("service")
    return TokenClaims(
        decodable=True,
        expires_at=expires_at,
        has_jti=bool(payload.get("jti")),
        service=service if isinstance(service, str) else None,
    )


def assess_token(
    token: object,
    *,
    source: str,
    now: datetime,
    warn_days: int = DEFAULT_WARN_DAYS,
) -> TokenReport:
    """
    Classify a token as ok, expiring, expired, legacy or undecodable.

    Expiry outranks the legacy flag (an expired token fails regardless); a
    legacy token is otherwise flagged because FastDeploy stops accepting it
    when the legacy grace ends.
    """
    claims = decode_token_claims(token)
    if not claims.decodable:
        return TokenReport(
            source=source,
            status=STATUS_UNDECODABLE,
            detail="not a decodable JWT",
        )

    if claims.expires_at is not None and claims.expires_at <= now:
        status = STATUS_EXPIRED
    elif claims.expires_at is not None and claims.expires_at <= now + timedelta(days=warn_days):
        status = STATUS_EXPIRING
    elif not claims.has_jti:
        status = STATUS_LEGACY
    else:
        status = STATUS_OK

    detail = "" if claims.has_jti else "no jti (legacy token)"
    return TokenReport(
        source=source,
        status=status,
        expires_at=claims.expires_at,
        has_jti=claims.has_jti,
        service=claims.service,
        detail=detail,
    )


def resolve_target_token(target: Any) -> tuple[str, object]:
    """
    Resolve the token a backup of ``target`` would use, and where it comes from.

    Mirrors ``get_fastdeploy_config`` and ``FastDeployClient``: the target's
    own token, then the endpoint's ``service_tokens`` entry or default token,
    then the global ``FASTDEPLOY_SERVICE_TOKEN``. Returns ``(source, token)``;
    ``token`` is empty when nothing resolves.

    Raises:
        EndpointConfigError: If the target's endpoint configuration is invalid.
    """
    endpoint_key = target.fastdeploy_endpoint_key or ""
    service_name = target.fastdeploy_service or ""
    config = get_fastdeploy_config(
        endpoint_key=endpoint_key or None,
        service_name=service_name or None,
        token_override=target.service_token or None,
    )
    token: object = config.get("token") or ""

    if target.service_token:
        return "target", token
    if endpoint_key:
        endpoint = settings.FASTDEPLOY_ENDPOINTS.get(endpoint_key, {})
        service_tokens = endpoint.get("service_tokens") or {}
        if isinstance(service_tokens, dict) and service_name in service_tokens:
            return f"endpoint {endpoint_key} (service_tokens)", token
        return f"endpoint {endpoint_key}", token
    return "global FASTDEPLOY_SERVICE_TOKEN", settings.FASTDEPLOY_SERVICE_TOKEN or ""


def assess_target(
    target: Any,
    *,
    now: datetime,
    warn_days: int = DEFAULT_WARN_DAYS,
) -> TokenReport:
    """Assess the token a backup of ``target`` would use."""
    try:
        source, token = resolve_target_token(target)
    except EndpointConfigError:
        # The key is not echoed: it failed validation, so it may hold anything
        # (even a token pasted into the wrong field).
        return TokenReport(
            source="endpoint (invalid configuration)",
            status=STATUS_MISSING,
            detail="endpoint configuration does not resolve a token",
        )
    if not token:
        return TokenReport(source=source, status=STATUS_MISSING, detail="no token configured")
    return assess_token(token, source=source, now=now, warn_days=warn_days)


def assess_global_token(
    *,
    now: datetime,
    warn_days: int = DEFAULT_WARN_DAYS,
) -> TokenReport:
    """Assess the global ``FASTDEPLOY_SERVICE_TOKEN`` setting."""
    source = "global FASTDEPLOY_SERVICE_TOKEN"
    token = settings.FASTDEPLOY_SERVICE_TOKEN or ""
    if not token:
        # Not an error by itself: targets that would fall back to it are
        # reported as "missing" on their own row.
        return TokenReport(source=source, status=STATUS_UNSET, detail="not set")
    return assess_token(token, source=source, now=now, warn_days=warn_days)
