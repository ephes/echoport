"""
Stale run reaping for Echoport.

Backups and restores are polled by the process that started them (a cron
scheduler run, a CLI command, or a daemon thread inside the web process).
If that process dies (deploy, gunicorn restart, OOM kill, killed cron run),
its run stays PENDING or RUNNING forever. The one-active-run-per-target
constraints then block every later backup or restore of that target.

The reaper marks such runs as TIMEOUT once they are older than the target's
``timeout_seconds`` plus a grace period (``ECHOPORT_STALE_RUN_GRACE_SECONDS``).
A live engine stops polling after ``timeout_seconds``, so the grace only has
to cover poll request latency and thread start-up delay.

The engines write their final state through ``save_if_still_active`` so a
run that was reaped can never flip back to success (or failure) if the
original process turns out to be alive after all.
"""

import logging
from collections.abc import Sequence
from typing import Any

from django.conf import settings
from django.utils import timezone

logger = logging.getLogger(__name__)

DEFAULT_STALE_RUN_GRACE_SECONDS = 15 * 60


def get_stale_run_grace_seconds() -> int:
    """Grace period (seconds) added to a target's timeout before reaping."""
    grace = getattr(settings, "ECHOPORT_STALE_RUN_GRACE_SECONDS", DEFAULT_STALE_RUN_GRACE_SECONDS)
    try:
        grace = int(grace)
    except (TypeError, ValueError):
        logger.warning(
            f"Invalid ECHOPORT_STALE_RUN_GRACE_SECONDS {grace!r}; "
            f"using {DEFAULT_STALE_RUN_GRACE_SECONDS}"
        )
        return DEFAULT_STALE_RUN_GRACE_SECONDS
    return max(grace, 0)


def reap_stale(
    model: Any,
    active_statuses: Sequence[str],
    timeout_status: str,
    kind: str,
    target=None,
    now=None,
    exclude_ids=(),
) -> list[int]:
    """
    Mark active runs of ``model`` that outlived their deadline as timed out.

    A run is stale when ``started_at`` (set when the run row is created, so it
    also covers PENDING runs that never started a deployment) is older than
    ``target.timeout_seconds`` plus the grace period.

    Returns the ids of the runs that were reaped.
    """
    now = now or timezone.now()
    grace = get_stale_run_grace_seconds()

    runs = model.objects.filter(status__in=active_statuses).select_related("target")
    if target is not None:
        runs = runs.filter(target=target)
    if exclude_ids:
        runs = runs.exclude(pk__in=list(exclude_ids))

    reaped: list[int] = []
    for run in runs:
        limit = run.target.timeout_seconds + grace
        age = (now - run.started_at).total_seconds()
        if age <= limit:
            continue

        message = (
            f"Reaped as stale: {kind} was still {run.status} {int(age)}s after it started "
            f"(timeout {run.target.timeout_seconds}s + grace {grace}s). "
            f"The process running it was most likely killed or restarted."
        )
        # Conditional update: only reap if nothing finished the run meanwhile.
        updated = model.objects.filter(pk=run.pk, status=run.status).update(
            status=timeout_status,
            error_message=message,
            finished_at=now,
        )
        if updated:
            logger.warning(
                f"Reaped stale {kind} run {run.pk} for target '{run.target.name}' "
                f"(status {run.status}, age {int(age)}s)"
            )
            reaped.append(run.pk)
    return reaped


def save_if_still_active(
    run: Any,
    active_statuses: Sequence[str],
    fields: Sequence[str],
    insert_if_missing: bool = True,
) -> bool:
    """
    Persist ``fields`` of ``run`` only while the stored run is in ``active_statuses``.

    Returns False (and reloads ``run`` from the database) when the stored run
    has already moved on, e.g. because it was reaped as stale.
    If the row no longer exists at all (for example after a self-restore
    replaced Echoport's own database), the run is saved normally, as before,
    unless ``insert_if_missing`` is False (then False is returned).
    """
    model = type(run)
    values = {field: getattr(run, field) for field in fields}
    if model.objects.filter(pk=run.pk, status__in=active_statuses).update(**values):
        return True

    stored = model.objects.filter(pk=run.pk).first()
    if stored is None:
        if not insert_if_missing:
            logger.warning(f"{model.__name__} {run.pk} no longer exists; not recreating it")
            return False
        run.save()
        return True

    logger.warning(
        f"Not overwriting {model.__name__} {run.pk}: it is already '{stored.status}' "
        f"(wanted '{run.status}'); it was most likely reaped as stale"
    )
    run.refresh_from_db()
    return False


def reap_all_stale_runs(target=None, now=None) -> tuple[list[int], list[int]]:
    """Reap stale backup and restore runs (optionally only for ``target``)."""
    from .backup_engine import reap_stale_runs as reap_stale_backups
    from .restore_engine import reap_stale_runs as reap_stale_restores

    return (
        reap_stale_backups(target=target, now=now),
        reap_stale_restores(target=target, now=now),
    )
