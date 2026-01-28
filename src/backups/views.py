"""
Views for Echoport backup dashboard.
"""

import logging
import threading

from django.contrib.auth.decorators import login_required
from django.db import close_old_connections
from django.db.models import Prefetch
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.http import require_POST

from .backup_engine import get_active_run, start_backup, _mark_run_failed
from .models import BackupRun, BackupRunStatus, BackupStatus, BackupTarget, BackupTrigger

logger = logging.getLogger(__name__)


def _run_backup_in_thread(run_id: int) -> None:
    """
    Run backup in a background thread for an existing run record.

    Takes run_id instead of objects to avoid Django ORM issues
    with objects crossing thread boundaries.
    """
    try:
        # Get fresh DB connection for this thread
        close_old_connections()

        # Fetch run and target fresh in this thread
        run = BackupRun.objects.select_related("target").get(id=run_id)
        target = run.target

        # Run the backup, passing the existing run to avoid re-creation
        start_backup(target, existing_run=run)

    except Exception as e:
        logger.error(f"Background backup failed for run {run_id}: {e}")
    finally:
        # Always close connections when thread exits
        close_old_connections()


@login_required
def dashboard(request):
    """
    Main dashboard showing all backup targets with their status.
    """
    # Use Prefetch with ordered queryset to enable efficient in-memory filtering
    # This avoids N+1 queries that would occur when calling get_last_run() etc.
    targets = BackupTarget.objects.prefetch_related(
        Prefetch(
            "runs",
            queryset=BackupRun.objects.order_by("-started_at"),
        )
    ).all()

    # Enrich targets with computed data using prefetched runs
    for target in targets:
        runs = list(target.runs.all())  # Uses prefetched data, no new query
        target.last_run = runs[0] if runs else None
        target.last_success = next(
            (r for r in runs if r.status == BackupRunStatus.SUCCESS), None
        )
        target.active_run = next(
            (r for r in runs if r.status in [BackupRunStatus.PENDING, BackupRunStatus.RUNNING]),
            None,
        )

    context = {
        "targets": targets,
        "status_choices": BackupRunStatus,
    }

    return render(request, "backups/dashboard.html", context)


@login_required
def target_detail(request, target_id):
    """
    Show details for a specific backup target including run history.
    """
    target = get_object_or_404(BackupTarget, id=target_id)
    runs = target.runs.all()[:50]  # Last 50 runs

    context = {
        "target": target,
        "runs": runs,
        "active_run": get_active_run(target),
    }

    return render(request, "backups/target_detail.html", context)


@login_required
def run_detail(request, run_id):
    """
    Show details for a specific backup run.
    """
    run = get_object_or_404(BackupRun.objects.select_related("target"), id=run_id)

    context = {
        "run": run,
        "target": run.target,
    }

    return render(request, "backups/run_detail.html", context)


@login_required
@require_POST
def trigger_backup(request, target_id):
    """
    Trigger a manual backup for the specified target.
    Returns immediately with the updated target card; backup runs in background.

    To avoid UI race conditions, we create a PENDING run record synchronously
    before starting the background thread. This ensures the UI immediately
    shows the backup as running.
    """
    target = get_object_or_404(BackupTarget, id=target_id)
    triggered_by = request.user.username

    # Check if target is active
    if target.status != BackupStatus.ACTIVE:
        logger.warning(f"Cannot backup inactive target '{target.name}' (status: {target.status})")
        # Still render the card, which will show the target's current state
    # Check if backup is already running
    elif get_active_run(target):
        logger.warning(f"Concurrent backup attempt blocked for target '{target.name}'")
    else:
        run = None
        try:
            # Create PENDING run synchronously so UI shows it immediately
            # This avoids the race condition where the background thread
            # hasn't created the run yet when we query for active_run
            run = BackupRun.objects.create(
                target=target,
                status=BackupRunStatus.PENDING,
                trigger=BackupTrigger.MANUAL,
                triggered_by=triggered_by,
                storage_bucket=target.storage_bucket,
            )
            logger.info(f"Created backup run {run.id} for target '{target.name}'")

            # Start backup in background thread
            thread = threading.Thread(
                target=_run_backup_in_thread,
                args=(run.id,),
                daemon=True,
            )
            thread.start()

        except Exception as e:
            logger.error(f"Error triggering backup for '{target.name}': {e}")
            # If we created the run but thread failed, mark it as failed
            if run:
                _mark_run_failed(run, f"Failed to start backup thread: {e}")

    # Refresh target data for response
    target.last_run = target.get_last_run()
    target.last_success = target.get_last_successful_run()
    target.active_run = get_active_run(target)

    # Check if this is an HTMX request
    if request.htmx:
        return render(
            request,
            "backups/partials/target_card.html",
            {"target": target, "status_choices": BackupRunStatus},
        )

    return redirect("backups:dashboard")


@login_required
def backup_status(request, target_id):
    """
    HTMX endpoint to poll backup status.
    Returns the updated target card partial.
    """
    target = get_object_or_404(BackupTarget, id=target_id)

    target.last_run = target.get_last_run()
    target.last_success = target.get_last_successful_run()
    target.active_run = get_active_run(target)

    # If still running, tell HTMX to continue polling
    headers = {}
    if target.active_run:
        headers["HX-Trigger-After-Swap"] = "continuePolling"

    response = render(
        request,
        "backups/partials/target_card.html",
        {"target": target, "status_choices": BackupRunStatus},
    )

    for key, value in headers.items():
        response[key] = value

    return response
