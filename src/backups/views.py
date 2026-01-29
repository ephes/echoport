"""
Views for Echoport backup dashboard.
"""

import logging
import threading
from datetime import datetime

from croniter import CroniterBadCronError, CroniterBadDateError, croniter
from django.contrib.admin.views.decorators import staff_member_required
from django.contrib.auth.decorators import login_required
from django.db import close_old_connections, transaction
from django.db.models import Prefetch
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_POST

from .backup_engine import get_active_run, start_backup, _mark_run_failed
from .restore_engine import (
    get_active_restore,
    start_restore,
    _mark_run_failed as _mark_restore_failed,
)
from .models import (
    BackupRun,
    BackupRunStatus,
    BackupStatus,
    BackupTarget,
    BackupTrigger,
    RestoreRun,
    RestoreRunStatus,
    RestoreTrigger,
)

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
        # Medium: Handle DoesNotExist in case transaction wasn't committed
        try:
            run = BackupRun.objects.select_related("target").get(id=run_id)
        except BackupRun.DoesNotExist:
            logger.error(f"Backup run {run_id} not found - transaction may not have committed")
            return

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
    target = run.target

    context = {
        "run": run,
        "target": target,
        "active_backup": get_active_run(target),
        "active_restore": get_active_restore(target),
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
    # Check if restore is running (don't create run that will fail precondition check)
    elif get_active_restore(target):
        logger.warning(f"Cannot backup while restore is running for target '{target.name}'")
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

            # Medium: Use transaction.on_commit to ensure the run is visible
            # to the background thread before it starts
            def start_thread():
                thread = threading.Thread(
                    target=_run_backup_in_thread,
                    args=(run.id,),
                    daemon=True,
                )
                thread.start()

            transaction.on_commit(start_thread)

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


def _run_restore_in_thread(restore_id: int) -> None:
    """
    Run restore in a background thread for an existing restore record.

    Takes restore_id instead of objects to avoid Django ORM issues
    with objects crossing thread boundaries.
    """
    try:
        # Get fresh DB connection for this thread
        close_old_connections()

        # Medium: Handle DoesNotExist in case transaction wasn't committed
        try:
            restore_run = RestoreRun.objects.select_related("backup_run", "target").get(id=restore_id)
        except RestoreRun.DoesNotExist:
            logger.error(f"Restore run {restore_id} not found - transaction may not have committed")
            return

        backup_run = restore_run.backup_run

        # Run the restore, passing the existing run to avoid re-creation
        start_restore(backup_run, existing_run=restore_run)

    except Exception as e:
        logger.error(f"Background restore failed for run {restore_id}: {e}")
    finally:
        # Always close connections when thread exits
        close_old_connections()


@staff_member_required
@require_POST
def trigger_restore(request, run_id):
    """
    Trigger a restore from the specified backup run.
    Returns immediately; restore runs in background.

    Medium: Requires staff permission since restore is a destructive operation.

    To avoid UI race conditions, we create a PENDING restore record synchronously
    before starting the background thread.
    """
    backup_run = get_object_or_404(
        BackupRun.objects.select_related("target"),
        id=run_id,
        status=BackupRunStatus.SUCCESS,
    )
    target = backup_run.target
    triggered_by = request.user.username

    # Check preconditions before creating run (to avoid stuck PENDING runs)
    if not backup_run.checksum_sha256:
        logger.warning(f"Cannot restore from backup {run_id}: missing checksum")
        return redirect("backups:run_detail", run_id=run_id)

    if get_active_run(target):
        logger.warning(f"Cannot restore while backup is running for target '{target.name}'")
        return redirect("backups:run_detail", run_id=run_id)

    if get_active_restore(target):
        logger.warning(f"Concurrent restore attempt blocked for target '{target.name}'")
        return redirect("backups:run_detail", run_id=run_id)

    restore_run = None
    try:
        # Create PENDING restore run synchronously so UI shows it immediately
        restore_run = RestoreRun.objects.create(
            backup_run=backup_run,
            target=target,
            status=RestoreRunStatus.PENDING,
            trigger=RestoreTrigger.MANUAL,
            triggered_by=triggered_by,
        )
        logger.info(f"Created restore run {restore_run.id} from backup {backup_run.id}")

        # Medium: Use transaction.on_commit to ensure the run is visible
        # to the background thread before it starts
        def start_thread():
            thread = threading.Thread(
                target=_run_restore_in_thread,
                args=(restore_run.id,),
                daemon=True,
            )
            thread.start()

        transaction.on_commit(start_thread)

        # Redirect to restore detail page
        return redirect("backups:restore_detail", restore_id=restore_run.id)

    except Exception as e:
        logger.error(f"Error triggering restore from backup {run_id}: {e}")
        # If we created the run but thread failed, mark it as failed
        if restore_run:
            _mark_restore_failed(restore_run, f"Failed to start restore thread: {e}")

        return redirect("backups:run_detail", run_id=run_id)


@login_required
def restore_detail(request, restore_id):
    """
    Show details for a specific restore run.
    """
    restore_run = get_object_or_404(
        RestoreRun.objects.select_related("backup_run", "target"),
        id=restore_id,
    )

    context = {
        "restore": restore_run,
        "backup_run": restore_run.backup_run,
        "target": restore_run.target,
    }

    return render(request, "backups/restore_detail.html", context)


@login_required
def restore_status(request, restore_id):
    """
    HTMX endpoint to poll restore status.
    Returns the updated restore status partial.
    """
    restore_run = get_object_or_404(
        RestoreRun.objects.select_related("backup_run", "target"),
        id=restore_id,
    )

    # If still running, tell HTMX to continue polling
    headers = {}
    if restore_run.is_active:
        headers["HX-Trigger-After-Swap"] = "continuePolling"

    response = render(
        request,
        "backups/partials/restore_status.html",
        {"restore": restore_run},
    )

    for key, value in headers.items():
        response[key] = value

    return response


def health_status(request):
    """
    Public JSON endpoint for monitoring systems (e.g., NyxMon).

    Returns overall health status and per-target backup status.
    No authentication required so external monitoring can poll it.

    Security: Does not expose error messages (may contain paths/tokens).
    """
    now = timezone.now()
    targets = BackupTarget.objects.filter(status=BackupStatus.ACTIVE)

    target_statuses = []
    recent_failures = []
    any_overdue = False
    any_failures = False
    any_invalid_schedule = False

    for target in targets:
        last_success = target.get_last_successful_run()
        last_run = target.get_last_run()

        # Calculate next scheduled time and overdue status
        next_scheduled = None
        overdue = False
        overdue_hours = None
        invalid_schedule = False

        if target.schedule:
            try:
                cron = croniter(target.schedule, now)
                next_scheduled_dt = cron.get_next(datetime)
                # Ensure timezone-aware for consistent ISO output
                if timezone.is_naive(next_scheduled_dt):
                    next_scheduled_dt = timezone.make_aware(next_scheduled_dt)
                next_scheduled = next_scheduled_dt

                # Check if overdue: last success should be after the previous scheduled time
                prev_scheduled = croniter(target.schedule, now).get_prev(datetime)
                # Ensure timezone-aware for comparison
                if timezone.is_naive(prev_scheduled):
                    prev_scheduled = timezone.make_aware(prev_scheduled)

                if last_success:
                    if last_success.started_at < prev_scheduled:
                        overdue = True
                        overdue_hours = round((now - prev_scheduled).total_seconds() / 3600, 1)
                        any_overdue = True
                else:
                    # No successful backup ever - considered overdue if scheduled
                    overdue = True
                    any_overdue = True
            except (KeyError, ValueError, CroniterBadCronError, CroniterBadDateError):
                # Invalid cron expression - surface this to operators
                invalid_schedule = True
                any_invalid_schedule = True

        # Determine target status (order matters: overdue > invalid > failed/timeout > ok)
        if overdue:
            status = "overdue"
        elif invalid_schedule:
            status = "invalid_schedule"
        elif last_run and last_run.status in [BackupRunStatus.FAILED, BackupRunStatus.TIMEOUT]:
            status = "last_failed"
            any_failures = True
        else:
            status = "ok"

        target_info = {
            "name": target.name,
            "status": status,
            "last_successful_backup": (
                last_success.started_at.isoformat() if last_success else None
            ),
            "next_scheduled": next_scheduled.isoformat() if next_scheduled else None,
            "overdue": overdue,
        }
        if overdue_hours is not None:
            target_info["overdue_hours"] = overdue_hours

        target_statuses.append(target_info)

        # Collect recent failures (last 7 days)
        # Security: Only expose status and timestamp, not error messages
        failed_runs = target.runs.filter(
            status__in=[BackupRunStatus.FAILED, BackupRunStatus.TIMEOUT],
            started_at__gte=now - timezone.timedelta(days=7),
        ).order_by("-started_at")[:5]

        for run in failed_runs:
            any_failures = True
            recent_failures.append({
                "target": target.name,
                "timestamp": run.started_at.isoformat(),
                "status": run.status,
            })

    # Determine overall status
    # unhealthy = overdue backups (data at risk)
    # degraded = recent failures or invalid schedules (needs attention)
    # healthy = all good
    if any_overdue:
        overall_status = "unhealthy"
    elif any_failures or any_invalid_schedule:
        overall_status = "degraded"
    else:
        overall_status = "healthy"

    # Sort failures by timestamp (newest first)
    recent_failures.sort(key=lambda x: x["timestamp"], reverse=True)

    return JsonResponse({
        "status": overall_status,
        "checked_at": now.isoformat(),
        "targets": target_statuses,
        "recent_failures": recent_failures[:10],  # Limit to 10 most recent
    })
