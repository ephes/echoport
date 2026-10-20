"""
Restore orchestration engine for Echoport.

Coordinates restore execution through FastDeploy.

This module is fully synchronous to avoid Django's SynchronousOnlyOperation
errors when mixing async code with ORM operations.
"""

import logging
import time

from django.conf import settings
from django.db import IntegrityError, OperationalError, close_old_connections, connection, transaction
from django.utils import timezone

from .fastdeploy_client import (
    DeploymentNotFoundError,
    DeploymentStartError,
    EndpointConfigError,
    FastDeployClient,
    FastDeployError,
    get_fastdeploy_config,
)
from .models import (
    BackupRun,
    BackupRunStatus,
    BackupStatus,
    BackupTarget,
    RestoreRun,
    RestoreRunStatus,
    RestoreTrigger,
)

from .run_reaper import reap_stale, save_if_still_active

ACTIVE_RESTORE_STATUSES = [RestoreRunStatus.PENDING, RestoreRunStatus.RUNNING]


# Import lazily to avoid circular imports
def _get_active_backup(target: BackupTarget):
    """Check if a backup is running for this target."""
    from .backup_engine import get_active_run
    return get_active_run(target)


def _reap_stale_backups(target: BackupTarget) -> list[int]:
    """Reap stale backup runs for this target (they would block the restore)."""
    from .backup_engine import reap_stale_runs as reap_stale_backup_runs
    return reap_stale_backup_runs(target=target)

logger = logging.getLogger(__name__)


class RestoreError(Exception):
    """Base exception for restore errors."""

    pass


class ConcurrentRestoreError(RestoreError):
    """A restore is already running for this target."""

    pass


class ConcurrentBackupError(RestoreError):
    """A backup is running for this target, cannot restore."""

    pass


class RestoreTimeoutError(RestoreError):
    """Restore exceeded timeout."""

    pass


class MissingChecksumError(RestoreError):
    """Backup has no checksum, cannot safely restore."""

    pass


def restore_blocked_reason(target: BackupTarget) -> str | None:
    """
    Why no restore may be started for ``target``, or None if one may.

    The single rule shared by the restore command, the web UI and
    ``start_restore``: only active targets can be restored; paused and
    disabled targets cannot.
    """
    if target.status != BackupStatus.ACTIVE:
        return f"Backup target '{target.name}' is not active (status: {target.status})"
    return None


def reserve_restore(
    backup_id: int, triggered_by: str = "", existing_run: RestoreRun | None = None
) -> RestoreRun:
    """Validate fresh state and reserve PENDING under the cleanup writer guard."""
    from .reservations import lock_target

    try:
        # This lookup only identifies the lock; all decisions use fresh rows below.
        target_id = BackupRun.objects.values_list("target_id", flat=True).get(pk=backup_id)
        with transaction.atomic():
            lock_target(target_id)
            backup = BackupRun.objects.select_related("target").get(pk=backup_id)
            target = backup.target
            if target.pk != target_id:
                raise RestoreError("Backup target changed; retry restore")
            reason = restore_blocked_reason(target)
            if reason:
                raise RestoreError(reason)
            if backup.deletion_pending_at:
                raise RestoreError(f"Backup {backup_id} is pending deletion and cannot be restored")
            if backup.status != BackupRunStatus.SUCCESS:
                raise RestoreError(f"Cannot restore from backup run {backup_id} with status '{backup.status}'")
            if not backup.checksum_sha256:
                raise MissingChecksumError(f"Cannot restore from backup run {backup_id}: missing checksum for integrity verification")
            if not backup.storage_bucket or not backup.storage_key:
                raise RestoreError("Backup has missing storage coordinates")
            active_backup = _get_active_backup(target)
            if active_backup:
                raise ConcurrentBackupError(f"Cannot restore while backup {active_backup.id} is running for target '{target.name}'")
            if existing_run:
                current = RestoreRun.objects.get(pk=existing_run.pk)
                if current.backup_run_id != backup_id:
                    raise RestoreError(f"existing_run {current.id} is for backup {current.backup_run_id}, not {backup_id}")
                if current.target_id != target_id:
                    raise RestoreError(f"existing_run {current.id} target mismatch")
                if current.status != RestoreRunStatus.PENDING:
                    raise RestoreError(f"existing_run {current.id} has status '{current.status}', expected 'pending'; no longer pending")
                current.backup_run = backup
                return current
            if get_active_restore(target):
                raise ConcurrentRestoreError(f"A restore is already running for target '{target.name}'")
            return RestoreRun.objects.create(
                backup_run=backup, target=target, status=RestoreRunStatus.PENDING,
                trigger=RestoreTrigger.MANUAL, triggered_by=triggered_by,
            )
    except (BackupRun.DoesNotExist, RestoreRun.DoesNotExist) as error:
        raise RestoreError("Backup or restore reservation no longer exists") from error
    except OperationalError as error:
        raise ConcurrentBackupError(f"Cannot acquire lock for restore: {error}") from error
    except IntegrityError as error:
        raise ConcurrentRestoreError("A restore is already running for this target") from error


def start_restore(
    backup_run: BackupRun,
    triggered_by: str = "",
    existing_run: RestoreRun | None = None,
) -> RestoreRun:
    """
    Start a restore from the given backup run (synchronous).

    This function:
    1. Creates a RestoreRun record (or uses existing_run if provided)
    2. Starts a FastDeploy deployment with restore context
    3. Polls deployment status until complete or timeout
    4. Parses ECHOPORT_RESULT from step messages
    5. Updates RestoreRun with results

    Args:
        backup_run: BackupRun to restore from
        triggered_by: User or system that triggered the restore
        existing_run: Optional pre-created RestoreRun to continue (for UI race avoidance)

    Returns:
        RestoreRun with final status

    Raises:
        ConcurrentRestoreError: If a restore is already running for this target
        RestoreError: For other restore failures
    """
    # Ensure fresh DB connection when called from background thread
    close_old_connections()

    target = backup_run.target

    # Runs left active by a killed process would block this target forever;
    # reap them before checking for concurrent operations.
    reap_stale_runs(target=target, exclude_ids=[existing_run.pk] if existing_run else ())
    _reap_stale_backups(target)

    # A failed precondition must leave a pre-created run FAILED, not PENDING.
    # The mark is written here, after the atomic block below has rolled back:
    # written inside it, it would be rolled back together with the error.
    try:
        run = reserve_restore(backup_run.pk, triggered_by, existing_run)
        backup_run = run.backup_run
        target = backup_run.target
    except RestoreError as error:
        if existing_run:
            # Only a reservation that is still PENDING failed here; a duplicate
            # continuation must not fail a restore another worker is running.
            existing_run.status = RestoreRunStatus.FAILED
            existing_run.error_message = str(error)
            existing_run.finished_at = timezone.now()
            save_if_still_active(
                existing_run,
                (RestoreRunStatus.PENDING,),
                ["status", "error_message", "finished_at"],
                insert_if_missing=False,
            )
        raise

    try:
        # Build context for FastDeploy
        context = _build_restore_context(backup_run, run)

        # Get FastDeploy endpoint config for this target
        # Pass service_name to select the correct service-specific token
        fd_config = get_fastdeploy_config(
            target.fastdeploy_endpoint_key,
            target.fastdeploy_service,
            token_override=target.service_token,
        )

        # Start the deployment using sync client
        with FastDeployClient(
            base_url=fd_config.get("base_url"),
            service_token=fd_config.get("token"),
        ) as client:
            # Claim the run (PENDING -> RUNNING) before launching anything.
            # If it was reaped as stale meanwhile (e.g. this worker was paused),
            # another operation may already own the target: do not deploy.
            run.status = RestoreRunStatus.RUNNING
            if not save_if_still_active(
                run, [RestoreRunStatus.PENDING], ["status"], insert_if_missing=False
            ):
                raise RestoreError(
                    f"Restore run {run.id} is no longer pending (status '{run.status}'); "
                    f"not starting a deployment"
                )

            try:
                deployment_id = client.start_deployment(
                    target.fastdeploy_service,
                    context,
                )
                run.fastdeploy_deployment_id = deployment_id
                if not save_if_still_active(run, ACTIVE_RESTORE_STATUSES, ["fastdeploy_deployment_id"]):
                    raise RestoreError(
                        f"Restore run {run.id} was finalized (status '{run.status}') while "
                        f"deployment {deployment_id} was starting; not tracking it"
                    )

            except DeploymentStartError as e:
                logger.error(f"Failed to start deployment: {e}")
                _mark_run_failed(run, str(e))
                raise RestoreError(f"Failed to start restore deployment: {e}") from e

            # Poll for completion
            poll_interval = getattr(settings, "FASTDEPLOY_POLL_INTERVAL", 5)
            timeout = target.timeout_seconds
            elapsed = 0

            while elapsed < timeout:
                time.sleep(poll_interval)
                elapsed += poll_interval

                try:
                    status = client.get_deployment_status(deployment_id)
                except DeploymentNotFoundError:
                    logger.error(f"Deployment {deployment_id} disappeared")
                    _mark_run_failed(run, "Deployment not found")
                    raise RestoreError("Deployment disappeared during execution")
                except FastDeployError as e:
                    logger.warning(f"Error polling deployment status: {e}")
                    continue  # Retry on transient errors

                if status.is_finished:
                    return _handle_deployment_finished(run, status, client)

                logger.debug(
                    f"Restore {run.id} still running (elapsed: {elapsed}s, timeout: {timeout}s)"
                )

            # Timeout reached
            logger.error(f"Restore {run.id} timed out after {timeout}s")
            _mark_run_timeout(run)
            raise RestoreTimeoutError(
                f"Restore timed out after {timeout} seconds"
            )

    except (RestoreError, RestoreTimeoutError):
        raise
    except EndpointConfigError as e:
        logger.error(f"FastDeploy configuration error: {e}")
        _mark_run_failed(run, str(e))
        raise RestoreError(f"FastDeploy configuration error: {e}") from e
    except Exception as e:
        logger.exception(f"Unexpected error during restore: {e}")
        _mark_run_failed(run, str(e))
        raise RestoreError(f"Unexpected error: {e}") from e
    finally:
        # Clean up DB connections when done
        close_old_connections()


def _build_restore_context(backup_run: BackupRun, run: RestoreRun) -> dict:
    """Build the context dictionary to pass to FastDeploy for restore."""
    target = backup_run.target

    context = {
        "ECHOPORT_ACTION": "restore",
        "ECHOPORT_TARGET": target.name,
        "ECHOPORT_RESTORE_ID": str(run.id),
        "ECHOPORT_DB_PATH": target.db_path,
        "ECHOPORT_BACKUP_FILES": ",".join(target.backup_files) if target.backup_files else "",
        "ECHOPORT_BUCKET": backup_run.storage_bucket,
        "ECHOPORT_KEY": backup_run.storage_key,
        "ECHOPORT_CHECKSUM": backup_run.checksum_sha256,
        "ECHOPORT_SERVICE_NAME": target.service_name,
    }

    # Add restore_owner if configured (for automatic chown after restore)
    if target.restore_owner:
        context["ECHOPORT_RESTORE_OWNER"] = target.restore_owner

    return context


def _handle_deployment_finished(
    run: RestoreRun,
    status,
    client: FastDeployClient,
) -> RestoreRun:
    """Handle a finished deployment and update the run record."""

    # Collect logs from steps
    logs = _collect_step_logs(status.steps)
    run.logs = logs

    if status.is_successful:
        # Parse the restore result from step messages
        result = client.parse_echoport_result(status.steps)

        if result and result.success:
            run.status = RestoreRunStatus.SUCCESS
            run.files_restored = result.file_count
            logger.info(
                f"Restore {run.id} completed successfully: "
                f"{result.file_count} files restored"
            )
        elif result and not result.success:
            run.status = RestoreRunStatus.FAILED
            run.error_message = result.error or "Restore reported failure"
            logger.error(f"Restore {run.id} reported failure: {result.error}")
        else:
            # High: No ECHOPORT_RESULT found - treat as failure to avoid hiding partial restores
            run.status = RestoreRunStatus.FAILED
            run.error_message = "Restore completed but no result was reported - status unknown"
            logger.error(
                f"Restore {run.id} deployment succeeded but no ECHOPORT_RESULT found - marking as failed"
            )
    else:
        # Deployment failed
        failed_step = status.failed_step
        error_msg = failed_step.get("message", "Unknown error") if failed_step else "Deployment failed"
        run.status = RestoreRunStatus.FAILED
        run.error_message = error_msg
        logger.error(f"Restore {run.id} deployment failed: {error_msg}")

    run.finished_at = timezone.now()

    # Best-effort DB connection refresh before final save.
    # For self-restore scenarios (e.g., echoport restoring its own DB),
    # the underlying SQLite file may have been replaced during restore.
    # close_old_connections() only closes connections past their max age;
    # connection.close() forces the active connection closed so Django
    # re-opens against the new file on next access.
    try:
        connection.close()
    except Exception:
        pass

    # Never overwrite a run that was reaped as stale meanwhile.
    save_if_still_active(run, ACTIVE_RESTORE_STATUSES, _FINAL_FIELDS)
    return run


# Fields written when a restore run reaches a final state.
_FINAL_FIELDS = ["status", "logs", "files_restored", "error_message", "finished_at"]


def _collect_step_logs(steps: list[dict]) -> str:
    """Collect log messages from all steps."""
    log_parts = []
    for step in steps:
        name = step.get("name", "unknown")
        state = step.get("state", "unknown")
        message = step.get("message", "")
        log_parts.append(f"[{name}] ({state})")
        if message:
            log_parts.append(message)
    return "\n".join(log_parts)


def _mark_run_failed(run: RestoreRun, error_message: str) -> None:
    """Mark a restore run as failed."""
    run.status = RestoreRunStatus.FAILED
    run.error_message = error_message
    run.finished_at = timezone.now()
    save_if_still_active(run, ACTIVE_RESTORE_STATUSES, ["status", "error_message", "finished_at"])


def _mark_run_timeout(run: RestoreRun) -> None:
    """Mark a restore run as timed out."""
    run.status = RestoreRunStatus.TIMEOUT
    run.error_message = f"Restore timed out after {run.target.timeout_seconds} seconds"
    run.finished_at = timezone.now()
    save_if_still_active(run, ACTIVE_RESTORE_STATUSES, ["status", "error_message", "finished_at"])


def get_active_restore(target: BackupTarget) -> RestoreRun | None:
    """Get the currently active restore run for a target, if any."""
    return target.restore_runs.filter(status__in=ACTIVE_RESTORE_STATUSES).first()


def reap_stale_runs(target: BackupTarget | None = None, now=None, exclude_ids=()) -> list[int]:
    """
    Mark restore runs left PENDING/RUNNING by a killed process as TIMEOUT.

    See ``backups.run_reaper`` for the staleness rule. Returns reaped run ids.
    """
    return reap_stale(
        RestoreRun,
        ACTIVE_RESTORE_STATUSES,
        RestoreRunStatus.TIMEOUT,
        "restore",
        target=target,
        now=now,
        exclude_ids=exclude_ids,
    )
