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
    FastDeployClient,
    FastDeployError,
)
from .models import (
    BackupRun,
    BackupRunStatus,
    BackupTarget,
    RestoreRun,
    RestoreRunStatus,
    RestoreTrigger,
)

# Import lazily to avoid circular imports
def _get_active_backup(target: BackupTarget):
    """Check if a backup is running for this target."""
    from .backup_engine import get_active_run
    return get_active_run(target)

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

    # Helper to mark existing_run as failed if preconditions fail
    def _fail_existing_run_and_raise(error: Exception) -> None:
        if existing_run:
            _mark_run_failed(existing_run, str(error))
        raise error

    # Validate backup run is restorable
    if backup_run.status != BackupRunStatus.SUCCESS:
        _fail_existing_run_and_raise(RestoreError(
            f"Cannot restore from backup run {backup_run.id} with status '{backup_run.status}'"
        ))

    # High: Require checksum for restore integrity verification
    if not backup_run.checksum_sha256:
        _fail_existing_run_and_raise(MissingChecksumError(
            f"Cannot restore from backup run {backup_run.id}: missing checksum for integrity verification"
        ))

    # High: Cross-lock check and run creation must be atomic to prevent races.
    # Use select_for_update within a transaction to serialize backup/restore operations.
    # Fall back to simple check on SQLite which doesn't support select_for_update.
    def _get_or_create_run_with_lock() -> RestoreRun:
        """Validate/create run while holding the lock."""
        # Check for concurrent backup
        active_backup = _get_active_backup(target)
        if active_backup:
            _fail_existing_run_and_raise(ConcurrentBackupError(
                f"Cannot restore while backup {active_backup.id} is running for target '{target.name}'"
            ))

        if existing_run:
            # Validate the existing run is usable
            if existing_run.backup_run_id != backup_run.id:
                _fail_existing_run_and_raise(RestoreError(
                    f"existing_run {existing_run.id} is for backup {existing_run.backup_run_id}, "
                    f"not {backup_run.id}"
                ))
            if existing_run.target_id != target.id:
                _fail_existing_run_and_raise(RestoreError(
                    f"existing_run {existing_run.id} target mismatch: "
                    f"expected '{target.name}', got target_id {existing_run.target_id}"
                ))
            if existing_run.status != RestoreRunStatus.PENDING:
                _fail_existing_run_and_raise(RestoreError(
                    f"existing_run {existing_run.id} has status '{existing_run.status}', "
                    f"expected '{RestoreRunStatus.PENDING}'"
                ))
            logger.info(f"Continuing restore run {existing_run.id} for backup {backup_run.id}")
            return existing_run
        else:
            # Create the restore run record
            try:
                new_run = RestoreRun.objects.create(
                    backup_run=backup_run,
                    target=target,
                    status=RestoreRunStatus.PENDING,
                    trigger=RestoreTrigger.MANUAL,
                    triggered_by=triggered_by,
                )
                logger.info(f"Created restore run {new_run.id} for backup {backup_run.id}")
                return new_run
            except IntegrityError as e:
                logger.warning(f"Concurrent restore prevented for target '{target.name}': {e}")
                raise ConcurrentRestoreError(
                    f"A restore is already running for target '{target.name}'"
                ) from e

    if connection.features.has_select_for_update:
        with transaction.atomic():
            # Lock the target row to serialize backup/restore operations
            # Narrow exception handling to only the lock acquisition
            try:
                BackupTarget.objects.select_for_update(nowait=True).get(id=target.id)
            except OperationalError as e:
                # select_for_update(nowait=True) raises OperationalError on lock contention
                _fail_existing_run_and_raise(ConcurrentBackupError(
                    f"Cannot acquire lock on target '{target.name}' - another operation may be in progress: {e}"
                ))
            # Run creation happens inside atomic block but outside lock try/except
            # so other DB errors bubble up naturally
            run = _get_or_create_run_with_lock()
    else:
        # SQLite fallback: no row locking, but unique constraints still prevent concurrent same-type ops
        run = _get_or_create_run_with_lock()

    try:
        # Build context for FastDeploy
        context = _build_restore_context(backup_run, run)

        # Start the deployment using sync client
        with FastDeployClient() as client:
            try:
                deployment_id = client.start_deployment(
                    target.fastdeploy_service,
                    context,
                )
                run.fastdeploy_deployment_id = deployment_id
                run.status = RestoreRunStatus.RUNNING
                run.save(update_fields=["fastdeploy_deployment_id", "status"])

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

    return {
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
    run.save()
    return run


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
    run.save()


def _mark_run_timeout(run: RestoreRun) -> None:
    """Mark a restore run as timed out."""
    run.status = RestoreRunStatus.TIMEOUT
    run.error_message = f"Restore timed out after {run.target.timeout_seconds} seconds"
    run.finished_at = timezone.now()
    run.save()


def get_active_restore(target: BackupTarget) -> RestoreRun | None:
    """Get the currently active restore run for a target, if any."""
    return target.restore_runs.filter(
        status__in=[RestoreRunStatus.PENDING, RestoreRunStatus.RUNNING]
    ).first()
