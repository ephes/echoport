"""
Backup orchestration engine for Echoport.

Coordinates backup execution through FastDeploy.

This module is fully synchronous to avoid Django's SynchronousOnlyOperation
errors when mixing async code with ORM operations.
"""

import logging
import time
from datetime import datetime

from django.conf import settings
from django.db import IntegrityError, close_old_connections
from django.utils import timezone

from .fastdeploy_client import (
    DeploymentNotFoundError,
    DeploymentStartError,
    FastDeployClient,
    FastDeployError,
)
from .models import BackupRun, BackupRunStatus, BackupTarget, BackupTrigger

logger = logging.getLogger(__name__)


class BackupError(Exception):
    """Base exception for backup errors."""

    pass


class ConcurrentBackupError(BackupError):
    """A backup is already running for this target."""

    pass


class BackupTimeoutError(BackupError):
    """Backup exceeded timeout."""

    pass


def start_backup(
    target: BackupTarget,
    trigger: str = BackupTrigger.MANUAL,
    triggered_by: str = "",
    existing_run: BackupRun | None = None,
) -> BackupRun:
    """
    Start a backup for the given target (synchronous).

    This function:
    1. Creates a BackupRun record (or uses existing_run if provided)
    2. Starts a FastDeploy deployment with backup context
    3. Polls deployment status until complete or timeout
    4. Parses ECHOPORT_RESULT from step messages
    5. Updates BackupRun with results

    Args:
        target: BackupTarget to back up
        trigger: What triggered this backup (manual, scheduled, api)
        triggered_by: User or system that triggered the backup
        existing_run: Optional pre-created BackupRun to continue (for UI race avoidance)

    Returns:
        BackupRun with final status

    Raises:
        ConcurrentBackupError: If a backup is already running for this target
        BackupError: For other backup failures
    """
    # Ensure fresh DB connection when called from background thread
    close_old_connections()

    # Use existing run or create a new one
    if existing_run:
        # Validate the existing run is usable
        if existing_run.target_id != target.id:
            raise BackupError(
                f"existing_run {existing_run.id} belongs to target '{existing_run.target.name}', "
                f"not '{target.name}'"
            )
        if existing_run.status != BackupRunStatus.PENDING:
            raise BackupError(
                f"existing_run {existing_run.id} has status '{existing_run.status}', "
                f"expected '{BackupRunStatus.PENDING}'"
            )
        run = existing_run
        logger.info(f"Continuing backup run {run.id} for target '{target.name}'")
    else:
        # Create the backup run record
        # The DB constraint will prevent concurrent backups
        try:
            run = BackupRun.objects.create(
                target=target,
                status=BackupRunStatus.PENDING,
                trigger=trigger,
                triggered_by=triggered_by,
                storage_bucket=target.storage_bucket,
            )
            logger.info(f"Created backup run {run.id} for target '{target.name}'")
        except IntegrityError as e:
            logger.warning(f"Concurrent backup prevented for target '{target.name}': {e}")
            raise ConcurrentBackupError(
                f"A backup is already running for target '{target.name}'"
            ) from e

    try:
        # Build context for FastDeploy
        context = _build_backup_context(target, run)

        # Start the deployment using sync client
        with FastDeployClient() as client:
            try:
                deployment_id = client.start_deployment(
                    target.fastdeploy_service,
                    context,
                )
                run.fastdeploy_deployment_id = deployment_id
                run.status = BackupRunStatus.RUNNING
                run.save(update_fields=["fastdeploy_deployment_id", "status"])

            except DeploymentStartError as e:
                logger.error(f"Failed to start deployment: {e}")
                _mark_run_failed(run, str(e))
                raise BackupError(f"Failed to start backup deployment: {e}") from e

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
                    raise BackupError("Deployment disappeared during execution")
                except FastDeployError as e:
                    logger.warning(f"Error polling deployment status: {e}")
                    continue  # Retry on transient errors

                if status.is_finished:
                    return _handle_deployment_finished(run, status, client)

                logger.debug(
                    f"Backup {run.id} still running (elapsed: {elapsed}s, timeout: {timeout}s)"
                )

            # Timeout reached
            logger.error(f"Backup {run.id} timed out after {timeout}s")
            _mark_run_timeout(run)
            raise BackupTimeoutError(
                f"Backup timed out after {timeout} seconds"
            )

    except (BackupError, BackupTimeoutError):
        raise
    except Exception as e:
        logger.exception(f"Unexpected error during backup: {e}")
        _mark_run_failed(run, str(e))
        raise BackupError(f"Unexpected error: {e}") from e
    finally:
        # Clean up DB connections when done
        close_old_connections()


def _build_backup_context(target: BackupTarget, run: BackupRun) -> dict:
    """Build the context dictionary to pass to FastDeploy."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")

    return {
        "ECHOPORT_TARGET": target.name,
        "ECHOPORT_RUN_ID": str(run.id),
        "ECHOPORT_DB_PATH": target.db_path,
        "ECHOPORT_BACKUP_FILES": ",".join(target.backup_files) if target.backup_files else "",
        "ECHOPORT_BUCKET": target.storage_bucket,
        "ECHOPORT_KEY_PREFIX": f"{target.name}/{timestamp}",
        "ECHOPORT_TIMESTAMP": timestamp,
    }


def _handle_deployment_finished(
    run: BackupRun,
    status,
    client: FastDeployClient,
) -> BackupRun:
    """Handle a finished deployment and update the run record."""

    # Collect logs from steps
    logs = _collect_step_logs(status.steps)
    run.logs = logs

    if status.is_successful:
        # Parse the backup result from step messages (not raw logs)
        # The result is embedded in a step's message field as ECHOPORT_RESULT:{json}
        result = client.parse_echoport_result(status.steps)

        if result and result.success:
            run.status = BackupRunStatus.SUCCESS
            run.storage_key = result.key
            run.size_bytes = result.size_bytes
            run.checksum_sha256 = result.checksum_sha256
            run.file_count = result.file_count
            logger.info(
                f"Backup {run.id} completed successfully: {result.key} "
                f"({result.size_bytes} bytes, {result.file_count} files)"
            )
        elif result and not result.success:
            run.status = BackupRunStatus.FAILED
            run.error_message = result.error or "Backup reported failure"
            logger.error(f"Backup {run.id} reported failure: {result.error}")
        else:
            # No ECHOPORT_RESULT found but deployment succeeded
            # This might happen if the backup script didn't output the result
            run.status = BackupRunStatus.SUCCESS
            logger.warning(
                f"Backup {run.id} deployment succeeded but no ECHOPORT_RESULT found"
            )
    else:
        # Deployment failed
        failed_step = status.failed_step
        error_msg = failed_step.get("message", "Unknown error") if failed_step else "Deployment failed"
        run.status = BackupRunStatus.FAILED
        run.error_message = error_msg
        logger.error(f"Backup {run.id} deployment failed: {error_msg}")

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


def _mark_run_failed(run: BackupRun, error_message: str) -> None:
    """Mark a backup run as failed."""
    run.status = BackupRunStatus.FAILED
    run.error_message = error_message
    run.finished_at = timezone.now()
    run.save()


def _mark_run_timeout(run: BackupRun) -> None:
    """Mark a backup run as timed out."""
    run.status = BackupRunStatus.TIMEOUT
    run.error_message = f"Backup timed out after {run.target.timeout_seconds} seconds"
    run.finished_at = timezone.now()
    run.save()


def get_active_run(target: BackupTarget) -> BackupRun | None:
    """Get the currently active backup run for a target, if any."""
    return target.runs.filter(
        status__in=[BackupRunStatus.PENDING, BackupRunStatus.RUNNING]
    ).first()
