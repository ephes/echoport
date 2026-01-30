"""
MinIO client wrapper using the mc CLI tool.

This module provides a thin wrapper around the MinIO mc CLI for operations
needed by the retention cleanup system. It matches the approach used in
the backup.py script which also shells out to mc for uploads.
"""

import json
import logging
import subprocess

from django.conf import settings

logger = logging.getLogger(__name__)


def _get_mc_path() -> str:
    """Get the path to the mc CLI tool."""
    return getattr(settings, "MINIO_MC_PATH", "/usr/local/bin/mc")


def _get_minio_alias() -> str:
    """Get the MinIO alias configured in mc."""
    return getattr(settings, "MINIO_ALIAS", "minio")


def _is_object_not_found_error(output: str) -> bool:
    """
    Check if mc --json output indicates an object-not-found error.

    Uses structured JSON parsing to accurately detect when deletion failed
    because the object was already deleted, without false positives for
    bucket/alias/host errors.

    mc rm --json outputs lines like:
    {"status":"error","error":{"message":"Object does not exist.","cause":{"error":{"Code":"NoSuchKey",...}}}}

    Note: mc may emit non-JSON lines (warnings, progress) mixed with JSON.
    We parse each line separately and continue scanning on decode errors.
    """
    for line in output.strip().split("\n"):
        if not line:
            continue

        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            # Non-JSON line (warning, progress, etc.) - skip and continue
            continue

        try:
            if data.get("status") == "error":
                error = data.get("error", {})
                cause = error.get("cause", {}).get("error", {})

                # Check for S3 NoSuchKey error code (most reliable)
                if cause.get("Code") == "NoSuchKey":
                    return True

                # Fallback: check message for object-specific patterns
                # These are more specific than generic "not found"
                message = error.get("message", "").lower()
                if "object does not exist" in message:
                    return True
        except (KeyError, TypeError, AttributeError):
            # Malformed JSON structure - skip and continue
            continue

    return False


def delete_object(bucket: str, key: str) -> bool:
    """
    Delete an object from MinIO.

    Args:
        bucket: The bucket name (e.g., "backups")
        key: The object key (e.g., "nyxmon/2026-01-28T02-00-00.tar.gz")

    Returns:
        True if deletion succeeded or object already doesn't exist, False otherwise
    """
    mc_path = _get_mc_path()
    alias = _get_minio_alias()
    object_path = f"{alias}/{bucket}/{key}"

    try:
        result = subprocess.run(
            [mc_path, "rm", "--json", object_path],
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode == 0:
            logger.info(f"Deleted object from MinIO: {object_path}")
            return True

        # Check for object-not-found using structured JSON output
        # This is idempotent - previous run may have deleted MinIO object
        # but failed to delete DB record
        #
        # Note: mc --json may emit to stdout or stderr depending on version,
        # so we check both streams for the NoSuchKey error
        if _is_object_not_found_error(result.stdout) or _is_object_not_found_error(result.stderr):
            logger.info(
                f"Object already deleted from MinIO (idempotent): {object_path}"
            )
            return True

        # Log the actual error for debugging
        logger.error(
            f"Failed to delete object from MinIO: {object_path} - "
            f"stdout: {result.stdout.strip()}, stderr: {result.stderr.strip()}"
        )
        return False

    except subprocess.TimeoutExpired:
        logger.error(f"Timeout deleting object from MinIO: {object_path}")
        return False
    except FileNotFoundError:
        logger.error(f"mc CLI not found at {mc_path}")
        return False
    except Exception as e:
        logger.error(f"Error deleting object from MinIO: {object_path} - {e}")
        return False


def object_exists(bucket: str, key: str) -> bool:
    """
    Check if an object exists in MinIO.

    Args:
        bucket: The bucket name (e.g., "backups")
        key: The object key (e.g., "nyxmon/2026-01-28T02-00-00.tar.gz")

    Returns:
        True if the object exists, False otherwise
    """
    mc_path = _get_mc_path()
    alias = _get_minio_alias()
    object_path = f"{alias}/{bucket}/{key}"

    try:
        result = subprocess.run(
            [mc_path, "stat", "--json", object_path],
            capture_output=True,
            text=True,
            timeout=30,
        )

        # mc stat --json returns 0 if object exists, non-zero otherwise
        return result.returncode == 0

    except subprocess.TimeoutExpired:
        logger.error(f"Timeout checking object in MinIO: {object_path}")
        return False
    except FileNotFoundError:
        logger.error(f"mc CLI not found at {mc_path}")
        return False
    except Exception as e:
        logger.error(f"Error checking object in MinIO: {object_path} - {e}")
        return False
