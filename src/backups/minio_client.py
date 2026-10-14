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


_OBJECT_NOT_FOUND_MESSAGE = "object does not exist"


def _is_object_not_found_message(message: object) -> bool:
    """Exact, case-insensitive match on mc's "Object does not exist" text."""
    if not isinstance(message, str):
        return False
    return message.strip().rstrip(".").strip().lower() == _OBJECT_NOT_FOUND_MESSAGE


def _is_object_not_found_error(output: str) -> bool:
    """
    Check if mc --json output indicates an object-not-found error.

    Uses structured JSON parsing to accurately detect when deletion failed
    because the object was already deleted, without false positives for
    bucket/alias/host errors.

    Real mc (verified for releases from 2025-01 to 2026-09, including the
    production mc 2025-08-13) prints, with exit code 1:
    {"status":"error","error":{"message":"Failed to remove `minio/b/k`.",
     "cause":{"message":"Object does not exist","error":{}},"type":"error"}}

    An S3-style ``cause.error.Code == "NoSuchKey"`` is accepted as well.
    The message must equal "Object does not exist" (case-insensitive, a
    trailing "." ignored); a substring match is not enough, so an unknown
    alias ("Requested path ... not found") or a missing bucket
    ("Bucket `x` does not exist.") stays an error.

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
                error = data.get("error") or {}
                cause = error.get("cause") or {}
                cause_error = cause.get("error") or {}

                # S3 NoSuchKey error code
                if cause_error.get("Code") == "NoSuchKey":
                    return True

                if _is_object_not_found_message(cause.get("message")) or (
                    _is_object_not_found_message(error.get("message"))
                ):
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


def list_prefix(bucket: str, prefix: str) -> list[str] | None:
    """
    List object keys below ``<prefix>/`` (recursively).

    Returns the keys (relative to the prefix), an empty list when nothing is
    stored below it, or None when mc fails. mc also prints nothing and exits 0
    when the alias is unknown and the path is taken as a missing local
    directory, so an empty list alone does not prove the alias works.
    """
    if not _is_clean_key(prefix):
        logger.error(f"Refusing to list MinIO prefix {bucket}/{prefix!r}")
        return None

    mc_path = _get_mc_path()
    alias = _get_minio_alias()
    folder = f"{alias}/{bucket}/{prefix}/"

    try:
        result = subprocess.run(
            [mc_path, "ls", "--recursive", "--json", folder],
            capture_output=True,
            text=True,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout listing MinIO prefix: {folder}")
        return None
    except FileNotFoundError:
        logger.error(f"mc CLI not found at {mc_path}")
        return None
    except Exception as e:
        logger.error(f"Error listing MinIO prefix: {folder} - {e}")
        return None

    if result.returncode != 0:
        logger.error(
            f"Failed to list MinIO prefix: {folder} - "
            f"stdout: {result.stdout.strip()}, stderr: {result.stderr.strip()}"
        )
        return None

    keys = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            logger.error(f"Unexpected mc ls output for {folder}: {line}")
            return None
        if not isinstance(data, dict) or data.get("status") != "success":
            logger.error(f"mc ls reported an error for {folder}: {line}")
            return None
        keys.append(str(data.get("key", "")))
    return keys


def _is_clean_key(key: str) -> bool:
    """
    True for a key without leading/trailing slash or empty, "." or ".." segments.

    Prefix operations act on exactly ``<key>/``; odd spellings such as
    ``a/b/`` are refused instead of normalized, so the shared-key check in
    cleanup and the mc path always mean the same prefix.
    """
    return bool(key) and all(seg not in ("", ".", "..") for seg in key.split("/"))


def delete_prefix(bucket: str, prefix: str) -> bool:
    """
    Delete everything below ``<prefix>/`` and verify the prefix is empty.

    ``mc rm`` on a prefix without a trailing slash and without ``--recursive``
    reports success but deletes nothing, so this runs
    ``mc rm --recursive --force <alias>/<bucket>/<prefix>/`` and then lists
    the prefix again. Returns True only when the listing is empty.
    """
    # At least <target>/<run>: an empty prefix would empty the bucket and a
    # single segment would remove every run of a target (and its rolling
    # current/ objects).
    if not _is_clean_key(prefix) or len(prefix.split("/")) < 2:
        logger.error(f"Refusing recursive delete of MinIO prefix {bucket}/{prefix!r}")
        return False

    mc_path = _get_mc_path()
    alias = _get_minio_alias()
    folder = f"{alias}/{bucket}/{prefix}/"

    try:
        result = subprocess.run(
            [mc_path, "rm", "--recursive", "--force", "--json", folder],
            capture_output=True,
            text=True,
            timeout=600,
        )
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout deleting MinIO prefix: {folder}")
        return False
    except FileNotFoundError:
        logger.error(f"mc CLI not found at {mc_path}")
        return False
    except Exception as e:
        logger.error(f"Error deleting MinIO prefix: {folder} - {e}")
        return False

    if result.returncode != 0:
        logger.error(
            f"Failed to delete MinIO prefix: {folder} - "
            f"stdout: {result.stdout.strip()}, stderr: {result.stderr.strip()}"
        )
        return False

    remaining = list_prefix(bucket, prefix)
    if remaining is None:
        return False
    if remaining:
        logger.error(
            f"MinIO prefix not empty after delete: {folder} "
            f"({len(remaining)} object(s) left, e.g. {remaining[0]})"
        )
        return False

    logger.info(f"Deleted prefix from MinIO: {folder}")
    return True


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
