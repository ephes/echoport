"""
System checks for Echoport's settings.

Django runs them before most management commands (``migrate``, all of
Echoport's own such as ``cleanup_old_backups`` and the scheduled backups) and
in ``manage.py check``; an error stops the command before it does anything.
"""

from django.conf import settings
from django.core.checks import Error, register

from backups.validation import get_storage_aliases


@register()
def check_minio_alias(app_configs, **kwargs):
    """
    MINIO_ALIAS must be one of MINIO_ALIASES.

    MINIO_ALIAS is the storage_alias of every new target (and of the targets
    migration 0012 found). Outside the allowlist, retention of those targets
    fails closed on every run; an empty MINIO_ALIAS would silently create new
    targets with retention paused. Both are configuration mistakes, caught here
    before anything runs. Pausing a target is ``set_storage_alias <target>
    paused``, never an empty MINIO_ALIAS.
    """
    alias = getattr(settings, "MINIO_ALIAS", None)
    allowed = get_storage_aliases()
    shown = ", ".join(allowed) or "none configured"
    if not isinstance(alias, str) or not alias.strip():
        return [
            Error(
                f"MINIO_ALIAS is empty ({alias!r}); new targets would get it as storage_alias, "
                "which pauses their retention.",
                hint=f"Set MINIO_ALIAS to one of MINIO_ALIASES ({shown}). "
                "Pause one target with manage.py set_storage_alias <target> paused.",
                id="backups.E001",
            )
        ]
    if alias not in allowed:
        return [
            Error(
                f"MINIO_ALIAS {alias!r} is not one of MINIO_ALIASES ({shown}).",
                hint="New targets get MINIO_ALIAS as storage_alias, and retention refuses an alias "
                "outside MINIO_ALIASES. Add it to MINIO_ALIASES or pick one of them.",
                id="backups.E002",
            )
        ]
    return []
