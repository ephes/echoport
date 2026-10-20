"""Short shared writer reservation; never hold it across external commands."""
from django.db import connection
from django.db.models import F

from .models import BackupTarget


def lock_target(target_id: int) -> None:
    # PostgreSQL also coordinates with backup_engine's existing target lock.
    if connection.features.has_select_for_update:
        BackupTarget.objects.select_for_update(nowait=True).get(pk=target_id)
    else:
        # The first deciding operation is a write, acquiring SQLite's writer
        # reservation even when the value is unchanged. Stale snapshots fail closed.
        BackupTarget.objects.filter(pk=target_id).update(id=F("id"))
