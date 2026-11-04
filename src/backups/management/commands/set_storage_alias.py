"""
Management command to set the object store a target's retention deletes from.

Usage:
    python manage.py set_storage_alias nyxmon rustfs
    python manage.py set_storage_alias nyxmon paused

The alias is one of MINIO_ALIASES; ``paused`` stores an empty alias, under
which cleanup_old_backups skips the target (while its archives move between
stores, neither store holds all of them).

Every change goes through the same barrier: it holds cleanup_old_backups'
lock file, so no cleanup run and no storage delete is in flight, and refuses
while a run of the target has a pending deletion intent (the next cleanup
finishes it through the old alias; then run this again). A deletion that
retention started before the change would otherwise look at the wrong store,
where "object does not exist" counts as deleted.
"""

from django.core.management.base import BaseCommand, CommandError
from django.db import OperationalError, transaction
from django.utils import timezone

from backups.management.commands.cleanup_old_backups import (
    _format_ids,
    _get_lock_file_path,
    acquire_cleanup_lock,
    release_cleanup_lock,
)
from backups.models import STORAGE_ALIAS_PAUSED, BackupRun, BackupTarget, exact_name_hint
from backups.validation import validate_storage_alias

PAUSED = "paused"


def _shown(alias: str) -> str:
    return PAUSED if alias == STORAGE_ALIAS_PAUSED else alias


class Command(BaseCommand):
    help = "Set the mc alias (object store) retention uses for one backup target, or pause its retention"

    def add_arguments(self, parser):
        parser.add_argument("target", help="Backup target name (exact)")
        parser.add_argument("alias", help=f"One of MINIO_ALIASES, or '{PAUSED}'")

    def handle(self, *args, **options):
        name = options["target"]
        wanted = STORAGE_ALIAS_PAUSED if options["alias"] == PAUSED else options["alias"]
        if wanted == STORAGE_ALIAS_PAUSED and options["alias"] != PAUSED:
            raise CommandError(f"Name an alias or '{PAUSED}'.")
        check = validate_storage_alias(wanted)
        if not check.is_valid:
            raise CommandError(check.error)

        try:
            lock_file = acquire_cleanup_lock()
        except OSError as error:
            raise CommandError(f"Failed to acquire lock file {_get_lock_file_path()}: {error}")
        if lock_file is None:
            raise CommandError("cleanup_old_backups is running; nothing changed. Retry when it has finished.")
        try:
            before = self._set(name, wanted)
        finally:
            release_cleanup_lock(lock_file)

        if before == wanted:
            self.stdout.write(f"Storage alias of '{name}' is already {_shown(wanted)}")
        else:
            self.stdout.write(
                self.style.SUCCESS(f"Storage alias of '{name}': {_shown(before)} -> {_shown(wanted)}")
            )

    def _set(self, name: str, wanted: str) -> str:
        """Change the alias under the target lock; returns the alias before."""
        from backups.reservations import lock_target

        try:
            target = BackupTarget.objects.get(name=name)
        except BackupTarget.DoesNotExist:
            raise CommandError(f"Target not found: {name}.{exact_name_hint(name)}")
        try:
            with transaction.atomic(durable=True):
                lock_target(target.pk)
                before = BackupTarget.objects.values_list("storage_alias", flat=True).get(pk=target.pk)
                pending = list(
                    BackupRun.objects.filter(target_id=target.pk, deletion_pending_at__isnull=False)
                    .order_by("pk")
                    .values_list("pk", flat=True)
                )
                if pending:
                    raise CommandError(
                        f"Deletion of backup(s) {_format_ids(pending)} of '{name}' is pending; nothing changed. "
                        f"Finish it with cleanup_old_backups --target {name} (through {_shown(before)}), "
                        "then run this again."
                    )
                if before != wanted:
                    BackupTarget.objects.filter(pk=target.pk).update(storage_alias=wanted, updated_at=timezone.now())
        except OperationalError as error:
            raise CommandError(f"Target '{name}' is locked ({error}); nothing changed. Retry.")
        return before
