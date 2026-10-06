"""
Management command to report FastDeploy service tokens that need attention.

Lists the token every non-disabled backup target would use, plus the global
FASTDEPLOY_SERVICE_TOKEN, with its source, expiry, legacy flag and status
(ok, expiring, expired, legacy, undecodable, missing). Exits 1 when any
token needs attention, so it can run from cron or a monitor.

Tokens are decoded without verification and only ``exp``, the presence of
``jti`` and the ``service`` claim are shown. Token values are never printed.

Usage:
    python manage.py check_service_tokens
    python manage.py check_service_tokens --warn-days 30
"""

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.utils import timezone

from backups.models import BackupStatus, BackupTarget
from backups.service_tokens import (
    DEFAULT_WARN_DAYS,
    TokenReport,
    assess_global_token,
    assess_target,
)


def _format_row(name: str, report: TokenReport) -> str:
    expires = report.expires_at.strftime("%Y-%m-%d %H:%M UTC") if report.expires_at else "-"
    legacy = "yes" if report.is_legacy else "no"
    service = report.service or "-"
    line = (
        f"{name}: status={report.status} source={report.source} "
        f"expires={expires} legacy={legacy} service={service}"
    )
    if report.detail:
        line += f" ({report.detail})"
    return line


class Command(BaseCommand):
    help = "Report FastDeploy service tokens that are legacy, expiring, expired or unusable"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--warn-days",
            type=int,
            default=DEFAULT_WARN_DAYS,
            help=f"Flag tokens expiring within this many days (default {DEFAULT_WARN_DAYS})",
        )

    def handle(self, *args, **options) -> None:
        warn_days = options["warn_days"]
        if warn_days < 0:
            raise CommandError("--warn-days must not be negative")
        now = timezone.now()

        rows: list[tuple[str, TokenReport]] = [
            ("(global)", assess_global_token(now=now, warn_days=warn_days))
        ]
        targets = BackupTarget.objects.exclude(status=BackupStatus.DISABLED).order_by("name")
        for target in targets:
            rows.append((target.name, assess_target(target, now=now, warn_days=warn_days)))

        attention = 0
        for name, report in rows:
            line = _format_row(name, report)
            if report.needs_attention:
                attention += 1
                self.stdout.write(self.style.WARNING(line))
            else:
                self.stdout.write(line)

        if attention:
            self.stderr.write(
                self.style.ERROR(f"{attention} service token(s) need attention")
            )
            raise SystemExit(1)
        self.stdout.write(self.style.SUCCESS("All service tokens ok"))
