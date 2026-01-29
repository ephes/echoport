"""
Custom template tags for the backups app.
"""

from datetime import datetime

from croniter import CroniterBadCronError, CroniterBadDateError, croniter
from django import template
from django.utils import timezone

register = template.Library()


@register.simple_tag
def next_scheduled_run(target):
    """
    Calculate the next scheduled run datetime for a backup target.

    Usage:
        {% load backup_tags %}
        {% next_scheduled_run target as next_run %}
        {% if next_run %}
            Next: {{ next_run|date:"M j, H:i" }}
        {% endif %}

    Returns None if the target has no schedule or an invalid schedule.
    """
    if not target.schedule:
        return None

    try:
        now = timezone.now()
        cron = croniter(target.schedule, now)
        return cron.get_next(datetime)
    except (KeyError, ValueError, CroniterBadCronError, CroniterBadDateError):
        # Invalid cron expression
        return None
