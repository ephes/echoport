"""
Management command to ensure a superuser exists.

Creates the superuser if it doesn't exist, updates password only if changed.
Credentials are passed via environment variables for security.

Usage:
    ADMIN_USERNAME=admin ADMIN_EMAIL=admin@example.com ADMIN_PASSWORD=secret \
        python manage.py ensure_superuser
"""

import os

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = "Ensure a superuser exists (creates if missing, updates password only if changed)"

    def handle(self, *args, **options):
        User = get_user_model()

        username = os.environ.get("ADMIN_USERNAME")
        email = os.environ.get("ADMIN_EMAIL", "")
        password = os.environ.get("ADMIN_PASSWORD")

        if not username:
            raise CommandError("ADMIN_USERNAME environment variable required")

        if not password:
            raise CommandError("ADMIN_PASSWORD environment variable required")

        user = User.objects.filter(username=username).first()
        if user:
            changed = False

            # Only update password if it changed (avoids session invalidation)
            if not user.check_password(password):
                user.set_password(password)
                changed = True

            if not user.is_staff:
                user.is_staff = True
                changed = True

            if not user.is_superuser:
                user.is_superuser = True
                changed = True

            if email and user.email != email:
                user.email = email
                changed = True

            if changed:
                user.save()
                self.stdout.write(self.style.SUCCESS(f"Updated superuser '{username}'"))
            else:
                self.stdout.write(f"Superuser '{username}' already up to date")
        else:
            User.objects.create_superuser(username=username, email=email, password=password)
            self.stdout.write(self.style.SUCCESS(f"Created superuser '{username}'"))
