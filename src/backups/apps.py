from django.apps import AppConfig


class BackupsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "backups"

    def ready(self):
        from backups import checks  # noqa: F401  (registers the system checks)
