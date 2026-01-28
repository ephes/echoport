from django.urls import path

from . import views

app_name = "backups"

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("targets/<int:target_id>/", views.target_detail, name="target_detail"),
    path("runs/<int:run_id>/", views.run_detail, name="run_detail"),
    path(
        "targets/<int:target_id>/backup/",
        views.trigger_backup,
        name="trigger_backup",
    ),
    path(
        "targets/<int:target_id>/status/",
        views.backup_status,
        name="backup_status",
    ),
]
