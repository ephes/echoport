# Generated manually

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("backups", "0004_restorerun"),
    ]

    operations = [
        migrations.AddField(
            model_name="backuptarget",
            name="fastdeploy_endpoint_key",
            field=models.CharField(
                blank=True,
                default="",
                help_text="Key to select FastDeploy endpoint from settings (e.g., 'staging'). If blank, uses default FASTDEPLOY_BASE_URL/TOKEN.",
                max_length=50,
            ),
        ),
        migrations.AddField(
            model_name="backuptarget",
            name="restore_owner",
            field=models.CharField(
                blank=True,
                default="",
                help_text="User:group to chown restored files (e.g., 'marina:marina'). If blank, ownership is not changed after restore.",
                max_length=100,
            ),
        ),
    ]
