"""
Test settings for running the suite against a real PostgreSQL server.

The default test settings use SQLite, which has no row locks, so the
select_for_update branches only run for real here. See README, Tests.
"""

import os

from .test import *  # noqa: F401, F403

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "HOST": os.environ.get("ECHOPORT_TEST_POSTGRES_HOST", "127.0.0.1"),
        "PORT": os.environ.get("ECHOPORT_TEST_POSTGRES_PORT", "5432"),
        "NAME": os.environ.get("ECHOPORT_TEST_POSTGRES_NAME", "echoport"),
        "USER": os.environ.get("ECHOPORT_TEST_POSTGRES_USER", "postgres"),
        "PASSWORD": os.environ.get("ECHOPORT_TEST_POSTGRES_PASSWORD", ""),
    }
}
