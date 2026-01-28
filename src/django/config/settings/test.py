"""
Test settings for Echoport.
"""

from .base import *  # noqa: F401, F403

DEBUG = False

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

# Faster password hashing for tests
PASSWORD_HASHERS = [
    "django.contrib.auth.hashers.MD5PasswordHasher",
]

# Use a test FastDeploy URL
FASTDEPLOY_BASE_URL = "http://testserver:8000"
FASTDEPLOY_SERVICE_TOKEN = "test-token"
