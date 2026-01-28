"""
Production settings for Echoport.
"""

import os

from .base import *  # noqa: F401, F403

DEBUG = False

SECRET_KEY = env("SECRET_KEY")  # noqa: F405

ALLOWED_HOSTS = env.list("ALLOWED_HOSTS", default=[])  # noqa: F405

# Use whitenoise for static files
STORAGES = {
    "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
    "staticfiles": {
        "BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage"
    },
}

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
] + MIDDLEWARE[1:]  # noqa: F405

# Security settings
SECURE_BROWSER_XSS_FILTER = True
SECURE_CONTENT_TYPE_NOSNIFF = True
X_FRAME_OPTIONS = "DENY"

# CSRF settings
CSRF_COOKIE_SECURE = True
SESSION_COOKIE_SECURE = True

# Database - use PostgreSQL in production if configured
if os.environ.get("DATABASE_URL"):
    import dj_database_url

    DATABASES = {
        "default": dj_database_url.config(conn_max_age=600),
    }
