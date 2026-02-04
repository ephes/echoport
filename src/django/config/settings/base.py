"""
Django settings for Echoport project.

For more information on this file, see
https://docs.djangoproject.com/en/5.2/topics/settings/
"""

from pathlib import Path
from typing import Any
from warnings import filterwarnings

import environ

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Site root is two levels up from BASE_DIR (src/django -> src -> site root)
SITE_ROOT = BASE_DIR.parent.parent

# Load env file - check site root first (production), then BASE_DIR (local dev)
env = environ.Env(
    DEBUG=(bool, False)
)
env_file = SITE_ROOT / ".env"
if not env_file.exists():
    env_file = BASE_DIR / ".env"
if env_file.exists():
    environ.Env.read_env(str(env_file))

ALLOWED_HOSTS: list[str] = []

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = "django-insecure-echoport-change-me-in-production"

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

# Application definition
DJANGO_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.humanize",  # For intcomma, naturaltime, etc.
]

THIRD_PARTY_APPS: list[str] = [
    "django_htmx",
]

LOCAL_APPS = [
    "backups.apps.BackupsConfig",
]

INSTALLED_APPS = DJANGO_APPS + THIRD_PARTY_APPS + LOCAL_APPS

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "django_htmx.middleware.HtmxMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES: list[dict[str, Any]] = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "config.wsgi.application"

# Database
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    }
}

# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

# Authentication
LOGIN_REDIRECT_URL = "/"
LOGOUT_REDIRECT_URL = "/"

# Internationalization
LANGUAGE_CODE = "en-us"
TIME_ZONE = "Europe/Berlin"
USE_I18N = True
USE_TZ = True

# Static files (CSS, JavaScript, Images)
STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
STATICFILES_DIRS: list[str] = []

# Default primary key field type
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

MEDIA_ROOT = BASE_DIR / "media"

STORAGES = {
    "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
    "staticfiles": {"BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage"},
}

ADMIN_URL = "admin/"

filterwarnings(
    "ignore", "The FORMS_URLFIELD_ASSUME_HTTPS transitional setting is deprecated."
)
FORMS_URLFIELD_ASSUME_HTTPS = True

# Echoport paths
ECHOPORT_CACHE_DIR = env("ECHOPORT_CACHE_DIR", default="")  # For scheduler lock file

# Path allowlist for backup targets
# Only paths under these prefixes can be configured for backup
# Configurable to support different deployment layouts
ECHOPORT_ALLOWED_PATH_PREFIXES = env.list(
    "ECHOPORT_ALLOWED_PATH_PREFIXES",
    default=["/home/", "/opt/", "/var/lib/"]
)

# MinIO settings (for retention cleanup)
MINIO_MC_PATH = env("MINIO_MC_PATH", default="/usr/local/bin/mc")
MINIO_ALIAS = env("MINIO_ALIAS", default="minio")

# FastDeploy integration settings
# Default endpoint (used when target has no fastdeploy_endpoint_key)
FASTDEPLOY_BASE_URL = env("FASTDEPLOY_BASE_URL", default="http://localhost:8000")
FASTDEPLOY_SERVICE_TOKEN = env("FASTDEPLOY_SERVICE_TOKEN", default="")
FASTDEPLOY_POLL_INTERVAL = 5  # seconds
FASTDEPLOY_DEFAULT_TIMEOUT = 600  # seconds (10 minutes)

# Per-target FastDeploy endpoints (keyed by endpoint name)
# Maps endpoint_key -> {"base_url": "...", "token": "..."}
# Tokens should be loaded from env vars (SOPS-backed in production).
# Example: FASTDEPLOY_ENDPOINTS = {"staging": {"base_url": "...", "token": "..."}}
FASTDEPLOY_ENDPOINTS: dict[str, dict[str, str]] = {}

# Load staging endpoint if configured
_staging_url = env("FASTDEPLOY_STAGING_BASE_URL", default="")
_staging_token = env("FASTDEPLOY_STAGING_SERVICE_TOKEN", default="")
if _staging_url and _staging_token:
    FASTDEPLOY_ENDPOINTS["staging"] = {
        "base_url": _staging_url,
        "token": _staging_token,
    }

# Logging configuration
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "{levelname} {asctime} {module} {message}",
            "style": "{",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
    },
    "root": {
        "handlers": ["console"],
        "level": "INFO",
    },
    "loggers": {
        "backups": {
            "handlers": ["console"],
            "level": "DEBUG",
            "propagate": False,
        },
    },
}
