"""Tests for per-target FastDeploy endpoint configuration."""

import pytest
from unittest.mock import patch

from backups.fastdeploy_client import (
    get_fastdeploy_config,
    EndpointConfigError,
)


class TestGetFastdeployConfig:
    """Tests for get_fastdeploy_config function."""

    def test_empty_key_returns_none_values(self):
        """Empty endpoint key returns None values (uses defaults)."""
        config = get_fastdeploy_config("")
        assert config == {"base_url": None, "token": None}

    def test_none_key_returns_none_values(self):
        """None endpoint key returns None values (uses defaults)."""
        config = get_fastdeploy_config(None)
        assert config == {"base_url": None, "token": None}

    @patch("backups.fastdeploy_client.settings")
    def test_valid_endpoint_key_returns_config(self, mock_settings):
        """Valid endpoint key returns configured base_url and token."""
        mock_settings.FASTDEPLOY_ENDPOINTS = {
            "staging": {
                "base_url": "https://staging.example.com",
                "token": "staging-token-123",
            }
        }

        config = get_fastdeploy_config("staging")

        assert config == {
            "base_url": "https://staging.example.com",
            "token": "staging-token-123",
        }

    @patch("backups.fastdeploy_client.settings")
    def test_missing_endpoint_key_raises_error(self, mock_settings):
        """Missing endpoint key raises EndpointConfigError."""
        mock_settings.FASTDEPLOY_ENDPOINTS = {
            "production": {"base_url": "https://prod.example.com", "token": "prod-token"}
        }

        with pytest.raises(EndpointConfigError) as exc_info:
            get_fastdeploy_config("staging")

        assert "staging" in str(exc_info.value)
        assert "not found" in str(exc_info.value)
        assert "production" in str(exc_info.value)  # Shows available endpoints

    @patch("backups.fastdeploy_client.settings")
    def test_incomplete_endpoint_missing_url_raises_error(self, mock_settings):
        """Endpoint with missing base_url raises EndpointConfigError."""
        mock_settings.FASTDEPLOY_ENDPOINTS = {
            "staging": {"token": "staging-token"}  # Missing base_url
        }

        with pytest.raises(EndpointConfigError) as exc_info:
            get_fastdeploy_config("staging")

        assert "incomplete" in str(exc_info.value)
        assert "base_url" in str(exc_info.value)

    @patch("backups.fastdeploy_client.settings")
    def test_incomplete_endpoint_missing_token_raises_error(self, mock_settings):
        """Endpoint with missing token raises EndpointConfigError."""
        mock_settings.FASTDEPLOY_ENDPOINTS = {
            "staging": {"base_url": "https://staging.example.com"}  # Missing token
        }

        with pytest.raises(EndpointConfigError) as exc_info:
            get_fastdeploy_config("staging")

        assert "incomplete" in str(exc_info.value)
        assert "token" in str(exc_info.value)

    @patch("backups.fastdeploy_client.settings")
    def test_incomplete_endpoint_empty_values_raises_error(self, mock_settings):
        """Endpoint with empty string values raises EndpointConfigError."""
        mock_settings.FASTDEPLOY_ENDPOINTS = {
            "staging": {"base_url": "", "token": ""}
        }

        with pytest.raises(EndpointConfigError) as exc_info:
            get_fastdeploy_config("staging")

        assert "incomplete" in str(exc_info.value)

    @patch("backups.fastdeploy_client.settings")
    def test_no_endpoints_configured_raises_error(self, mock_settings):
        """Endpoint key with no FASTDEPLOY_ENDPOINTS raises EndpointConfigError."""
        mock_settings.FASTDEPLOY_ENDPOINTS = {}

        with pytest.raises(EndpointConfigError) as exc_info:
            get_fastdeploy_config("staging")

        assert "staging" in str(exc_info.value)
        assert "(none)" in str(exc_info.value)


class TestServiceTokens:
    """Tests for per-service token selection."""

    @patch("backups.fastdeploy_client.settings")
    def test_service_token_used_when_available(self, mock_settings):
        """Service-specific token is used when configured."""
        mock_settings.FASTDEPLOY_ENDPOINTS = {
            "staging": {
                "base_url": "https://staging.example.com",
                "token": "default-token",
                "service_tokens": {
                    "marina-backup": "marina-backup-token",
                    "marina-restore": "marina-restore-token",
                },
            }
        }

        config = get_fastdeploy_config("staging", "marina-backup")

        assert config == {
            "base_url": "https://staging.example.com",
            "token": "marina-backup-token",
        }

    @patch("backups.fastdeploy_client.settings")
    def test_default_token_used_when_service_not_in_service_tokens(self, mock_settings):
        """Default token is used when service is not in service_tokens."""
        mock_settings.FASTDEPLOY_ENDPOINTS = {
            "staging": {
                "base_url": "https://staging.example.com",
                "token": "default-token",
                "service_tokens": {
                    "other-service": "other-token",
                },
            }
        }

        config = get_fastdeploy_config("staging", "marina-backup")

        assert config == {
            "base_url": "https://staging.example.com",
            "token": "default-token",
        }

    @patch("backups.fastdeploy_client.settings")
    def test_default_token_used_when_no_service_tokens_configured(self, mock_settings):
        """Default token is used when service_tokens is not configured."""
        mock_settings.FASTDEPLOY_ENDPOINTS = {
            "staging": {
                "base_url": "https://staging.example.com",
                "token": "default-token",
            }
        }

        config = get_fastdeploy_config("staging", "marina-backup")

        assert config == {
            "base_url": "https://staging.example.com",
            "token": "default-token",
        }

    @patch("backups.fastdeploy_client.settings")
    def test_default_token_used_when_service_name_is_none(self, mock_settings):
        """Default token is used when service_name is None."""
        mock_settings.FASTDEPLOY_ENDPOINTS = {
            "staging": {
                "base_url": "https://staging.example.com",
                "token": "default-token",
                "service_tokens": {
                    "marina-backup": "marina-backup-token",
                },
            }
        }

        config = get_fastdeploy_config("staging", None)

        assert config == {
            "base_url": "https://staging.example.com",
            "token": "default-token",
        }

    @patch("backups.fastdeploy_client.settings")
    def test_error_when_no_default_and_service_not_found(self, mock_settings):
        """Error is raised when no default token and service not in service_tokens."""
        mock_settings.FASTDEPLOY_ENDPOINTS = {
            "staging": {
                "base_url": "https://staging.example.com",
                "service_tokens": {
                    "other-service": "other-token",
                },
            }
        }

        with pytest.raises(EndpointConfigError) as exc_info:
            get_fastdeploy_config("staging", "marina-backup")

        assert "incomplete" in str(exc_info.value)
        assert "token" in str(exc_info.value)
        assert "marina-backup" in str(exc_info.value)

    @patch("backups.fastdeploy_client.settings")
    def test_service_tokens_only_config_works(self, mock_settings):
        """Endpoint with only service_tokens (no default) works for known services."""
        mock_settings.FASTDEPLOY_ENDPOINTS = {
            "staging": {
                "base_url": "https://staging.example.com",
                "service_tokens": {
                    "marina-backup": "marina-backup-token",
                },
            }
        }

        config = get_fastdeploy_config("staging", "marina-backup")

        assert config == {
            "base_url": "https://staging.example.com",
            "token": "marina-backup-token",
        }

    @patch("backups.fastdeploy_client.settings")
    def test_service_tokens_null_falls_back_to_default(self, mock_settings):
        """service_tokens: null (from YAML) falls back to default token."""
        mock_settings.FASTDEPLOY_ENDPOINTS = {
            "staging": {
                "base_url": "https://staging.example.com",
                "token": "default-token",
                "service_tokens": None,  # Explicit null from YAML
            }
        }

        config = get_fastdeploy_config("staging", "marina-backup")

        assert config == {
            "base_url": "https://staging.example.com",
            "token": "default-token",
        }

    @patch("backups.fastdeploy_client.settings")
    def test_service_tokens_invalid_type_raises_error(self, mock_settings):
        """service_tokens with invalid type raises EndpointConfigError."""
        mock_settings.FASTDEPLOY_ENDPOINTS = {
            "staging": {
                "base_url": "https://staging.example.com",
                "token": "default-token",
                "service_tokens": "not-a-dict",  # Invalid type
            }
        }

        with pytest.raises(EndpointConfigError) as exc_info:
            get_fastdeploy_config("staging", "marina-backup")

        assert "invalid service_tokens" in str(exc_info.value)
        assert "expected dict" in str(exc_info.value)
        assert "str" in str(exc_info.value)


class TestTokenOverride:
    """Tests for token_override parameter."""

    @patch("backups.fastdeploy_client.settings")
    def test_token_override_with_endpoint_key(self, mock_settings):
        """token_override should override endpoint token."""
        mock_settings.FASTDEPLOY_ENDPOINTS = {
            "staging": {
                "base_url": "https://staging.example.com",
                "token": "endpoint-token",
            }
        }

        config = get_fastdeploy_config("staging", token_override="my-override-token")

        assert config == {
            "base_url": "https://staging.example.com",
            "token": "my-override-token",
        }

    def test_token_override_without_endpoint_key(self):
        """token_override without endpoint_key returns override with None base_url."""
        config = get_fastdeploy_config(token_override="my-override-token")

        assert config == {
            "base_url": None,
            "token": "my-override-token",
        }

    @patch("backups.fastdeploy_client.settings")
    def test_token_override_with_endpoint_that_has_no_token(self, mock_settings):
        """token_override should work even if endpoint has no token."""
        mock_settings.FASTDEPLOY_ENDPOINTS = {
            "staging": {
                "base_url": "https://staging.example.com",
                # No token at all
            }
        }

        config = get_fastdeploy_config("staging", token_override="my-override-token")

        assert config == {
            "base_url": "https://staging.example.com",
            "token": "my-override-token",
        }

    @patch("backups.fastdeploy_client.settings")
    def test_token_override_with_endpoint_missing_base_url_raises(self, mock_settings):
        """token_override with endpoint missing base_url should still raise."""
        mock_settings.FASTDEPLOY_ENDPOINTS = {
            "staging": {
                "token": "endpoint-token",
                # No base_url
            }
        }

        with pytest.raises(EndpointConfigError) as exc_info:
            get_fastdeploy_config("staging", token_override="my-override-token")

        assert "base_url" in str(exc_info.value)

    @patch("backups.fastdeploy_client.settings")
    def test_empty_token_override_does_not_override(self, mock_settings):
        """Empty string token_override should not override."""
        mock_settings.FASTDEPLOY_ENDPOINTS = {
            "staging": {
                "base_url": "https://staging.example.com",
                "token": "endpoint-token",
            }
        }

        config = get_fastdeploy_config("staging", token_override="")

        assert config == {
            "base_url": "https://staging.example.com",
            "token": "endpoint-token",
        }

    def test_empty_token_override_without_endpoint_returns_defaults(self):
        """Empty token_override without endpoint key returns None defaults."""
        config = get_fastdeploy_config(token_override="")

        assert config == {"base_url": None, "token": None}
