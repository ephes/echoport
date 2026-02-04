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
