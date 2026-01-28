"""
FastDeploy API client for Echoport.

Provides synchronous HTTP client for triggering deployments and polling status.
"""

import json
import logging
import re
from dataclasses import dataclass
from typing import Any

import httpx
from django.conf import settings

logger = logging.getLogger(__name__)


class FastDeployError(Exception):
    """Base exception for FastDeploy client errors."""

    pass


class DeploymentStartError(FastDeployError):
    """Failed to start a deployment."""

    pass


class DeploymentNotFoundError(FastDeployError):
    """Deployment not found."""

    pass


@dataclass
class DeploymentStatus:
    """Status of a FastDeploy deployment."""

    id: int
    service_id: int
    started: str | None
    finished: str | None
    steps: list[dict[str, Any]]

    @property
    def is_finished(self) -> bool:
        return self.finished is not None

    @property
    def is_successful(self) -> bool:
        if not self.is_finished:
            return False
        # Check if all steps completed successfully
        for step in self.steps:
            if step.get("state") not in ("success", "skipped"):
                return False
        return True

    @property
    def failed_step(self) -> dict[str, Any] | None:
        """Return the first failed step, if any."""
        for step in self.steps:
            if step.get("state") == "failure":
                return step
        return None


@dataclass
class BackupResult:
    """
    Parsed result from ECHOPORT_RESULT in deployment step messages.

    Note: The full manifest is stored in the tarball, not transmitted here.
    This keeps the payload small to avoid FastDeploy's 4KB message truncation.
    """

    success: bool
    bucket: str
    key: str
    size_bytes: int
    checksum_sha256: str
    file_count: int = 0
    error: str | None = None


class FastDeployClient:
    """
    Synchronous HTTP client for FastDeploy API.

    Usage:
        with FastDeployClient() as client:
            deployment_id = client.start_deployment("my-service", {"key": "value"})
            status = client.get_deployment_status(deployment_id)
    """

    def __init__(
        self,
        base_url: str | None = None,
        service_token: str | None = None,
        timeout: float = 30.0,
    ):
        self.base_url = (base_url or settings.FASTDEPLOY_BASE_URL).rstrip("/")
        self.service_token = service_token or settings.FASTDEPLOY_SERVICE_TOKEN
        self.timeout = timeout
        self._client: httpx.Client | None = None

    def __enter__(self):
        self._client = httpx.Client(
            base_url=self.base_url,
            timeout=self.timeout,
            headers={
                "Authorization": f"Bearer {self.service_token}",
                "Content-Type": "application/json",
            },
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            self._client.close()
            self._client = None

    @property
    def client(self) -> httpx.Client:
        if self._client is None:
            raise RuntimeError("Client not initialized. Use 'with' context manager.")
        return self._client

    def start_deployment(
        self,
        service_name: str,
        context: dict[str, Any] | None = None,
    ) -> int:
        """
        Start a new deployment on FastDeploy.

        Args:
            service_name: Name of the registered FastDeploy service
            context: Context dictionary to pass to the deployment script

        Returns:
            Deployment ID

        Raises:
            DeploymentStartError: If deployment fails to start
        """
        payload = {"env": context or {}}

        logger.info(f"Starting deployment for service '{service_name}'")
        logger.debug(f"Deployment context: {context}")

        try:
            response = self.client.post(
                "/deployments/",
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
            deployment_id = data["id"]
            logger.info(f"Deployment started with ID: {deployment_id}")
            return deployment_id

        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to start deployment: {e.response.status_code} - {e.response.text}")
            raise DeploymentStartError(f"HTTP {e.response.status_code}: {e.response.text}") from e
        except httpx.RequestError as e:
            logger.error(f"Request error starting deployment: {e}")
            raise DeploymentStartError(str(e)) from e

    def get_deployment_status(self, deployment_id: int) -> DeploymentStatus:
        """
        Get the current status of a deployment.

        Args:
            deployment_id: The deployment ID to check

        Returns:
            DeploymentStatus object

        Raises:
            DeploymentNotFoundError: If deployment doesn't exist
        """
        try:
            response = self.client.get(f"/deployments/{deployment_id}")
            response.raise_for_status()
            data = response.json()

            return DeploymentStatus(
                id=data["id"],
                service_id=data["service_id"],
                started=data.get("started"),
                finished=data.get("finished"),
                steps=data.get("steps", []),
            )

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise DeploymentNotFoundError(f"Deployment {deployment_id} not found") from e
            raise FastDeployError(f"HTTP {e.response.status_code}: {e.response.text}") from e
        except httpx.RequestError as e:
            raise FastDeployError(str(e)) from e

    @staticmethod
    def parse_echoport_result(steps: list[dict[str, Any]]) -> BackupResult | None:
        """
        Parse ECHOPORT_RESULT from deployment steps.

        The backup script emits ECHOPORT_RESULT as a step with the JSON
        payload in the message field, prefixed with "ECHOPORT_RESULT:".

        This approach ensures the result reaches Echoport because FastDeploy
        only captures valid JSON lines (step messages), not arbitrary output.

        Args:
            steps: List of step dictionaries from deployment status

        Returns:
            BackupResult if found, None otherwise
        """
        pattern = r"ECHOPORT_RESULT:(\{.*\})"

        # Search through all step messages for ECHOPORT_RESULT
        for step in steps:
            message = step.get("message", "")
            if not message:
                continue

            match = re.search(pattern, message)
            if match:
                try:
                    data = json.loads(match.group(1))
                    return BackupResult(
                        success=data.get("success", False),
                        bucket=data.get("bucket", ""),
                        key=data.get("key", ""),
                        size_bytes=data.get("size_bytes", 0),
                        checksum_sha256=data.get("checksum_sha256", ""),
                        file_count=data.get("file_count", 0),
                        error=data.get("error"),
                    )
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse ECHOPORT_RESULT JSON: {e}")
                    continue

        logger.warning("No ECHOPORT_RESULT found in step messages")
        return None
