import json

import pytest


@pytest.fixture
def backup_target(db):
    """Create a test backup target."""
    from backups.models import BackupTarget

    return BackupTarget.objects.create(
        name="test-target",
        description="Test backup target",
        fastdeploy_service="echoport-backup",
        service_name="test-target.service",
        db_path="/tmp/test.db",
        backup_files=["/tmp/test.txt"],
        schedule="0 2 * * *",
        status="active",
    )


def running_status(deployment_id: int = 7):
    """A FastDeploy deployment that has not finished yet."""
    from backups.fastdeploy_client import DeploymentStatus

    return DeploymentStatus(
        id=deployment_id,
        service_id=1,
        started="2026-01-01T00:00:00",
        finished=None,
        steps=[{"name": "start", "state": "running", "message": ""}],
    )


def finished_status(steps: list[dict], deployment_id: int = 7):
    """A finished FastDeploy deployment with the given steps."""
    from backups.fastdeploy_client import DeploymentStatus

    return DeploymentStatus(
        id=deployment_id,
        service_id=1,
        started="2026-01-01T00:00:00",
        finished="2026-01-01T00:01:00",
        steps=steps,
    )


def result_step(payload: dict, state: str = "success") -> dict:
    """A step whose message carries an ECHOPORT_RESULT payload."""
    return {
        "name": "result",
        "state": state,
        "message": "ECHOPORT_RESULT:" + json.dumps(payload),
    }


class FakeFastDeploy:
    """
    In-memory stand-in for FastDeployClient.

    Scripted with a deployment id (or a start error) and a sequence of
    status responses. Each entry is a DeploymentStatus to return or an
    exception to raise; once the sequence is exhausted the last entry is
    repeated (an empty sequence means "still running"). Result parsing
    uses the real ECHOPORT_RESULT parser.
    """

    def __init__(self):
        self.deployment_id = 7
        self.start_error: Exception | None = None
        self.statuses: list = []
        self.client_kwargs: list[dict] = []
        self.started: list[tuple[str, dict]] = []
        self.polled: list[int] = []
        self.sleeps: list[float] = []
        self.exited = 0

    # Used in place of the FastDeployClient class: "constructing" returns self.
    def __call__(self, base_url=None, service_token=None, timeout=30.0):
        self.client_kwargs.append({"base_url": base_url, "service_token": service_token})
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.exited += 1

    def start_deployment(self, service_name, context=None):
        self.started.append((service_name, context))
        if self.start_error is not None:
            raise self.start_error
        return self.deployment_id

    def get_deployment_status(self, deployment_id):
        self.polled.append(deployment_id)
        if not self.statuses:
            response = running_status(deployment_id)
        elif len(self.statuses) == 1:
            response = self.statuses[0]
        else:
            response = self.statuses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    @staticmethod
    def parse_echoport_result(steps):
        from backups.fastdeploy_client import FastDeployClient

        return FastDeployClient.parse_echoport_result(steps)


@pytest.fixture
def fake_fastdeploy(monkeypatch, settings):
    """Replace FastDeployClient and sleeping in both engines with a scripted fake."""
    fake = FakeFastDeploy()
    settings.FASTDEPLOY_POLL_INTERVAL = 1
    monkeypatch.setattr("backups.backup_engine.FastDeployClient", fake)
    monkeypatch.setattr("backups.restore_engine.FastDeployClient", fake)
    # Both engines call time.sleep() through the shared time module; record
    # the requested intervals instead of sleeping.
    monkeypatch.setattr("backups.backup_engine.time.sleep", fake.sleeps.append)
    return fake
