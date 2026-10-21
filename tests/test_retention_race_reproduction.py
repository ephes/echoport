"""Baseline-only evidence probe: distinct processes own disposable SQLite files."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

PROBE = Path(__file__).parent / "reproductions/retention_race/probe.py"


@pytest.mark.parametrize("pathway", ["ui", "direct"])
@pytest.mark.parametrize("ordering", ["race", "restore-first", "cleanup-first"])
def test_actual_baseline_interleavings(pathway, ordering):
    environment = {"PATH": os.defpath, "PYTHONDONTWRITEBYTECODE": "1"}
    result = subprocess.run(
        [sys.executable, str(PROBE), "--pathway", pathway, "--ordering", ordering],
        env=environment,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    summary = json.loads(result.stdout.splitlines()[-1])
    assert summary["baseline_revision"] == "51678035ef9c2b1299b4229220118ff7e7fbb7eb"
    assert summary["file_backed_sqlite"] and summary["separate_connections"]
    assert summary["external_calls"] == summary["owned_thread_survivors"] == 0
    assert summary["dangling_metadata"] is (ordering == "race")
    if ordering == "race":
        assert summary["backup_status"] == "success"
        assert summary["cleanup_result"] == "error"
        assert summary["restore_statuses"] == (
            ["pending"] if pathway == "ui" else ["failed"]
        )
    elif ordering == "restore-first":
        assert summary["fake_object_present"]
        assert summary["cleanup_result"] == "skipped"
    else:
        assert summary["cleanup_result"] == "deleted"
        assert not summary["backup_retained"] and not summary["restore_statuses"]
