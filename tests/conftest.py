from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def squeue_data() -> dict:
    return json.loads((FIXTURE_DIR / "squeue_output.json").read_text())


@pytest.fixture
def sacct_data() -> dict:
    return json.loads((FIXTURE_DIR / "sacct_output.json").read_text())


@pytest.fixture
def sinfo_data() -> dict:
    return json.loads((FIXTURE_DIR / "sinfo_output.json").read_text())


@pytest.fixture
def mock_slurm(monkeypatch, squeue_data, sacct_data, sinfo_data):
    """Patch subprocess.run to return fixture Slurm JSON."""

    def fake_run(cmd, **kwargs):
        cmd_str = " ".join(cmd)
        if "squeue" in cmd_str:
            data = json.dumps(squeue_data)
        elif "sacct" in cmd_str:
            data = json.dumps(sacct_data)
        elif "sinfo" in cmd_str:
            data = json.dumps(sinfo_data)
        elif "seff" in cmd_str:
            data = (
                "Job ID: 4349800\n"
                "Cluster: ascend\n"
                "User/Group: alice/pas2979\n"
                "State: COMPLETED (exit code 0)\n"
                "Cores: 4\n"
                "CPU Utilized: 86:06:40\n"
                "CPU Efficiency: 89.66% of 96:00:00 core-walltime\n"
                "Job Wall-clock time: 24:00:00\n"
                "Memory Utilized: 8.00 GB\n"
                "Memory Efficiency: 50.00% of 16.00 GB\n"
            )
            return subprocess.CompletedProcess(cmd, 0, data, "")
        else:
            return subprocess.CompletedProcess(cmd, 1, "", "command not found")
        return subprocess.CompletedProcess(cmd, 0, data, "")

    monkeypatch.setattr("subprocess.run", fake_run)


@pytest.fixture
def tmp_db(tmp_path):
    """Return a path for a temporary SQLite database."""
    return str(tmp_path / "test.db")
