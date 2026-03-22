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
        elif "scontrol" in cmd_str and "node" in cmd_str:
            data = (FIXTURE_DIR / "scontrol_nodes_output.json").read_text()
        elif "sshare" in cmd_str:
            data = (FIXTURE_DIR / "sshare_output.txt").read_text()
            return subprocess.CompletedProcess(cmd, 0, data, "")
        elif "sinfo" in cmd_str:
            data = json.dumps(sinfo_data)
        elif "osc-seff" in cmd_str:
            data = (
                "Job ID: 4349800\n"
                "Cluster: cardinal\n"
                "User/Group: alice/pas2979\n"
                "State: COMPLETED (exit code 0)\n"
                "Nodes: 1\n"
                "Cores per Node: 4\n"
                "CPU Utilized: 86:06:40\n"
                "CPU Efficiency: 89.66% of 96:00:00 core-walltime\n"
                "Job Wall-clock time: 24:00:00\n"
                "Memory Utilized: 8.00 GB\n"
                "Memory Efficiency: 50.00% of 16.00 GB\n"
                "GPUs per Node: 2\n"
                "Total GPUs: 2\n"
                "GPU Memory Utilized: 30.00 GB\n"
                "GPU Memory Efficiency: 75.00% of 40.00 GB\n"
                "GPU Utilization: 20:00:00\n"
                "GPU Efficiency: 83.33% of 24:00:00 GPU-walltime\n"
            )
            return subprocess.CompletedProcess(cmd, 0, data, "")
        elif "gpu-seff" in cmd_str:
            data = json.dumps({
                "job_id": "4349800",
                "gpus": [
                    {"gpu_id": 0, "memory_used_mb": 15360,
                     "memory_total_mb": 20480,
                     "utilization_pct": 82.0},
                    {"gpu_id": 1, "memory_used_mb": 15360,
                     "memory_total_mb": 20480,
                     "utilization_pct": 84.0},
                ],
                "total_gpu_memory_used_mb": 30720,
                "total_gpu_memory_mb": 40960,
                "avg_gpu_utilization_pct": 83.0,
            })
            return subprocess.CompletedProcess(cmd, 0, data, "")
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
