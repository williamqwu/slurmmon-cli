"""Cluster summary widget showing node counts and CPU utilization."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.widgets import Static, ProgressBar
from textual.containers import Horizontal

from slurmmon_cli.models import ClusterInfo


class ClusterSummary(Static):
    """Displays cluster node/CPU/job summary."""

    def update_data(self, info: ClusterInfo | None, running: int, pending: int) -> None:
        if info is None:
            self.update("Cluster info unavailable")
            return
        pct = info.alloc_cpus / info.total_cpus * 100 if info.total_cpus > 0 else 0
        bar_w = 30
        filled = int(pct / 100 * bar_w)
        bar = "\u2588" * filled + "\u2591" * (bar_w - filled)

        text = (
            f" Nodes: {info.alloc_nodes}/{info.total_nodes} alloc"
            f"  {info.idle_nodes} idle  {info.mixed_nodes} mixed"
            f"  {info.down_nodes} down\n"
            f" CPUs:  [{bar}] {pct:.0f}%\n"
            f" Jobs:  {running} running  {pending} pending"
        )
        self.update(text)
