"""GPU usage chart widget with switchable metrics."""

from __future__ import annotations

from textual.widgets import Static

CHART_MODES = ["gpu_hours", "all_nodes", "full_nodes"]
MODE_TITLES = {
    "gpu_hours": "GPU Usage by User (GPU-hours)",
    "all_nodes": "Nodes by User (All: full + partial)",
    "full_nodes": "Nodes by User (Full/Exclusive only)",
}


class GpuChart(Static):
    """GPU usage bar chart with switchable metric mode."""

    DEFAULT_CSS = """
    GpuChart {
        height: 1fr;
        min-height: 8;
    }
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._rows: list[dict] = []
        self._mode = "gpu_hours"

    def set_data(self, rows: list[dict]) -> None:
        self._rows = rows
        self._render_chart()

    def cycle_mode(self) -> None:
        idx = CHART_MODES.index(self._mode)
        self._mode = CHART_MODES[(idx + 1) % len(CHART_MODES)]
        self._render_chart()

    def _render_chart(self) -> None:
        if not self._rows:
            self.update(" No data. Run 'slurmmon-cli collect' first.")
            return

        title = MODE_TITLES.get(self._mode, "Chart")

        # Extract values based on mode
        entries: list[tuple[str, int]] = []
        for r in self._rows[:20]:
            user = r.get("user", "?")
            if self._mode == "gpu_hours":
                val = r.get("gpu_tres_mins", 0) // 60
            elif self._mode == "all_nodes":
                val = r.get("full_nodes", 0) + r.get("partial_nodes", 0)
            elif self._mode == "full_nodes":
                val = r.get("full_nodes", 0)
            else:
                val = 0
            entries.append((user, val))

        # Filter zeros for node modes
        if self._mode in ("all_nodes", "full_nodes"):
            entries = [(u, v) for u, v in entries if v > 0]

        if not entries:
            self.update(f" {title}\n\n (no data for this metric)")
            return

        max_val = max(v for _, v in entries) or 1
        bar_width = 40

        lines = [f" {title}  [press 'c' to switch]\n"]
        for user, val in entries:
            ratio = val / max_val
            filled = int(ratio * bar_width)
            bar = "\u2588" * filled + "\u2591" * (bar_width - filled)
            lines.append(f" {user:<14} {bar} {val:>8,}\n")

        self.update("".join(lines))
