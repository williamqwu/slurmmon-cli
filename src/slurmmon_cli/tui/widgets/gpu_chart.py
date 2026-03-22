"""GPU usage chart widget with textual-plotext fallback."""

from __future__ import annotations

from textual.widgets import Static

try:
    from textual_plotext import PlotextPlot
    HAS_PLOTEXT = True
except ImportError:
    HAS_PLOTEXT = False


class GpuChart(Static):
    """GPU usage bar chart. Uses plotext if available, text bars otherwise."""

    DEFAULT_CSS = """
    GpuChart {
        height: 1fr;
        min-height: 8;
    }
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._rows: list[dict] = []

    def set_data(self, rows: list[dict]) -> None:
        self._rows = rows
        self._render_chart()

    def _render_chart(self) -> None:
        if not self._rows:
            self.update(" No GPU usage data. Run 'slurmmon-cli collect' first.")
            return

        if HAS_PLOTEXT:
            self._render_plotext()
        else:
            self._render_text()

    def _render_plotext(self) -> None:
        # For plotext, we render as text since we're extending Static
        # A full PlotextPlot widget would need to be composed, not embedded
        # Fall back to text rendering which works in a Static
        self._render_text()

    def _render_text(self) -> None:
        if not self._rows:
            return

        max_val = max((r.get("gpu_tres_mins", 0) for r in self._rows), default=1)
        if max_val == 0:
            max_val = 1
        bar_width = 40

        lines = [" GPU Usage by User (GPU-hours)\n"]
        for r in self._rows[:20]:
            user = r.get("user", "?")
            gpu_hrs = r.get("gpu_tres_mins", 0) // 60
            ratio = r.get("gpu_tres_mins", 0) / max_val
            filled = int(ratio * bar_width)
            bar = "\u2588" * filled + "\u2591" * (bar_width - filled)
            lines.append(f" {user:<14} {bar} {gpu_hrs:>8,}\n")

        self.update("".join(lines))
