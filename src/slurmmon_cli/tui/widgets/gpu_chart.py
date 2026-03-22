"""GPU usage chart widget with switchable metrics and keyboard navigation."""

from __future__ import annotations

from rich.segment import Segment
from rich.style import Style
from textual.events import Key
from textual.message import Message
from textual.strip import Strip
from textual.widget import Widget

CHART_MODES = ["gpu_hours", "all_nodes", "full_nodes"]
MODE_TITLES = {
    "gpu_hours": "GPU Usage by User (GPU-hours)",
    "all_nodes": "Nodes by User (All: full + partial)",
    "full_nodes": "Nodes by User (Full/Exclusive only)",
}

_NORMAL = Style()
_SELECTED = Style(color="white", bgcolor="blue", bold=True)
_TITLE = Style(color="cyan", bold=True)
_DIM = Style(dim=True)
_BAR_FULL = "\u2588"
_BAR_EMPTY = "\u2591"
_BAR_WIDTH = 40


class GpuChart(Widget, can_focus=True):
    """GPU usage bar chart with switchable metric, keyboard nav, and Enter."""

    class UserSelected(Message):
        """Posted when the user presses Enter on a chart bar."""
        def __init__(self, user: str, account: str | None) -> None:
            self.user = user
            self.account = account
            super().__init__()

    DEFAULT_CSS = """
    GpuChart {
        height: 1fr;
        min-height: 8;
    }
    GpuChart:focus {
        border: tall $accent;
    }
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._rows: list[dict] = []
        self._mode = "gpu_hours"
        self._entries: list[tuple[str, int, dict]] = []  # (user, value, raw_row)
        self._selected_idx: int = 0
        self._render_lines: list[list[tuple[str, Style]]] = []

    def set_data(self, rows: list[dict]) -> None:
        self._rows = rows
        self._rebuild()

    def cycle_mode(self) -> None:
        idx = CHART_MODES.index(self._mode)
        self._mode = CHART_MODES[(idx + 1) % len(CHART_MODES)]
        self._rebuild()

    def _rebuild(self) -> None:
        self._render_lines = []
        self._entries = []

        if not self._rows:
            self._render_lines.append([
                (" Collecting data... press [r] to refresh.", _DIM),
            ])
            self.refresh()
            return

        title = MODE_TITLES.get(self._mode, "Chart")

        # Extract values based on mode
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
            self._entries.append((user, val, r))

        # Filter zeros for node modes
        if self._mode in ("all_nodes", "full_nodes"):
            self._entries = [(u, v, r) for u, v, r in self._entries if v > 0]

        if not self._entries:
            self._render_lines.append([
                (f" {title}", _TITLE),
            ])
            self._render_lines.append([])
            self._render_lines.append([
                (" (no data for this metric)", _DIM),
            ])
            self.refresh()
            return

        # Clamp selection
        self._selected_idx = min(self._selected_idx, len(self._entries) - 1)

        max_val = max(v for _, v, _ in self._entries) or 1

        # Title line
        self._render_lines.append([
            (f" {title}", _TITLE),
        ])

        # Bar lines
        for i, (user, val, _row) in enumerate(self._entries):
            is_sel = (i == self._selected_idx) and self.has_focus
            ratio = val / max_val
            filled = int(ratio * _BAR_WIDTH)
            bar = _BAR_FULL * filled + _BAR_EMPTY * (_BAR_WIDTH - filled)
            style = _SELECTED if is_sel else _NORMAL
            self._render_lines.append([
                (f" {user:<14} {bar} {val:>8,} ", style),
            ])

        self.refresh()

    def on_key(self, event: Key) -> None:
        if not self._entries:
            return
        if event.key == "down":
            self._selected_idx = min(self._selected_idx + 1, len(self._entries) - 1)
            self._rebuild()
            event.stop()
        elif event.key == "up":
            self._selected_idx = max(self._selected_idx - 1, 0)
            self._rebuild()
            event.stop()
        elif event.key == "enter":
            _, _, row = self._entries[self._selected_idx]
            self.post_message(self.UserSelected(
                user=row.get("user", "?"),
                account=row.get("account"),
            ))
            event.stop()

    def on_focus(self, _) -> None:
        self._rebuild()

    def on_blur(self, _) -> None:
        self._rebuild()

    def get_content_height(self, container, viewport, width) -> int:
        return max(3, len(self._render_lines))

    def render_line(self, y: int) -> Strip:
        if y >= len(self._render_lines):
            return Strip([])
        line = self._render_lines[y]
        return Strip([Segment(text, style) for text, style in line])
