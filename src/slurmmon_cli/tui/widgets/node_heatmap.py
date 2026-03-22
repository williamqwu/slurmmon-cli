"""Node utilization heatmap widget - color-coded grid of nodes."""

from __future__ import annotations

from rich.segment import Segment
from rich.style import Style
from textual.strip import Strip
from textual.widget import Widget

from slurmmon_cli.models import NodeUtilization

# Colors for load ratio ranges
_GREEN = Style(color="white", bgcolor="green")
_YELLOW = Style(color="black", bgcolor="yellow")
_RED = Style(color="white", bgcolor="red")
_GRAY = Style(color="white", bgcolor="#444444")
_CELL_WIDTH = 10


def _load_style(n: NodeUtilization) -> Style:
    if n.load_ratio is None or n.cpus_alloc == 0:
        return _GRAY
    if n.load_ratio >= 0.8:
        return _GREEN
    if n.load_ratio >= 0.5:
        return _YELLOW
    return _RED


class NodeHeatmap(Widget):
    """Grid of nodes colored by CPU load ratio."""

    DEFAULT_CSS = """
    NodeHeatmap {
        height: auto;
        min-height: 3;
    }
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._node_data: list[NodeUtilization] = []
        self._cols = 1

    def set_data(self, nodes: list[NodeUtilization]) -> None:
        self._node_data = sorted(nodes, key=lambda n: n.name)
        self._stale = True
        self.refresh()

    def get_content_height(self, container, viewport, width) -> int:
        if not self._node_data:
            return 2
        self._cols = max(1, width // _CELL_WIDTH)
        rows = (len(self._node_data) + self._cols - 1) // self._cols
        return rows * 2 + 1  # 2 lines per row (name + pct) + header

    def render_line(self, y: int) -> Strip:
        width = self.size.width
        if not self._node_data:
            return Strip([Segment(" No node data", Style(color="yellow"))])

        self._cols = max(1, width // _CELL_WIDTH)

        if y == 0:
            # Header line
            legend = (
                " Node Utilization: "
                "[\u2588 >=80%] "
                "[\u2588 50-80%] "
                "[\u2588 <50%] "
                "[\u2588 idle]"
            )
            return Strip([
                Segment(" Node Utilization: ", Style(bold=True)),
                Segment("\u2588 >=80% ", _GREEN),
                Segment(" "),
                Segment("\u2588 50-80% ", _YELLOW),
                Segment(" "),
                Segment("\u2588 <50% ", _RED),
                Segment(" "),
                Segment("\u2588 idle", _GRAY),
            ])

        data_y = y - 1
        row_idx = data_y // 2
        is_pct_line = data_y % 2 == 1

        segments: list[Segment] = [Segment(" ")]
        for col in range(self._cols):
            idx = row_idx * self._cols + col
            if idx >= len(self._node_data):
                break
            n = self._node_data[idx]
            style = _load_style(n)

            if is_pct_line:
                # Second line: load percentage
                if n.load_ratio is not None and n.cpus_alloc > 0:
                    pct = f"{n.load_ratio * 100:.0f}%"
                else:
                    pct = "--"
                cell = f" {pct:^{_CELL_WIDTH - 2}} "
            else:
                # First line: node name
                name = n.name[-6:] if len(n.name) > 6 else n.name
                cell = f" {name:^{_CELL_WIDTH - 2}} "

            segments.append(Segment(cell, style))

        return Strip(segments)
