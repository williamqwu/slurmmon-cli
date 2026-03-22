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
_LINES_PER_NODE = 3  # name, user, load%

SORT_MODES = ["name", "load_asc", "load_desc", "users"]
SORT_LABELS = {
    "name": "Sort: name",
    "load_asc": "Sort: load (worst first)",
    "load_desc": "Sort: load (best first)",
    "users": "Sort: user count",
}


def _is_exclusive(n: NodeUtilization) -> bool:
    """Check if a node is dedicated to a single user (full allocation)."""
    return (
        len(n.users) == 1
        and n.cpus_alloc > 0
        and n.cpus_alloc >= n.cpus_total * 0.9
    )


def _load_style(n: NodeUtilization) -> Style:
    if n.load_ratio is None or n.cpus_alloc == 0:
        return _GRAY
    if n.load_ratio >= 0.8:
        return _GREEN
    if n.load_ratio >= 0.5:
        return _YELLOW
    return _RED


class NodeHeatmap(Widget):
    """Grid of nodes colored by CPU load ratio.

    Exclusive-use nodes (single user, >=90% CPUs) get box-drawing borders.
    """

    DEFAULT_CSS = """
    NodeHeatmap {
        height: auto;
        min-height: 3;
    }
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._node_data: list[NodeUtilization] = []
        self._show_users = False
        self._sort_mode = "name"
        self._cols = 1

    def set_data(self, nodes: list[NodeUtilization], show_users: bool = False) -> None:
        self._node_data = list(nodes)
        self._show_users = show_users
        self._apply_sort()
        self.refresh()

    def _apply_sort(self) -> None:
        if self._sort_mode == "name":
            self._node_data.sort(key=lambda n: n.name)
        elif self._sort_mode == "load_asc":
            self._node_data.sort(key=lambda n: n.load_ratio if n.load_ratio is not None else 999)
        elif self._sort_mode == "load_desc":
            self._node_data.sort(key=lambda n: -(n.load_ratio or 0))
        elif self._sort_mode == "users":
            self._node_data.sort(key=lambda n: len(n.users), reverse=True)

    def cycle_sort(self) -> None:
        idx = SORT_MODES.index(self._sort_mode)
        self._sort_mode = SORT_MODES[(idx + 1) % len(SORT_MODES)]
        self._apply_sort()
        self.refresh()

    def get_content_height(self, container, viewport, width) -> int:
        if not self._node_data:
            return 2
        self._cols = max(1, width // _CELL_WIDTH)
        rows = (len(self._node_data) + self._cols - 1) // self._cols
        return rows * _LINES_PER_NODE + 1

    def render_line(self, y: int) -> Strip:
        width = self.size.width
        if not self._node_data:
            return Strip([Segment(" No node data", Style(color="yellow"))])

        self._cols = max(1, width // _CELL_WIDTH)

        if y == 0:
            # Legend + sort mode
            return Strip([
                Segment(" "),
                Segment("\u2588 >=80%", _GREEN),
                Segment(" "),
                Segment("\u2588 50-80%", _YELLOW),
                Segment(" "),
                Segment("\u2588 <50%", _RED),
                Segment(" "),
                Segment("\u2588 idle", _GRAY),
                Segment("  \u250c\u2500\u2510=exclusive  ", Style(bold=True)),
                Segment(SORT_LABELS.get(self._sort_mode, ""), Style(dim=True)),
            ])

        data_y = y - 1
        row_idx = data_y // _LINES_PER_NODE
        line_in_cell = data_y % _LINES_PER_NODE

        segments: list[Segment] = [Segment(" ")]
        cw = _CELL_WIDTH

        for col in range(self._cols):
            idx = row_idx * self._cols + col
            if idx >= len(self._node_data):
                break
            n = self._node_data[idx]
            style = _load_style(n)
            exclusive = _is_exclusive(n)

            inner_w = cw - 2  # space for border chars or padding

            if line_in_cell == 0:
                # Name line
                name = n.name[-inner_w:] if len(n.name) > inner_w else n.name
                if exclusive:
                    # Top border: corner + horizontal line + corner
                    fill = "\u2500" * (inner_w - len(name))
                    cell = f"\u250c{name}{fill}\u2510"
                else:
                    cell = f" {name:^{inner_w}} "
            elif line_in_cell == 1:
                # User line
                if n.users:
                    if exclusive or len(n.users) == 1:
                        uname = n.users[0][:inner_w]
                    else:
                        uname = f"{len(n.users)} users"
                        if len(uname) > inner_w:
                            uname = uname[:inner_w]
                else:
                    uname = "-"
                if exclusive:
                    cell = f"\u2502{uname:^{inner_w}}\u2502"
                else:
                    cell = f" {uname:^{inner_w}} "
            else:
                # Load% line
                if n.load_ratio is not None and n.cpus_alloc > 0:
                    pct = f"{n.load_ratio * 100:.0f}%"
                else:
                    pct = "--"
                if exclusive:
                    fill = "\u2500" * (inner_w - len(pct))
                    cell = f"\u2514{pct}{fill}\u2518"
                else:
                    cell = f" {pct:^{inner_w}} "

            segments.append(Segment(cell, style))

        return Strip(segments)
