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
# Exclusive-use node: special border style (cyan background for single-user full nodes)
_EXCL_GREEN = Style(color="white", bgcolor="dark_green", bold=True)
_EXCL_YELLOW = Style(color="black", bgcolor="dark_goldenrod", bold=True)
_EXCL_RED = Style(color="white", bgcolor="dark_red", bold=True)
_CELL_WIDTH = 10
_LINES_PER_NODE = 3  # name, user, load%


def _is_exclusive(n: NodeUtilization) -> bool:
    """Check if a node is dedicated to a single user (full allocation)."""
    return (
        len(n.users) == 1
        and n.cpus_alloc > 0
        and n.cpus_alloc >= n.cpus_total * 0.9  # >= 90% of CPUs allocated
    )


def _load_style(n: NodeUtilization) -> Style:
    if n.load_ratio is None or n.cpus_alloc == 0:
        return _GRAY
    exclusive = _is_exclusive(n)
    if n.load_ratio >= 0.8:
        return _EXCL_GREEN if exclusive else _GREEN
    if n.load_ratio >= 0.5:
        return _EXCL_YELLOW if exclusive else _YELLOW
    return _EXCL_RED if exclusive else _RED


class NodeHeatmap(Widget):
    """Grid of nodes colored by CPU load ratio.

    Exclusive-use nodes (single user, >=90% CPUs) are shown in bold
    with darker background to distinguish them from shared nodes.
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
        self._cols = 1

    def set_data(self, nodes: list[NodeUtilization], show_users: bool = False) -> None:
        self._node_data = sorted(nodes, key=lambda n: n.name)
        self._show_users = show_users
        self.refresh()

    def get_content_height(self, container, viewport, width) -> int:
        if not self._node_data:
            return 2
        self._cols = max(1, width // _CELL_WIDTH)
        rows = (len(self._node_data) + self._cols - 1) // self._cols
        return rows * _LINES_PER_NODE + 1  # +1 for header/legend

    def render_line(self, y: int) -> Strip:
        width = self.size.width
        if not self._node_data:
            return Strip([Segment(" No node data", Style(color="yellow"))])

        self._cols = max(1, width // _CELL_WIDTH)

        if y == 0:
            return Strip([
                Segment(" Nodes: ", Style(bold=True)),
                Segment("\u2588 >=80% ", _GREEN),
                Segment(" "),
                Segment("\u2588 50-80% ", _YELLOW),
                Segment(" "),
                Segment("\u2588 <50% ", _RED),
                Segment(" "),
                Segment("\u2588 idle ", _GRAY),
                Segment(" "),
                Segment("[bold]=exclusive[/] ", Style(bold=True)),
            ])

        data_y = y - 1
        row_idx = data_y // _LINES_PER_NODE
        line_in_cell = data_y % _LINES_PER_NODE

        segments: list[Segment] = [Segment(" ")]
        for col in range(self._cols):
            idx = row_idx * self._cols + col
            if idx >= len(self._node_data):
                break
            n = self._node_data[idx]
            style = _load_style(n)
            exclusive = _is_exclusive(n)

            if line_in_cell == 0:
                # Line 1: node name (with exclusive marker)
                name = n.name[-6:] if len(n.name) > 6 else n.name
                if exclusive:
                    name = f"*{name}"
                cell = f" {name:^{_CELL_WIDTH - 2}} "
            elif line_in_cell == 1:
                # Line 2: user(s)
                if n.users:
                    uname = n.users[0][:6]
                    if len(n.users) > 1:
                        uname = f"{uname}+{len(n.users) - 1}"
                else:
                    uname = "-"
                cell = f" {uname:^{_CELL_WIDTH - 2}} "
            else:
                # Line 3: load percentage
                if n.load_ratio is not None and n.cpus_alloc > 0:
                    pct = f"{n.load_ratio * 100:.0f}%"
                else:
                    pct = "--"
                cell = f" {pct:^{_CELL_WIDTH - 2}} "

            segments.append(Segment(cell, style))

        return Strip(segments)
