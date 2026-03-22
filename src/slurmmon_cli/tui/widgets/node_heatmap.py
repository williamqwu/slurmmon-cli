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
_HEADER_STYLE = Style(color="cyan", bold=True)
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


def _render_node_cell(n: NodeUtilization, line_in_cell: int) -> tuple[str, Style]:
    """Render one line of a node cell. Returns (cell_text, style)."""
    style = _load_style(n)
    exclusive = _is_exclusive(n)
    inner_w = _CELL_WIDTH - 2

    if line_in_cell == 0:
        name = n.name[-inner_w:] if len(n.name) > inner_w else n.name
        if exclusive:
            fill = "\u2500" * (inner_w - len(name))
            cell = f"\u250c{name}{fill}\u2510"
        else:
            cell = f" {name:^{inner_w}} "
    elif line_in_cell == 1:
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
        if n.load_ratio is not None and n.cpus_alloc > 0:
            pct = f"{n.load_ratio * 100:.0f}%"
        else:
            pct = "--"
        if exclusive:
            fill = "\u2500" * (inner_w - len(pct))
            cell = f"\u2514{pct}{fill}\u2518"
        else:
            cell = f" {pct:^{inner_w}} "

    return cell, style


class NodeHeatmap(Widget):
    """Grid of nodes colored by CPU load ratio, grouped by partition.

    Exclusive-use nodes (single user, >=90% CPUs) get box-drawing borders.
    Nodes can be filtered to show only selected partitions.
    """

    DEFAULT_CSS = """
    NodeHeatmap {
        height: auto;
        min-height: 3;
    }
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._all_nodes: list[NodeUtilization] = []
        self._show_users = False
        self._sort_mode = "name"
        self._group_by_partition = True
        self._partition_filter: set[str] | None = None  # None = show all
        self._available_partitions: list[str] = []
        self._cols = 1
        # Pre-computed render lines
        self._render_lines: list[list[tuple[str, Style]]] = []

    def set_data(self, nodes: list[NodeUtilization], show_users: bool = False) -> None:
        self._all_nodes = list(nodes)
        self._show_users = show_users
        # Collect available partitions
        parts: set[str] = set()
        for n in nodes:
            parts.update(n.partitions)
        self._available_partitions = sorted(parts)
        self._rebuild()

    def _apply_sort(self, nodes: list[NodeUtilization]) -> list[NodeUtilization]:
        if self._sort_mode == "name":
            return sorted(nodes, key=lambda n: n.name)
        elif self._sort_mode == "load_asc":
            return sorted(nodes, key=lambda n: n.load_ratio if n.load_ratio is not None else 999)
        elif self._sort_mode == "load_desc":
            return sorted(nodes, key=lambda n: -(n.load_ratio or 0))
        elif self._sort_mode == "users":
            return sorted(nodes, key=lambda n: len(n.users), reverse=True)
        return nodes

    def _rebuild(self) -> None:
        """Rebuild the render line cache."""
        self._render_lines = []
        width = self.size.width if self.size.width > 0 else 120
        self._cols = max(1, width // _CELL_WIDTH)

        # Filter nodes
        filtered = self._all_nodes
        if self._partition_filter:
            filtered = [
                n for n in filtered
                if any(p in self._partition_filter for p in n.partitions)
            ]

        if not filtered:
            self._render_lines.append([(" No nodes to display", Style(color="yellow"))])
            self.refresh()
            return

        # Legend line
        filter_label = ""
        if self._partition_filter:
            filter_label = f"  Showing: {','.join(sorted(self._partition_filter))}"
        else:
            filter_label = "  Showing: all partitions"

        self._render_lines.append([
            (" ", Style()),
            ("\u2588 >=80%", _GREEN), (" ", Style()),
            ("\u2588 50-80%", _YELLOW), (" ", Style()),
            ("\u2588 <50%", _RED), (" ", Style()),
            ("\u2588 idle", _GRAY),
            ("  \u250c\u2500\u2510=exclusive  ", Style(bold=True)),
            (SORT_LABELS.get(self._sort_mode, ""), Style(dim=True)),
            (filter_label, Style(dim=True)),
        ])

        if self._group_by_partition:
            # Group by partition
            part_nodes: dict[str, list[NodeUtilization]] = {}
            seen: set[str] = set()
            partitions_to_show = self._partition_filter or set(self._available_partitions)
            for p in sorted(partitions_to_show):
                pnodes = [n for n in filtered if p in n.partitions and n.name not in seen]
                if pnodes:
                    part_nodes[p] = pnodes
                    for n in pnodes:
                        seen.add(n.name)
            for part_name, nodes in part_nodes.items():
                sorted_nodes = self._apply_sort(nodes)
                count = len(sorted_nodes)
                alloc = sum(1 for n in sorted_nodes if n.cpus_alloc > 0)
                self._render_lines.append([
                    (f" [{part_name}] ({alloc}/{count} allocated)", _HEADER_STYLE),
                ])
                self._add_node_grid(sorted_nodes)
        else:
            sorted_nodes = self._apply_sort(filtered)
            self._add_node_grid(sorted_nodes)

        self.refresh()

    def _add_node_grid(self, nodes: list[NodeUtilization]) -> None:
        """Add grid rows for a list of nodes to _render_lines."""
        cols = self._cols
        for row_start in range(0, len(nodes), cols):
            row_nodes = nodes[row_start:row_start + cols]
            for line_idx in range(_LINES_PER_NODE):
                line: list[tuple[str, Style]] = [(" ", Style())]
                for n in row_nodes:
                    cell, style = _render_node_cell(n, line_idx)
                    line.append((cell, style))
                self._render_lines.append(line)

    def cycle_sort(self) -> None:
        idx = SORT_MODES.index(self._sort_mode)
        self._sort_mode = SORT_MODES[(idx + 1) % len(SORT_MODES)]
        self._rebuild()

    def cycle_partition(self) -> None:
        """Cycle through partition filters: all -> each partition -> all."""
        if not self._available_partitions:
            return
        if self._partition_filter is None:
            # Show first partition only
            self._partition_filter = {self._available_partitions[0]}
        else:
            current = sorted(self._partition_filter)
            if len(current) == 1:
                idx = self._available_partitions.index(current[0])
                next_idx = idx + 1
                if next_idx < len(self._available_partitions):
                    self._partition_filter = {self._available_partitions[next_idx]}
                else:
                    self._partition_filter = None  # back to all
            else:
                self._partition_filter = None
        self._rebuild()

    def get_content_height(self, container, viewport, width) -> int:
        if width != (self._cols * _CELL_WIDTH) and self._all_nodes:
            self._cols = max(1, width // _CELL_WIDTH)
            self._rebuild()
        return max(2, len(self._render_lines))

    def render_line(self, y: int) -> Strip:
        if y >= len(self._render_lines):
            return Strip([])
        line = self._render_lines[y]
        return Strip([Segment(text, style) for text, style in line])
