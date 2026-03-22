"""Node utilization heatmap widget - color-coded grid of nodes."""

from __future__ import annotations

from rich.segment import Segment
from rich.style import Style
from textual.strip import Strip
from textual.widget import Widget

from slurmmon_cli.models import NodeUtilization

# Colors for utilization ranges
_GREEN = Style(color="black", bgcolor="green")
_YELLOW = Style(color="black", bgcolor="yellow")
_RED = Style(color="white", bgcolor="red")
_GRAY = Style(color="white", bgcolor="#444444")
_HEADER_STYLE = Style(color="cyan", bold=True)
_CELL_WIDTH = 10
_LINES_PER_NODE = 3  # name, user, metric

SORT_MODES = ["name", "load_asc", "load_desc", "users"]
SORT_LABELS = {
    "name": "Sort: name",
    "load_asc": "Sort: metric (worst first)",
    "load_desc": "Sort: metric (best first)",
    "users": "Sort: user count",
}

VIEW_MODES = ["cpu_load", "memory", "gpu_alloc"]
VIEW_LABELS = {
    "cpu_load": "View: CPU load",
    "memory": "View: Memory usage",
    "gpu_alloc": "View: GPU allocation",
}


def _is_exclusive(n: NodeUtilization) -> bool:
    return (
        len(n.users) == 1
        and n.cpus_alloc > 0
        and n.cpus_alloc >= n.cpus_total * 0.9
    )


def _ratio_style(ratio: float | None) -> Style:
    """Color style based on a 0-1 utilization ratio."""
    if ratio is None:
        return _GRAY
    if ratio >= 0.8:
        return _GREEN
    if ratio >= 0.5:
        return _YELLOW
    return _RED


def _get_node_metric(n: NodeUtilization, view: str) -> tuple[float | None, str]:
    """Get (ratio, display_string) for a node based on view mode."""
    if view == "cpu_load":
        ratio = n.load_ratio if n.cpus_alloc > 0 else None
        if ratio is not None:
            label = f"{ratio * 100:.0f}%"
        else:
            label = "--"
    elif view == "memory":
        if n.mem_total_mb > 0 and n.mem_alloc_mb > 0:
            ratio = n.mem_alloc_mb / n.mem_total_mb
            mem_gb = n.mem_alloc_mb / 1024
            label = f"{mem_gb:.0f}G"
        else:
            ratio = None
            label = "--"
    elif view == "gpu_alloc":
        if n.gpus_total > 0:
            ratio = n.gpus_alloc / n.gpus_total
            label = f"{n.gpus_alloc}/{n.gpus_total}"
        elif n.cpus_alloc > 0:
            # Non-GPU node that is allocated
            ratio = None
            label = "no gpu"
        else:
            ratio = None
            label = "--"
    else:
        ratio = None
        label = "--"
    return ratio, label


def _render_node_cell(n: NodeUtilization, line_in_cell: int,
                      view: str) -> tuple[str, Style]:
    """Render one line of a node cell. Returns (cell_text, style)."""
    ratio, metric_label = _get_node_metric(n, view)
    style = _ratio_style(ratio) if n.cpus_alloc > 0 else _GRAY
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
        if exclusive:
            fill = "\u2500" * (inner_w - len(metric_label))
            cell = f"\u2514{metric_label}{fill}\u2518"
        else:
            cell = f" {metric_label:^{inner_w}} "

    return cell, style


class NodeHeatmap(Widget):
    """Grid of nodes colored by utilization metric, grouped by partition.

    View modes: CPU load, memory usage, GPU allocation.
    Exclusive-use nodes get box-drawing borders.
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
        self._view_mode = "cpu_load"
        self._group_by_partition = True
        self._partition_filter: set[str] | None = None
        self._available_partitions: list[str] = []
        self._cols = 1
        self._render_lines: list[list[tuple[str, Style]]] = []

    def set_data(self, nodes: list[NodeUtilization], show_users: bool = False) -> None:
        self._all_nodes = list(nodes)
        self._show_users = show_users
        parts: set[str] = set()
        for n in nodes:
            parts.update(n.partitions)
        self._available_partitions = sorted(parts)
        self._rebuild()

    def _sort_key(self, n: NodeUtilization):
        """Sort key based on current view mode metric."""
        ratio, _ = _get_node_metric(n, self._view_mode)
        return ratio if ratio is not None else 999

    def _apply_sort(self, nodes: list[NodeUtilization]) -> list[NodeUtilization]:
        if self._sort_mode == "name":
            return sorted(nodes, key=lambda n: n.name)
        elif self._sort_mode == "load_asc":
            return sorted(nodes, key=self._sort_key)
        elif self._sort_mode == "load_desc":
            return sorted(nodes, key=lambda n: -self._sort_key(n) if self._sort_key(n) != 999 else 999)
        elif self._sort_mode == "users":
            return sorted(nodes, key=lambda n: len(n.users), reverse=True)
        return nodes

    def _rebuild(self) -> None:
        self._render_lines = []
        width = self.size.width if self.size.width > 0 else 120
        self._cols = max(1, width // _CELL_WIDTH)

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
        filter_info = ""
        if self._partition_filter:
            filter_info = f"  [{','.join(sorted(self._partition_filter))}]"
        else:
            filter_info = "  [all partitions]"

        view_label = VIEW_LABELS.get(self._view_mode, "")
        sort_label = SORT_LABELS.get(self._sort_mode, "")

        self._render_lines.append([
            (" ", Style()),
            ("\u2588>=80%", _GREEN), (" ", Style()),
            ("\u2588 50-80%", _YELLOW), (" ", Style()),
            ("\u2588<50%", _RED), (" ", Style()),
            ("\u2588idle", _GRAY),
            ("  \u250c\u2500\u2510=exclusive(1 user, full node) ", Style(bold=True)),
            (view_label, Style(color="cyan")),
            ("  ", Style()),
            (sort_label, Style(dim=True)),
            (filter_info, Style(dim=True)),
        ])

        if self._group_by_partition:
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
        cols = self._cols
        for row_start in range(0, len(nodes), cols):
            row_nodes = nodes[row_start:row_start + cols]
            for line_idx in range(_LINES_PER_NODE):
                line: list[tuple[str, Style]] = [(" ", Style())]
                for n in row_nodes:
                    cell, style = _render_node_cell(n, line_idx, self._view_mode)
                    line.append((cell, style))
                self._render_lines.append(line)

    def cycle_sort(self) -> None:
        idx = SORT_MODES.index(self._sort_mode)
        self._sort_mode = SORT_MODES[(idx + 1) % len(SORT_MODES)]
        self._rebuild()

    def cycle_view(self) -> None:
        """Cycle between CPU load, memory, GPU allocation views."""
        idx = VIEW_MODES.index(self._view_mode)
        self._view_mode = VIEW_MODES[(idx + 1) % len(VIEW_MODES)]
        self._rebuild()

    def cycle_partition(self) -> None:
        if not self._available_partitions:
            return
        if self._partition_filter is None:
            self._partition_filter = {self._available_partitions[0]}
        else:
            current = sorted(self._partition_filter)
            if len(current) == 1:
                idx = self._available_partitions.index(current[0])
                next_idx = idx + 1
                if next_idx < len(self._available_partitions):
                    self._partition_filter = {self._available_partitions[next_idx]}
                else:
                    self._partition_filter = None
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
