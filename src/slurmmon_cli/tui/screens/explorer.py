"""Explorer screen - GPU/resource usage analysis with tabbed views."""

from __future__ import annotations

import json

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Static, TabbedContent, TabPane
from textual import work

from slurmmon_cli.tui.widgets.node_heatmap import NodeHeatmap
from slurmmon_cli.tui.widgets.gpu_chart import GpuChart


class ExplorerScreen(Screen):
    """GPU and resource usage explorer with tabbed analysis views."""

    BINDINGS = [
        Binding("r", "refresh", "Refresh", show=True),
        Binding("o", "cycle_sort", "Sort nodes", show=True),
        Binding("v", "cycle_view", "Node view", show=True),
        Binding("p", "cycle_partition", "Partition filter", show=True),
        Binding("c", "cycle_chart", "Chart mode", show=True),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        with TabbedContent(id="explorer-tabs"):
            with TabPane("GPU Users", id="tab-gpu"):
                yield DataTable(id="gpu-table")
                yield Static(
                    " FAIRSHARE: Slurm scheduling priority (0-1). "
                    "Higher = higher priority. Based on fair share of resources vs recent usage.",
                    id="fairshare-note",
                )
            with TabPane("CPU Users", id="tab-cpu"):
                yield DataTable(id="cpu-table")
            with TabPane("Accounts", id="tab-accounts"):
                yield DataTable(id="account-table")
            with TabPane("Nodes", id="tab-nodes"):
                yield NodeHeatmap(id="node-heatmap")
            with TabPane("GPU Chart", id="tab-chart"):
                yield GpuChart(id="gpu-chart")
        yield Footer()

    def on_mount(self) -> None:
        # GPU users table
        gt = self.query_one("#gpu-table", DataTable)
        gt.add_columns(
            "#", "USER", "ACCOUNT", "GPU-HOURS", "JOBS(R/P)",
            "NODES(F/P)", "FAIRSHARE", "GPU TYPES",
        )
        gt.cursor_type = "row"

        # CPU users table
        ct = self.query_one("#cpu-table", DataTable)
        ct.add_columns("#", "USER", "ACCOUNT", "CPU-HOURS", "GPU-HOURS", "FAIRSHARE")
        ct.cursor_type = "row"

        # Account table
        at = self.query_one("#account-table", DataTable)
        at.add_columns(
            "#", "ACCOUNT", "GPU-HOURS", "CPU-HOURS", "USERS",
            "JOBS(R/P)", "NODES(F/P)",
        )
        at.cursor_type = "row"

        self._load_all_tabs()

    def _load_all_tabs(self) -> None:
        self._load_gpu_data()
        self._load_cpu_data()
        self._load_account_data()
        self._load_node_data()

    @work(thread=True)
    def _load_gpu_data(self) -> None:
        from slurmmon_cli.tui.data import fetch_gpu_rankings
        db_path = getattr(self.app, "db_path", None)
        rows = fetch_gpu_rankings(db_path, "gpu", top=30)
        self.app.call_from_thread(self._update_gpu_table, rows)

    def _update_gpu_table(self, rows: list[dict]) -> None:
        gt = self.query_one("#gpu-table", DataTable)
        gt.clear()
        for i, r in enumerate(rows, 1):
            gpu_hrs = r.get("gpu_tres_mins", 0) // 60
            fair = f"{r['fairshare']:.2f}" if r.get("fairshare") is not None else "-"
            types = self._format_gpu_types(r.get("gpu_type_mins"))
            jr = r.get("gpu_jobs_running", 0)
            jp = r.get("gpu_jobs_pending", 0)
            jobs_str = f"{jr}/{jp}"
            fn = r.get("full_nodes", 0)
            pn = r.get("partial_nodes", 0)
            nodes_str = f"{fn}/{pn}"
            gt.add_row(str(i), r.get("user", "?"), r.get("account", "-"),
                       f"{gpu_hrs:,}", jobs_str, nodes_str, fair, types)

        # Also update chart
        chart = self.query_one("#gpu-chart", GpuChart)
        chart.set_data(rows)

    @work(thread=True)
    def _load_cpu_data(self) -> None:
        from slurmmon_cli.tui.data import fetch_gpu_rankings
        db_path = getattr(self.app, "db_path", None)
        rows = fetch_gpu_rankings(db_path, "cpu", top=30)
        self.app.call_from_thread(self._update_cpu_table, rows)

    def _update_cpu_table(self, rows: list[dict]) -> None:
        ct = self.query_one("#cpu-table", DataTable)
        ct.clear()
        for i, r in enumerate(rows, 1):
            cpu_hrs = r.get("cpu_tres_mins", 0) // 60
            gpu_hrs = r.get("gpu_tres_mins", 0) // 60
            fair = f"{r['fairshare']:.2f}" if r.get("fairshare") is not None else "-"
            ct.add_row(str(i), r.get("user", "?"), r.get("account", "-"),
                       f"{cpu_hrs:,}", f"{gpu_hrs:,}", fair)

    @work(thread=True)
    def _load_account_data(self) -> None:
        from slurmmon_cli.tui.data import fetch_gpu_rankings
        db_path = getattr(self.app, "db_path", None)
        rows = fetch_gpu_rankings(db_path, "account", top=30)
        self.app.call_from_thread(self._update_account_table, rows)

    def _update_account_table(self, rows: list[dict]) -> None:
        at = self.query_one("#account-table", DataTable)
        at.clear()
        for i, r in enumerate(rows, 1):
            gpu_hrs = r.get("gpu_tres_mins", 0) // 60
            cpu_hrs = r.get("cpu_tres_mins", 0) // 60
            jr = r.get("jobs_running", 0)
            jp = r.get("jobs_pending", 0)
            fn = r.get("full_nodes", 0)
            pn = r.get("partial_nodes", 0)
            at.add_row(str(i), r.get("account", "?"),
                       f"{gpu_hrs:,}", f"{cpu_hrs:,}", str(r.get("num_users", 0)),
                       f"{jr}/{jp}", f"{fn}/{pn}")

    @work(thread=True)
    def _load_node_data(self) -> None:
        from slurmmon_cli.tui.data import fetch_node_data
        nodes, _ = fetch_node_data()
        self.app.call_from_thread(self._update_node_heatmap, nodes)

    def _update_node_heatmap(self, nodes) -> None:
        heatmap = self.query_one("#node-heatmap", NodeHeatmap)
        allocated = [n for n in nodes if n.cpus_alloc > 0]
        heatmap.set_data(allocated, show_users=True)

    @staticmethod
    def _format_gpu_types(gpu_type_mins_str: str | None) -> str:
        if not gpu_type_mins_str:
            return "-"
        try:
            types = json.loads(gpu_type_mins_str)
        except (json.JSONDecodeError, TypeError):
            return "-"
        if not types:
            return "-"
        parts = [f"{t}:{v:,}" for t, v in sorted(types.items(), key=lambda x: x[1], reverse=True)]
        return " ".join(parts)

    def action_refresh(self) -> None:
        self._load_all_tabs()

    def action_cycle_sort(self) -> None:
        heatmap = self.query_one("#node-heatmap", NodeHeatmap)
        heatmap.cycle_sort()

    def action_cycle_view(self) -> None:
        heatmap = self.query_one("#node-heatmap", NodeHeatmap)
        heatmap.cycle_view()

    def action_cycle_partition(self) -> None:
        heatmap = self.query_one("#node-heatmap", NodeHeatmap)
        heatmap.cycle_partition()

    def action_cycle_chart(self) -> None:
        chart = self.query_one("#gpu-chart", GpuChart)
        chart.cycle_mode()

    def on_node_heatmap_node_selected(self, message: NodeHeatmap.NodeSelected) -> None:
        from slurmmon_cli.tui.screens.node_detail import NodeDetailScreen
        self.app.push_screen(NodeDetailScreen(message.node))
