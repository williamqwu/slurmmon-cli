"""Explorer screen - GPU/resource usage analysis with tabbed views."""

from __future__ import annotations

import json

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Static, TabbedContent, TabPane
from textual.worker import work

from slurmmon_cli.tui.widgets.node_heatmap import NodeHeatmap
from slurmmon_cli.tui.widgets.gpu_chart import GpuChart


class ExplorerScreen(Screen):
    """GPU and resource usage explorer with tabbed analysis views."""

    BINDINGS = [
        Binding("r", "refresh", "Refresh", show=True),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        with TabbedContent(id="explorer-tabs"):
            with TabPane("GPU Users", id="tab-gpu"):
                yield DataTable(id="gpu-table")
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
        gt.add_columns("#", "USER", "ACCOUNT", "GPU-HOURS", "FAIRSHARE", "GPU TYPES")
        gt.cursor_type = "row"

        # CPU users table
        ct = self.query_one("#cpu-table", DataTable)
        ct.add_columns("#", "USER", "ACCOUNT", "CPU-HOURS", "GPU-HOURS", "FAIRSHARE")
        ct.cursor_type = "row"

        # Account table
        at = self.query_one("#account-table", DataTable)
        at.add_columns("#", "ACCOUNT", "GPU-HOURS", "CPU-HOURS", "USERS")
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
            gt.add_row(str(i), r.get("user", "?"), r.get("account", "-"),
                       f"{gpu_hrs:,}", fair, types)

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
            at.add_row(str(i), r.get("account", "?"),
                       f"{gpu_hrs:,}", f"{cpu_hrs:,}", str(r.get("num_users", 0)))

    @work(thread=True)
    def _load_node_data(self) -> None:
        from slurmmon_cli.tui.data import fetch_node_data
        nodes, _ = fetch_node_data()
        self.app.call_from_thread(self._update_node_heatmap, nodes)

    def _update_node_heatmap(self, nodes) -> None:
        heatmap = self.query_one("#node-heatmap", NodeHeatmap)
        allocated = [n for n in nodes if n.cpus_alloc > 0]
        allocated.sort(key=lambda n: n.load_ratio if n.load_ratio is not None else 999)
        heatmap.set_data(allocated)

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
