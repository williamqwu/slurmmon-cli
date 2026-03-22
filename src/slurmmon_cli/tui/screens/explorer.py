"""Explorer screen - GPU/resource usage analysis with tabbed views."""

from __future__ import annotations

import json

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import DataTable, Header, Static, TabbedContent, TabPane
from textual import work

from slurmmon_cli.tui.widgets.node_heatmap import NodeHeatmap
from slurmmon_cli.tui.widgets.gpu_chart import GpuChart


class ExplorerScreen(Screen):
    """GPU and resource usage explorer with tabbed analysis views."""

    BINDINGS = [
        Binding("r", "refresh", "Refresh", show=False),
        Binding("o", "cycle_sort", show=False),
        Binding("v", "cycle_view", show=False),
        Binding("p", "cycle_partition", show=False),
        Binding("c", "cycle_chart", show=False),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(
            " Collecting cluster data (squeue, sinfo, sacct, sshare)...",
            id="collect-status",
        )
        with TabbedContent(id="explorer-tabs"):
            with TabPane("GPU Users", id="tab-gpu"):
                yield Static(
                    " Top users by GPU-hours (sshare)  "
                    "\\[enter] detail  \\[esc] back  |  "
                    "FAIRSHARE: priority 0-1, higher = higher",
                    id="gpu-hint",
                )
                yield DataTable(id="gpu-table")
            with TabPane("CPU Users", id="tab-cpu"):
                yield Static(
                    " Top users by CPU-hours (sshare)  "
                    "\\[enter] detail  \\[esc] back",
                    id="cpu-hint",
                )
                yield DataTable(id="cpu-table")
            with TabPane("Accounts", id="tab-accounts"):
                yield Static(
                    " Top accounts by GPU-hours  "
                    "\\[enter] detail  \\[esc] back",
                    id="accounts-hint",
                )
                yield DataTable(id="account-table")
            with TabPane("Nodes", id="tab-nodes"):
                yield Static(
                    " \\[o] sort  \\[v] view  \\[p] partition  \\[arrows] navigate  \\[enter] detail",
                    id="nodes-hint",
                )
                yield Static(
                    " Live node utilization heatmap  \\[esc] back from detail",
                    id="nodes-desc",
                )
                yield NodeHeatmap(id="node-heatmap")
            with TabPane("GPU Chart", id="tab-chart"):
                yield Static(
                    " GPU usage visualization  "
                    "\\[c] metric  \\[up/down] navigate  \\[enter] detail  \\[esc] back",
                    id="chart-hint",
                )
                yield GpuChart(id="gpu-chart")
        from slurmmon_cli.tui.widgets.grouped_footer import GroupedFooter, footer_markup
        yield GroupedFooter(footer_markup("\\[R]efresh"))

    def on_mount(self) -> None:
        self._gpu_rows: list[dict] = []
        self._cpu_rows: list[dict] = []
        self._account_rows: list[dict] = []

        gt = self.query_one("#gpu-table", DataTable)
        gt.add_columns(
            "#", "USER", "ACCOUNT", "GPU-HOURS", "JOBS(R/P)",
            "NODES(F/P)", "FAIRSHARE", "GPU TYPES",
        )
        gt.cursor_type = "row"

        ct = self.query_one("#cpu-table", DataTable)
        ct.add_columns("#", "USER", "ACCOUNT", "CPU-HOURS", "GPU-HOURS", "FAIRSHARE")
        ct.cursor_type = "row"

        at = self.query_one("#account-table", DataTable)
        at.add_columns(
            "#", "ACCOUNT", "GPU-HOURS", "CPU-HOURS", "USERS",
            "JOBS(R/P)", "NODES(F/P)",
        )
        at.cursor_type = "row"

        # Poll until initial collection is done, then load data
        self._startup_timer = self.set_interval(2.0, self._poll_for_data)

    def _poll_for_data(self) -> None:
        """Check if initial collection is done; load data and stop polling."""
        try:
            if getattr(self.app, '_collect_done', False):
                self._startup_timer.stop()
                self._hide_collect_status()
                self._load_all_tabs()
        except Exception:
            pass

    def _hide_collect_status(self) -> None:
        try:
            self.query_one("#collect-status", Static).display = False
        except Exception:
            pass

    def on_screen_resume(self) -> None:
        """Reload data when user switches to this screen."""
        self._hide_collect_status()
        # Save cursor positions so they survive the reload
        self._saved_cursors = {}
        for tid in ("gpu-table", "cpu-table", "account-table"):
            try:
                self._saved_cursors[tid] = self.query_one(
                    f"#{tid}", DataTable
                ).cursor_row
            except Exception:
                pass
        self._load_all_tabs()

    def on_initial_collect_done(self) -> None:
        """Called by the app when background collection finishes."""
        self._hide_collect_status()
        self._load_all_tabs()

    def _get_cluster(self) -> str | None:
        name = getattr(self.app, "cluster_name", "")
        return name if name else None

    def _load_all_tabs(self) -> None:
        self._load_gpu_data()
        self._load_cpu_data()
        self._load_account_data()
        self._load_node_data()

    # --- GPU Users ---

    @work(thread=True)
    def _load_gpu_data(self) -> None:
        from slurmmon_cli.tui.data import fetch_gpu_rankings
        db_path = getattr(self.app, "db_path", None)
        rows = fetch_gpu_rankings(db_path, "gpu", top=30, cluster=self._get_cluster())
        self.app.call_from_thread(self._update_gpu_table, rows)

    def _update_gpu_table(self, rows: list[dict]) -> None:
        self._gpu_rows = rows
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

        saved = getattr(self, '_saved_cursors', {}).get('gpu-table', 0)
        if rows and 0 < saved < len(rows):
            gt.move_cursor(row=saved)

        chart = self.query_one("#gpu-chart", GpuChart)
        chart.set_data(rows)

    # --- CPU Users ---

    @work(thread=True)
    def _load_cpu_data(self) -> None:
        from slurmmon_cli.tui.data import fetch_gpu_rankings
        db_path = getattr(self.app, "db_path", None)
        rows = fetch_gpu_rankings(db_path, "cpu", top=30, cluster=self._get_cluster())
        self.app.call_from_thread(self._update_cpu_table, rows)

    def _update_cpu_table(self, rows: list[dict]) -> None:
        self._cpu_rows = rows
        ct = self.query_one("#cpu-table", DataTable)
        ct.clear()
        for i, r in enumerate(rows, 1):
            cpu_hrs = r.get("cpu_tres_mins", 0) // 60
            gpu_hrs = r.get("gpu_tres_mins", 0) // 60
            fair = f"{r['fairshare']:.2f}" if r.get("fairshare") is not None else "-"
            ct.add_row(str(i), r.get("user", "?"), r.get("account", "-"),
                       f"{cpu_hrs:,}", f"{gpu_hrs:,}", fair)
        saved = getattr(self, '_saved_cursors', {}).get('cpu-table', 0)
        if rows and 0 < saved < len(rows):
            ct.move_cursor(row=saved)

    # --- Accounts ---

    @work(thread=True)
    def _load_account_data(self) -> None:
        from slurmmon_cli.tui.data import fetch_gpu_rankings
        db_path = getattr(self.app, "db_path", None)
        rows = fetch_gpu_rankings(db_path, "account", top=30, cluster=self._get_cluster())
        self.app.call_from_thread(self._update_account_table, rows)

    def _update_account_table(self, rows: list[dict]) -> None:
        self._account_rows = rows
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
        saved = getattr(self, '_saved_cursors', {}).get('account-table', 0)
        if rows and 0 < saved < len(rows):
            at.move_cursor(row=saved)

    # --- Nodes ---

    @work(thread=True)
    def _load_node_data(self) -> None:
        from slurmmon_cli.tui.data import fetch_node_data
        nodes, _ = fetch_node_data()
        self.app.call_from_thread(self._update_node_heatmap, nodes)

    def _update_node_heatmap(self, nodes) -> None:
        heatmap = self.query_one("#node-heatmap", NodeHeatmap)
        allocated = [n for n in nodes if n.cpus_alloc > 0]
        heatmap.set_data(allocated, show_users=True)

    # --- Helpers ---

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

    # --- Actions ---

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

    # --- Detail drill-downs ---

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        table_id = event.data_table.id
        idx = event.cursor_row
        if table_id == "gpu-table" and idx < len(self._gpu_rows):
            r = self._gpu_rows[idx]
            from slurmmon_cli.tui.screens.user_detail import UserDetailScreen
            self.app.push_screen(UserDetailScreen(
                user=r["user"], account=r.get("account"), gpu_only=True,
            ))
        elif table_id == "cpu-table" and idx < len(self._cpu_rows):
            r = self._cpu_rows[idx]
            from slurmmon_cli.tui.screens.user_detail import UserDetailScreen
            self.app.push_screen(UserDetailScreen(
                user=r["user"], account=r.get("account"), gpu_only=False,
            ))
        elif table_id == "account-table" and idx < len(self._account_rows):
            r = self._account_rows[idx]
            from slurmmon_cli.tui.screens.account_detail import AccountDetailScreen
            self.app.push_screen(AccountDetailScreen(account=r["account"]))

    def on_node_heatmap_node_selected(self, message: NodeHeatmap.NodeSelected) -> None:
        from slurmmon_cli.tui.screens.node_detail import NodeDetailScreen
        self.app.push_screen(NodeDetailScreen(message.node))

    def on_gpu_chart_user_selected(self, message: GpuChart.UserSelected) -> None:
        from slurmmon_cli.tui.screens.user_detail import UserDetailScreen
        self.app.push_screen(UserDetailScreen(
            user=message.user, account=message.account, gpu_only=True,
        ))
