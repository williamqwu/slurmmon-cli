"""Monitor screen - real-time cluster dashboard."""

from __future__ import annotations

import time

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Static
from textual import work

from slurmmon_cli.tui.formatting import format_duration, format_mem
from slurmmon_cli.tui.widgets.cluster_summary import ClusterSummary


class MonitorScreen(Screen):
    """Real-time Slurm cluster monitoring dashboard."""

    BINDINGS = [
        Binding("r", "refresh", "Refresh", show=True),
        Binding("u", "toggle_user", "User filter", show=True),
        Binding("p", "toggle_partition", "Partition filter", show=True),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        yield ClusterSummary(id="cluster-summary")
        yield Static("PARTITIONS", classes="section-label")
        yield DataTable(id="partition-table")
        yield Static("RUNNING JOBS", classes="section-label")
        yield DataTable(id="running-table")
        yield Static("PENDING JOBS", classes="section-label")
        yield DataTable(id="pending-table")
        yield Footer()

    def on_mount(self) -> None:
        # Partition table
        pt = self.query_one("#partition-table", DataTable)
        pt.add_columns("PARTITION", "NODES(A/I/O/T)", "CPUS", "STATE", "TIMELIMIT")
        pt.cursor_type = "row"

        # Running jobs table
        rt = self.query_one("#running-table", DataTable)
        rt.add_columns("JOBID", "USER", "ACCT", "PARTITION", "CPUS", "MEM", "ELAPSED", "LIMIT")
        rt.cursor_type = "row"

        # Pending jobs table
        pdt = self.query_one("#pending-table", DataTable)
        pdt.add_columns("JOBID", "USER", "ACCT", "PARTITION", "CPUS", "MEM", "WAITING", "REASON")
        pdt.cursor_type = "row"

        self._refresh_data()
        refresh_s = getattr(self.app, "refresh_interval", 30)
        self.set_interval(refresh_s, self._refresh_data)

    def _refresh_data(self) -> None:
        self._fetch_worker()

    @work(thread=True)
    def _fetch_worker(self) -> None:
        from slurmmon_cli.tui.data import fetch_live, fetch_from_db

        app = self.app
        from_db = getattr(app, "from_db", False)
        db_path = getattr(app, "db_path", None)
        user_filter = getattr(app, "user_filter", None)
        partition_filter = getattr(app, "partition_filter", None)

        if from_db:
            jobs, info = fetch_from_db(db_path, user_filter)
        else:
            jobs, info = fetch_live(user_filter, partition_filter)

        self.app.call_from_thread(self._update_display, jobs, info)

    def _update_display(self, jobs, info) -> None:
        now = time.time()
        running = [j for j in jobs if j.state == "RUNNING"]
        pending = [j for j in jobs if j.state == "PENDING"]

        # Cluster summary
        summary = self.query_one("#cluster-summary", ClusterSummary)
        summary.update_data(info, len(running), len(pending))

        # Update subtitle with timestamp
        cluster_name = info.cluster_name if info else "unknown"
        self.app.sub_title = f"{cluster_name} | {time.strftime('%H:%M:%S')}"

        # Partition table
        pt = self.query_one("#partition-table", DataTable)
        pt.clear()
        if info:
            for p in info.partitions:
                nodes_str = f"{p.alloc_nodes}/{p.idle_nodes}/{p.other_nodes}/{p.total_nodes}"
                cpus_str = f"{p.total_cpus - p.avail_cpus}/{p.total_cpus}"
                pt.add_row(p.name, nodes_str, cpus_str, p.state, p.max_time or "-")

        # Running jobs
        rt = self.query_one("#running-table", DataTable)
        rt.clear()
        sorted_running = sorted(running, key=lambda j: j.elapsed_s or 0, reverse=True)
        for j in sorted_running[:100]:
            rt.add_row(
                j.job_id,
                j.user,
                j.account or "-",
                j.partition or "-",
                str(j.num_cpus),
                format_mem(j.req_mem_mb),
                format_duration(j.elapsed_s),
                format_duration(j.time_limit_s),
            )

        # Pending jobs
        pdt = self.query_one("#pending-table", DataTable)
        pdt.clear()
        sorted_pending = sorted(
            pending,
            key=lambda j: (now - j.submit_time) if j.submit_time else 0,
            reverse=True,
        )
        for j in sorted_pending[:100]:
            wait = format_duration(now - j.submit_time if j.submit_time else None)
            pdt.add_row(
                j.job_id,
                j.user,
                j.account or "-",
                j.partition or "-",
                str(j.num_cpus),
                format_mem(j.req_mem_mb),
                wait,
                j.reason or "-",
            )

    def action_refresh(self) -> None:
        self._refresh_data()

    def action_toggle_user(self) -> None:
        app = self.app
        if getattr(app, "user_filter", None):
            app.user_filter = None
        else:
            app.user_filter = "me"  # placeholder
        self._refresh_data()

    def action_toggle_partition(self) -> None:
        app = self.app
        if getattr(app, "partition_filter", None):
            app.partition_filter = None
        self._refresh_data()
