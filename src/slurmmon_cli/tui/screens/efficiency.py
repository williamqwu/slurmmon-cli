"""Efficiency screen - job analysis, queue health, trends, waste detection."""

from __future__ import annotations

import os
import time

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import DataTable, Header, Static, TabbedContent, TabPane
from textual import work

from slurmmon_cli.tui.formatting import format_duration, sparkline


class EfficiencyScreen(Screen):
    """Job efficiency analysis, queue health, cluster trends, waste detection."""

    BINDINGS = [
        Binding("r", "refresh", "Refresh", show=False),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        with TabbedContent(id="efficiency-tabs"):
            with TabPane("Your Jobs", id="tab-your-jobs"):
                yield Static("", id="your-jobs-header")
                yield DataTable(id="your-jobs-table")
            with TabPane("Queue Health", id="tab-queue"):
                yield Static("Loading...", id="queue-health")
            with TabPane("Cluster Trends", id="tab-trends"):
                yield Static("Loading...", id="cluster-trends")
            with TabPane("Waste Detector", id="tab-waste"):
                yield Static("", id="waste-header")
                yield DataTable(id="waste-table")
                yield Static("", id="waste-nodes")
        from slurmmon_cli.tui.widgets.grouped_footer import GroupedFooter, footer_markup
        yield GroupedFooter(footer_markup("\\[R]efresh"))

    def on_mount(self) -> None:
        jt = self.query_one("#your-jobs-table", DataTable)
        jt.add_columns("JOBID", "PARTITION", "CPUS", "GPUS", "ELAPSED", "CPU EFF", "MEM EFF", "STATE")
        jt.cursor_type = "row"

        wt = self.query_one("#waste-table", DataTable)
        wt.add_columns("JOBID", "USER", "PARTITION", "CPUS", "CPU EFF", "MEM EFF", "STATE")
        wt.cursor_type = "row"

        # Poll until initial collection is done, then load data
        self._startup_timer = self.set_interval(2.0, self._poll_for_data)

    def _poll_for_data(self) -> None:
        """Check if initial collection is done; load data and stop polling."""
        try:
            if getattr(self.app, '_collect_done', False):
                self._startup_timer.stop()
                self._load_all()
        except Exception:
            pass

    def on_screen_resume(self) -> None:
        """Reload data when user switches to this screen."""
        self._load_all()

    def on_initial_collect_done(self) -> None:
        """Called by the app when background collection finishes."""
        self._load_all()

    def _load_all(self) -> None:
        self._load_your_jobs()
        self._load_queue_health()
        self._load_cluster_trends()
        self._load_waste()

    @work(thread=True)
    def _load_your_jobs(self) -> None:
        from slurmmon_cli.tui.data import fetch_user_efficiency
        db_path = getattr(self.app, "db_path", None)
        user = os.environ.get("USER", "")
        rows = fetch_user_efficiency(db_path, user=user, limit=50)
        self.app.call_from_thread(self._update_your_jobs, rows, user)

    def _update_your_jobs(self, rows: list[dict], user: str) -> None:
        header = self.query_one("#your-jobs-header", Static)
        header.update(f" Completed jobs for {user} ({len(rows)} shown)")

        jt = self.query_one("#your-jobs-table", DataTable)
        jt.clear()
        for r in rows:
            cpu_eff = f"{r['cpu_eff']:.0f}%" if r.get("cpu_eff") is not None else "-"
            mem_eff = f"{r['mem_eff']:.0f}%" if r.get("mem_eff") is not None else "-"
            jt.add_row(
                str(r.get("job_id", "")),
                r.get("partition") or "-",
                str(r.get("num_cpus", 0)),
                str(r.get("num_gpus", 0)),
                format_duration(r.get("elapsed_s")),
                cpu_eff,
                mem_eff,
                r.get("state", ""),
            )

    @work(thread=True)
    def _load_queue_health(self) -> None:
        from slurmmon_cli.tui.data import fetch_queue_health
        db_path = getattr(self.app, "db_path", None)
        data = fetch_queue_health(db_path)
        self.app.call_from_thread(self._update_queue_health, data)

    def _update_queue_health(self, data: dict) -> None:
        stats = data.get("stats", {})
        by_hour = data.get("by_hour", [])
        by_size = data.get("by_size", [])

        lines = []
        count = stats.get("count", 0)
        if count == 0:
            lines.append(" No wait time data. Run 'slurmmon-cli collect' to gather data.\n")
        else:
            lines.append(f" Wait Time Summary ({count} jobs analyzed)\n\n")
            lines.append(f"   Mean:   {format_duration(stats.get('mean'))}\n")
            lines.append(f"   Median: {format_duration(stats.get('median'))}\n")
            lines.append(f"   P90:    {format_duration(stats.get('p90'))}\n")
            lines.append(f"   P99:    {format_duration(stats.get('p99'))}\n")
            lines.append(f"   Max:    {format_duration(stats.get('max'))}\n")

        if by_hour:
            lines.append("\n Wait Time by Hour of Day (submission time)\n")
            waits = [r.get("avg_wait", 0) or 0 for r in by_hour]
            hours = [r.get("hour", 0) for r in by_hour]
            spark = sparkline(waits, width=24)
            lines.append(f"   {spark}\n")
            # Show best and worst hours
            if waits:
                best_idx = waits.index(min(waits))
                worst_idx = waits.index(max(waits))
                lines.append(
                    f"   Best hour: {hours[best_idx]:02d}:00 "
                    f"({format_duration(waits[best_idx])} avg wait)\n"
                )
                lines.append(
                    f"   Worst hour: {hours[worst_idx]:02d}:00 "
                    f"({format_duration(waits[worst_idx])} avg wait)\n"
                )

        if by_size:
            lines.append("\n Wait Time by Job Size (CPUs)\n\n")
            lines.append(f"   {'CPUS':>8}  {'COUNT':>6}  {'AVG WAIT':>10}  {'MAX WAIT':>10}\n")
            for r in by_size:
                lines.append(
                    f"   {r.get('cpu_range', '?'):>8}  {r.get('count', 0):>6}  "
                    f"{format_duration(r.get('avg_wait')):>10}  "
                    f"{format_duration(r.get('max_wait')):>10}\n"
                )

        self.query_one("#queue-health", Static).update("".join(lines))

    @work(thread=True)
    def _load_cluster_trends(self) -> None:
        from slurmmon_cli.tui.data import fetch_cluster_trends
        db_path = getattr(self.app, "db_path", None)
        rows = fetch_cluster_trends(db_path, limit=100)
        self.app.call_from_thread(self._update_cluster_trends, rows)

    def _update_cluster_trends(self, rows: list[dict]) -> None:
        if not rows:
            self.query_one("#cluster-trends", Static).update(
                " No snapshot data. Run 'slurmmon-cli collect' to gather data."
            )
            return

        running = [r.get("running_jobs", 0) or 0 for r in rows]
        pending = [r.get("pending_jobs", 0) or 0 for r in rows]
        alloc = [r.get("alloc_nodes", 0) or 0 for r in rows]
        total_nodes = rows[-1].get("total_nodes", 0) or 0

        lines = [f" Cluster Trends ({len(rows)} snapshots)\n\n"]

        lines.append(f" Running Jobs:  {sparkline(running, width=50)}\n")
        lines.append(
            f"   Current: {running[-1]}  "
            f"Min: {min(running)}  Max: {max(running)}  "
            f"Avg: {sum(running) // len(running)}\n\n"
        )

        lines.append(f" Pending Jobs:  {sparkline(pending, width=50)}\n")
        lines.append(
            f"   Current: {pending[-1]}  "
            f"Min: {min(pending)}  Max: {max(pending)}  "
            f"Avg: {sum(pending) // len(pending)}\n\n"
        )

        lines.append(f" Allocated Nodes:  {sparkline(alloc, width=50)}\n")
        pct = alloc[-1] / total_nodes * 100 if total_nodes else 0
        lines.append(
            f"   Current: {alloc[-1]}/{total_nodes} ({pct:.0f}%)  "
            f"Min: {min(alloc)}  Max: {max(alloc)}\n"
        )

        # Time range
        import datetime
        if rows:
            t0 = rows[0].get("timestamp", 0)
            t1 = rows[-1].get("timestamp", 0)
            d0 = datetime.datetime.fromtimestamp(t0).strftime("%m-%d %H:%M") if t0 else "?"
            d1 = datetime.datetime.fromtimestamp(t1).strftime("%m-%d %H:%M") if t1 else "?"
            lines.append(f"\n Period: {d0} to {d1}\n")

        self.query_one("#cluster-trends", Static).update("".join(lines))

    @work(thread=True)
    def _load_waste(self) -> None:
        from slurmmon_cli.tui.data import fetch_waste_report
        db_path = getattr(self.app, "db_path", None)
        report = fetch_waste_report(db_path)
        self.app.call_from_thread(self._update_waste, report)

    def _update_waste(self, report: dict) -> None:
        low_eff = report.get("low_efficiency_jobs", [])
        under_by_part = report.get("underutilized_nodes_by_partition", {})

        header = self.query_one("#waste-header", Static)
        header.update(f" Low Efficiency Jobs (CPU eff < 50%): {len(low_eff)} found")

        wt = self.query_one("#waste-table", DataTable)
        wt.clear()
        for r in low_eff:
            cpu_eff = f"{r['cpu_eff_pct']:.0f}%" if r.get("cpu_eff_pct") is not None else "-"
            mem_eff = f"{r['mem_eff_pct']:.0f}%" if r.get("mem_eff_pct") is not None else "-"
            wt.add_row(
                str(r.get("job_id", "")),
                r.get("user", ""),
                r.get("partition") or "-",
                str(r.get("num_cpus", 0)),
                cpu_eff,
                mem_eff,
                r.get("state", ""),
            )

        # Underutilized exclusive nodes grouped by partition
        nodes_widget = self.query_one("#waste-nodes", Static)
        total_count = sum(len(v) for v in under_by_part.values())
        if total_count > 0:
            lines = [
                f"\n Underutilized Exclusive Nodes (load < 30%, single user, full alloc): "
                f"{total_count} found\n"
            ]
            for part_name in sorted(under_by_part.keys()):
                part_nodes = under_by_part[part_name]
                lines.append(f"\n   [{part_name}]\n")
                lines.append(
                    f"   {'NODE':<10} {'USER':<12} {'LOAD%':>6}  "
                    f"{'CPU(a/t)':>10}  {'GPU(a/t)':>10}\n"
                )
                for n in part_nodes:
                    lr = n.get("load_ratio")
                    pct = f"{lr * 100:.0f}%" if lr is not None else "-"
                    cpu = f"{n.get('cpus_alloc', 0)}/{n.get('cpus_total', 0)}"
                    gpu = f"{n.get('gpus_alloc', 0)}/{n.get('gpus_total', 0)}"
                    lines.append(
                        f"   {n.get('name', '?'):<10} {n.get('user', '-'):<12} {pct:>6}  "
                        f"{cpu:>10}  {gpu:>10}\n"
                    )
            nodes_widget.update("".join(lines))
        else:
            nodes_widget.update(
                "\n No underutilized exclusive nodes detected (all full-alloc nodes have load >= 30%)."
            )

    def action_refresh(self) -> None:
        self._load_all()
