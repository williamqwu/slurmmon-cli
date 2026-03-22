"""Efficiency screen - GPU-focused job analysis, queue, activity, waste."""

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
    """GPU-focused efficiency analysis: jobs, queue, activity, waste."""

    BINDINGS = [
        Binding("r", "refresh", "Refresh", show=False),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        with TabbedContent(id="efficiency-tabs"):
            with TabPane("GPU Jobs", id="tab-gpu-jobs"):
                yield Static(
                    " Your GPU jobs (running + completed)  \\[enter] select  \\[esc] back",
                    id="gpu-jobs-hint",
                )
                yield Static("", id="gpu-jobs-header")
                yield DataTable(id="gpu-jobs-table")
            with TabPane("GPU Queue", id="tab-gpu-queue"):
                yield Static(
                    " GPU wait time analysis  \\[r] refresh",
                    id="gpu-queue-hint",
                )
                yield Static("Loading...", id="gpu-queue-content")
            with TabPane("GPU Activity", id="tab-gpu-activity"):
                yield Static(
                    " Live GPU allocation and demand  \\[r] refresh",
                    id="gpu-activity-hint",
                )
                yield Static("Loading...", id="gpu-activity-content")
            with TabPane("GPU Waste", id="tab-gpu-waste"):
                yield Static(
                    " GPU resource waste detection  \\[r] refresh",
                    id="gpu-waste-hint",
                )
                yield Static("", id="gpu-waste-header")
                yield DataTable(id="gpu-waste-table")
                yield Static("", id="gpu-waste-extra")
        from slurmmon_cli.tui.widgets.grouped_footer import GroupedFooter, footer_markup
        yield GroupedFooter(footer_markup("\\[R]efresh", tabs=True))

    def on_mount(self) -> None:
        jt = self.query_one("#gpu-jobs-table", DataTable)
        jt.add_columns(
            "JOBID", "PARTITION", "GPUS", "CPUS", "ELAPSED", "LIMIT",
            "TIME%", "CPU EFF", "MEM EFF", "STATE",
        )
        jt.cursor_type = "row"

        wt = self.query_one("#gpu-waste-table", DataTable)
        wt.add_columns(
            "JOBID", "USER", "PARTITION", "GPUS", "CPUS",
            "CPU EFF", "ELAPSED", "STATE",
        )
        wt.cursor_type = "row"

        self._startup_timer = self.set_interval(2.0, self._poll_for_data)

    def _poll_for_data(self) -> None:
        try:
            if getattr(self.app, '_collect_done', False):
                self._startup_timer.stop()
                self._load_all()
        except Exception:
            pass

    def on_screen_resume(self) -> None:
        self._load_all()

    def on_initial_collect_done(self) -> None:
        self._load_all()

    def _load_all(self) -> None:
        self._load_gpu_jobs()
        self._load_gpu_queue()
        self._load_gpu_activity()
        self._load_gpu_waste()

    # --- Tab 1: GPU Jobs ---

    @work(thread=True)
    def _load_gpu_jobs(self) -> None:
        from slurmmon_cli.tui.data import fetch_gpu_user_jobs
        db_path = getattr(self.app, "db_path", None)
        user = os.environ.get("USER", "")
        rows = fetch_gpu_user_jobs(db_path, user=user, limit=50)
        self.app.call_from_thread(self._update_gpu_jobs, rows, user)

    def _update_gpu_jobs(self, rows: list[dict], user: str) -> None:
        running = sum(1 for r in rows if r.get("state") == "RUNNING")
        completed = len(rows) - running
        header = self.query_one("#gpu-jobs-header", Static)
        header.update(
            f" GPU jobs for {user}: {running} running, {completed} completed/other"
        )

        jt = self.query_one("#gpu-jobs-table", DataTable)
        jt.clear()
        for r in rows:
            cpu_eff = f"{r['cpu_eff']:.0f}%" if r.get("cpu_eff") is not None else "-"
            mem_eff = f"{r['mem_eff']:.0f}%" if r.get("mem_eff") is not None else "-"
            time_pct = f"{r['time_pct']:.0f}%" if r.get("time_pct") is not None else "-"
            jt.add_row(
                str(r.get("job_id", "")),
                r.get("partition") or "-",
                str(r.get("num_gpus", 0)),
                str(r.get("num_cpus", 0)),
                format_duration(r.get("elapsed_s")),
                format_duration(r.get("time_limit_s")),
                time_pct,
                cpu_eff,
                mem_eff,
                r.get("state", ""),
            )
        if not rows:
            header.update(f" No GPU jobs found for {user}")

    # --- Tab 2: GPU Queue ---

    @work(thread=True)
    def _load_gpu_queue(self) -> None:
        from slurmmon_cli.tui.data import fetch_gpu_queue
        db_path = getattr(self.app, "db_path", None)
        data = fetch_gpu_queue(db_path)
        self.app.call_from_thread(self._update_gpu_queue, data)

    def _update_gpu_queue(self, data: dict) -> None:
        summary = data.get("summary", {})
        by_count = data.get("by_count", [])
        by_partition = data.get("by_partition", [])

        lines: list[str] = []

        # GPU vs CPU-only comparison
        gpu_s = summary.get("gpu", {})
        cpu_s = summary.get("cpu_only", {})
        lines.append(" GPU vs CPU-only Wait Times\n\n")
        lines.append(f"   {'':>12}  {'GPU JOBS':>12}  {'CPU-ONLY':>12}\n")
        lines.append(f"   {'Jobs':>12}  {gpu_s.get('count', 0):>12,}  {cpu_s.get('count', 0):>12,}\n")
        lines.append(f"   {'Mean':>12}  {format_duration(gpu_s.get('mean')):>12}  {format_duration(cpu_s.get('mean')):>12}\n")
        lines.append(f"   {'Median':>12}  {format_duration(gpu_s.get('median')):>12}  {format_duration(cpu_s.get('median')):>12}\n")
        lines.append(f"   {'P90':>12}  {format_duration(gpu_s.get('p90')):>12}  {format_duration(cpu_s.get('p90')):>12}\n")
        lines.append(f"   {'Max':>12}  {format_duration(gpu_s.get('max')):>12}  {format_duration(cpu_s.get('max')):>12}\n")

        # Wait by GPU count
        if by_count:
            lines.append("\n Wait Time by GPU Count (higher GPU counts = longer waits)\n\n")
            lines.append(f"   {'GPUS':>6}  {'JOBS':>6}  {'AVG WAIT':>12}  {'MIN':>10}  {'MAX':>10}\n")
            for r in by_count:
                lines.append(
                    f"   {r.get('num_gpus', 0):>6}  {r.get('count', 0):>6}  "
                    f"{format_duration(r.get('avg_wait')):>12}  "
                    f"{format_duration(r.get('min_wait')):>10}  "
                    f"{format_duration(r.get('max_wait')):>10}\n"
                )

        # Wait by partition
        if by_partition:
            lines.append("\n Wait Time by GPU Partition\n\n")
            lines.append(f"   {'PARTITION':<20}  {'JOBS':>6}  {'AVG GPUs':>8}  {'AVG WAIT':>12}  {'MAX':>10}\n")
            for r in by_partition:
                avg_gpus = r.get("avg_gpus", 0) or 0
                lines.append(
                    f"   {r.get('partition', '?'):<20}  {r.get('count', 0):>6}  "
                    f"{avg_gpus:>8.1f}  "
                    f"{format_duration(r.get('avg_wait')):>12}  "
                    f"{format_duration(r.get('max_wait')):>10}\n"
                )

        self.query_one("#gpu-queue-content", Static).update(
            "".join(lines) if lines else " No GPU queue data available."
        )

    # --- Tab 3: GPU Activity ---

    @work(thread=True)
    def _load_gpu_activity(self) -> None:
        from slurmmon_cli.tui.data import fetch_gpu_activity
        db_path = getattr(self.app, "db_path", None)
        data = fetch_gpu_activity(db_path)
        self.app.call_from_thread(self._update_gpu_activity, data)

    def _update_gpu_activity(self, data: dict) -> None:
        part_gpus = data.get("partition_gpus", [])
        top_users = data.get("top_users", [])
        pending = data.get("pending", [])
        trend = data.get("trend", [])

        lines: list[str] = []

        # Per-partition GPU allocation
        if part_gpus:
            lines.append(" GPU Allocation by Partition (live)\n\n")
            lines.append(f"   {'PARTITION':<20}  {'TYPE':<6}  {'ALLOC':>6}  {'TOTAL':>6}  {'IDLE':>5}  {'UTIL%':>6}  {'NODES':>5}\n")
            for p in part_gpus:
                lines.append(
                    f"   {p['partition']:<20}  {p.get('gpu_type', '-'):<6}  "
                    f"{p['alloc']:>6}  {p['total']:>6}  {p['idle']:>5}  "
                    f"{p['pct']:>5.0f}%  {p['nodes']:>5}\n"
                )

        # Top GPU consumers
        if top_users:
            total_gpus = sum(u.get("gpus", 0) for u in top_users)
            lines.append(f"\n Top GPU Consumers ({total_gpus} GPUs in use by top {len(top_users)} users)\n\n")
            lines.append(f"   {'USER':<14}  {'ACCOUNT':<12}  {'GPUS':>5}  {'JOBS':>5}  {'CPUS':>6}  {'PARTITIONS'}\n")
            for u in top_users:
                lines.append(
                    f"   {u['user']:<14}  {u.get('account', '-'):<12}  "
                    f"{u['gpus']:>5}  {u['jobs']:>5}  {u.get('cpus', 0):>6}  "
                    f"{u.get('partitions', '-')}\n"
                )

        # Pending GPU demand
        if pending:
            total_pending = sum(p.get("gpus_requested", 0) for p in pending)
            lines.append(f"\n Pending GPU Demand ({total_pending} GPUs requested)\n\n")
            lines.append(f"   {'PARTITION':<20}  {'JOBS':>6}  {'GPUs REQ':>8}  {'AVG/JOB':>8}\n")
            for p in pending:
                avg = p.get("avg_gpus_per_job", 0) or 0
                lines.append(
                    f"   {p['partition']:<20}  {p['jobs']:>6}  "
                    f"{p['gpus_requested']:>8}  {avg:>8.1f}\n"
                )

        # GPU trend from snapshots
        if trend:
            alloc_vals = [r.get("alloc_gpus", 0) or 0 for r in trend]
            total_vals = [r.get("total_gpus", 0) or 0 for r in trend]
            if any(v > 0 for v in total_vals):
                lines.append(f"\n GPU Allocation Trend ({len(trend)} snapshots)\n")
                lines.append(f"   {sparkline(alloc_vals, width=50)}\n")
                pct_now = alloc_vals[-1] / total_vals[-1] * 100 if total_vals[-1] else 0
                lines.append(
                    f"   Current: {alloc_vals[-1]}/{total_vals[-1]} ({pct_now:.0f}%)  "
                    f"Min: {min(alloc_vals)}  Max: {max(alloc_vals)}\n"
                )

        self.query_one("#gpu-activity-content", Static).update(
            "".join(lines) if lines else " No GPU activity data available."
        )

    # --- Tab 4: GPU Waste ---

    @work(thread=True)
    def _load_gpu_waste(self) -> None:
        from slurmmon_cli.tui.data import fetch_gpu_waste
        db_path = getattr(self.app, "db_path", None)
        report = fetch_gpu_waste(db_path)
        self.app.call_from_thread(self._update_gpu_waste, report)

    def _update_gpu_waste(self, report: dict) -> None:
        low_cpu = report.get("low_cpu_eff", [])
        walltime = report.get("walltime_waste", [])
        under_nodes = report.get("underutilized_gpu_nodes", [])

        # Low CPU efficiency table
        header = self.query_one("#gpu-waste-header", Static)
        header.update(
            f" GPU Jobs with Low CPU Efficiency (<50%): {len(low_cpu)} found"
        )

        wt = self.query_one("#gpu-waste-table", DataTable)
        wt.clear()
        for r in low_cpu:
            cpu_eff = f"{r['cpu_eff']:.0f}%" if r.get("cpu_eff") is not None else "-"
            wt.add_row(
                str(r.get("job_id", "")),
                r.get("user", ""),
                r.get("partition") or "-",
                str(r.get("num_gpus", 0)),
                str(r.get("num_cpus", 0)),
                cpu_eff,
                format_duration(r.get("elapsed_s")),
                r.get("state", ""),
            )

        # Extra section: walltime waste + underutilized GPU nodes
        lines: list[str] = []

        if walltime:
            lines.append(
                f"\n GPU Jobs with Walltime Waste (<30% of limit used): "
                f"{len(walltime)} found\n\n"
            )
            lines.append(
                f"   {'JOBID':<12}  {'USER':<14}  {'PARTITION':<12}  "
                f"{'GPUS':>5}  {'ELAPSED':>10}  {'LIMIT':>10}  {'USED%':>6}  {'STATE'}\n"
            )
            for r in walltime:
                lines.append(
                    f"   {str(r.get('job_id', '')):>12}  {r.get('user', ''):>14}  "
                    f"{r.get('partition', '-'):<12}  {r.get('num_gpus', 0):>5}  "
                    f"{format_duration(r.get('elapsed_s')):>10}  "
                    f"{format_duration(r.get('time_limit_s')):>10}  "
                    f"{r.get('time_used_pct', 0):>5.0f}%  {r.get('state', '')}\n"
                )

        if under_nodes:
            lines.append(
                f"\n Underutilized GPU Nodes (allocated but load <30%): "
                f"{len(under_nodes)} found\n\n"
            )
            lines.append(
                f"   {'NODE':<10}  {'USER':<14}  {'LOAD%':>6}  "
                f"{'CPU(a/t)':>10}  {'GPU(a/t)':>10}  {'TYPE':<6}  {'PARTITION'}\n"
            )
            for n in under_nodes:
                lines.append(
                    f"   {n['name']:<10}  {n['user']:<14}  {n['load_pct']:>5.0f}%  "
                    f"{n['cpus']:>10}  {n['gpus']:>10}  {n.get('gpu_type', '-'):<6}  "
                    f"{n.get('partitions', '-')}\n"
                )

        if not walltime and not under_nodes:
            lines.append(
                "\n No walltime waste or underutilized GPU nodes detected."
            )

        self.query_one("#gpu-waste-extra", Static).update("".join(lines))

    def action_refresh(self) -> None:
        self._load_all()
