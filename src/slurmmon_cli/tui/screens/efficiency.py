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
        Binding("f", "toggle_fullnode", show=False),
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
                    " Live GPU allocation and demand  "
                    "\\[enter] user detail  \\[r] refresh",
                    id="gpu-activity-hint",
                )
                yield Static("Loading...", id="gpu-activity-alloc")
                yield Static(
                    " Top GPU Consumers",
                    id="gpu-consumers-label",
                )
                yield DataTable(id="gpu-consumers-table")
                yield Static("", id="gpu-activity-extra")
            with TabPane("GPU Waste", id="tab-gpu-waste"):
                yield Static(
                    " GPU waste detection  "
                    "\\[enter] user detail  \\[f] toggle full-node filter  \\[r] refresh",
                    id="gpu-waste-hint",
                )
                yield Static("", id="gpu-waste-header")
                yield DataTable(id="gpu-waste-table")
                yield Static("", id="gpu-waste-extra")
        from slurmmon_cli.tui.widgets.grouped_footer import GroupedFooter, footer_markup
        yield GroupedFooter(footer_markup("\\[R]efresh", tabs=True))

    def on_mount(self) -> None:
        self._activity_users: list[dict] = []
        self._waste_rows: list[dict] = []
        self._fullnode_only = False

        jt = self.query_one("#gpu-jobs-table", DataTable)
        jt.add_columns(
            "JOBID", "PARTITION", "GPUS", "CPUS", "ELAPSED", "LIMIT",
            "TIME%", "CPU EFF", "MEM EFF", "STATE",
        )
        jt.cursor_type = "row"

        ct = self.query_one("#gpu-consumers-table", DataTable)
        ct.add_columns("USER", "ACCOUNT", "GPUS", "JOBS", "CPUS", "PARTITIONS")
        ct.cursor_type = "row"

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
        cluster = getattr(self.app, "cluster_name", None) or None
        rows = fetch_gpu_user_jobs(db_path, user=user, limit=50, cluster=cluster)
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
        cluster = getattr(self.app, "cluster_name", None) or None
        data = fetch_gpu_queue(db_path, cluster=cluster)
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
            lines.append("\n Wait Time by GPU Count\n")
            lines.append("   (Actual wait depends on scheduling policy, fairshare, and partition.\n")
            lines.append("    Policies are subject to change by cluster administrators.)\n\n")
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
        cluster = getattr(self.app, "cluster_name", None) or None
        data = fetch_gpu_activity(db_path, cluster=cluster)
        self.app.call_from_thread(self._update_gpu_activity, data)

    def _update_gpu_activity(self, data: dict) -> None:
        part_gpus = data.get("partition_gpus", [])
        top_users = data.get("top_users", [])
        pending = data.get("pending", [])
        trend = data.get("trend", [])

        # Allocation + trend + pending as Static text
        lines: list[str] = []
        if part_gpus:
            lines.append(" GPU Allocation by Partition (live)\n\n")
            lines.append(f"   {'PARTITION':<20}  {'TYPE':<6}  {'ALLOC':>6}  {'TOTAL':>6}  {'IDLE':>5}  {'UTIL%':>6}  {'NODES':>5}\n")
            for p in part_gpus:
                lines.append(
                    f"   {p['partition']:<20}  {p.get('gpu_type', '-'):<6}  "
                    f"{p['alloc']:>6}  {p['total']:>6}  {p['idle']:>5}  "
                    f"{p['pct']:>5.0f}%  {p['nodes']:>5}\n"
                )

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

        self.query_one("#gpu-activity-alloc", Static).update(
            "".join(lines) if lines else " No GPU activity data."
        )

        # Top consumers as DataTable (Enter to drill down)
        self._activity_users = top_users
        ct = self.query_one("#gpu-consumers-table", DataTable)
        ct.clear()
        total_gpus = sum(u.get("gpus", 0) for u in top_users)
        self.query_one("#gpu-consumers-label", Static).update(
            f" Top GPU Consumers ({total_gpus} GPUs in use by top {len(top_users)} users)"
        )
        for u in top_users:
            ct.add_row(
                u["user"],
                u.get("account", "-"),
                str(u["gpus"]),
                str(u["jobs"]),
                str(u.get("cpus", 0)),
                u.get("partitions", "-"),
            )

        # Pending demand as extra Static
        extra_lines: list[str] = []
        if pending:
            total_pending = sum(p.get("gpus_requested", 0) for p in pending)
            extra_lines.append(f"\n Pending GPU Demand ({total_pending} GPUs requested)\n\n")
            extra_lines.append(f"   {'PARTITION':<20}  {'JOBS':>6}  {'GPUs REQ':>8}  {'AVG/JOB':>8}\n")
            for p in pending:
                avg = p.get("avg_gpus_per_job", 0) or 0
                extra_lines.append(
                    f"   {p['partition']:<20}  {p['jobs']:>6}  "
                    f"{p['gpus_requested']:>8}  {avg:>8.1f}\n"
                )
        self.query_one("#gpu-activity-extra", Static).update("".join(extra_lines))

    # --- Tab 4: GPU Waste ---

    @work(thread=True)
    def _load_gpu_waste(self) -> None:
        from slurmmon_cli.tui.data import fetch_gpu_waste
        db_path = getattr(self.app, "db_path", None)
        cluster = getattr(self.app, "cluster_name", None) or None
        report = fetch_gpu_waste(db_path, cluster=cluster)
        self.app.call_from_thread(self._update_gpu_waste, report)

    def _update_gpu_waste(self, report: dict) -> None:
        self._waste_rows = report.get("low_cpu_eff", [])
        walltime = report.get("walltime_waste", [])
        self._under_nodes_all = report.get("underutilized_gpu_nodes", [])

        # Low CPU efficiency table
        header = self.query_one("#gpu-waste-header", Static)
        header.update(
            f" GPU Jobs with Low CPU Efficiency (<50%): {len(self._waste_rows)} found"
        )

        wt = self.query_one("#gpu-waste-table", DataTable)
        wt.clear()
        for r in self._waste_rows:
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

        lines.extend(self._render_under_nodes())

        if not walltime and not self._under_nodes_all:
            lines.append(
                "\n No walltime waste or underutilized GPU nodes detected."
            )

        self.query_one("#gpu-waste-extra", Static).update("".join(lines))

    def _render_under_nodes(self) -> list[str]:
        """Render underutilized GPU nodes, respecting full-node filter."""
        nodes = self._get_filtered_under_nodes()
        if not nodes:
            return []
        filter_label = " (full-node only)" if self._fullnode_only else ""
        lines = [
            f"\n Underutilized GPU Nodes (load <30%){filter_label}: "
            f"{len(nodes)} found   [f] toggle filter\n\n",
            f"   {'NODE':<10}  {'USER':<14}  {'LOAD%':>6}  "
            f"{'CPU(a/t)':>10}  {'GPU(a/t)':>10}  {'TYPE':<6}  {'PARTITION'}\n",
        ]
        for n in nodes:
            lines.append(
                f"   {n['name']:<10}  {n['user']:<14}  {n['load_pct']:>5.0f}%  "
                f"{n['cpus']:>10}  {n['gpus']:>10}  {n.get('gpu_type', '-'):<6}  "
                f"{n.get('partitions', '-')}\n"
            )
        return lines

    def _get_filtered_under_nodes(self) -> list[dict]:
        nodes = getattr(self, "_under_nodes_all", [])
        if not self._fullnode_only:
            return nodes
        # Full-node: user allocated >= 90% of CPUs
        return [
            n for n in nodes
            if self._is_full_node(n)
        ]

    @staticmethod
    def _is_full_node(n: dict) -> bool:
        """Check if the node allocation looks like a full-node job."""
        cpus_str = n.get("cpus", "0/0")
        parts = cpus_str.split("/")
        if len(parts) == 2:
            try:
                alloc, total = int(parts[0]), int(parts[1])
                return total > 0 and alloc >= total * 0.9
            except ValueError:
                pass
        return False

    def action_toggle_fullnode(self) -> None:
        """Toggle full-node-only filter for underutilized GPU nodes."""
        self._fullnode_only = not self._fullnode_only
        # Re-render just the extra section
        try:
            extra = self.query_one("#gpu-waste-extra", Static)
            # Rebuild from existing data
            lines: list[str] = []
            # Keep walltime section from current text (above the underutilized section)
            current = str(extra.renderable) if extra.renderable else ""
            # Find walltime section boundary
            under_idx = current.find("\n Underutilized GPU Nodes")
            if under_idx < 0:
                under_idx = current.find("\n No walltime waste")
            if under_idx > 0:
                lines.append(current[:under_idx])
            else:
                lines.append(current)
            lines.extend(self._render_under_nodes())
            extra.update("".join(lines))
            self.notify(
                f"Filter: {'full-node jobs only' if self._fullnode_only else 'all GPU nodes'}",
                timeout=2,
            )
        except Exception:
            pass

    # --- Enter drill-downs ---

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        table_id = event.data_table.id
        idx = event.cursor_row
        if table_id == "gpu-consumers-table" and idx < len(self._activity_users):
            u = self._activity_users[idx]
            from slurmmon_cli.tui.screens.user_detail import UserDetailScreen
            self.app.push_screen(UserDetailScreen(
                user=u["user"], account=u.get("account"), gpu_only=True,
            ))
        elif table_id == "gpu-waste-table" and idx < len(self._waste_rows):
            r = self._waste_rows[idx]
            from slurmmon_cli.tui.screens.user_detail import UserDetailScreen
            self.app.push_screen(UserDetailScreen(
                user=r["user"], account=r.get("account"), gpu_only=True,
            ))

    def action_refresh(self) -> None:
        self._load_all()
