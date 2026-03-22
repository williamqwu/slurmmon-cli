"""Node detail modal - shows jobs and stats for a selected node."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import DataTable, Static
from textual import work

from slurmmon_cli.models import NodeUtilization
from slurmmon_cli.tui.formatting import format_duration, format_mem


class NodeDetailScreen(ModalScreen):
    """Modal showing detailed info for a single node."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close", show=True),
    ]

    DEFAULT_CSS = """
    NodeDetailScreen {
        align: center middle;
    }
    #node-detail-container {
        width: 90%;
        min-width: 80;
        max-width: 140;
        max-height: 85%;
        border: thick $accent;
        padding: 1 2;
        background: $surface;
    }
    """

    def __init__(self, node: NodeUtilization, **kwargs):
        super().__init__(**kwargs)
        self._node = node

    def compose(self) -> ComposeResult:
        with VerticalScroll(id="node-detail-container"):
            yield Static("", id="node-info")
            yield Static("Jobs on this node:", classes="settings-section")
            yield DataTable(id="node-jobs-table")

    def on_mount(self) -> None:
        n = self._node

        # Node info summary
        load_pct = f"{n.load_ratio * 100:.1f}%" if n.load_ratio is not None else "-"
        gpu_str = f"{n.gpus_alloc}/{n.gpus_total}"
        if n.gpu_type:
            gpu_str += f" {n.gpu_type}"
        mem_alloc = format_mem(n.mem_alloc_mb)
        mem_total = format_mem(n.mem_total_mb)
        users = ", ".join(n.users) if n.users else "-"
        partitions = ", ".join(n.partitions) if n.partitions else "-"

        info = (
            f" Node: {n.name}    State: {n.state}    Partitions: {partitions}\n"
            f" CPU: {n.cpu_load:.1f} load / {n.cpus_alloc} alloc / {n.cpus_total} total"
            f"    Load ratio: {load_pct}\n"
            f" Memory: {mem_alloc} alloc / {mem_total} total\n"
            f" GPU: {gpu_str}\n"
            f" Users: {users}"
        )
        self.query_one("#node-info", Static).update(info)

        # Jobs table
        jt = self.query_one("#node-jobs-table", DataTable)
        jt.add_columns("JOBID", "USER", "ACCOUNT", "CPUS", "GPUS", "MEM", "ELAPSED", "LIMIT")
        jt.cursor_type = "row"

        self._load_jobs()

    @work(thread=True)
    def _load_jobs(self) -> None:
        from slurmmon_cli.slurm import get_jobs_on_node
        jobs = get_jobs_on_node(self._node.name)
        self.app.call_from_thread(self._update_jobs, jobs)

    def _update_jobs(self, jobs) -> None:
        jt = self.query_one("#node-jobs-table", DataTable)
        jt.clear()
        for j in jobs:
            jt.add_row(
                j.job_id,
                j.user,
                j.account or "-",
                str(j.num_cpus),
                str(j.num_gpus),
                format_mem(j.req_mem_mb),
                format_duration(j.elapsed_s),
                format_duration(j.time_limit_s),
            )
        if not jobs:
            info = self.query_one("#node-info", Static)
            info.update(info.renderable + "\n\n (No running jobs found on this node)")

    def action_dismiss(self) -> None:
        self.app.pop_screen()
