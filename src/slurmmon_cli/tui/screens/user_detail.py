"""User detail modal - shows active jobs for a selected user."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import DataTable, Static
from textual import work

from slurmmon_cli.tui.formatting import format_duration, format_mem


def _build_grafana_url(nodes: list[str]) -> str:
    """Build an OSC Grafana node-metrics URL for a set of nodes."""
    base = (
        "https://grafana.osc.edu/d/qc1PWAUWz/cluster-metrics"
        "?orgId=1&var-cluster=All"
    )
    host_params = "".join(f"&var-host={n}" for n in sorted(nodes))
    return f"{base}{host_params}&var-jobid=&from=now-24h&to=now"


class UserDetailScreen(ModalScreen):
    """Modal showing running/pending jobs for a single user."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close", show=True),
        Binding("g", "grafana", "Grafana URL", show=True),
    ]

    DEFAULT_CSS = """
    UserDetailScreen {
        align: center middle;
    }
    #user-detail-container {
        width: 90%;
        min-width: 90;
        max-width: 160;
        max-height: 85%;
        border: thick $accent;
        padding: 1 2;
        background: $surface;
    }
    """

    def __init__(self, user: str, account: str | None = None,
                 gpu_only: bool = False, **kwargs):
        super().__init__(**kwargs)
        self._user = user
        self._account = account
        self._gpu_only = gpu_only
        self._jobs: list = []

    def compose(self) -> ComposeResult:
        with VerticalScroll(id="user-detail-container"):
            yield Static("", id="user-info")
            yield DataTable(id="user-jobs-table")
            yield Static(
                " \\[g] copy Grafana URL for running nodes  \\[esc] close",
                id="user-detail-hint",
            )

    def on_mount(self) -> None:
        jt = self.query_one("#user-jobs-table", DataTable)
        jt.add_columns(
            "JOBID", "ACCOUNT", "STATE", "PARTITION", "CPUS", "GPUS",
            "MEM", "NODES", "ELAPSED", "LIMIT", "CLUSTER",
        )
        jt.cursor_type = "row"

        label = "GPU jobs" if self._gpu_only else "Jobs"
        self.query_one("#user-info", Static).update(
            f" User: {self._user}    ({label}, loading...)"
        )
        self._load_jobs()

    @work(thread=True)
    def _load_jobs(self) -> None:
        from slurmmon_cli.tui.data import fetch_user_jobs
        db_path = getattr(self.app, "db_path", None)
        jobs = fetch_user_jobs(db_path, self._user, gpu_only=self._gpu_only)
        self.app.call_from_thread(self._update_jobs, jobs)

    def _update_jobs(self, jobs) -> None:
        self._jobs = jobs
        running = sum(1 for j in jobs if j.state == "RUNNING")
        pending = sum(1 for j in jobs if j.state == "PENDING")
        gpus = sum(j.num_gpus for j in jobs if j.state == "RUNNING")
        cpus = sum(j.num_cpus for j in jobs if j.state == "RUNNING")
        accounts = sorted({j.account for j in jobs if j.account})
        clusters = sorted({j.cluster for j in jobs if j.cluster})
        label = "GPU jobs" if self._gpu_only else "Jobs"
        acct_str = ", ".join(accounts) if accounts else "-"
        cluster_str = ", ".join(clusters) if clusters else "-"
        self.query_one("#user-info", Static).update(
            f" User: {self._user}    Account(s): {acct_str}"
            f"    Cluster(s): {cluster_str}\n"
            f" {label}: {running} running, {pending} pending"
            f"    CPUs: {cpus}    GPUs: {gpus}"
        )

        jt = self.query_one("#user-jobs-table", DataTable)
        jt.clear()
        for j in jobs:
            jt.add_row(
                j.job_id,
                j.account or "-",
                j.state,
                j.partition or "-",
                str(j.num_cpus),
                str(j.num_gpus),
                format_mem(j.req_mem_mb),
                j.node_list or "-",
                format_duration(j.elapsed_s),
                format_duration(j.time_limit_s),
                j.cluster or "-",
            )
        if not jobs:
            self.query_one("#user-info", Static).update(
                f" User: {self._user}\n\n"
                f" No active {label.lower()} found."
            )

    def action_grafana(self) -> None:
        """Build Grafana URL from running job nodes and copy to clipboard."""
        from slurmmon_cli.slurm import expand_node_list

        all_nodes: set[str] = set()
        for j in self._jobs:
            if j.state == "RUNNING" and j.node_list:
                all_nodes.update(expand_node_list(j.node_list))
        if not all_nodes:
            self.notify("No running nodes to generate URL for.", severity="warning")
            return
        url = _build_grafana_url(sorted(all_nodes))
        self.app.copy_to_clipboard(url)
        self.notify(
            f"Grafana URL copied ({len(all_nodes)} nodes)",
            severity="information",
        )

    def action_dismiss(self) -> None:
        self.app.pop_screen()
