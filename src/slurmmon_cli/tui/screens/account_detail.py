"""Account detail modal - shows users and jobs for a selected account."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import DataTable, Static
from textual import work

from slurmmon_cli.tui.formatting import format_duration, format_mem
from slurmmon_cli.tui.screens.user_detail import _build_grafana_url


class AccountDetailScreen(ModalScreen):
    """Modal showing running/pending jobs for a single account."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close", show=True),
        Binding("g", "grafana", "Grafana URL", show=True),
    ]

    DEFAULT_CSS = """
    AccountDetailScreen {
        align: center middle;
    }
    #account-detail-container {
        width: 90%;
        min-width: 100;
        max-width: 170;
        max-height: 85%;
        border: thick $accent;
        padding: 1 2;
        background: $surface;
    }
    """

    def __init__(self, account: str, **kwargs):
        super().__init__(**kwargs)
        self._account = account
        self._jobs: list = []

    def compose(self) -> ComposeResult:
        with VerticalScroll(id="account-detail-container"):
            yield Static("", id="account-info")
            yield DataTable(id="account-jobs-table")
            yield Static(
                " \\[g] copy Grafana URL for running nodes  \\[esc] close",
                id="account-detail-hint",
            )

    def on_mount(self) -> None:
        jt = self.query_one("#account-jobs-table", DataTable)
        jt.add_columns(
            "USER", "JOBID", "STATE", "PARTITION", "CPUS", "GPUS",
            "MEM", "NODES", "ELAPSED", "LIMIT", "CLUSTER",
        )
        jt.cursor_type = "row"

        self.query_one("#account-info", Static).update(
            f" Account: {self._account}    (loading...)"
        )
        self._load_jobs()

    @work(thread=True)
    def _load_jobs(self) -> None:
        from slurmmon_cli.tui.data import fetch_account_jobs
        db_path = getattr(self.app, "db_path", None)
        jobs = fetch_account_jobs(db_path, self._account)
        self.app.call_from_thread(self._update_jobs, jobs)

    def _update_jobs(self, jobs) -> None:
        self._jobs = jobs
        running = sum(1 for j in jobs if j.state == "RUNNING")
        pending = sum(1 for j in jobs if j.state == "PENDING")
        users = len({j.user for j in jobs})
        gpus = sum(j.num_gpus for j in jobs if j.state == "RUNNING")
        clusters = sorted({j.cluster for j in jobs if j.cluster})
        cluster_str = ", ".join(clusters) if clusters else "-"
        self.query_one("#account-info", Static).update(
            f" Account: {self._account}    Cluster(s): {cluster_str}\n"
            f" Users: {users}    Jobs: {running} running, {pending} pending"
            f"    GPUs in use: {gpus}"
        )

        jt = self.query_one("#account-jobs-table", DataTable)
        jt.clear()
        for j in jobs:
            jt.add_row(
                j.user,
                j.job_id,
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
            self.query_one("#account-info", Static).update(
                f" Account: {self._account}\n\n"
                f" No active jobs found."
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
