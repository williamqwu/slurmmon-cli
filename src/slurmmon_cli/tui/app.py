"""Main Textual app for slurmmon-cli."""

from __future__ import annotations

import logging

from textual.app import App
from textual.binding import Binding
from textual import work

log = logging.getLogger(__name__)


class SlurmmonApp(App):
    """Multi-screen Slurm monitoring TUI."""

    TITLE = "slurmmon-cli"
    CSS_PATH = "styles/default.tcss"
    ENABLE_COMMAND_PALETTE = False

    BINDINGS = [
        Binding("m", "switch_screen('monitor')", "[M]onitor [X]plore [E]fficiency [S]ettings", show=True),
        Binding("x", "switch_screen('explorer')", "Explore", show=False),
        Binding("e", "switch_screen('efficiency')", "Efficiency", show=False),
        Binding("s", "push_screen('settings')", "Settings", show=False),
        Binding("q", "quit", "Quit", show=True),
    ]

    def __init__(
        self,
        db_path: str | None = None,
        refresh: int = 30,
        user_filter: str | None = None,
        partition_filter: str | None = None,
        from_db: bool = False,
        config=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.db_path = db_path
        self.refresh_interval = refresh
        self.user_filter = user_filter
        self.partition_filter = partition_filter
        self.from_db = from_db
        self.config = config
        self.cluster_name = ""

    def on_mount(self) -> None:
        # Detect cluster synchronously before installing screens
        # so Explorer has the cluster name immediately
        self._detect_cluster_sync()

        from slurmmon_cli.tui.screens.monitor import MonitorScreen
        from slurmmon_cli.tui.screens.explorer import ExplorerScreen
        from slurmmon_cli.tui.screens.efficiency import EfficiencyScreen
        from slurmmon_cli.tui.screens.settings import SettingsScreen

        self.install_screen(MonitorScreen(), name="monitor")
        self.install_screen(ExplorerScreen(), name="explorer")
        self.install_screen(EfficiencyScreen(), name="efficiency")
        self.install_screen(SettingsScreen(), name="settings")
        self.push_screen("monitor")

        # Background collection so Explorer tabs have data on first visit
        if not self.from_db:
            self._initial_collect()

    def _detect_cluster_sync(self) -> None:
        """Detect cluster name synchronously from sinfo."""
        try:
            from slurmmon_cli.slurm import get_cluster_info
            info = get_cluster_info()
            if info and info.cluster_name and info.cluster_name != "unknown":
                self.cluster_name = info.cluster_name
                self.sub_title = self.cluster_name
        except Exception:
            pass

    @work(thread=True)
    def _initial_collect(self) -> None:
        """Run one collect cycle in background to populate the DB."""
        try:
            from slurmmon_cli.storage.collector import collect_snapshot
            from slurmmon_cli.storage.database import Database

            cfg = self.config
            sshare_interval = int(cfg.get("general", "sshare_interval")) if cfg else 1800
            db = Database(self.db_path)
            db.connect()
            try:
                stats = collect_snapshot(db, sshare_interval=sshare_interval)
                if not self.cluster_name:
                    self.cluster_name = stats.get("cluster", "")
            finally:
                db.close()
        except Exception as exc:
            log.debug("Initial collection failed: %s", exc)


def run_dashboard(
    db_path: str | None = None,
    refresh: int = 30,
    user_filter: str | None = None,
    partition_filter: str | None = None,
    from_db: bool = False,
    config=None,
) -> None:
    """Launch the Textual TUI dashboard."""
    app = SlurmmonApp(
        db_path=db_path,
        refresh=refresh,
        user_filter=user_filter,
        partition_filter=partition_filter,
        from_db=from_db,
        config=config,
    )
    app.run(mouse=False)
