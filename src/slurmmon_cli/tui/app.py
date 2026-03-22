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
        Binding("m", "switch_screen('monitor')", "Monitor", show=False),
        Binding("x", "switch_screen('explorer')", "Explore", show=False),
        Binding("e", "switch_screen('efficiency')", "Efficiency", show=False),
        Binding("question_mark", "push_screen('settings')", "Settings", show=False),
        Binding("q", "quit", "Quit", show=False),
        Binding("left", "hscroll(-3)", "Scroll left", show=False),
        Binding("right", "hscroll(3)", "Scroll right", show=False),
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
        self._collect_done = False

    def on_mount(self) -> None:
        from slurmmon_cli.tui.screens.monitor import MonitorScreen
        from slurmmon_cli.tui.screens.explorer import ExplorerScreen
        from slurmmon_cli.tui.screens.efficiency import EfficiencyScreen
        from slurmmon_cli.tui.screens.settings import SettingsScreen

        self.install_screen(MonitorScreen(), name="monitor")
        self.install_screen(ExplorerScreen(), name="explorer")
        self.install_screen(EfficiencyScreen(), name="efficiency")
        self.install_screen(SettingsScreen(), name="settings")
        self.push_screen("monitor")

        # Detect cluster + collect in background
        if not self.from_db:
            self._initial_collect()

    @work(thread=True)
    def _initial_collect(self) -> None:
        """Detect cluster and run one collect cycle in background."""
        try:
            from slurmmon_cli.slurm import get_cluster_info, run_slurm_command
            from slurmmon_cli.storage.collector import collect_snapshot
            from slurmmon_cli.storage.database import Database

            # Detect cluster (try sinfo first, then scontrol)
            info = get_cluster_info()
            if info and info.cluster_name and info.cluster_name != "unknown":
                self.cluster_name = info.cluster_name
            if not self.cluster_name:
                # Fallback: try scontrol for cluster name
                try:
                    import subprocess, json as _json
                    result = subprocess.run(
                        ["scontrol", "show", "config"],
                        capture_output=True, text=True, timeout=10,
                    )
                    for line in result.stdout.splitlines():
                        if line.strip().startswith("ClusterName"):
                            self.cluster_name = line.split("=", 1)[1].strip()
                            break
                except Exception:
                    pass
            if self.cluster_name:
                self.sub_title = self.cluster_name

            # Then collect (force sshare on TUI startup so explorer has data)
            db = Database(self.db_path)
            db.connect()
            try:
                # Fix stale cluster names from earlier runs
                if self.cluster_name:
                    for table in ("user_usage", "jobs"):
                        db.conn.execute(
                            f"UPDATE {table} SET cluster = ? "
                            "WHERE cluster IN ('', 'unknown')",
                            (self.cluster_name,),
                        )
                    db.conn.commit()
                stats = collect_snapshot(db, sshare_interval=0,
                                        cluster_override=self.cluster_name)
                if not self.cluster_name:
                    self.cluster_name = stats.get("cluster", "")
                    if self.cluster_name:
                        self.sub_title = self.cluster_name
            finally:
                db.close()
        except Exception as exc:
            log.debug("Initial collection failed: %s", exc)
        finally:
            self.call_from_thread(self._on_collect_done)

    def action_hscroll(self, delta: int) -> None:
        """Scroll the focused widget horizontally."""
        focused = self.focused
        if focused is not None and hasattr(focused, "scroll_to"):
            focused.scroll_to(
                x=focused.scroll_x + delta, animate=False,
            )

    def _on_collect_done(self) -> None:
        """Notify the active screen that initial collection is complete."""
        self._collect_done = True
        # Only notify the currently visible screen (it is mounted and can
        # spawn workers safely).  Other screens will pick up data via
        # on_screen_resume or on_mount when the user navigates to them.
        screen = self.screen
        if hasattr(screen, "on_initial_collect_done"):
            screen.on_initial_collect_done()


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
