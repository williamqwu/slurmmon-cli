"""Settings screen - configuration UI."""

from __future__ import annotations

import os

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Header, Input, Label, Static, Switch


class SettingsScreen(ModalScreen):
    """Configuration settings overlay (Escape to close)."""

    BINDINGS = [
        Binding("escape", "dismiss_settings", "Close", show=True),
    ]

    def compose(self) -> ComposeResult:
        with VerticalScroll(id="settings-container"):
            yield Static("Settings", classes="settings-section")
            yield Static("")

            yield Static("General", classes="settings-section")
            yield Label("OSC mode (gpu-seff/osc-seff)")
            yield Switch(id="osc-switch", value=False)
            yield Static("")
            yield Label("Refresh interval (seconds)")
            yield Input(id="refresh-input", value="30", type="integer")
            yield Static("")
            yield Label("Retention days")
            yield Input(id="retention-input", value="30", type="integer")
            yield Static("")
            yield Label("sshare interval (seconds)")
            yield Input(id="sshare-input", value="1800", type="integer")

            yield Static("")
            yield Static("Database", classes="settings-section")
            yield Static("", id="db-info")

    def on_mount(self) -> None:
        cfg = getattr(self.app, "config", None)
        if cfg:
            self.query_one("#osc-switch", Switch).value = cfg.getboolean("general", "osc")
            self.query_one("#refresh-input", Input).value = cfg.get("general", "refresh_interval")
            self.query_one("#retention-input", Input).value = cfg.get("general", "retention_days")
            self.query_one("#sshare-input", Input).value = cfg.get("general", "sshare_interval")

        self._update_db_info()

    def _update_db_info(self) -> None:
        db_path = getattr(self.app, "db_path", None)
        from slurmmon_cli.storage.database import Database
        db = Database(db_path)
        info_text = f"Path: {db.db_path}\n"
        try:
            size = os.path.getsize(db.db_path) if os.path.exists(db.db_path) else 0
            info_text += f"Size: {size / 1024:.1f} KB\n"
            with db:
                jobs = db.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
                snaps = db.conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
                usage = db.conn.execute("SELECT COUNT(*) FROM user_usage").fetchone()[0]
            info_text += f"Jobs: {jobs}  Snapshots: {snaps}  Usage rows: {usage}"
        except Exception:
            info_text += "(database not initialized)"
        self.query_one("#db-info", Static).update(info_text)

    def on_switch_changed(self, event: Switch.Changed) -> None:
        cfg = getattr(self.app, "config", None)
        if cfg and event.switch.id == "osc-switch":
            cfg.set("general", "osc", str(event.value).lower())

    def on_input_changed(self, event: Input.Changed) -> None:
        cfg = getattr(self.app, "config", None)
        if not cfg:
            return
        mapping = {
            "refresh-input": "refresh_interval",
            "retention-input": "retention_days",
            "sshare-input": "sshare_interval",
        }
        key = mapping.get(event.input.id)
        if key and event.value.strip().isdigit():
            cfg.set("general", key, event.value.strip())

    def action_dismiss_settings(self) -> None:
        cfg = getattr(self.app, "config", None)
        if cfg:
            try:
                cfg.save()
            except Exception:
                pass
        self.app.pop_screen()
