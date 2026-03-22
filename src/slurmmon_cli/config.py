"""Configuration management for slurmmon-cli."""

from __future__ import annotations

import configparser
import os
from pathlib import Path

DEFAULTS = {
    "general": {
        "osc": "false",
        "db_path": "",
        "refresh_interval": "30",
        "retention_days": "30",
    },
}

DEFAULT_CONFIG_PATH = os.path.join(Path.home(), ".slurmmon-cli", "config.ini")


class SlurmmonConfig:
    """INI-based config backed by configparser with in-memory defaults."""

    def __init__(self, path: str | None = None):
        self.path = path or DEFAULT_CONFIG_PATH
        self._parser = configparser.ConfigParser()
        # Load defaults
        self._parser.read_dict(DEFAULTS)
        # Overlay file if it exists
        if os.path.isfile(self.path):
            self._parser.read(self.path)

    def get(self, section: str, key: str) -> str:
        return self._parser.get(section, key, fallback="")

    def getboolean(self, section: str, key: str) -> bool:
        return self._parser.getboolean(section, key, fallback=False)

    def getint(self, section: str, key: str) -> int:
        return self._parser.getint(section, key, fallback=0)

    def set(self, section: str, key: str, value: str) -> None:
        if not self._parser.has_section(section):
            self._parser.add_section(section)
        self._parser.set(section, key, value)

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "w") as f:
            self._parser.write(f)

    def sections(self) -> list[str]:
        return self._parser.sections()

    def items(self, section: str) -> list[tuple[str, str]]:
        return list(self._parser.items(section))


def load_config(path: str | None = None) -> SlurmmonConfig:
    """Load config from path or default location."""
    return SlurmmonConfig(path)
