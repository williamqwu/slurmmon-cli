"""SQLite connection manager with WAL mode."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from slurmwatch.storage.schema import ensure_schema


class Database:
    """Lightweight SQLite connection wrapper."""

    def __init__(self, db_path: str | None = None):
        if db_path is None:
            db_dir = Path.home() / ".slurmwatch"
            db_dir.mkdir(exist_ok=True)
            db_path = str(db_dir / "data.db")
        self.db_path = db_path
        self._conn: sqlite3.Connection | None = None

    def connect(self, readonly: bool = False) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        if readonly:
            uri = f"file:{self.db_path}?mode=ro"
            self._conn = sqlite3.connect(uri, uri=True)
        else:
            # Ensure parent directory exists
            os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
            self._conn = sqlite3.connect(self.db_path)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            ensure_schema(self._conn)
        self._conn.row_factory = sqlite3.Row
        return self._conn

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            return self.connect()
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *exc):
        self.close()
