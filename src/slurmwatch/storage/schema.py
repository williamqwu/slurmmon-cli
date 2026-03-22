"""SQLite schema for slurmwatch historical data."""

from __future__ import annotations

import sqlite3

SCHEMA_VERSION = "1"

DDL = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id        TEXT PRIMARY KEY,
    user          TEXT NOT NULL,
    account       TEXT,
    partition     TEXT,
    state         TEXT NOT NULL,
    num_cpus      INTEGER,
    num_gpus      INTEGER DEFAULT 0,
    req_mem_mb    REAL,
    submit_time   REAL,
    start_time    REAL,
    end_time      REAL,
    time_limit_s  INTEGER,
    elapsed_s     INTEGER,
    node_list     TEXT,
    exit_code     TEXT,
    cpu_time_s    REAL,
    max_rss_mb    REAL,
    reason        TEXT,
    last_seen     REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_user ON jobs (user);
CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs (state);
CREATE INDEX IF NOT EXISTS idx_jobs_submit ON jobs (submit_time);
CREATE INDEX IF NOT EXISTS idx_jobs_user_state ON jobs (user, state);
CREATE INDEX IF NOT EXISTS idx_jobs_account ON jobs (account);

CREATE TABLE IF NOT EXISTS snapshots (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp     REAL NOT NULL,
    total_nodes   INTEGER,
    idle_nodes    INTEGER,
    alloc_nodes   INTEGER,
    down_nodes    INTEGER,
    mixed_nodes   INTEGER,
    total_cpus    INTEGER,
    alloc_cpus    INTEGER,
    running_jobs  INTEGER,
    pending_jobs  INTEGER
);

CREATE INDEX IF NOT EXISTS idx_snap_time ON snapshots (timestamp);

CREATE TABLE IF NOT EXISTS partitions (
    name          TEXT PRIMARY KEY,
    state         TEXT,
    total_nodes   INTEGER,
    idle_nodes    INTEGER,
    alloc_nodes   INTEGER,
    other_nodes   INTEGER,
    total_cpus    INTEGER,
    avail_cpus    INTEGER,
    max_time      TEXT,
    last_updated  REAL
);

CREATE TABLE IF NOT EXISTS metadata (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Create tables if they don't exist and check schema version."""
    conn.executescript(DDL)
    row = conn.execute(
        "SELECT value FROM metadata WHERE key = 'schema_version'"
    ).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('schema_version', ?)",
            (SCHEMA_VERSION,),
        )
        conn.commit()
