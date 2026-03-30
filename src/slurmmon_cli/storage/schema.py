"""SQLite schema for slurmmon-cli historical data."""

from __future__ import annotations

import sqlite3

SCHEMA_VERSION = "6"

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
    last_seen     REAL NOT NULL,
    cluster       TEXT DEFAULT ''
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
    pending_jobs  INTEGER,
    total_gpus    INTEGER DEFAULT 0,
    alloc_gpus    INTEGER DEFAULT 0,
    cluster       TEXT DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_snap_time ON snapshots (timestamp);
CREATE INDEX IF NOT EXISTS idx_snap_cluster ON snapshots (cluster);

CREATE TABLE IF NOT EXISTS partitions (
    name          TEXT NOT NULL,
    state         TEXT,
    total_nodes   INTEGER,
    idle_nodes    INTEGER,
    alloc_nodes   INTEGER,
    other_nodes   INTEGER,
    total_cpus    INTEGER,
    avail_cpus    INTEGER,
    max_time      TEXT,
    last_updated  REAL,
    cluster       TEXT DEFAULT '',
    PRIMARY KEY (name, cluster)
);

CREATE TABLE IF NOT EXISTS metadata (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_usage (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at   REAL NOT NULL,
    account        TEXT NOT NULL,
    user           TEXT NOT NULL,
    raw_usage      INTEGER,
    fairshare      REAL,
    cpu_tres_mins  INTEGER,
    gpu_tres_mins  INTEGER,
    gpu_type_mins  TEXT,
    cluster        TEXT DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_uu_collected ON user_usage (collected_at);
CREATE INDEX IF NOT EXISTS idx_uu_user ON user_usage (user);
CREATE INDEX IF NOT EXISTS idx_uu_gpu ON user_usage (gpu_tres_mins);
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Create tables if they don't exist and apply migrations."""
    # Check current version before running DDL
    try:
        row = conn.execute(
            "SELECT value FROM metadata WHERE key = 'schema_version'"
        ).fetchone()
        current = row[0] if row else None
    except Exception:
        current = None

    # Migration: v2 -> v3: add cluster column to user_usage
    if current and current < "3":
        try:
            conn.execute("ALTER TABLE user_usage ADD COLUMN cluster TEXT DEFAULT ''")
            conn.commit()
        except Exception:
            pass  # column already exists

    # Migration: v3 -> v4: add cluster column to jobs
    if current and current < "4":
        try:
            conn.execute("ALTER TABLE jobs ADD COLUMN cluster TEXT DEFAULT ''")
            conn.commit()
        except Exception:
            pass

    # Migration: v4 -> v5: add GPU counts to snapshots
    if current and current < "5":
        for col in ("total_gpus", "alloc_gpus"):
            try:
                conn.execute(f"ALTER TABLE snapshots ADD COLUMN {col} INTEGER DEFAULT 0")
            except Exception:
                pass
        conn.commit()

    # Migration: v5 -> v6: add cluster to snapshots; composite PK for partitions
    if current and current < "6":
        try:
            conn.execute("ALTER TABLE snapshots ADD COLUMN cluster TEXT DEFAULT ''")
        except Exception:
            pass
        # Recreate partitions with composite PK (name, cluster)
        try:
            conn.execute("""CREATE TABLE IF NOT EXISTS partitions_new (
                name TEXT NOT NULL, state TEXT, total_nodes INTEGER,
                idle_nodes INTEGER, alloc_nodes INTEGER, other_nodes INTEGER,
                total_cpus INTEGER, avail_cpus INTEGER, max_time TEXT,
                last_updated REAL, cluster TEXT DEFAULT '',
                PRIMARY KEY (name, cluster)
            )""")
            conn.execute(
                "INSERT OR IGNORE INTO partitions_new "
                "SELECT name, state, total_nodes, idle_nodes, alloc_nodes, "
                "other_nodes, total_cpus, avail_cpus, max_time, last_updated, "
                "'' FROM partitions"
            )
            conn.execute("DROP TABLE partitions")
            conn.execute("ALTER TABLE partitions_new RENAME TO partitions")
        except Exception:
            pass
        conn.commit()

    conn.executescript(DDL)
    # Create cluster indexes (safe now that columns exist)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_uu_cluster ON user_usage (cluster)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_cluster ON jobs (cluster)")

    if current is None:
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('schema_version', ?)",
            (SCHEMA_VERSION,),
        )
    elif current != SCHEMA_VERSION:
        conn.execute(
            "UPDATE metadata SET value = ? WHERE key = 'schema_version'",
            (SCHEMA_VERSION,),
        )
    conn.commit()
