"""Data collector: snapshots Slurm state into SQLite."""

from __future__ import annotations

import logging
import time

from slurmwatch.models import ClusterInfo, Job
from slurmwatch.slurm import get_cluster_info, get_job_history, get_queue
from slurmwatch.storage.database import Database

log = logging.getLogger(__name__)


def _upsert_jobs(db: Database, jobs: list[Job], now: float) -> int:
    """Upsert jobs into the database. Returns count of upserted rows."""
    if not jobs:
        return 0
    conn = db.conn
    conn.executemany(
        """INSERT INTO jobs (
            job_id, user, account, partition, state, num_cpus, num_gpus,
            req_mem_mb, submit_time, start_time, end_time, time_limit_s,
            elapsed_s, node_list, exit_code, cpu_time_s, max_rss_mb,
            reason, last_seen
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET
            state = excluded.state,
            start_time = COALESCE(excluded.start_time, start_time),
            end_time = COALESCE(excluded.end_time, end_time),
            elapsed_s = COALESCE(excluded.elapsed_s, elapsed_s),
            node_list = COALESCE(excluded.node_list, node_list),
            exit_code = COALESCE(excluded.exit_code, exit_code),
            cpu_time_s = COALESCE(excluded.cpu_time_s, cpu_time_s),
            max_rss_mb = COALESCE(excluded.max_rss_mb, max_rss_mb),
            reason = excluded.reason,
            last_seen = excluded.last_seen
        """,
        [
            (
                j.job_id, j.user, j.account, j.partition, j.state,
                j.num_cpus, j.num_gpus, j.req_mem_mb,
                j.submit_time, j.start_time, j.end_time,
                j.time_limit_s, j.elapsed_s, j.node_list,
                j.exit_code, j.cpu_time_s, j.max_rss_mb,
                j.reason, now,
            )
            for j in jobs
        ],
    )
    conn.commit()
    return len(jobs)


def _insert_snapshot(db: Database, info: ClusterInfo, now: float,
                     running: int, pending: int) -> None:
    """Insert a cluster-level snapshot row."""
    conn = db.conn
    conn.execute(
        """INSERT INTO snapshots (
            timestamp, total_nodes, idle_nodes, alloc_nodes, down_nodes,
            mixed_nodes, total_cpus, alloc_cpus, running_jobs, pending_jobs
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            now, info.total_nodes, info.idle_nodes, info.alloc_nodes,
            info.down_nodes, info.mixed_nodes, info.total_cpus,
            info.alloc_cpus, running, pending,
        ),
    )
    conn.commit()


def _update_partitions(db: Database, info: ClusterInfo, now: float) -> None:
    """Replace partition records with current state."""
    conn = db.conn
    conn.executemany(
        """INSERT INTO partitions (
            name, state, total_nodes, idle_nodes, alloc_nodes, other_nodes,
            total_cpus, avail_cpus, max_time, last_updated
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            state = excluded.state,
            total_nodes = excluded.total_nodes,
            idle_nodes = excluded.idle_nodes,
            alloc_nodes = excluded.alloc_nodes,
            other_nodes = excluded.other_nodes,
            total_cpus = excluded.total_cpus,
            avail_cpus = excluded.avail_cpus,
            max_time = excluded.max_time,
            last_updated = excluded.last_updated
        """,
        [
            (
                p.name, p.state, p.total_nodes, p.idle_nodes, p.alloc_nodes,
                p.other_nodes, p.total_cpus, p.avail_cpus, p.max_time, now,
            )
            for p in info.partitions
        ],
    )
    conn.commit()


def _get_last_collect_time(db: Database) -> str:
    """Get last collection timestamp for sacct --starttime, or default."""
    row = db.conn.execute(
        "SELECT value FROM metadata WHERE key = 'last_collect_time'"
    ).fetchone()
    if row:
        return row[0]
    return "now-24hours"


def _set_last_collect_time(db: Database, t: float) -> None:
    db.conn.execute(
        """INSERT INTO metadata (key, value) VALUES ('last_collect_time', ?)
           ON CONFLICT(key) DO UPDATE SET value = excluded.value""",
        (str(int(t)),),
    )
    db.conn.commit()


def prune_old_jobs(db: Database, retention_days: int = 30) -> int:
    """Delete jobs older than retention_days. Returns deleted count."""
    cutoff = time.time() - (retention_days * 86400)
    cursor = db.conn.execute(
        "DELETE FROM jobs WHERE last_seen < ?", (cutoff,)
    )
    db.conn.commit()
    return cursor.rowcount


def collect_snapshot(db: Database) -> dict:
    """Run one collection cycle. Returns summary stats."""
    now = time.time()
    stats: dict = {"timestamp": now, "queue_jobs": 0, "history_jobs": 0, "pruned": 0}

    # 1. Current queue
    queue_jobs = get_queue()
    if queue_jobs:
        _upsert_jobs(db, queue_jobs, now)
        stats["queue_jobs"] = len(queue_jobs)

    # 2. Cluster info
    cluster_info = get_cluster_info()
    if cluster_info:
        running = sum(1 for j in queue_jobs if j.state == "RUNNING")
        pending = sum(1 for j in queue_jobs if j.state == "PENDING")
        _insert_snapshot(db, cluster_info, now, running, pending)
        _update_partitions(db, cluster_info, now)

    # 3. Recently completed jobs
    starttime = _get_last_collect_time(db)
    history_jobs = get_job_history(starttime=starttime)
    if history_jobs:
        _upsert_jobs(db, history_jobs, now)
        stats["history_jobs"] = len(history_jobs)

    _set_last_collect_time(db, now)

    # 4. Prune
    stats["pruned"] = prune_old_jobs(db)

    return stats


def run_collector(db_path: str | None = None, interval: int = 300,
                  daemon: bool = False, retention_days: int = 30) -> None:
    """Main collector entry point."""
    db = Database(db_path)
    db.connect()
    try:
        while True:
            try:
                stats = collect_snapshot(db)
                log.info(
                    "Collected: %d queue + %d history jobs, pruned %d",
                    stats["queue_jobs"], stats["history_jobs"], stats["pruned"],
                )
            except Exception:
                log.exception("Collection cycle failed")
            if not daemon:
                break
            time.sleep(interval)
    finally:
        db.close()
