"""GPU-focused queue, activity, and waste analysis."""

from __future__ import annotations

import sqlite3
from statistics import mean, median


def _percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * p / 100.0
    f = int(k)
    c = f + 1
    if c >= len(sorted_vals):
        return sorted_vals[-1]
    return sorted_vals[f] + (k - f) * (sorted_vals[c] - sorted_vals[f])


# ---------------------------------------------------------------------------
# GPU Queue wait time analysis
# ---------------------------------------------------------------------------

def gpu_wait_summary(conn: sqlite3.Connection) -> dict:
    """Compare GPU vs non-GPU job wait times."""
    def _stats(rows):
        waits = sorted(r[0] for r in rows)
        if not waits:
            return {"count": 0, "mean": 0, "median": 0, "p90": 0, "max": 0}
        return {
            "count": len(waits),
            "mean": round(mean(waits)),
            "median": round(median(waits)),
            "p90": round(_percentile(waits, 90)),
            "max": round(waits[-1]),
        }

    base = ("start_time IS NOT NULL AND submit_time IS NOT NULL "
            "AND start_time > submit_time")
    gpu_rows = conn.execute(
        f"SELECT start_time - submit_time FROM jobs WHERE {base} AND num_gpus > 0"
    ).fetchall()
    cpu_rows = conn.execute(
        f"SELECT start_time - submit_time FROM jobs WHERE {base} AND "
        "(num_gpus IS NULL OR num_gpus = 0)"
    ).fetchall()
    return {"gpu": _stats(gpu_rows), "cpu_only": _stats(cpu_rows)}


def gpu_wait_by_count(conn: sqlite3.Connection) -> list[dict]:
    """GPU job wait times bucketed by GPU count."""
    rows = conn.execute("""
        SELECT num_gpus,
               COUNT(*) AS count,
               ROUND(AVG(start_time - submit_time)) AS avg_wait,
               ROUND(MIN(start_time - submit_time)) AS min_wait,
               ROUND(MAX(start_time - submit_time)) AS max_wait
        FROM jobs
        WHERE num_gpus > 0
              AND start_time IS NOT NULL AND submit_time IS NOT NULL
              AND start_time > submit_time
        GROUP BY num_gpus ORDER BY num_gpus
    """).fetchall()
    return [dict(r) for r in rows]


def gpu_wait_by_partition(conn: sqlite3.Connection) -> list[dict]:
    """GPU job wait times bucketed by partition."""
    rows = conn.execute("""
        SELECT partition,
               COUNT(*) AS count,
               AVG(num_gpus) AS avg_gpus,
               ROUND(AVG(start_time - submit_time)) AS avg_wait,
               ROUND(MIN(start_time - submit_time)) AS min_wait,
               ROUND(MAX(start_time - submit_time)) AS max_wait
        FROM jobs
        WHERE num_gpus > 0
              AND start_time IS NOT NULL AND submit_time IS NOT NULL
              AND start_time > submit_time
        GROUP BY partition ORDER BY avg_wait DESC
    """).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# GPU Activity
# ---------------------------------------------------------------------------

def gpu_running_by_user(conn: sqlite3.Connection, top: int = 15) -> list[dict]:
    """Top GPU consumers right now (running jobs)."""
    rows = conn.execute("""
        SELECT user, account,
               SUM(num_gpus) AS gpus,
               COUNT(*) AS jobs,
               SUM(num_cpus) AS cpus,
               GROUP_CONCAT(DISTINCT partition) AS partitions
        FROM jobs
        WHERE state = 'RUNNING' AND num_gpus > 0
        GROUP BY user, account
        ORDER BY gpus DESC
        LIMIT ?
    """, (top,)).fetchall()
    return [dict(r) for r in rows]


def gpu_pending_summary(conn: sqlite3.Connection) -> list[dict]:
    """Pending GPU demand by partition."""
    rows = conn.execute("""
        SELECT partition,
               COUNT(*) AS jobs,
               SUM(num_gpus) AS gpus_requested,
               AVG(num_gpus) AS avg_gpus_per_job,
               MIN(submit_time) AS oldest_submit
        FROM jobs
        WHERE state = 'PENDING' AND num_gpus > 0
        GROUP BY partition ORDER BY gpus_requested DESC
    """).fetchall()
    return [dict(r) for r in rows]


def gpu_snapshot_trend(conn: sqlite3.Connection, limit: int = 50) -> list[dict]:
    """Recent GPU allocation trend from snapshots."""
    rows = conn.execute("""
        SELECT timestamp, total_gpus, alloc_gpus, running_jobs, pending_jobs
        FROM snapshots
        WHERE total_gpus > 0
        ORDER BY timestamp DESC LIMIT ?
    """, (limit,)).fetchall()
    return [dict(r) for r in reversed(rows)]


# ---------------------------------------------------------------------------
# GPU Waste / Efficiency
# ---------------------------------------------------------------------------

def gpu_jobs_low_cpu_eff(conn: sqlite3.Connection, threshold: float = 50.0,
                         limit: int = 20) -> list[dict]:
    """GPU jobs with low CPU efficiency (wasting CPU alongside GPU)."""
    rows = conn.execute("""
        SELECT job_id, user, account, partition, num_cpus, num_gpus,
               elapsed_s, time_limit_s, state,
               ROUND(cpu_time_s / (num_cpus * elapsed_s) * 100.0, 1) AS cpu_eff
        FROM jobs
        WHERE num_gpus > 0 AND cpu_time_s IS NOT NULL
              AND elapsed_s > 0 AND num_cpus > 0
              AND state IN ('COMPLETED', 'FAILED', 'TIMEOUT', 'RUNNING')
              AND (cpu_time_s / (num_cpus * elapsed_s) * 100.0) < ?
        ORDER BY cpu_eff ASC
        LIMIT ?
    """, (threshold, limit)).fetchall()
    return [dict(r) for r in rows]


def gpu_jobs_walltime_waste(conn: sqlite3.Connection, threshold: float = 30.0,
                            limit: int = 20) -> list[dict]:
    """GPU jobs that used a small fraction of their time limit."""
    rows = conn.execute("""
        SELECT job_id, user, account, partition, num_gpus,
               elapsed_s, time_limit_s,
               ROUND(elapsed_s * 100.0 / time_limit_s, 1) AS time_used_pct,
               state
        FROM jobs
        WHERE num_gpus > 0 AND elapsed_s > 0 AND time_limit_s > 0
              AND state IN ('COMPLETED', 'FAILED', 'TIMEOUT')
              AND (elapsed_s * 100.0 / time_limit_s) < ?
        ORDER BY time_used_pct ASC
        LIMIT ?
    """, (threshold, limit)).fetchall()
    return [dict(r) for r in rows]


def gpu_user_jobs(conn: sqlite3.Connection, user: str,
                  limit: int = 50) -> list[dict]:
    """Recent GPU jobs for a specific user (running + completed)."""
    rows = conn.execute("""
        SELECT job_id, partition, num_cpus, num_gpus, elapsed_s, time_limit_s,
               cpu_time_s, req_mem_mb, max_rss_mb, state,
               CASE WHEN cpu_time_s IS NOT NULL AND elapsed_s > 0 AND num_cpus > 0
                   THEN ROUND(cpu_time_s / (num_cpus * elapsed_s) * 100.0, 1)
                   END AS cpu_eff,
               CASE WHEN max_rss_mb IS NOT NULL AND req_mem_mb > 0
                   THEN ROUND(max_rss_mb / req_mem_mb * 100.0, 1)
                   END AS mem_eff,
               CASE WHEN elapsed_s > 0 AND time_limit_s > 0
                   THEN ROUND(elapsed_s * 100.0 / time_limit_s, 1)
                   END AS time_pct
        FROM jobs
        WHERE user = ? AND num_gpus > 0
              AND state IN ('RUNNING', 'COMPLETED', 'FAILED', 'TIMEOUT')
        ORDER BY
            CASE state WHEN 'RUNNING' THEN 0 ELSE 1 END,
            submit_time DESC
        LIMIT ?
    """, (user, limit)).fetchall()
    return [dict(r) for r in rows]
