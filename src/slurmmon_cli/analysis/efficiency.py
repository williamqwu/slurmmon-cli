"""Job efficiency analysis queries."""

from __future__ import annotations

import sqlite3


def job_efficiency(conn: sqlite3.Connection, job_id: str) -> dict | None:
    """Single-job efficiency (CPU and memory)."""
    row = conn.execute(
        """SELECT job_id, user, partition, num_cpus, elapsed_s,
                  cpu_time_s, req_mem_mb, max_rss_mb, state
           FROM jobs WHERE job_id = ?""",
        (job_id,),
    ).fetchone()
    if not row:
        return None
    d = dict(row)
    d["cpu_eff_pct"] = None
    d["mem_eff_pct"] = None
    if d["cpu_time_s"] and d["elapsed_s"] and d["num_cpus"] and d["elapsed_s"] > 0:
        d["cpu_eff_pct"] = round(
            d["cpu_time_s"] / (d["num_cpus"] * d["elapsed_s"]) * 100, 1
        )
    if d["max_rss_mb"] and d["req_mem_mb"] and d["req_mem_mb"] > 0:
        d["mem_eff_pct"] = round(d["max_rss_mb"] / d["req_mem_mb"] * 100, 1)
    return d


def efficiency_summary(conn: sqlite3.Connection, user: str | None = None,
                       since: float | None = None) -> dict:
    """Aggregate efficiency stats."""
    conditions = ["elapsed_s > 0", "num_cpus > 0",
                  "state IN ('COMPLETED', 'FAILED', 'TIMEOUT')"]
    params: list = []
    if user:
        conditions.append("user = ?")
        params.append(user)
    if since:
        conditions.append("submit_time >= ?")
        params.append(since)
    where = "WHERE " + " AND ".join(conditions)

    row = conn.execute(
        f"""SELECT
            COUNT(*) AS total_jobs,
            ROUND(AVG(CASE WHEN cpu_time_s IS NOT NULL
                THEN cpu_time_s / (num_cpus * elapsed_s) * 100.0 END), 1) AS avg_cpu_eff,
            ROUND(AVG(CASE WHEN max_rss_mb IS NOT NULL AND req_mem_mb > 0
                THEN max_rss_mb / req_mem_mb * 100.0 END), 1) AS avg_mem_eff,
            SUM(CASE WHEN cpu_time_s IS NOT NULL THEN 1 ELSE 0 END) AS jobs_with_cpu_data,
            SUM(CASE WHEN max_rss_mb IS NOT NULL THEN 1 ELSE 0 END) AS jobs_with_mem_data
        FROM jobs {where}""",
        params,
    ).fetchone()
    return dict(row) if row else {"total_jobs": 0}


def low_efficiency_jobs(conn: sqlite3.Connection, threshold_pct: float = 50.0,
                        user: str | None = None, since: float | None = None,
                        limit: int = 50) -> list[dict]:
    """Jobs with CPU or memory efficiency below threshold."""
    conditions = ["elapsed_s > 0", "num_cpus > 0",
                  "state IN ('COMPLETED', 'FAILED', 'TIMEOUT')"]
    params: list = []
    if user:
        conditions.append("user = ?")
        params.append(user)
    if since:
        conditions.append("submit_time >= ?")
        params.append(since)
    where = "WHERE " + " AND ".join(conditions)

    rows = conn.execute(
        f"""SELECT * FROM (
            SELECT
                job_id, user, partition, num_cpus, elapsed_s,
                cpu_time_s, req_mem_mb, max_rss_mb, state,
                CASE WHEN cpu_time_s IS NOT NULL
                    THEN ROUND(cpu_time_s / (num_cpus * elapsed_s) * 100.0, 1)
                    END AS cpu_eff_pct,
                CASE WHEN max_rss_mb IS NOT NULL AND req_mem_mb > 0
                    THEN ROUND(max_rss_mb / req_mem_mb * 100.0, 1)
                    END AS mem_eff_pct
            FROM jobs {where}
        ) WHERE (cpu_eff_pct IS NOT NULL AND cpu_eff_pct < ?)
            OR (mem_eff_pct IS NOT NULL AND mem_eff_pct < ?)
        ORDER BY COALESCE(cpu_eff_pct, 100) ASC
        LIMIT ?""",
        params + [threshold_pct, threshold_pct, limit],
    ).fetchall()
    return [dict(r) for r in rows]
