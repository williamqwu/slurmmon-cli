"""Per-user job analysis queries."""

from __future__ import annotations

import sqlite3


def user_summary(conn: sqlite3.Connection, user: str | None = None,
                 since: float | None = None, sort: str = "jobs",
                 top: int = 20) -> list[dict]:
    """Per-user summary: job counts by state, CPU-hours, avg efficiency.

    Returns list of dicts sorted by *sort* column descending.
    """
    conditions = []
    params: list = []
    if user:
        conditions.append("user = ?")
        params.append(user)
    if since:
        conditions.append("submit_time >= ?")
        params.append(since)
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    query = f"""
        SELECT
            user,
            account,
            COUNT(*) AS total_jobs,
            SUM(CASE WHEN state = 'RUNNING' THEN 1 ELSE 0 END) AS running,
            SUM(CASE WHEN state = 'PENDING' THEN 1 ELSE 0 END) AS pending,
            SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) AS completed,
            SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) AS failed,
            SUM(CASE WHEN state NOT IN ('RUNNING','PENDING','COMPLETED','FAILED')
                THEN 1 ELSE 0 END) AS other,
            ROUND(SUM(CASE WHEN elapsed_s IS NOT NULL AND num_cpus IS NOT NULL
                THEN num_cpus * elapsed_s / 3600.0 ELSE 0 END), 1) AS cpu_hours,
            ROUND(AVG(CASE WHEN cpu_time_s IS NOT NULL AND elapsed_s > 0 AND num_cpus > 0
                THEN cpu_time_s / (num_cpus * elapsed_s) * 100.0 END), 1) AS avg_cpu_eff,
            ROUND(AVG(CASE WHEN max_rss_mb IS NOT NULL AND req_mem_mb > 0
                THEN max_rss_mb / req_mem_mb * 100.0 END), 1) AS avg_mem_eff
        FROM jobs
        {where}
        GROUP BY user, account
    """

    sort_col = {
        "jobs": "total_jobs",
        "cpus": "cpu_hours",
        "efficiency": "avg_cpu_eff",
        "user": "user",
    }.get(sort, "total_jobs")

    query += f" ORDER BY {sort_col} DESC LIMIT ?"
    params.append(top)

    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def user_jobs(conn: sqlite3.Connection, user: str,
              since: float | None = None, state: str | None = None,
              partition: str | None = None,
              sort: str = "submit", limit: int = 50) -> list[dict]:
    """Detailed job list for a specific user."""
    conditions = ["user = ?"]
    params: list = [user]
    if since:
        conditions.append("submit_time >= ?")
        params.append(since)
    if state:
        conditions.append("state = ?")
        params.append(state.upper())
    if partition:
        conditions.append("partition = ?")
        params.append(partition)
    where = "WHERE " + " AND ".join(conditions)

    sort_col = {
        "submit": "submit_time",
        "start": "start_time",
        "elapsed": "elapsed_s",
        "cpus": "num_cpus",
        "mem": "req_mem_mb",
    }.get(sort, "submit_time")

    query = f"""
        SELECT * FROM jobs {where}
        ORDER BY {sort_col} DESC LIMIT ?
    """
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]
