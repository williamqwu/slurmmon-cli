"""Queue wait time analysis queries."""

from __future__ import annotations

import sqlite3
from statistics import mean, median


def _wait_times(conn: sqlite3.Connection, partition: str | None = None,
                since: float | None = None) -> list[float]:
    """Fetch raw wait times (seconds) for jobs that have started."""
    conditions = ["start_time IS NOT NULL", "submit_time IS NOT NULL",
                  "start_time > submit_time"]
    params: list = []
    if partition:
        conditions.append("partition = ?")
        params.append(partition)
    if since:
        conditions.append("submit_time >= ?")
        params.append(since)
    where = "WHERE " + " AND ".join(conditions)

    rows = conn.execute(
        f"SELECT (start_time - submit_time) AS wait FROM jobs {where} ORDER BY wait",
        params,
    ).fetchall()
    return [r[0] for r in rows]


def _percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * p / 100.0
    f = int(k)
    c = f + 1
    if c >= len(sorted_vals):
        return sorted_vals[-1]
    return sorted_vals[f] + (k - f) * (sorted_vals[c] - sorted_vals[f])


def wait_time_stats(conn: sqlite3.Connection, partition: str | None = None,
                    since: float | None = None) -> dict:
    """Aggregate wait time statistics."""
    waits = _wait_times(conn, partition, since)
    if not waits:
        return {"count": 0, "mean": 0, "median": 0, "p90": 0, "p99": 0, "max": 0}
    return {
        "count": len(waits),
        "mean": round(mean(waits)),
        "median": round(median(waits)),
        "p90": round(_percentile(waits, 90)),
        "p99": round(_percentile(waits, 99)),
        "max": round(waits[-1]),
    }


def wait_time_by_hour(conn: sqlite3.Connection, partition: str | None = None,
                      since: float | None = None) -> list[dict]:
    """Wait time bucketed by hour-of-day (0-23) of submission."""
    conditions = ["start_time IS NOT NULL", "submit_time IS NOT NULL",
                  "start_time > submit_time"]
    params: list = []
    if partition:
        conditions.append("partition = ?")
        params.append(partition)
    if since:
        conditions.append("submit_time >= ?")
        params.append(since)
    where = "WHERE " + " AND ".join(conditions)

    rows = conn.execute(
        f"""SELECT
            CAST(strftime('%H', submit_time, 'unixepoch', 'localtime') AS INTEGER) AS hour,
            COUNT(*) AS count,
            ROUND(AVG(start_time - submit_time)) AS avg_wait,
            ROUND(MIN(start_time - submit_time)) AS min_wait,
            ROUND(MAX(start_time - submit_time)) AS max_wait
        FROM jobs {where}
        GROUP BY hour ORDER BY hour""",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def wait_time_by_size(conn: sqlite3.Connection, partition: str | None = None,
                      since: float | None = None) -> list[dict]:
    """Wait time bucketed by job CPU count ranges."""
    conditions = ["start_time IS NOT NULL", "submit_time IS NOT NULL",
                  "start_time > submit_time", "num_cpus IS NOT NULL"]
    params: list = []
    if partition:
        conditions.append("partition = ?")
        params.append(partition)
    if since:
        conditions.append("submit_time >= ?")
        params.append(since)
    where = "WHERE " + " AND ".join(conditions)

    rows = conn.execute(
        f"""SELECT
            CASE
                WHEN num_cpus = 1 THEN '1'
                WHEN num_cpus <= 4 THEN '2-4'
                WHEN num_cpus <= 8 THEN '5-8'
                WHEN num_cpus <= 16 THEN '9-16'
                WHEN num_cpus <= 32 THEN '17-32'
                WHEN num_cpus <= 64 THEN '33-64'
                ELSE '65+'
            END AS cpu_range,
            COUNT(*) AS count,
            ROUND(AVG(start_time - submit_time)) AS avg_wait,
            ROUND(MIN(start_time - submit_time)) AS min_wait,
            ROUND(MAX(start_time - submit_time)) AS max_wait
        FROM jobs {where}
        GROUP BY cpu_range ORDER BY MIN(num_cpus)""",
        params,
    ).fetchall()
    return [dict(r) for r in rows]
