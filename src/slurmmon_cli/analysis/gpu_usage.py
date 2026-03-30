"""GPU and resource usage analysis from sshare data."""

from __future__ import annotations

import json
import sqlite3


def _latest_collected_at(conn: sqlite3.Connection,
                        cluster: str | None = None) -> float | None:
    """Get the timestamp of the most recent sshare collection for a cluster."""
    if cluster:
        row = conn.execute(
            "SELECT MAX(collected_at) FROM user_usage WHERE cluster = ?",
            (cluster,),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT MAX(collected_at) FROM user_usage"
        ).fetchone()
    return row[0] if row and row[0] else None


def top_gpu_users(conn: sqlite3.Connection, top: int = 20,
                  cluster: str | None = None) -> list[dict]:
    """Top users ranked by total GPU-minutes, enriched with active job info."""
    ts = _latest_collected_at(conn, cluster=cluster)
    if ts is None:
        return []
    # Base: sshare GPU usage (over-fetch to compensate for inactive filtering)
    fetch_limit = top * 3
    params: list = [ts]
    cluster_filter = ""
    if cluster:
        cluster_filter = " AND cluster = ?"
        params.append(cluster)
    params.append(fetch_limit)
    rows = conn.execute(
        f"""SELECT user, account, gpu_tres_mins, cpu_tres_mins,
                  fairshare, gpu_type_mins
           FROM user_usage
           WHERE collected_at = ?{cluster_filter} AND gpu_tres_mins > 0
           ORDER BY gpu_tres_mins DESC
           LIMIT ?""",
        params,
    ).fetchall()
    results = [dict(r) for r in rows]

    # Enrich with active GPU job counts and node counts from jobs table
    active_sql = (
        "SELECT user,"
        " SUM(CASE WHEN state = 'RUNNING' THEN 1 ELSE 0 END) AS gpu_jobs_running,"
        " SUM(CASE WHEN state = 'PENDING' THEN 1 ELSE 0 END) AS gpu_jobs_pending,"
        " GROUP_CONCAT(DISTINCT CASE WHEN state = 'RUNNING' THEN node_list END) AS gpu_nodes"
        " FROM jobs"
        " WHERE num_gpus > 0 AND state IN ('RUNNING', 'PENDING')"
    )
    active_params: list = []
    if cluster:
        active_sql += " AND cluster = ?"
        active_params.append(cluster)
    active_sql += " GROUP BY user"
    active = conn.execute(active_sql, active_params).fetchall()
    active_map = {r["user"]: dict(r) for r in active}

    for row in results:
        info = active_map.get(row["user"], {})
        row["gpu_jobs_running"] = info.get("gpu_jobs_running", 0)
        row["gpu_jobs_pending"] = info.get("gpu_jobs_pending", 0)
        # Count distinct nodes from comma-joined node_list
        raw_nodes = info.get("gpu_nodes") or ""
        node_set = set()
        for part in raw_nodes.split(","):
            part = part.strip()
            if part:
                node_set.add(part)
        row["gpu_node_count"] = len(node_set)

    # Filter out users with no active GPU jobs and truncate to requested limit
    results = [
        r for r in results
        if r["gpu_jobs_running"] > 0 or r["gpu_jobs_pending"] > 0
    ][:top]

    return results


def top_cpu_users(conn: sqlite3.Connection, top: int = 20,
                  cluster: str | None = None) -> list[dict]:
    """Top users ranked by total CPU-minutes (from latest sshare snapshot)."""
    ts = _latest_collected_at(conn, cluster=cluster)
    if ts is None:
        return []
    params: list = [ts]
    cluster_filter = ""
    if cluster:
        cluster_filter = " AND cluster = ?"
        params.append(cluster)
    params.append(top)
    rows = conn.execute(
        f"""SELECT user, account, cpu_tres_mins, gpu_tres_mins, fairshare
           FROM user_usage
           WHERE collected_at = ?{cluster_filter} AND cpu_tres_mins > 0
           ORDER BY cpu_tres_mins DESC
           LIMIT ?""",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def top_gpu_accounts(conn: sqlite3.Connection, top: int = 20,
                     cluster: str | None = None) -> list[dict]:
    """Top accounts ranked by total GPU-minutes."""
    ts = _latest_collected_at(conn, cluster=cluster)
    if ts is None:
        return []
    params: list = [ts]
    cluster_filter = ""
    if cluster:
        cluster_filter = " AND cluster = ?"
        params.append(cluster)
    params.append(top)
    rows = conn.execute(
        f"""SELECT account,
                  SUM(gpu_tres_mins) AS gpu_tres_mins,
                  SUM(cpu_tres_mins) AS cpu_tres_mins,
                  COUNT(*) AS num_users
           FROM user_usage
           WHERE collected_at = ?{cluster_filter} AND gpu_tres_mins > 0
           GROUP BY account
           ORDER BY gpu_tres_mins DESC
           LIMIT ?""",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def top_gpu_requesters(conn: sqlite3.Connection, top: int = 20,
                       cluster: str | None = None) -> list[dict]:
    """Top users by GPUs currently requested (running + pending from jobs table)."""
    cluster_filter = ""
    params: list = []
    if cluster:
        cluster_filter = " AND cluster = ?"
        params.append(cluster)
    params.append(top)
    rows = conn.execute(
        f"""SELECT user, account,
                  SUM(CASE WHEN state = 'RUNNING' THEN num_gpus ELSE 0 END) AS gpus_running,
                  SUM(CASE WHEN state = 'PENDING' THEN num_gpus ELSE 0 END) AS gpus_pending,
                  SUM(num_gpus) AS gpus_total,
                  GROUP_CONCAT(DISTINCT partition) AS partitions
           FROM jobs
           WHERE state IN ('RUNNING', 'PENDING') AND num_gpus > 0{cluster_filter}
           GROUP BY user, account
           ORDER BY gpus_total DESC
           LIMIT ?""",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def gpu_usage_by_type(conn: sqlite3.Connection,
                      cluster: str | None = None) -> list[dict]:
    """Aggregate GPU usage by GPU type from latest snapshot."""
    ts = _latest_collected_at(conn, cluster=cluster)
    if ts is None:
        return []
    sql = "SELECT gpu_type_mins FROM user_usage WHERE collected_at = ? AND gpu_type_mins IS NOT NULL"
    params: list = [ts]
    if cluster:
        sql += " AND cluster = ?"
        params.append(cluster)
    rows = conn.execute(sql, params).fetchall()

    totals: dict[str, int] = {}
    for row in rows:
        try:
            types = json.loads(row[0])
        except (json.JSONDecodeError, TypeError):
            continue
        for gpu_type, mins in types.items():
            totals[gpu_type] = totals.get(gpu_type, 0) + mins

    return [
        {"gpu_type": t, "gpu_mins": m, "gpu_hours": round(m / 60)}
        for t, m in sorted(totals.items(), key=lambda x: x[1], reverse=True)
    ]


def usage_delta(conn: sqlite3.Connection, hours: int = 24,
                cluster: str | None = None) -> list[dict]:
    """Per-user GPU-minutes consumed in the last N hours.

    Compares latest sshare snapshot with one from ~hours ago.
    """
    ts_latest = _latest_collected_at(conn, cluster=cluster)
    if ts_latest is None:
        return []

    cutoff = ts_latest - (hours * 3600)
    # Find the closest earlier snapshot
    earlier_sql = "SELECT MAX(collected_at) FROM user_usage WHERE collected_at <= ?"
    earlier_params: list = [cutoff]
    if cluster:
        earlier_sql += " AND cluster = ?"
        earlier_params.append(cluster)
    row = conn.execute(earlier_sql, earlier_params).fetchone()
    ts_earlier = row[0] if row and row[0] else None
    if ts_earlier is None:
        return []

    # Join latest and earlier snapshots by user
    cluster_filter = ""
    cluster_join = ""
    if cluster:
        cluster_filter = " AND cur.cluster = ?"
        cluster_join = " AND cur.cluster = prev.cluster"
    join_params: list = [ts_earlier, ts_latest]
    if cluster:
        join_params.append(cluster)
    rows = conn.execute(
        f"""SELECT
            cur.user, cur.account,
            cur.gpu_tres_mins - COALESCE(prev.gpu_tres_mins, 0) AS gpu_delta,
            cur.cpu_tres_mins - COALESCE(prev.cpu_tres_mins, 0) AS cpu_delta,
            cur.gpu_tres_mins AS gpu_total
        FROM user_usage cur
        LEFT JOIN user_usage prev
            ON cur.user = prev.user AND prev.collected_at = ?{cluster_join}
        WHERE cur.collected_at = ?{cluster_filter}
            AND (cur.gpu_tres_mins - COALESCE(prev.gpu_tres_mins, 0)) > 0
        ORDER BY gpu_delta DESC""",
        join_params,
    ).fetchall()
    return [dict(r) for r in rows]
