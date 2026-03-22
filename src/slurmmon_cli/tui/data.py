"""Data fetching functions for the TUI (sync, called from worker threads)."""

from __future__ import annotations

import os

from slurmmon_cli.models import ClusterInfo, Job, NodeUtilization, PartitionInfo
from slurmmon_cli.slurm import (
    get_cluster_info, get_queue, get_node_utilization, get_running_jobs_by_node,
)


def fetch_live(
    user_filter: str | None = None,
    partition_filter: str | None = None,
) -> tuple[list[Job], ClusterInfo | None]:
    """Fetch live data from Slurm commands."""
    jobs = get_queue(user=user_filter)
    info = get_cluster_info()
    return jobs, info


def fetch_from_db(
    db_path: str | None,
    user_filter: str | None = None,
) -> tuple[list[Job], ClusterInfo | None]:
    """Fetch latest data from the SQLite database."""
    from slurmmon_cli.storage.database import Database

    db = Database(db_path)
    conn = db.connect(readonly=True)

    conditions = ["state IN ('RUNNING', 'PENDING')"]
    params: list = []
    if user_filter:
        conditions.append("user = ?")
        params.append(user_filter)
    where = "WHERE " + " AND ".join(conditions)

    rows = conn.execute(
        f"SELECT * FROM jobs {where} ORDER BY submit_time DESC", params
    ).fetchall()

    jobs = []
    for r in rows:
        jobs.append(Job(
            job_id=r["job_id"], user=r["user"], account=r["account"],
            partition=r["partition"], state=r["state"],
            num_cpus=r["num_cpus"] or 0, num_gpus=r["num_gpus"] or 0,
            req_mem_mb=r["req_mem_mb"], submit_time=r["submit_time"],
            start_time=r["start_time"], end_time=r["end_time"],
            time_limit_s=r["time_limit_s"], elapsed_s=r["elapsed_s"],
            node_list=r["node_list"], exit_code=r["exit_code"],
            cpu_time_s=r["cpu_time_s"], max_rss_mb=r["max_rss_mb"],
            reason=r["reason"],
        ))

    snap = conn.execute(
        "SELECT * FROM snapshots ORDER BY timestamp DESC LIMIT 1"
    ).fetchone()

    part_rows = conn.execute("SELECT * FROM partitions").fetchall()
    partitions = [
        PartitionInfo(
            name=p["name"], state=p["state"] or "UP",
            total_nodes=p["total_nodes"] or 0, idle_nodes=p["idle_nodes"] or 0,
            alloc_nodes=p["alloc_nodes"] or 0, other_nodes=p["other_nodes"] or 0,
            total_cpus=p["total_cpus"] or 0, avail_cpus=p["avail_cpus"] or 0,
            max_time=p["max_time"],
        )
        for p in part_rows
    ]

    if snap:
        info = ClusterInfo(
            cluster_name="cluster",
            partitions=partitions,
            total_nodes=snap["total_nodes"] or 0,
            idle_nodes=snap["idle_nodes"] or 0,
            alloc_nodes=snap["alloc_nodes"] or 0,
            down_nodes=snap["down_nodes"] or 0,
            mixed_nodes=snap["mixed_nodes"] or 0,
            total_cpus=snap["total_cpus"] or 0,
            alloc_cpus=snap["alloc_cpus"] or 0,
        )
    else:
        info = None

    db.close()
    return jobs, info


def fetch_node_data() -> tuple[list[NodeUtilization], dict[str, list[str]]]:
    """Fetch per-node utilization and user mapping."""
    nodes = get_node_utilization()
    node_users = get_running_jobs_by_node()
    for n in nodes:
        n.users = node_users.get(n.name, [])
    return nodes, node_users


def _rows_to_jobs(rows) -> list[Job]:
    """Convert sqlite3.Row results to Job objects."""
    return [
        Job(
            job_id=r["job_id"], user=r["user"], account=r["account"],
            partition=r["partition"], state=r["state"],
            num_cpus=r["num_cpus"] or 0, num_gpus=r["num_gpus"] or 0,
            req_mem_mb=r["req_mem_mb"], submit_time=r["submit_time"],
            start_time=r["start_time"], end_time=r["end_time"],
            time_limit_s=r["time_limit_s"], elapsed_s=r["elapsed_s"],
            node_list=r["node_list"], exit_code=r["exit_code"],
            cpu_time_s=r["cpu_time_s"], max_rss_mb=r["max_rss_mb"],
            reason=r["reason"],
            cluster=r["cluster"] if "cluster" in r.keys() else "",
        )
        for r in rows
    ]


def fetch_user_jobs(db_path: str | None, user: str,
                    gpu_only: bool = False) -> list[Job]:
    """Fetch running/pending jobs for a user from the DB."""
    from slurmmon_cli.storage.database import Database

    db = Database(db_path)
    with db:
        gpu_filter = " AND num_gpus > 0" if gpu_only else ""
        rows = db.conn.execute(
            f"""SELECT * FROM jobs
                WHERE user = ? AND state IN ('RUNNING', 'PENDING'){gpu_filter}
                ORDER BY state, num_gpus DESC, submit_time DESC""",
            (user,),
        ).fetchall()
    return _rows_to_jobs(rows)


def fetch_account_jobs(db_path: str | None, account: str) -> list[Job]:
    """Fetch running/pending jobs for an account from the DB."""
    from slurmmon_cli.storage.database import Database

    db = Database(db_path)
    with db:
        rows = db.conn.execute(
            """SELECT * FROM jobs
               WHERE account = ? AND state IN ('RUNNING', 'PENDING')
               ORDER BY user, state, num_gpus DESC, submit_time DESC""",
            (account,),
        ).fetchall()
    return _rows_to_jobs(rows)


def compute_user_node_breakdown(
    nodes: list[NodeUtilization],
) -> dict[str, dict[str, int]]:
    """Compute per-user full/partial node counts from live node data.

    A node is "full" for a user if that user is the sole occupant and
    >= 90% of CPUs are allocated. Otherwise it is "partial".
    """
    result: dict[str, dict[str, int]] = {}
    for n in nodes:
        if n.cpus_alloc == 0 or not n.users:
            continue
        exclusive = (
            len(n.users) == 1
            and n.cpus_alloc >= n.cpus_total * 0.9
        )
        for user in n.users:
            if user not in result:
                result[user] = {"full": 0, "partial": 0}
            if exclusive:
                result[user]["full"] += 1
            else:
                result[user]["partial"] += 1
    return result


def compute_account_node_breakdown(
    nodes: list[NodeUtilization],
    user_accounts: dict[str, str],
) -> dict[str, dict[str, int]]:
    """Compute per-account full/partial node counts."""
    result: dict[str, dict[str, int]] = {}
    for n in nodes:
        if n.cpus_alloc == 0 or not n.users:
            continue
        exclusive = (
            len(n.users) == 1
            and n.cpus_alloc >= n.cpus_total * 0.9
        )
        # Attribute to accounts of users on this node
        seen_accounts: set[str] = set()
        for user in n.users:
            acct = user_accounts.get(user, "unknown")
            if acct in seen_accounts:
                continue
            seen_accounts.add(acct)
            if acct not in result:
                result[acct] = {"full": 0, "partial": 0}
            if exclusive:
                result[acct]["full"] += 1
            else:
                result[acct]["partial"] += 1
    return result


def _detect_cluster() -> str:
    """Detect current cluster name, with scontrol fallback for empty sinfo."""
    try:
        info = get_cluster_info()
        if info and info.cluster_name and info.cluster_name != "unknown":
            return info.cluster_name
    except Exception:
        pass
    # Fallback: scontrol show config
    try:
        import subprocess as _sp
        result = _sp.run(
            ["scontrol", "show", "config"],
            capture_output=True, text=True, timeout=10,
        )
        for line in result.stdout.splitlines():
            if line.strip().startswith("ClusterName"):
                return line.split("=", 1)[1].strip()
    except Exception:
        pass
    return ""


def fetch_gpu_rankings(db_path: str | None, mode: str, top: int = 20,
                       cluster: str | None = None) -> list[dict]:
    """Fetch GPU/CPU usage rankings from DB, enriched with live node data."""
    from slurmmon_cli.storage.database import Database
    from slurmmon_cli.analysis.gpu_usage import (
        top_gpu_users, top_cpu_users, top_gpu_accounts,
        top_gpu_requesters, usage_delta,
    )

    # Auto-detect cluster if not provided
    if cluster is None:
        cluster = _detect_cluster()

    db = Database(db_path)
    with db:
        if mode == "gpu":
            rows = top_gpu_users(db.conn, top=top, cluster=cluster or None)
            # Enrich with live node data
            try:
                nodes, _ = fetch_node_data()
                breakdown = compute_user_node_breakdown(nodes)
                for r in rows:
                    info = breakdown.get(r.get("user", ""), {})
                    r["full_nodes"] = info.get("full", 0)
                    r["partial_nodes"] = info.get("partial", 0)
            except Exception:
                for r in rows:
                    r["full_nodes"] = 0
                    r["partial_nodes"] = 0
            return rows
        elif mode == "cpu":
            return top_cpu_users(db.conn, top=top, cluster=cluster or None)
        elif mode == "account":
            rows = top_gpu_accounts(db.conn, top=top, cluster=cluster or None)
            # Enrich accounts with job counts and node data
            try:
                # Job counts per account
                acct_jobs = {}
                for row in db.conn.execute(
                    """SELECT account,
                        SUM(CASE WHEN state='RUNNING' THEN 1 ELSE 0 END) AS jr,
                        SUM(CASE WHEN state='PENDING' THEN 1 ELSE 0 END) AS jp
                    FROM jobs WHERE state IN ('RUNNING','PENDING')
                    GROUP BY account"""
                ).fetchall():
                    acct_jobs[row["account"]] = {"jr": row["jr"], "jp": row["jp"]}

                # Node breakdown per account
                nodes, _ = fetch_node_data()
                # Build user->account map from jobs table
                user_accts = {}
                for row in db.conn.execute(
                    "SELECT DISTINCT user, account FROM jobs WHERE account IS NOT NULL"
                ).fetchall():
                    user_accts[row["user"]] = row["account"]
                acct_nodes = compute_account_node_breakdown(nodes, user_accts)

                for r in rows:
                    acct = r.get("account", "")
                    jinfo = acct_jobs.get(acct, {})
                    r["jobs_running"] = jinfo.get("jr", 0)
                    r["jobs_pending"] = jinfo.get("jp", 0)
                    ninfo = acct_nodes.get(acct, {})
                    r["full_nodes"] = ninfo.get("full", 0)
                    r["partial_nodes"] = ninfo.get("partial", 0)
            except Exception:
                for r in rows:
                    r["jobs_running"] = 0
                    r["jobs_pending"] = 0
                    r["full_nodes"] = 0
                    r["partial_nodes"] = 0
            return rows
        elif mode == "requests":
            return top_gpu_requesters(db.conn, top=top)
        elif mode == "delta":
            return usage_delta(db.conn, hours=24)
    return []


# --- Efficiency screen data ---

def fetch_user_efficiency(db_path: str | None, user: str | None = None,
                          limit: int = 50) -> list[dict]:
    """Fetch completed jobs with efficiency metrics for a user."""
    from slurmmon_cli.storage.database import Database

    if user is None:
        user = os.environ.get("USER", "")
    if not user:
        return []

    db = Database(db_path)
    with db:
        rows = db.conn.execute(
            """SELECT job_id, partition, num_cpus, num_gpus, elapsed_s,
                      cpu_time_s, req_mem_mb, max_rss_mb, state,
                      CASE WHEN cpu_time_s IS NOT NULL AND elapsed_s > 0 AND num_cpus > 0
                          THEN ROUND(cpu_time_s / (num_cpus * elapsed_s) * 100.0, 1)
                          END AS cpu_eff,
                      CASE WHEN max_rss_mb IS NOT NULL AND req_mem_mb > 0
                          THEN ROUND(max_rss_mb / req_mem_mb * 100.0, 1)
                          END AS mem_eff
               FROM jobs
               WHERE user = ? AND state IN ('COMPLETED', 'FAILED', 'TIMEOUT')
                     AND elapsed_s > 0
               ORDER BY submit_time DESC LIMIT ?""",
            (user, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def fetch_queue_health(db_path: str | None) -> dict:
    """Fetch queue wait time analysis."""
    from slurmmon_cli.storage.database import Database
    from slurmmon_cli.analysis.queue_time import (
        wait_time_stats, wait_time_by_hour, wait_time_by_size,
    )

    db = Database(db_path)
    with db:
        stats = wait_time_stats(db.conn)
        by_hour = wait_time_by_hour(db.conn)
        by_size = wait_time_by_size(db.conn)
    return {"stats": stats, "by_hour": by_hour, "by_size": by_size}


def fetch_cluster_trends(db_path: str | None, limit: int = 100) -> list[dict]:
    """Fetch recent cluster snapshots for trend display."""
    from slurmmon_cli.storage.database import Database

    db = Database(db_path)
    with db:
        rows = db.conn.execute(
            """SELECT timestamp, total_nodes, alloc_nodes, idle_nodes,
                      total_cpus, alloc_cpus, running_jobs, pending_jobs
               FROM snapshots ORDER BY timestamp DESC LIMIT ?""",
            (limit,),
        ).fetchall()
    return [dict(r) for r in reversed(rows)]  # chronological order


def fetch_waste_report(db_path: str | None) -> dict:
    """Fetch waste indicators: low-efficiency jobs + underutilized exclusive nodes."""
    from slurmmon_cli.storage.database import Database
    from slurmmon_cli.analysis.efficiency import low_efficiency_jobs

    db = Database(db_path)
    with db:
        low_eff = low_efficiency_jobs(db.conn, threshold_pct=50.0, limit=20)

    # Underutilized exclusive (full) nodes, grouped by partition
    under_by_partition: dict[str, list[dict]] = {}
    try:
        nodes, _ = fetch_node_data()
        for n in nodes:
            # Only flag exclusive nodes (single user, >=90% CPUs)
            is_excl = (
                len(n.users) == 1
                and n.cpus_alloc > 0
                and n.cpus_alloc >= n.cpus_total * 0.9
            )
            if not is_excl:
                continue
            if n.load_ratio is None or n.load_ratio >= 0.3:
                continue
            entry = {
                "name": n.name, "load_ratio": n.load_ratio,
                "cpus_alloc": n.cpus_alloc, "cpus_total": n.cpus_total,
                "gpus_alloc": n.gpus_alloc, "gpus_total": n.gpus_total,
                "user": n.users[0] if n.users else "-",
            }
            # Add to each partition this node belongs to
            for p in (n.partitions or ["unknown"]):
                under_by_partition.setdefault(p, []).append(entry)
    except Exception:
        pass

    return {
        "low_efficiency_jobs": low_eff,
        "underutilized_nodes_by_partition": under_by_partition,
    }
