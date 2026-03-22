"""Data fetching functions for the TUI (sync, called from worker threads)."""

from __future__ import annotations

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


def fetch_gpu_rankings(db_path: str | None, mode: str, top: int = 20) -> list[dict]:
    """Fetch GPU/CPU usage rankings from DB."""
    from slurmmon_cli.storage.database import Database
    from slurmmon_cli.analysis.gpu_usage import (
        top_gpu_users, top_cpu_users, top_gpu_accounts,
        top_gpu_requesters, usage_delta,
    )

    db = Database(db_path)
    with db:
        if mode == "gpu":
            return top_gpu_users(db.conn, top=top)
        elif mode == "cpu":
            return top_cpu_users(db.conn, top=top)
        elif mode == "account":
            return top_gpu_accounts(db.conn, top=top)
        elif mode == "requests":
            return top_gpu_requesters(db.conn, top=top)
        elif mode == "delta":
            return usage_delta(db.conn, hours=24)
    return []
