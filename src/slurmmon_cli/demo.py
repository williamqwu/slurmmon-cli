"""Demo mode: synthetic cluster data for screenshots and documentation.

Generates a fictional "nebula" cluster with realistic workloads and
anonymized usernames. Patches slurm.py functions so the entire TUI
operates against synthetic data without requiring a real Slurm cluster.

Usage: slurmmon-cli --demo
"""

from __future__ import annotations

import atexit
import json
import os
import random
import tempfile
import time

from slurmmon_cli.models import (
    ClusterInfo, Job, NodeUtilization, PartitionInfo, UserUsage,
)

CLUSTER = "nebula"

# (username, account, gpu_weight: 0=CPU-only, 1=light, 2=medium, 3=heavy)
_USERS = [
    ("alice",   "PXS0100", 3), ("bob",     "PXS0100", 2),
    ("charlie", "PXS0200", 3), ("diana",   "PXS0200", 2),
    ("edward",  "PXS0300", 3), ("fiona",   "PXS0300", 2),
    ("george",  "PXS0400", 3), ("helen",   "PXS0100", 1),
    ("ivan",    "PXS0200", 2), ("julia",   "PXS0500", 3),
    ("kevin",   "PXS0500", 2), ("lisa",    "PXS0300", 1),
    ("martin",  "PXS0400", 2), ("nancy",   "PXS0600", 0),
    ("oliver",  "PXS0600", 0), ("paula",   "PXS0100", 1),
    ("quinn",   "PXS0200", 1), ("rachel",  "PXS0300", 1),
    ("sam",     "PXS0400", 0), ("tina",    "PXS0500", 1),
    ("ulrich",  "PXS0600", 0), ("vera",    "PXS0100", 0),
    ("wendy",   "PXS0200", 0), ("xavier",  "PXS0300", 0),
    ("yuki",    "PXS0400", 0), ("zara",    "PXS0500", 0),
    ("alex",    "PXS0100", 0), ("blake",   "PXS0200", 0),
    ("casey",   "PXS0300", 0), ("drew",    "PXS0400", 0),
]

_PARTS = {
    "gpu":      dict(n=80,  pfx="g", gpus=4, gtype="a100", cpus=96,
                     mem=786432,  mt="7-00:00:00"),
    "cpu":      dict(n=120, pfx="c", gpus=0, gtype=None,   cpus=96,
                     mem=196608,  mt="7-00:00:00"),
    "debug":    dict(n=4,   pfx="d", gpus=1, gtype="a100", cpus=48,
                     mem=196608,  mt="1:00:00"),
    "largemem": dict(n=8,   pfx="m", gpus=0, gtype=None,   cpus=96,
                     mem=1572864, mt="3-00:00:00"),
}

_REASONS = ["Priority", "Resources", "QOSMaxGRESPerUser", "AssocMaxJobsLimit"]

# ---- Module state (set by _generate) ----
_nodes: list[NodeUtilization] = []
_queue: list[Job] = []
_history: list[Job] = []
_sshare: list[UserUsage] = []
_info: ClusterInfo | None = None
_jid = 1_000_000


def _nid() -> str:
    global _jid
    _jid += 1
    return str(_jid)


def _nn(pfx: str, i: int) -> str:
    return f"{pfx}{i:04d}"


# ---- Data generation ----

def _generate(now: float) -> None:
    """Generate all synthetic data into module-level state."""
    global _nodes, _queue, _history, _sshare, _info
    rng = random.Random(42)

    gpu_u = [(u, a) for u, a, w in _USERS if w >= 2]
    light_u = [(u, a) for u, a, w in _USERS if w == 1]
    all_gpu_u = gpu_u + light_u
    all_u = [(u, a) for u, a, _ in _USERS]

    nodes: list[NodeUtilization] = []
    running: list[Job] = []

    # ---- GPU nodes (80) ----
    p = _PARTS["gpu"]
    for i in range(1, p["n"] + 1):
        nm = _nn(p["pfx"], i)
        r = rng.random()

        if r < 0.55:
            # Full-node allocation
            u, a = rng.choice(gpu_u)
            ca, ga = p["cpus"], p["gpus"]
            cl = ca * rng.uniform(0.3, 0.95)
            st, uu = "ALLOCATED", [u]
            el = rng.randint(600, 5 * 86400)
            tl = rng.choice([86400, 172800, 259200, 604800])
            mr = rng.choice([384000, 512000, 786432])
            sub = now - el - rng.randint(0, 300)
            sta = sub + rng.randint(5, 300)
            eff = rng.uniform(0.1, 0.95)
            running.append(Job(
                job_id=_nid(), user=u, account=a, partition="gpu",
                state="RUNNING", num_cpus=ca, num_gpus=ga, req_mem_mb=mr,
                submit_time=sub, start_time=sta, end_time=None,
                time_limit_s=tl, elapsed_s=el, node_list=nm,
                exit_code=None, cpu_time_s=el * ca * eff,
                max_rss_mb=mr * rng.uniform(0.3, 0.8),
                reason=None, cluster=CLUSTER,
            ))

        elif r < 0.75:
            # Shared (1-3 users, partial GPUs)
            sel = rng.sample(all_gpu_u, min(rng.randint(1, 3), len(all_gpu_u)))
            cpg = p["cpus"] // p["gpus"]
            ga, ca, uu = 0, 0, []
            for u, a in sel:
                ug = rng.randint(1, 2)
                if ga + ug > p["gpus"]:
                    break
                ga += ug
                uc = ug * cpg
                ca += uc
                uu.append(u)
                el = rng.randint(300, 4 * 86400)
                tl = rng.choice([3600, 14400, 43200, 86400, 172800])
                mr = uc * rng.choice([4000, 8000])
                sub = now - el - rng.randint(0, 300)
                sta = sub + rng.randint(5, 600)
                running.append(Job(
                    job_id=_nid(), user=u, account=a, partition="gpu",
                    state="RUNNING", num_cpus=uc, num_gpus=ug, req_mem_mb=mr,
                    submit_time=sub, start_time=sta, end_time=None,
                    time_limit_s=tl, elapsed_s=el, node_list=nm,
                    exit_code=None, cpu_time_s=el * uc * rng.uniform(0.05, 0.9),
                    max_rss_mb=mr * rng.uniform(0.3, 0.7),
                    reason=None, cluster=CLUSTER,
                ))
            cl = ca * rng.uniform(0.2, 0.9)
            st = "MIXED"

        elif r < 0.90:
            # Idle
            ca, ga, cl, st, uu = 0, 0, 0.0, "IDLE", []

        else:
            # Underutilized (waste candidate: full node, very low load)
            u, a = rng.choice(gpu_u)
            ca, ga = p["cpus"], p["gpus"]
            cl = ca * rng.uniform(0.02, 0.15)
            st, uu = "ALLOCATED", [u]
            el = rng.randint(3600, 3 * 86400)
            tl = rng.choice([86400, 259200, 604800])
            sub = now - el - rng.randint(0, 300)
            sta = sub + rng.randint(5, 300)
            running.append(Job(
                job_id=_nid(), user=u, account=a, partition="gpu",
                state="RUNNING", num_cpus=ca, num_gpus=ga, req_mem_mb=786432,
                submit_time=sub, start_time=sta, end_time=None,
                time_limit_s=tl, elapsed_s=el, node_list=nm,
                exit_code=None, cpu_time_s=el * ca * rng.uniform(0.01, 0.1),
                max_rss_mb=786432 * rng.uniform(0.05, 0.2),
                reason=None, cluster=CLUSTER,
            ))

        lr = cl / ca if ca > 0 else None
        ma = int(p["mem"] * ca / p["cpus"] * rng.uniform(0.5, 0.9)) if ca else 0
        nodes.append(NodeUtilization(
            name=nm, state=st, cpus_total=p["cpus"], cpus_alloc=ca,
            cpu_load=cl, load_ratio=lr, mem_total_mb=p["mem"], mem_alloc_mb=ma,
            gpus_total=p["gpus"], gpus_alloc=ga, gpu_type=p["gtype"],
            partitions=["gpu"], users=uu,
        ))

    # ---- CPU nodes (120) ----
    p = _PARTS["cpu"]
    for i in range(1, p["n"] + 1):
        nm = _nn(p["pfx"], i)
        r = rng.random()

        if r < 0.50:
            u, a = rng.choice(all_u)
            ca = p["cpus"]
            cl = ca * rng.uniform(0.3, 0.95)
            st, uu = "ALLOCATED", [u]
            el = rng.randint(300, 5 * 86400)
            tl = rng.choice([3600, 14400, 86400, 259200, 604800])
            mr = rng.choice([96000, 128000, 196608])
            sub = now - el - rng.randint(0, 300)
            sta = sub + rng.randint(5, 120)
            running.append(Job(
                job_id=_nid(), user=u, account=a, partition="cpu",
                state="RUNNING", num_cpus=ca, num_gpus=0, req_mem_mb=mr,
                submit_time=sub, start_time=sta, end_time=None,
                time_limit_s=tl, elapsed_s=el, node_list=nm,
                exit_code=None, cpu_time_s=el * ca * rng.uniform(0.5, 0.98),
                max_rss_mb=mr * rng.uniform(0.4, 0.85),
                reason=None, cluster=CLUSTER,
            ))

        elif r < 0.75:
            sel = rng.sample(all_u, min(rng.randint(2, 4), len(all_u)))
            ca, uu = 0, []
            for u, a in sel:
                uc = rng.choice([8, 16, 24, 32, 48])
                if ca + uc > p["cpus"]:
                    break
                ca += uc
                uu.append(u)
                el = rng.randint(300, 3 * 86400)
                tl = rng.choice([3600, 14400, 86400, 172800])
                mr = uc * rng.choice([2000, 4000, 8000])
                sub = now - el - rng.randint(0, 300)
                sta = sub + rng.randint(5, 120)
                running.append(Job(
                    job_id=_nid(), user=u, account=a, partition="cpu",
                    state="RUNNING", num_cpus=uc, num_gpus=0, req_mem_mb=mr,
                    submit_time=sub, start_time=sta, end_time=None,
                    time_limit_s=tl, elapsed_s=el, node_list=nm,
                    exit_code=None, cpu_time_s=el * uc * rng.uniform(0.4, 0.95),
                    max_rss_mb=mr * rng.uniform(0.3, 0.8),
                    reason=None, cluster=CLUSTER,
                ))
            cl = ca * rng.uniform(0.3, 0.9)
            st = "MIXED"

        else:
            ca, cl, st, uu = 0, 0.0, "IDLE", []

        lr = cl / ca if ca > 0 else None
        ma = int(p["mem"] * ca / p["cpus"] * rng.uniform(0.5, 0.9)) if ca else 0
        nodes.append(NodeUtilization(
            name=nm, state=st, cpus_total=p["cpus"], cpus_alloc=ca,
            cpu_load=cl, load_ratio=lr, mem_total_mb=p["mem"], mem_alloc_mb=ma,
            gpus_total=0, gpus_alloc=0, gpu_type=None,
            partitions=["cpu"], users=uu,
        ))

    # ---- Debug + Largemem nodes ----
    for pname in ("debug", "largemem"):
        p = _PARTS[pname]
        for i in range(1, p["n"] + 1):
            nm = _nn(p["pfx"], i)
            if rng.random() < 0.3:
                u, a = rng.choice(all_u)
                ca = rng.choice([8, 16, p["cpus"]])
                ga = min(p["gpus"], rng.randint(0, max(p["gpus"], 1)))
                cl = ca * rng.uniform(0.3, 0.8)
                st = "ALLOCATED" if ca >= p["cpus"] else "MIXED"
                uu = [u]
                el = rng.randint(60, 3600)
                sub = now - el - rng.randint(0, 60)
                sta = sub + rng.randint(2, 30)
                running.append(Job(
                    job_id=_nid(), user=u, account=a, partition=pname,
                    state="RUNNING", num_cpus=ca, num_gpus=ga,
                    req_mem_mb=ca * 4000, submit_time=sub, start_time=sta,
                    end_time=None, time_limit_s=3600, elapsed_s=el,
                    node_list=nm, exit_code=None,
                    cpu_time_s=el * ca * rng.uniform(0.3, 0.9),
                    max_rss_mb=ca * 4000 * rng.uniform(0.2, 0.6),
                    reason=None, cluster=CLUSTER,
                ))
            else:
                ca, ga, cl, st, uu = 0, 0, 0.0, "IDLE", []

            lr = cl / ca if ca > 0 else None
            ma = int(p["mem"] * ca / p["cpus"] * 0.6) if ca else 0
            nodes.append(NodeUtilization(
                name=nm, state=st, cpus_total=p["cpus"], cpus_alloc=ca,
                cpu_load=cl, load_ratio=lr, mem_total_mb=p["mem"],
                mem_alloc_mb=ma, gpus_total=p["gpus"], gpus_alloc=ga,
                gpu_type=p["gtype"], partitions=[pname], users=uu,
            ))

    # ---- Pending jobs ----
    pending: list[Job] = []

    for _ in range(30):
        u, a = rng.choice(all_gpu_u)
        ng = rng.choice([1, 1, 2, 2, 4])
        nc = ng * 24
        sub = now - rng.randint(60, 7200)
        tl = rng.choice([3600, 14400, 43200, 86400, 172800])
        pending.append(Job(
            job_id=_nid(), user=u, account=a, partition="gpu",
            state="PENDING", num_cpus=nc, num_gpus=ng,
            req_mem_mb=nc * rng.choice([4000, 8000]),
            submit_time=sub, start_time=None, end_time=None,
            time_limit_s=tl, elapsed_s=0, node_list=None,
            exit_code=None, cpu_time_s=None, max_rss_mb=None,
            reason=rng.choice(_REASONS), cluster=CLUSTER,
        ))

    for _ in range(50):
        u, a = rng.choice(all_u)
        nc = rng.choice([1, 4, 8, 16, 32, 48, 96])
        sub = now - rng.randint(30, 3600)
        tl = rng.choice([3600, 14400, 86400, 172800, 604800])
        pending.append(Job(
            job_id=_nid(), user=u, account=a, partition="cpu",
            state="PENDING", num_cpus=nc, num_gpus=0,
            req_mem_mb=nc * rng.choice([2000, 4000, 8000]),
            submit_time=sub, start_time=None, end_time=None,
            time_limit_s=tl, elapsed_s=0, node_list=None,
            exit_code=None, cpu_time_s=None, max_rss_mb=None,
            reason=rng.choice(_REASONS[:3]), cluster=CLUSTER,
        ))

    # ---- Historical completed/failed jobs ----
    hist: list[Job] = []
    sw = [("COMPLETED", 85), ("FAILED", 8), ("TIMEOUT", 7)]

    for _ in range(2500):
        u, a, w = rng.choice(_USERS)
        is_gpu = rng.random() < (0.7 if w >= 2 else 0.3 if w == 1 else 0.02)

        if is_gpu:
            part = "gpu"
            ng = rng.choice([1, 1, 2, 2, 4])
            nc = ng * 24
            mr = nc * rng.choice([4000, 8000, 16000])
            tl = rng.choice([3600, 14400, 43200, 86400, 172800, 259200])
        else:
            part = rng.choices(["cpu", "largemem"], weights=[90, 10])[0]
            ng = 0
            nc = rng.choice([1, 4, 8, 16, 32, 48, 96])
            mr = nc * rng.choice([2000, 4000, 8000])
            if part == "largemem":
                mr *= 4
            tl = rng.choice([3600, 14400, 86400, 172800, 604800])

        state = rng.choices([s for s, _ in sw], weights=[w for _, w in sw])[0]
        et = now - rng.uniform(60, 7 * 86400)
        if state == "TIMEOUT":
            el = tl
        elif state == "COMPLETED":
            el = int(tl * rng.uniform(0.05, 0.95))
        else:
            el = int(tl * rng.uniform(0.01, 0.5))
        st = et - el
        sub = st - rng.randint(5, 3600)

        # CPU efficiency: some jobs are wasteful
        if state == "COMPLETED" and rng.random() < 0.2:
            eff = rng.uniform(0.01, 0.3)
        elif state == "COMPLETED":
            eff = rng.uniform(0.4, 0.98)
        else:
            eff = rng.uniform(0.01, 0.5)

        pfx = _PARTS[part]["pfx"]
        nn = _nn(pfx, rng.randint(1, _PARTS[part]["n"]))
        ec = "0:0" if state == "COMPLETED" else (
            "1:0" if state == "FAILED" else "0:9")

        hist.append(Job(
            job_id=_nid(), user=u, account=a, partition=part,
            state=state, num_cpus=nc, num_gpus=ng, req_mem_mb=mr,
            submit_time=sub, start_time=st, end_time=et,
            time_limit_s=tl, elapsed_s=el, node_list=nn,
            exit_code=ec, cpu_time_s=el * nc * eff,
            max_rss_mb=mr * rng.uniform(0.1, 0.85),
            reason=None, cluster=CLUSTER,
        ))

    # ---- sshare data ----
    ss: list[UserUsage] = []
    for u, a, w in _USERS:
        if w == 0:
            cm, gm, gtm = rng.randint(1000, 500000), 0, {}
        elif w == 1:
            cm = rng.randint(5000, 200000)
            gm = rng.randint(100, 50000)
            gtm = {"a100": gm}
        elif w == 2:
            cm = rng.randint(10000, 300000)
            gm = rng.randint(10000, 200000)
            gtm = {"a100": gm}
        else:
            cm = rng.randint(50000, 500000)
            gm = rng.randint(100000, 800000)
            gtm = {"a100": gm}
        ss.append(UserUsage(
            account=a, user=u, raw_usage=cm * 60,
            fairshare=round(rng.uniform(0.1, 1.0), 6),
            cpu_tres_mins=cm, gpu_tres_mins=gm, gpu_type_mins=gtm,
        ))

    # ---- ClusterInfo ----
    pis = []
    for pname, pi in _PARTS.items():
        pn = [n for n in nodes if pname in n.partitions]
        pis.append(PartitionInfo(
            name=pname, state="UP", total_nodes=len(pn),
            idle_nodes=sum(1 for n in pn if n.state == "IDLE"),
            alloc_nodes=sum(1 for n in pn if n.state == "ALLOCATED"),
            other_nodes=0,
            total_cpus=sum(n.cpus_total for n in pn),
            avail_cpus=sum(n.cpus_total - n.cpus_alloc for n in pn),
            max_time=pi["mt"],
        ))

    # Store in module state
    _nodes = nodes
    _queue = running + pending
    _history = hist
    _sshare = ss
    _info = ClusterInfo(
        cluster_name=CLUSTER, partitions=pis,
        total_nodes=len(nodes),
        idle_nodes=sum(1 for n in nodes if n.state == "IDLE"),
        alloc_nodes=sum(1 for n in nodes if n.state == "ALLOCATED"),
        down_nodes=0,
        mixed_nodes=sum(1 for n in nodes if n.state == "MIXED"),
        total_cpus=sum(n.cpus_total for n in nodes),
        alloc_cpus=sum(n.cpus_alloc for n in nodes),
    )


# ---- DB pre-population (data the collector cannot create) ----

def _populate_db(db_path: str, now: float) -> None:
    """Insert historical snapshots and an extra sshare snapshot."""
    from slurmmon_cli.storage.database import Database

    rng = random.Random(99)
    db = Database(db_path)
    db.connect()
    conn = db.conn

    total_gpus = sum(p["n"] * p["gpus"] for p in _PARTS.values())
    total_cpus = sum(p["n"] * p["cpus"] for p in _PARTS.values())
    total_nodes = sum(p["n"] for p in _PARTS.values())

    # Historical snapshots: every 3 hours for 7 days (56 rows)
    for hours_ago in range(3, 7 * 24 + 1, 3):
        t = now - hours_ago * 3600
        pct = min(0.45 + 0.30 * abs(rng.gauss(0, 0.3)), 0.95)
        conn.execute(
            """INSERT INTO snapshots (
                timestamp, total_nodes, idle_nodes, alloc_nodes, down_nodes,
                mixed_nodes, total_cpus, alloc_cpus, running_jobs, pending_jobs,
                total_gpus, alloc_gpus
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (t, total_nodes, int(total_nodes * (1 - pct) * 0.6),
             int(total_nodes * pct * 0.6), 0, int(total_nodes * pct * 0.4),
             total_cpus, int(total_cpus * pct),
             rng.randint(150, 250), rng.randint(40, 120),
             total_gpus, int(total_gpus * pct)),
        )
    conn.commit()

    # Extra sshare snapshot from 24h ago (for usage_delta)
    t_old = now - 86400
    for u in _sshare:
        f = 0.85  # slightly lower usage yesterday
        conn.execute(
            """INSERT INTO user_usage (
                collected_at, account, user, raw_usage, fairshare,
                cpu_tres_mins, gpu_tres_mins, gpu_type_mins, cluster
            ) VALUES (?,?,?,?,?,?,?,?,?)""",
            (t_old, u.account, u.user, int(u.raw_usage * f), u.fairshare,
             int(u.cpu_tres_mins * f), int(u.gpu_tres_mins * f),
             json.dumps({k: int(v * f) for k, v in u.gpu_type_mins.items()})
             if u.gpu_type_mins else None,
             CLUSTER),
        )
    conn.commit()
    db.close()


# ---- Monkey-patching ----

def _patch_slurm() -> None:
    """Replace slurm.py functions with synthetic-data versions."""
    import slurmmon_cli.slurm as slurm
    import slurmmon_cli.storage.collector as collector
    import slurmmon_cli.tui.data as data

    def fake_queue(user=None):
        jobs = _queue
        if user:
            jobs = [j for j in jobs if j.user == user]
        return list(jobs)

    def fake_cluster_info():
        return _info

    def fake_nodes():
        return list(_nodes)

    def fake_history(starttime="now-24hours", user=None):
        jobs = _history
        if user:
            jobs = [j for j in jobs if j.user == user]
        return jobs

    def fake_sshare():
        return list(_sshare)

    def fake_run_cmd(cmd, timeout=30):
        return None

    # Patch the slurm module itself (affects internal calls)
    slurm.get_queue = fake_queue
    slurm.get_cluster_info = fake_cluster_info
    slurm.get_node_utilization = fake_nodes
    slurm.get_job_history = fake_history
    slurm.get_sshare = fake_sshare
    slurm.run_slurm_command = fake_run_cmd

    # Patch collector's imported references
    collector.get_queue = fake_queue
    collector.get_cluster_info = fake_cluster_info
    collector.get_node_utilization = fake_nodes
    collector.get_job_history = fake_history
    collector.get_sshare = fake_sshare

    # Patch data.py's imported references
    data.get_cluster_info = fake_cluster_info
    data.get_queue = fake_queue
    data.get_node_utilization = fake_nodes


# ---- Cleanup ----

def _cleanup(db_path: str) -> None:
    for suffix in ("", "-wal", "-shm"):
        try:
            os.unlink(db_path + suffix)
        except OSError:
            pass


# ---- Public API ----

def setup_demo() -> str:
    """Activate demo mode. Returns path to a temp DB.

    Call this before launching the TUI or any CLI command. It:
    - generates synthetic cluster data (deterministic, seed=42)
    - monkey-patches slurm.py so no real Slurm commands are executed
    - pre-populates a temp DB with historical snapshots and sshare data
    - sets USER env var to "alice" (a heavy GPU user for efficiency screen)
    """
    now = time.time()
    _generate(now)
    _patch_slurm()

    fd, db_path = tempfile.mkstemp(suffix=".db", prefix="slurmmon-demo-")
    os.close(fd)
    _populate_db(db_path, now)

    # Run a collect cycle so all jobs/sshare are in the DB
    # (CLI commands query the DB, not live functions)
    from slurmmon_cli.storage.collector import collect_snapshot
    from slurmmon_cli.storage.database import Database
    db = Database(db_path)
    db.connect()
    collect_snapshot(db, sshare_interval=0, cluster_override=CLUSTER)
    db.close()

    os.environ["USER"] = "alice"
    atexit.register(_cleanup, db_path)
    return db_path
