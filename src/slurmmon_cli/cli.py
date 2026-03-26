"""CLI entry point for slurmmon-cli."""

from __future__ import annotations

import argparse
import json
import sys
import time

from slurmmon_cli import __version__


def _parse_since(value: str) -> float:
    """Parse a human time spec like '24h', '7d', '1w' into a Unix timestamp."""
    now = time.time()
    v = value.strip().lower()
    try:
        if v.endswith("h"):
            return now - float(v[:-1]) * 3600
        if v.endswith("d"):
            return now - float(v[:-1]) * 86400
        if v.endswith("w"):
            return now - float(v[:-1]) * 604800
        return float(v)
    except ValueError:
        print(f"Invalid time spec: {value}. Use e.g. 24h, 7d, 1w", file=sys.stderr)
        sys.exit(1)


def _format_duration(seconds: float | int | None) -> str:
    """Format seconds into human-readable duration."""
    if seconds is None or seconds < 0:
        return "-"
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    if s < 86400:
        h, rem = divmod(s, 3600)
        m = rem // 60
        return f"{h}h {m}m"
    d, rem = divmod(s, 86400)
    h = rem // 3600
    return f"{d}d {h}h"


def _format_mem(mb: float | None) -> str:
    if mb is None:
        return "-"
    if mb >= 1024:
        return f"{mb / 1024:.1f}G"
    return f"{mb:.0f}M"


def _pct(val: float | None) -> str:
    if val is None:
        return "-"
    return f"{val:.0f}%"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="slurmmon-cli",
        description="Lightweight CLI Slurm cluster job monitor",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--db", default=None, help="SQLite database path (default: $XDG_DATA_HOME/slurmmon-cli/data.db)")
    parser.add_argument("--config", default=None, help="Config file path (default: $XDG_CONFIG_HOME/slurmmon-cli/config.ini)")
    parser.add_argument("--demo", action="store_true", help="Launch with synthetic data (no Slurm cluster needed)")

    sub = parser.add_subparsers(dest="command")

    # dashboard
    p_dash = sub.add_parser("dashboard", aliases=["d"], help="Launch TUI dashboard")
    p_dash.add_argument("-r", "--refresh", type=int, default=30, help="Refresh interval in seconds")
    p_dash.add_argument("--user", help="Filter to specific user")
    p_dash.add_argument("--partition", help="Filter to specific partition")
    p_dash.add_argument("--from-db", action="store_true", help="Read from DB instead of live Slurm")

    # collect
    p_col = sub.add_parser("collect", help="Collect data snapshot")
    p_col.add_argument("--daemon", action="store_true", help="Run continuously")
    p_col.add_argument("--interval", type=int, default=300, help="Collection interval in seconds")
    p_col.add_argument("--retention", type=int, default=30, help="Data retention in days")

    # jobs
    p_jobs = sub.add_parser("jobs", help="List jobs")
    p_jobs.add_argument("--user", help="Filter by user")
    p_jobs.add_argument("--state", help="Filter by state")
    p_jobs.add_argument("--partition", help="Filter by partition")
    p_jobs.add_argument("--since", default="24h", help="Time window (e.g. 24h, 7d)")
    p_jobs.add_argument("--sort", default="submit", choices=["submit", "start", "elapsed", "cpus", "mem"])
    p_jobs.add_argument("--limit", type=int, default=50)

    # users
    p_users = sub.add_parser("users", help="User summary report")
    p_users.add_argument("--since", default="24h", help="Time window")
    p_users.add_argument("--sort", default="jobs", choices=["jobs", "cpus", "efficiency", "user"])
    p_users.add_argument("--top", type=int, default=20)

    # waits
    p_waits = sub.add_parser("waits", help="Queue wait time analysis")
    p_waits.add_argument("--partition", help="Filter by partition")
    p_waits.add_argument("--since", default="24h", help="Time window")
    p_waits.add_argument("--by-hour", action="store_true", help="Show by hour of day")
    p_waits.add_argument("--by-size", action="store_true", help="Show by job size")

    # efficiency
    p_eff = sub.add_parser("efficiency", help="Job efficiency report")
    p_eff.add_argument("--user", help="Filter by user")
    p_eff.add_argument("--job", help="Single job ID")
    p_eff.add_argument("--since", default="24h", help="Time window")
    p_eff.add_argument("--low", type=float, default=50, help="Threshold for low efficiency")
    p_eff.add_argument("--gpu", action="store_true", help="Show detailed GPU breakdown (requires osc=true)")

    # explore
    p_explore = sub.add_parser("explore", aliases=["x"], help="GPU and resource usage explorer")
    p_explore.add_argument("--by", default="gpu",
                           choices=["gpu", "cpu", "account", "requests", "delta", "nodes"],
                           help="Ranking dimension (default: gpu)")
    p_explore.add_argument("--top", type=int, default=20, help="Number of top entries")
    p_explore.add_argument("--hours", type=int, default=24, help="Delta window in hours (for --by delta)")

    # config
    p_cfg = sub.add_parser("config", help="Show or set config values")
    p_cfg_sub = p_cfg.add_subparsers(dest="config_command")
    p_cfg_sub.add_parser("show", help="Show current config")
    p_cfg_set = p_cfg_sub.add_parser("set", help="Set a config value")
    p_cfg_set.add_argument("key", help="Key in format section.key (e.g. general.osc)")
    p_cfg_set.add_argument("value", help="Value to set")

    # db
    p_db = sub.add_parser("db", help="Database management")
    p_db_sub = p_db.add_subparsers(dest="db_command")
    p_db_sub.add_parser("info", help="Show DB info")
    p_prune = p_db_sub.add_parser("prune", help="Prune old records")
    p_prune.add_argument("--days", type=int, default=30)
    p_db_sub.add_parser("vacuum", help="Vacuum database")

    return parser


def cmd_collect(args: argparse.Namespace) -> None:
    import logging
    from slurmmon_cli.storage.collector import run_collector

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    cfg = getattr(args, "_config", None)
    sshare_interval = int(cfg.get("general", "sshare_interval")) if cfg else 1800
    run_collector(
        db_path=args.db, interval=args.interval,
        daemon=args.daemon, retention_days=args.retention,
        sshare_interval=sshare_interval,
    )


def cmd_dashboard(args: argparse.Namespace) -> None:
    try:
        from slurmmon_cli.tui.app import run_dashboard
    except ImportError:
        print(
            "The interactive dashboard requires 'textual'.\n"
            "Install: pip install slurmmon-cli[tui]",
            file=sys.stderr,
        )
        sys.exit(1)
    cfg = getattr(args, "_config", None)
    run_dashboard(
        db_path=args.db, refresh=args.refresh,
        user_filter=args.user, partition_filter=args.partition,
        from_db=args.from_db, config=cfg,
    )


def cmd_jobs(args: argparse.Namespace) -> None:
    from slurmmon_cli.storage.database import Database
    from slurmmon_cli.analysis.users import user_jobs

    since = _parse_since(args.since)
    db = Database(args.db)
    with db:
        if args.user:
            rows = user_jobs(db.conn, args.user, since=since, state=args.state,
                             partition=args.partition, sort=args.sort, limit=args.limit)
        else:
            # Generic job list
            conditions = ["submit_time >= ?"]
            params: list = [since]
            if args.state:
                conditions.append("state = ?")
                params.append(args.state.upper())
            if args.partition:
                conditions.append("partition = ?")
                params.append(args.partition)
            where = "WHERE " + " AND ".join(conditions)
            sort_col = {"submit": "submit_time", "start": "start_time",
                        "elapsed": "elapsed_s", "cpus": "num_cpus",
                        "mem": "req_mem_mb"}.get(args.sort, "submit_time")
            rows = [
                dict(r) for r in db.conn.execute(
                    f"SELECT * FROM jobs {where} ORDER BY {sort_col} DESC LIMIT ?",
                    params + [args.limit],
                ).fetchall()
            ]

    if not rows:
        print("No jobs found.")
        return

    # Print table
    header = f"{'JOBID':<12} {'USER':<10} {'ACCT':<10} {'PART':<15} {'STATE':<12} {'CPUS':>5} {'MEM':>7} {'ELAPSED':>10}"
    print(header)
    print("-" * len(header))
    for r in rows:
        print(
            f"{r['job_id']:<12} {r['user']:<10} {(r.get('account') or '-'):<10} "
            f"{(r.get('partition') or '-'):<15} {r['state']:<12} {r.get('num_cpus', 0):>5} "
            f"{_format_mem(r.get('req_mem_mb')):>7} {_format_duration(r.get('elapsed_s')):>10}"
        )


def cmd_users(args: argparse.Namespace) -> None:
    from slurmmon_cli.storage.database import Database
    from slurmmon_cli.analysis.users import user_summary

    since = _parse_since(args.since)
    db = Database(args.db)
    with db:
        rows = user_summary(db.conn, since=since, sort=args.sort, top=args.top)

    if not rows:
        print("No data found.")
        return

    header = (
        f"{'USER':<12} {'ACCT':<10} {'RUNNING':>8} {'PENDING':>8} {'COMPLETED':>10} "
        f"{'FAILED':>7} {'CPU-HRS':>10} {'CPU EFF':>8} {'MEM EFF':>8}"
    )
    print(header)
    print("-" * len(header))
    for r in rows:
        print(
            f"{r['user']:<12} {(r.get('account') or '-'):<10} "
            f"{r.get('running', 0):>8} {r.get('pending', 0):>8} "
            f"{r.get('completed', 0):>10} {r.get('failed', 0):>7} "
            f"{r.get('cpu_hours', 0):>10.1f} {_pct(r.get('avg_cpu_eff')):>8} "
            f"{_pct(r.get('avg_mem_eff')):>8}"
        )


def cmd_waits(args: argparse.Namespace) -> None:
    from slurmmon_cli.storage.database import Database
    from slurmmon_cli.analysis.queue_time import (
        wait_time_stats, wait_time_by_hour, wait_time_by_size,
    )

    since = _parse_since(args.since)
    db = Database(args.db)
    with db:
        if args.by_hour:
            rows = wait_time_by_hour(db.conn, partition=args.partition, since=since)
            if not rows:
                print("No data found.")
                return
            header = f"{'HOUR':>5} {'COUNT':>7} {'AVG WAIT':>12} {'MIN':>10} {'MAX':>10}"
            print(header)
            print("-" * len(header))
            for r in rows:
                print(
                    f"{r['hour']:>5} {r['count']:>7} "
                    f"{_format_duration(r['avg_wait']):>12} "
                    f"{_format_duration(r['min_wait']):>10} "
                    f"{_format_duration(r['max_wait']):>10}"
                )
        elif args.by_size:
            rows = wait_time_by_size(db.conn, partition=args.partition, since=since)
            if not rows:
                print("No data found.")
                return
            header = f"{'CPUS':>8} {'COUNT':>7} {'AVG WAIT':>12} {'MIN':>10} {'MAX':>10}"
            print(header)
            print("-" * len(header))
            for r in rows:
                print(
                    f"{r['cpu_range']:>8} {r['count']:>7} "
                    f"{_format_duration(r['avg_wait']):>12} "
                    f"{_format_duration(r['min_wait']):>10} "
                    f"{_format_duration(r['max_wait']):>10}"
                )
        else:
            stats = wait_time_stats(db.conn, partition=args.partition, since=since)
            if stats["count"] == 0:
                print("No wait time data found.")
                return
            print(f"Jobs analyzed: {stats['count']}")
            print(f"Mean wait:     {_format_duration(stats['mean'])}")
            print(f"Median wait:   {_format_duration(stats['median'])}")
            print(f"P90 wait:      {_format_duration(stats['p90'])}")
            print(f"P99 wait:      {_format_duration(stats['p99'])}")
            print(f"Max wait:      {_format_duration(stats['max'])}")


def cmd_efficiency(args: argparse.Namespace) -> None:
    from slurmmon_cli.storage.database import Database
    from slurmmon_cli.analysis.efficiency import (
        job_efficiency, efficiency_summary, low_efficiency_jobs,
    )

    cfg = getattr(args, "_config", None)
    osc_enabled = cfg.getboolean("general", "osc") if cfg else False

    if args.job:
        # Detailed GPU breakdown via gpu-seff
        if getattr(args, "gpu", False):
            if not osc_enabled:
                print("GPU details require osc=true in config.", file=sys.stderr)
                print("Run: slurmmon-cli config set general.osc true", file=sys.stderr)
                return
            from slurmmon_cli.slurm import get_gpu_seff
            data = get_gpu_seff(args.job)
            if data:
                print(f"GPU details for job {data.get('job_id', args.job)}:")
                for gpu in data.get("gpus", []):
                    gpu_id = gpu.get("gpu_id", "?")
                    util = gpu.get("utilization_pct", 0)
                    mem_used = gpu.get("memory_used_mb", 0)
                    mem_total = gpu.get("memory_total_mb", 0)
                    mem_pct = mem_used / mem_total * 100 if mem_total > 0 else 0
                    print(f"  GPU {gpu_id}: util {util:.0f}%  mem {mem_used}M/{mem_total}M ({mem_pct:.0f}%)")
                avg = data.get("avg_gpu_utilization_pct", 0)
                print(f"  Average GPU utilization: {avg:.0f}%")
            else:
                print(f"gpu-seff failed for job {args.job}.")
            return

        # Single job - use auto-dispatcher (osc-seff or seff)
        from slurmmon_cli.slurm import get_job_efficiency_auto
        eff = get_job_efficiency_auto(args.job, osc=osc_enabled)
        if eff:
            print(f"Job {eff.job_id}:")
            print(f"  CPU Efficiency:  {eff.cpu_efficiency_pct:.1f}%")
            print(f"  CPU Utilized:    {eff.cpu_utilized}")
            print(f"  Wall-clock time: {eff.walltime}")
            print(f"  Memory Eff:      {eff.mem_efficiency_pct:.1f}%")
            if eff.gpu_efficiency_pct is not None:
                print(f"  GPU Efficiency:  {eff.gpu_efficiency_pct:.1f}%")
            if eff.gpu_mem_efficiency_pct is not None:
                print(f"  GPU Mem Eff:     {eff.gpu_mem_efficiency_pct:.1f}%")
            if eff.gpu_utilization:
                print(f"  GPU Utilized:    {eff.gpu_utilization}")
            if eff.gpu_mem_utilized:
                print(f"  GPU Mem Used:    {eff.gpu_mem_utilized}")
            if eff.num_gpus is not None:
                print(f"  Total GPUs:      {eff.num_gpus}")
            return
        # Fall back to DB
        db = Database(args.db)
        with db:
            d = job_efficiency(db.conn, args.job)
        if d:
            print(f"Job {d['job_id']} ({d['state']}):")
            print(f"  CPU Efficiency:  {_pct(d.get('cpu_eff_pct'))}")
            print(f"  Memory Eff:      {_pct(d.get('mem_eff_pct'))}")
        else:
            print(f"Job {args.job} not found.")
        return

    since = _parse_since(args.since)
    db = Database(args.db)
    with db:
        summary = efficiency_summary(db.conn, user=args.user, since=since)
        total = summary.get('total_jobs') or 0
        print(f"Jobs analyzed:    {total}")
        print(f"  With CPU data:  {summary.get('jobs_with_cpu_data') or 0}")
        print(f"  With mem data:  {summary.get('jobs_with_mem_data') or 0}")
        print(f"Avg CPU Eff:      {_pct(summary.get('avg_cpu_eff'))}")
        print(f"Avg Mem Eff:      {_pct(summary.get('avg_mem_eff'))}")

        low = low_efficiency_jobs(db.conn, threshold_pct=args.low,
                                  user=args.user, since=since)
        if low:
            print(f"\nLow efficiency jobs (< {args.low:.0f}%):")
            header = f"{'JOBID':<12} {'USER':<10} {'PART':<15} {'CPUS':>5} {'CPU EFF':>8} {'MEM EFF':>8}"
            print(header)
            print("-" * len(header))
            for r in low:
                print(
                    f"{r['job_id']:<12} {r['user']:<10} "
                    f"{(r.get('partition') or '-'):<15} {r.get('num_cpus', 0):>5} "
                    f"{_pct(r.get('cpu_eff_pct')):>8} {_pct(r.get('mem_eff_pct')):>8}"
                )


def _format_gpu_types(gpu_type_mins_str: str | None) -> str:
    """Format gpu_type_mins JSON into a readable string."""
    if not gpu_type_mins_str:
        return "-"
    try:
        types = json.loads(gpu_type_mins_str)
    except (json.JSONDecodeError, TypeError):
        return "-"
    if not types:
        return "-"
    parts = [f"{t}:{v:,}" for t, v in sorted(types.items(), key=lambda x: x[1], reverse=True)]
    return " ".join(parts)


def cmd_explore(args: argparse.Namespace) -> None:
    from slurmmon_cli.storage.database import Database
    from slurmmon_cli.analysis.gpu_usage import (
        top_gpu_users, top_cpu_users, top_gpu_accounts,
        top_gpu_requesters, usage_delta,
    )

    # Live-only modes (no DB needed)
    if args.by == "nodes":
        from slurmmon_cli.slurm import get_node_utilization, get_running_jobs_by_node
        nodes = get_node_utilization()
        if not nodes:
            print("Could not fetch node data.")
            return
        node_users = get_running_jobs_by_node()
        # Merge users into nodes
        for n in nodes:
            n.users = node_users.get(n.name, [])
        # Filter to allocated nodes only
        allocated = [n for n in nodes if n.cpus_alloc > 0]
        if not allocated:
            print("No allocated nodes found.")
            return
        # Sort by load ratio ascending (most underutilized first)
        allocated.sort(key=lambda n: n.load_ratio if n.load_ratio is not None else 999)
        top_nodes = allocated[:args.top]

        header = (
            f"{'':>3} {'NODE':<10} {'STATE':<8} {'USERS':<15} "
            f"{'CPU(load/alloc/tot)':>20} {'LOAD%':>6} "
            f"{'GPU(use/tot)':>12} {'MEM(alloc/tot)':>15}"
        )
        print(header)
        print("-" * len(header))
        for n in top_nodes:
            users_str = ",".join(n.users[:3]) if n.users else "-"
            if len(n.users) > 3:
                users_str += f"+{len(n.users) - 3}"
            # Mark exclusive nodes (single user, >=90% CPUs)
            exclusive = (
                len(n.users) == 1
                and n.cpus_alloc > 0
                and n.cpus_alloc >= n.cpus_total * 0.9
            )
            marker = " * " if exclusive else "   "
            cpu_str = f"{n.cpu_load:.1f}/{n.cpus_alloc}/{n.cpus_total}"
            load_pct = f"{n.load_ratio * 100:.0f}%" if n.load_ratio is not None else "-"
            gpu_str = f"{n.gpus_alloc}/{n.gpus_total}"
            if n.gpu_type:
                gpu_str += f" {n.gpu_type}"
            mem_alloc = _format_mem(n.mem_alloc_mb)
            mem_total = _format_mem(n.mem_total_mb)
            mem_str = f"{mem_alloc}/{mem_total}"
            print(
                f"{marker}{n.name:<10} {n.state:<8} {users_str:<15} "
                f"{cpu_str:>20} {load_pct:>6} "
                f"{gpu_str:>12} {mem_str:>15}"
            )

        exclusive_count = sum(
            1 for n in allocated
            if len(n.users) == 1 and n.cpus_alloc >= n.cpus_total * 0.9
        )
        underutil = sum(1 for n in allocated if n.load_ratio is not None and n.load_ratio < 0.5)
        notes = []
        if exclusive_count:
            notes.append(f"{exclusive_count} exclusive-use node(s) marked with *")
        if underutil:
            notes.append(f"{underutil} node(s) with load ratio < 50%")
        if notes:
            print(f"\n{'; '.join(notes)}")
        return

    db = Database(args.db)

    if args.by == "requests":
        # Live squeue data - works even without sshare collection
        with db:
            rows = top_gpu_requesters(db.conn, top=args.top)
        if not rows:
            print("No GPU jobs found in queue.")
            return
        header = f"{'#':>3}  {'USER':<15} {'ACCOUNT':<12} {'GPUS(R/P)':>10}  {'PARTITIONS':<20}"
        print(header)
        print("-" * len(header))
        for i, r in enumerate(rows, 1):
            rp = f"{r.get('gpus_running', 0)}/{r.get('gpus_pending', 0)}"
            parts = r.get("partitions") or "-"
            print(f"{i:>3}  {r['user']:<15} {(r.get('account') or '-'):<12} {rp:>10}  {parts:<20}")
        return

    with db:
        if args.by == "gpu":
            rows = top_gpu_users(db.conn, top=args.top)
            if not rows:
                print("No GPU usage data. Run 'slurmmon-cli collect' first.")
                return
            header = (
                f"{'#':>3}  {'USER':<15} {'ACCOUNT':<12} {'GPU-HOURS':>10}  "
                f"{'JOBS(R/P)':>10}  {'NODES':>6}  {'FAIRSHARE':>10}  {'GPU TYPES':<20}"
            )
            print(header)
            print("-" * len(header))
            for i, r in enumerate(rows, 1):
                gpu_hrs = r.get("gpu_tres_mins", 0) // 60
                fair = f"{r['fairshare']:.2f}" if r.get("fairshare") is not None else "-"
                types = _format_gpu_types(r.get("gpu_type_mins"))
                jr = r.get("gpu_jobs_running", 0)
                jp = r.get("gpu_jobs_pending", 0)
                jobs_str = f"{jr}/{jp}"
                nodes = r.get("gpu_node_count", 0)
                print(
                    f"{i:>3}  {r['user']:<15} {(r.get('account') or '-'):<12} {gpu_hrs:>10,}  "
                    f"{jobs_str:>10}  {nodes:>6}  {fair:>10}  {types:<20}"
                )

        elif args.by == "cpu":
            rows = top_cpu_users(db.conn, top=args.top)
            if not rows:
                print("No CPU usage data. Run 'slurmmon-cli collect' first.")
                return
            header = f"{'#':>3}  {'USER':<15} {'ACCOUNT':<12} {'CPU-HOURS':>10}  {'GPU-HOURS':>10}  {'FAIRSHARE':>10}"
            print(header)
            print("-" * len(header))
            for i, r in enumerate(rows, 1):
                cpu_hrs = r.get("cpu_tres_mins", 0) // 60
                gpu_hrs = r.get("gpu_tres_mins", 0) // 60
                fair = f"{r['fairshare']:.2f}" if r.get("fairshare") is not None else "-"
                print(f"{i:>3}  {r['user']:<15} {(r.get('account') or '-'):<12} {cpu_hrs:>10,}  {gpu_hrs:>10,}  {fair:>10}")

        elif args.by == "account":
            rows = top_gpu_accounts(db.conn, top=args.top)
            if not rows:
                print("No GPU usage data. Run 'slurmmon-cli collect' first.")
                return
            header = f"{'#':>3}  {'ACCOUNT':<20} {'GPU-HOURS':>10}  {'CPU-HOURS':>10}  {'USERS':>6}"
            print(header)
            print("-" * len(header))
            for i, r in enumerate(rows, 1):
                gpu_hrs = r.get("gpu_tres_mins", 0) // 60
                cpu_hrs = r.get("cpu_tres_mins", 0) // 60
                print(f"{i:>3}  {r['account']:<20} {gpu_hrs:>10,}  {cpu_hrs:>10,}  {r.get('num_users', 0):>6}")

        elif args.by == "delta":
            rows = usage_delta(db.conn, hours=args.hours)
            if not rows:
                print(f"No delta data. Need at least 2 sshare snapshots {args.hours}h apart.")
                return
            header = f"{'#':>3}  {'USER':<15} {'ACCOUNT':<12} {'GPU-HRS DELTA':>14}  {'GPU-HRS TOTAL':>14}"
            print(header)
            print("-" * len(header))
            for i, r in enumerate(rows, 1):
                delta_hrs = r.get("gpu_delta", 0) // 60
                total_hrs = r.get("gpu_total", 0) // 60
                print(f"{i:>3}  {r['user']:<15} {(r.get('account') or '-'):<12} {delta_hrs:>14,}  {total_hrs:>14,}")


def cmd_config(args: argparse.Namespace) -> None:
    from slurmmon_cli.config import load_config

    cfg = getattr(args, "_config", None) or load_config(args.config)

    if args.config_command == "set":
        parts = args.key.split(".", 1)
        if len(parts) != 2:
            print("Key must be in format section.key (e.g. general.osc)", file=sys.stderr)
            sys.exit(1)
        section, key = parts
        cfg.set(section, key, args.value)
        cfg.save()
        print(f"Set {section}.{key} = {args.value}")
        print(f"Saved to {cfg.path}")
    else:
        # show (default)
        print(f"Config: {cfg.path}")
        for section in cfg.sections():
            print(f"\n[{section}]")
            for key, val in cfg.items(section):
                print(f"  {key} = {val}")


def cmd_db(args: argparse.Namespace) -> None:
    from slurmmon_cli.storage.database import Database

    db = Database(args.db)

    if args.db_command == "info":
        import os
        with db:
            jobs = db.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            snaps = db.conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
            parts = db.conn.execute("SELECT COUNT(*) FROM partitions").fetchone()[0]

            oldest = db.conn.execute("SELECT MIN(submit_time) FROM jobs").fetchone()[0]
            newest = db.conn.execute("SELECT MAX(last_seen) FROM jobs").fetchone()[0]
            usage = db.conn.execute("SELECT COUNT(*) FROM user_usage").fetchone()[0]

        db_size = os.path.getsize(db.db_path) if os.path.exists(db.db_path) else 0
        print(f"Database: {db.db_path}")
        print(f"Size:     {db_size / 1024:.1f} KB")
        print(f"Jobs:     {jobs}")
        print(f"Snapshots: {snaps}")
        print(f"Partitions: {parts}")
        print(f"Usage rows: {usage}")
        if oldest:
            import datetime
            print(f"Oldest job: {datetime.datetime.fromtimestamp(oldest):%Y-%m-%d %H:%M}")
        if newest:
            import datetime
            print(f"Last seen:  {datetime.datetime.fromtimestamp(newest):%Y-%m-%d %H:%M}")

    elif args.db_command == "prune":
        from slurmmon_cli.storage.collector import prune_old_jobs
        with db:
            pruned = prune_old_jobs(db, retention_days=args.days)
        print(f"Pruned {pruned} old jobs.")

    elif args.db_command == "vacuum":
        import os
        before = os.path.getsize(db.db_path) if os.path.exists(db.db_path) else 0
        with db:
            db.conn.execute("VACUUM")
        after = os.path.getsize(db.db_path) if os.path.exists(db.db_path) else 0
        print(f"Vacuumed: {before / 1024:.1f} KB -> {after / 1024:.1f} KB")

    else:
        print("Usage: slurmmon-cli db {info|prune|vacuum}", file=sys.stderr)


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        # Launch TUI by default
        args.command = "dashboard"
        args.refresh = 30
        args.user = None
        args.partition = None
        args.from_db = False

    # Demo mode: synthetic data, no Slurm required
    if getattr(args, "demo", False):
        from slurmmon_cli.demo import setup_demo
        args.db = setup_demo()

    # Load config and attach to args for handler access
    from slurmmon_cli.config import load_config
    args._config = load_config(getattr(args, "config", None))

    handlers = {
        "dashboard": cmd_dashboard, "d": cmd_dashboard,
        "collect": cmd_collect,
        "jobs": cmd_jobs,
        "users": cmd_users,
        "waits": cmd_waits,
        "efficiency": cmd_efficiency,
        "explore": cmd_explore, "x": cmd_explore,
        "config": cmd_config,
        "db": cmd_db,
    }
    handler = handlers.get(args.command)
    if handler:
        handler(args)
    else:
        parser.print_help()
