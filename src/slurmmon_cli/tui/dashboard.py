"""Curses-based TUI dashboard for slurmmon-cli."""

from __future__ import annotations

import curses
import time

from slurmmon_cli.models import ClusterInfo, Job
from slurmmon_cli.slurm import get_cluster_info, get_queue
from slurmmon_cli.tui.panels import (
    init_colors,
    render_cluster_summary,
    render_footer,
    render_header,
    render_job_counts,
    render_job_table,
    render_partition_table,
    render_separator,
)


def _fetch_live(user_filter: str | None = None) -> tuple[list[Job], ClusterInfo | None]:
    """Fetch live data from Slurm commands."""
    jobs = get_queue(user=user_filter)
    info = get_cluster_info()
    return jobs, info


def _fetch_from_db(db_path: str | None, user_filter: str | None = None
                   ) -> tuple[list[Job], ClusterInfo | None]:
    """Fetch latest data from the SQLite database."""
    from slurmmon_cli.storage.database import Database
    from slurmmon_cli.models import PartitionInfo

    db = Database(db_path)
    conn = db.connect(readonly=True)

    # Jobs: get current running/pending from DB
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

    # Cluster info from latest snapshot + partitions
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
            cluster_name="ascend",
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


def _main(stdscr, db_path: str | None, refresh: int,
          user_filter: str | None, partition_filter: str | None,
          from_db: bool) -> None:
    """Main curses loop."""
    curses.curs_set(0)
    init_colors()
    stdscr.timeout(refresh * 1000)

    sort_key = "elapsed"
    sort_keys = ["elapsed", "cpus", "wait"]
    sort_idx = 0

    scroll_running = 0
    scroll_pending = 0

    jobs: list[Job] = []
    info: ClusterInfo | None = None
    last_refresh: float | None = None
    need_refresh = True

    while True:
        if need_refresh:
            if from_db:
                jobs, info = _fetch_from_db(db_path, user_filter)
            else:
                jobs, info = _fetch_live(user_filter)
            last_refresh = time.time()
            need_refresh = False

        height, width = stdscr.getmaxyx()
        stdscr.erase()

        cluster_name = info.cluster_name if info else "unknown"

        y = render_header(stdscr, 0, width, cluster_name, refresh)
        y = render_separator(stdscr, y, width)
        y = render_cluster_summary(stdscr, y, width, info)
        y = render_job_counts(stdscr, y, width, jobs)
        y = render_partition_table(stdscr, y, width, info, partition_filter)

        # Split remaining space between running and pending tables
        remaining = height - y - 2  # Leave room for footer
        running_jobs = [j for j in jobs if j.state == "RUNNING"]
        pending_jobs = [j for j in jobs if j.state == "PENDING"]

        running_rows = max(3, remaining * 2 // 3)
        pending_rows = max(2, remaining - running_rows)

        y = render_job_table(
            stdscr, y, width, running_jobs,
            "RUNNING JOBS", running_rows,
            show_reason=False, sort_key=sort_key,
            scroll_offset=scroll_running,
        )
        y = render_job_table(
            stdscr, y, width, pending_jobs,
            "PENDING JOBS", pending_rows,
            show_reason=True, sort_key="wait",
            scroll_offset=scroll_pending,
        )

        render_footer(stdscr, y, width, refresh, last_refresh)

        stdscr.refresh()

        key = stdscr.getch()
        if key in (ord("q"), ord("Q")):
            break
        elif key == ord("r"):
            need_refresh = True
        elif key == ord("u"):
            # Cycle user filter: current -> None -> prompt
            if user_filter:
                user_filter = None
            else:
                curses.curs_set(1)
                stdscr.timeout(-1)
                stdscr.addnstr(height - 1, 0, "User filter: " + " " * 30, width)
                stdscr.move(height - 1, 13)
                curses.echo()
                try:
                    user_input = stdscr.getstr(height - 1, 13, 20).decode().strip()
                except Exception:
                    user_input = ""
                curses.noecho()
                curses.curs_set(0)
                stdscr.timeout(refresh * 1000)
                user_filter = user_input if user_input else None
            need_refresh = True
        elif key == ord("p"):
            if partition_filter:
                partition_filter = None
            else:
                curses.curs_set(1)
                stdscr.timeout(-1)
                stdscr.addnstr(height - 1, 0, "Partition: " + " " * 30, width)
                stdscr.move(height - 1, 11)
                curses.echo()
                try:
                    part_input = stdscr.getstr(height - 1, 11, 20).decode().strip()
                except Exception:
                    part_input = ""
                curses.noecho()
                curses.curs_set(0)
                stdscr.timeout(refresh * 1000)
                partition_filter = part_input if part_input else None
            need_refresh = True
        elif key == ord("s"):
            sort_idx = (sort_idx + 1) % len(sort_keys)
            sort_key = sort_keys[sort_idx]
        elif key in (ord("j"), curses.KEY_DOWN):
            scroll_running += 1
        elif key in (ord("k"), curses.KEY_UP):
            scroll_running = max(0, scroll_running - 1)
        elif key == ord("J"):
            scroll_pending += 1
        elif key == ord("K"):
            scroll_pending = max(0, scroll_pending - 1)
        elif key == curses.KEY_RESIZE:
            pass  # Redraw on next loop
        elif key == -1:
            # Timeout - time to refresh
            need_refresh = True


def run_dashboard(db_path: str | None = None, refresh: int = 30,
                  user_filter: str | None = None,
                  partition_filter: str | None = None,
                  from_db: bool = False) -> None:
    """Launch the TUI dashboard."""
    curses.wrapper(
        _main, db_path, refresh, user_filter, partition_filter, from_db
    )
