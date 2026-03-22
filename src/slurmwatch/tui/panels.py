"""Panel renderers for the curses TUI dashboard."""

from __future__ import annotations

import curses
import time
from typing import Any

from slurmwatch.models import ClusterInfo, Job
from slurmwatch.tui.formatting import (
    format_duration, format_mem, progress_bar, truncate,
)

# Color pair IDs
C_HEADER = 1   # Cyan
C_GREEN = 2    # Green (running/idle)
C_YELLOW = 3   # Yellow (pending/mixed)
C_RED = 4      # Red (failed/down)
C_DIM = 5      # Dim


def init_colors() -> None:
    """Initialize curses color pairs."""
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(C_HEADER, curses.COLOR_CYAN, -1)
    curses.init_pair(C_GREEN, curses.COLOR_GREEN, -1)
    curses.init_pair(C_YELLOW, curses.COLOR_YELLOW, -1)
    curses.init_pair(C_RED, curses.COLOR_RED, -1)
    curses.init_pair(C_DIM, curses.COLOR_WHITE, -1)


def _addstr(win, y: int, x: int, text: str,
            attr: int = 0, max_width: int | None = None) -> None:
    """Safe addstr that clips to window boundaries."""
    height, width = win.getmaxyx()
    if y < 0 or y >= height or x >= width:
        return
    if max_width is None:
        max_width = width - x
    text = text[:max_width]
    try:
        win.addnstr(y, x, text, max_width, attr)
    except curses.error:
        pass


def render_header(win, y: int, width: int, cluster_name: str,
                  refresh_s: int) -> int:
    """Render the title bar. Returns next y."""
    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    title = f" slurmwatch   {cluster_name}   {now_str}"
    keys = " [q]uit [u]ser [r]efresh [s]ort [p]art"
    line = title + " " * max(0, width - len(title) - len(keys)) + keys
    _addstr(win, y, 0, line[:width], curses.color_pair(C_HEADER) | curses.A_BOLD)
    return y + 1


def render_separator(win, y: int, width: int) -> int:
    _addstr(win, y, 0, "─" * width, curses.color_pair(C_DIM) | curses.A_DIM)
    return y + 1


def render_cluster_summary(win, y: int, width: int,
                           info: ClusterInfo | None) -> int:
    """Render cluster node/CPU/job summary. Returns next y."""
    if info is None:
        _addstr(win, y, 1, "Cluster info unavailable", curses.color_pair(C_RED))
        return y + 1

    # Nodes line
    node_line = (
        f" Nodes: {info.alloc_nodes}/{info.total_nodes} alloc"
        f"  {info.idle_nodes} idle  {info.mixed_nodes} mixed"
        f"  {info.down_nodes} down"
    )
    _addstr(win, y, 0, node_line)
    y += 1

    # CPU bar
    bar_width = max(20, width - 30)
    bar = progress_bar(info.alloc_cpus, info.total_cpus, bar_width)
    pct = info.alloc_cpus / info.total_cpus * 100 if info.total_cpus > 0 else 0
    cpu_line = f" CPUs:  {bar} {pct:.0f}%"
    _addstr(win, y, 0, cpu_line)
    y += 1

    return y


def render_job_counts(win, y: int, width: int,
                      jobs: list[Job]) -> int:
    """Render running/pending job counts."""
    running = sum(1 for j in jobs if j.state == "RUNNING")
    pending = sum(1 for j in jobs if j.state == "PENDING")
    line = f" Jobs:  {running} running  {pending} pending"
    _addstr(win, y, 0, line)
    # Color the counts
    return y + 1


def render_partition_table(win, y: int, width: int,
                           info: ClusterInfo | None,
                           partition_filter: str | None = None) -> int:
    """Render partition table. Returns next y."""
    if info is None or not info.partitions:
        return y

    y = render_separator(win, y, width)

    header = f" {'PARTITION':<20} {'NODES(A/I/O/T)':>16} {'CPUS':>12} {'STATE':>8} {'TIMELIMIT':>12}"
    _addstr(win, y, 0, header[:width], curses.color_pair(C_HEADER))
    y += 1

    partitions = info.partitions
    if partition_filter:
        partitions = [p for p in partitions if p.name == partition_filter]

    for p in partitions:
        height = win.getmaxyx()[0]
        if y >= height - 4:  # Leave room for job tables + footer
            break
        nodes_str = f"{p.alloc_nodes}/{p.idle_nodes}/{p.other_nodes}/{p.total_nodes}"
        cpus_str = f"{p.total_cpus - p.avail_cpus}/{p.total_cpus}"
        max_time = p.max_time or "-"
        state = p.state

        color = 0
        if state.upper() in ("DOWN", "DRAIN", "INACTIVE", "INACT"):
            color = curses.color_pair(C_DIM) | curses.A_DIM

        line = f" {truncate(p.name, 20):<20} {nodes_str:>16} {cpus_str:>12} {state:>8} {max_time:>12}"
        _addstr(win, y, 0, line[:width], color)
        y += 1

    return y


def render_job_table(win, y: int, width: int, jobs: list[Job],
                     title: str, max_rows: int,
                     show_reason: bool = False,
                     sort_key: str = "elapsed",
                     scroll_offset: int = 0) -> int:
    """Render a job table (running or pending). Returns next y."""
    height = win.getmaxyx()[0]
    if y >= height - 2:
        return y

    y = render_separator(win, y, width)

    # Title
    _addstr(win, y, 1, title, curses.color_pair(C_HEADER) | curses.A_BOLD)
    y += 1

    if not jobs:
        _addstr(win, y, 2, "(none)", curses.color_pair(C_DIM) | curses.A_DIM)
        return y + 1

    # Header
    if show_reason:
        hdr = f" {'JOBID':<12} {'USER':<10} {'ACCT':<10} {'PART':<15} {'CPUS':>5} {'MEM':>7} {'WAITING':>10} {'REASON':<12}"
    else:
        hdr = f" {'JOBID':<12} {'USER':<10} {'ACCT':<10} {'PART':<15} {'CPUS':>5} {'MEM':>7} {'ELAPSED':>10} {'LIMIT':>10}"
    _addstr(win, y, 0, hdr[:width], curses.color_pair(C_HEADER))
    y += 1

    # Sort
    now = time.time()
    if sort_key == "elapsed":
        sorted_jobs = sorted(jobs, key=lambda j: j.elapsed_s or 0, reverse=True)
    elif sort_key == "cpus":
        sorted_jobs = sorted(jobs, key=lambda j: j.num_cpus, reverse=True)
    elif sort_key == "wait":
        sorted_jobs = sorted(
            jobs,
            key=lambda j: (now - j.submit_time) if j.submit_time else 0,
            reverse=True,
        )
    else:
        sorted_jobs = jobs

    # Apply scroll
    visible = sorted_jobs[scroll_offset: scroll_offset + max_rows]

    for job in visible:
        if y >= height - 1:
            break
        acct = truncate(job.account or "-", 10)
        part = truncate(job.partition or "-", 15)
        mem = format_mem(job.req_mem_mb)

        state_color = 0
        if job.state == "RUNNING":
            state_color = curses.color_pair(C_GREEN)
        elif job.state == "PENDING":
            state_color = curses.color_pair(C_YELLOW)
        elif job.state in ("FAILED", "TIMEOUT", "CANCELLED"):
            state_color = curses.color_pair(C_RED)

        if show_reason:
            wait = format_duration(now - job.submit_time if job.submit_time else None)
            reason = truncate(job.reason or "-", 12)
            line = f" {truncate(job.job_id, 12):<12} {truncate(job.user, 10):<10} {acct:<10} {part:<15} {job.num_cpus:>5} {mem:>7} {wait:>10} {reason:<12}"
        else:
            elapsed = format_duration(job.elapsed_s)
            limit = format_duration(job.time_limit_s)
            line = f" {truncate(job.job_id, 12):<12} {truncate(job.user, 10):<10} {acct:<10} {part:<15} {job.num_cpus:>5} {mem:>7} {elapsed:>10} {limit:>10}"

        _addstr(win, y, 0, line[:width], state_color)
        y += 1

    # Show scroll indicator if needed
    if len(sorted_jobs) > max_rows + scroll_offset:
        remaining = len(sorted_jobs) - max_rows - scroll_offset
        _addstr(win, y, 2, f"... {remaining} more (j/k to scroll)",
                curses.color_pair(C_DIM) | curses.A_DIM)
        y += 1

    return y


def render_footer(win, y: int, width: int, refresh_s: int,
                  last_refresh: float | None) -> int:
    """Render footer at specified y position."""
    height = win.getmaxyx()[0]
    y = height - 1
    if y < 0:
        return y

    if last_refresh:
        last_str = time.strftime("%H:%M:%S", time.localtime(last_refresh))
        next_t = last_refresh + refresh_s
        next_str = time.strftime("%H:%M:%S", time.localtime(next_t))
        footer = f" Last: {last_str}  Next: {next_str}  Interval: {refresh_s}s"
    else:
        footer = f" Refreshing...  Interval: {refresh_s}s"

    _addstr(win, y, 0, footer[:width], curses.color_pair(C_DIM) | curses.A_DIM)
    return y
