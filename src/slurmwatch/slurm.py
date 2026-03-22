"""Interface to Slurm CLI commands. Parses JSON output into model dataclasses."""

from __future__ import annotations

import json
import logging
import re
import subprocess
from typing import Any

from slurmwatch.models import ClusterInfo, Job, JobEfficiency, PartitionInfo

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def extract_val(obj: Any) -> Any:
    """Unwrap Slurm OpenAPI ``{"set": bool, "infinite": bool, "number": N}``.

    Returns None if *set* is false, ``float('inf')`` if *infinite* is true,
    otherwise the *number* value.  If *obj* is not a dict with the expected
    keys, return it unchanged.
    """
    if not isinstance(obj, dict) or "set" not in obj:
        return obj
    if not obj.get("set", False):
        return None
    if obj.get("infinite", False):
        return float("inf")
    return obj.get("number")


def parse_mem_mb(value: Any) -> float | None:
    """Normalise a memory specification to megabytes.

    Handles strings like ``"4G"``, ``"4096M"``, ``"4096"`` (assumed MB),
    plain ints/floats (assumed MB), and Slurm ``{"set":…}`` wrappers.
    """
    value = extract_val(value)
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) if value > 0 else None
    if isinstance(value, str):
        value = value.strip().upper()
        if not value or value == "0":
            return None
        m = re.match(r"^([\d.]+)\s*([GMKT]?)B?$", value)
        if m:
            num = float(m.group(1))
            unit = m.group(2)
            multipliers = {"G": 1024, "T": 1024 * 1024, "K": 1 / 1024, "M": 1, "": 1}
            return num * multipliers.get(unit, 1)
    return None


def parse_tres_gpus(tres: Any) -> int:
    """Extract GPU count from a TRES string or object.

    TRES strings look like ``"cpu=4,mem=16G,node=1,billing=4,gres/gpu=2"``.
    """
    if isinstance(tres, dict):
        # Slurm JSON sometimes nests TRES as objects
        allocated = tres.get("allocated", [])
        if isinstance(allocated, list):
            for item in allocated:
                if isinstance(item, dict) and item.get("type") == "gres" and "gpu" in str(item.get("name", "")):
                    return int(item.get("count", 0))
        return 0
    if isinstance(tres, str):
        m = re.search(r"gres/gpu[^=]*=(\d+)", tres)
        return int(m.group(1)) if m else 0
    return 0


def _parse_slurm_time(val: Any) -> float | None:
    """Parse a Slurm timestamp (epoch int, ``{"set":…}`` wrapper, or 0)."""
    val = extract_val(val)
    if val is None or val == 0:
        return None
    if isinstance(val, (int, float)) and val > 0:
        return float(val)
    return None


def _parse_elapsed(val: Any) -> int | None:
    """Parse elapsed seconds from Slurm JSON (int or ``{"set":…}``)."""
    val = extract_val(val)
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return int(val) if val >= 0 else None
    return None


def _safe_str(val: Any) -> str | None:
    if val is None or val == "":
        return None
    return str(val)


# ---------------------------------------------------------------------------
# Slurm command runner
# ---------------------------------------------------------------------------

def run_slurm_command(cmd: list[str], timeout: int = 30) -> dict | None:
    """Run a Slurm command and return parsed JSON, or None on failure."""
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
        )
        if result.returncode != 0:
            log.warning("Command %s failed (rc=%d): %s", cmd, result.returncode, result.stderr.strip())
            return None
        return json.loads(result.stdout)
    except subprocess.TimeoutExpired:
        log.warning("Command %s timed out after %ds", cmd, timeout)
        return None
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("Command %s error: %s", cmd, exc)
        return None


# ---------------------------------------------------------------------------
# squeue
# ---------------------------------------------------------------------------

def _parse_squeue_job(raw: dict) -> Job:
    """Parse a single job dict from ``squeue --json`` output."""
    # job_id: may be plain int or nested
    job_id_val = raw.get("job_id", 0)
    if isinstance(job_id_val, dict):
        job_id_val = extract_val(job_id_val)
    job_id = str(job_id_val)

    # Array task: append _N if it's an array job
    array_task = extract_val(raw.get("array_task_id"))
    array_job = extract_val(raw.get("array_job_id"))
    if array_task is not None and array_job and array_job > 0:
        job_id = f"{array_job}_{array_task}"

    # State: may be a list
    state_raw = raw.get("job_state", "UNKNOWN")
    state = state_raw[0] if isinstance(state_raw, list) and state_raw else str(state_raw)

    # CPUs
    cpus_raw = raw.get("cpus", raw.get("number_cpus", 0))
    if isinstance(cpus_raw, dict):
        num_cpus = cpus_raw.get("number", extract_val(cpus_raw) or 0)
    else:
        num_cpus = int(cpus_raw) if cpus_raw else 0

    # GPUs from TRES
    num_gpus = parse_tres_gpus(raw.get("tres_req_str", raw.get("tres", "")))

    # Memory
    req_mem = parse_mem_mb(raw.get("memory_per_node", raw.get("min_memory_node")))
    if req_mem is None:
        req_mem = parse_mem_mb(raw.get("memory_per_cpu", raw.get("min_memory_cpu")))
        if req_mem is not None and num_cpus > 0:
            req_mem *= num_cpus

    # Node list
    node_list_raw = raw.get("nodes", raw.get("node_list", ""))
    if isinstance(node_list_raw, dict):
        node_list_raw = node_list_raw.get("nodes", "")

    # Pending reason
    reason_raw = raw.get("state_reason", raw.get("reason", ""))
    if isinstance(reason_raw, list):
        reason_raw = reason_raw[0] if reason_raw else None

    # Time limit
    time_limit = raw.get("time_limit", raw.get("timelimit"))
    if isinstance(time_limit, dict):
        tl_val = extract_val(time_limit)
        time_limit_s = int(tl_val * 60) if tl_val is not None and tl_val != float("inf") else None
    elif isinstance(time_limit, (int, float)):
        time_limit_s = int(time_limit * 60) if time_limit > 0 else None
    else:
        time_limit_s = None

    return Job(
        job_id=job_id,
        user=str(raw.get("user_name", raw.get("user", "unknown"))),
        account=_safe_str(raw.get("account")),
        partition=_safe_str(raw.get("partition")),
        state=state,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        req_mem_mb=req_mem,
        submit_time=_parse_slurm_time(raw.get("submit_time")),
        start_time=_parse_slurm_time(raw.get("start_time")),
        end_time=None,  # squeue doesn't have end_time
        time_limit_s=time_limit_s,
        elapsed_s=_parse_elapsed(raw.get("time", raw.get("elapsed_time"))),
        node_list=_safe_str(node_list_raw),
        exit_code=None,
        cpu_time_s=None,
        max_rss_mb=None,
        reason=_safe_str(reason_raw) if reason_raw and reason_raw != "None" else None,
    )


def get_queue(user: str | None = None) -> list[Job]:
    """Fetch current job queue via ``squeue --json``."""
    cmd = ["squeue", "--json"]
    if user:
        cmd.extend(["--user", user])
    data = run_slurm_command(cmd)
    if data is None:
        return []
    jobs = []
    for raw in data.get("jobs", []):
        try:
            jobs.append(_parse_squeue_job(raw))
        except Exception as exc:
            log.debug("Failed to parse squeue job: %s", exc)
    return jobs


# ---------------------------------------------------------------------------
# sacct
# ---------------------------------------------------------------------------

def _parse_sacct_job(raw: dict) -> Job:
    """Parse a single job dict from ``sacct --json`` output."""
    # job_id
    job_id_val = raw.get("job_id", 0)
    job_id = str(job_id_val)

    # Array task
    array_info = raw.get("array", {})
    if isinstance(array_info, dict):
        array_job_id = array_info.get("job_id", 0)
        array_task_id = extract_val(array_info.get("task_id"))
        if array_task_id is not None and array_job_id and array_job_id > 0:
            job_id = f"{array_job_id}_{array_task_id}"

    # State
    state_raw = raw.get("state", raw.get("derived_exit_code", {}))
    if isinstance(state_raw, dict):
        state = str(state_raw.get("current", state_raw.get("value", "UNKNOWN")))
        if isinstance(state, list):
            state = state[0] if state else "UNKNOWN"
    elif isinstance(state_raw, list):
        state = state_raw[0] if state_raw else "UNKNOWN"
    else:
        state = str(state_raw)

    # Association info
    assoc = raw.get("association", {})
    account = _safe_str(raw.get("account", assoc.get("account")))
    partition = _safe_str(raw.get("partition", assoc.get("partition")))

    # User
    user = str(raw.get("user", "unknown"))

    # Resources
    alloc_cpus = raw.get("allocation_nodes", 0)
    required = raw.get("required", {})
    num_cpus = raw.get("cpus", required.get("CPUs", alloc_cpus))
    if isinstance(num_cpus, dict):
        num_cpus = extract_val(num_cpus) or 0
    num_cpus = int(num_cpus)

    num_gpus = parse_tres_gpus(raw.get("tres", raw.get("tres_req_str", "")))

    # Memory
    req_mem = parse_mem_mb(required.get("memory_per_node", required.get("memory")))
    if req_mem is None:
        req_mem = parse_mem_mb(required.get("memory_per_cpu"))
        if req_mem is not None and num_cpus > 0:
            req_mem *= num_cpus

    # Times
    time_info = raw.get("time", {})
    if isinstance(time_info, dict):
        submit_time = _parse_slurm_time(time_info.get("submission"))
        start_time = _parse_slurm_time(time_info.get("start"))
        end_time = _parse_slurm_time(time_info.get("end"))
        elapsed_s = _parse_elapsed(time_info.get("elapsed"))
        time_limit_val = extract_val(time_info.get("limit"))
        time_limit_s = int(time_limit_val * 60) if time_limit_val and time_limit_val != float("inf") else None
    else:
        submit_time = _parse_slurm_time(raw.get("submit_time"))
        start_time = _parse_slurm_time(raw.get("start_time"))
        end_time = _parse_slurm_time(raw.get("end_time"))
        elapsed_s = _parse_elapsed(raw.get("elapsed"))
        time_limit_s = None

    # Exit code
    exit_code_raw = raw.get("exit_code", {})
    if isinstance(exit_code_raw, dict):
        exit_code = f"{exit_code_raw.get('return_code', 0)}:{exit_code_raw.get('signal', {}).get('signal_id', 0)}"
    else:
        exit_code = _safe_str(exit_code_raw)

    # CPU time and RSS (efficiency data)
    steps = raw.get("steps", [])
    cpu_time_s = None
    max_rss_mb = None
    if isinstance(steps, list):
        for step in steps:
            if not isinstance(step, dict):
                continue
            stats = step.get("statistics", step.get("stats", {}))
            if not isinstance(stats, dict):
                continue
            cpu_info = stats.get("cpu", {})
            if isinstance(cpu_info, dict):
                actual = cpu_info.get("actual_frequency", cpu_info.get("actual", {}))
                if isinstance(actual, dict):
                    cpu_s = extract_val(actual.get("seconds"))
                    if cpu_s and (cpu_time_s is None or cpu_s > cpu_time_s):
                        cpu_time_s = float(cpu_s)
            mem_info = stats.get("memory", {})
            if isinstance(mem_info, dict):
                rss_raw = mem_info.get("rss", mem_info.get("max"))
                if isinstance(rss_raw, dict):
                    rss_val = extract_val(rss_raw.get("max", rss_raw))
                else:
                    rss_val = rss_raw
                if rss_val and isinstance(rss_val, (int, float)):
                    rss_mb = rss_val / (1024 * 1024)  # bytes to MB
                    if max_rss_mb is None or rss_mb > max_rss_mb:
                        max_rss_mb = rss_mb

    # Fallback: top-level tres usage
    if cpu_time_s is None:
        tres_usage = raw.get("tres", {})
        if isinstance(tres_usage, dict):
            allocated = tres_usage.get("allocated", [])
            if isinstance(allocated, list):
                for item in allocated:
                    if isinstance(item, dict) and item.get("type") == "cpu":
                        cpu_time_s = float(item.get("count", 0))

    return Job(
        job_id=job_id,
        user=user,
        account=account,
        partition=partition,
        state=state,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        req_mem_mb=req_mem,
        submit_time=submit_time,
        start_time=start_time,
        end_time=end_time,
        time_limit_s=time_limit_s,
        elapsed_s=elapsed_s,
        node_list=_safe_str(raw.get("nodes", "")),
        exit_code=exit_code,
        cpu_time_s=cpu_time_s,
        max_rss_mb=max_rss_mb,
        reason=None,
    )


def get_job_history(starttime: str = "now-24hours", user: str | None = None) -> list[Job]:
    """Fetch completed job history via ``sacct --json``."""
    cmd = ["sacct", "--json", f"--starttime={starttime}"]
    if user:
        cmd.extend(["--user", user])
    data = run_slurm_command(cmd)
    if data is None:
        return []
    jobs = []
    for raw in data.get("jobs", []):
        try:
            jobs.append(_parse_sacct_job(raw))
        except Exception as exc:
            log.debug("Failed to parse sacct job: %s", exc)
    return jobs


# ---------------------------------------------------------------------------
# sinfo
# ---------------------------------------------------------------------------

def get_cluster_info() -> ClusterInfo | None:
    """Fetch cluster/partition info via ``sinfo --json``."""
    data = run_slurm_command(["sinfo", "--json"])
    if data is None:
        return None

    partitions: dict[str, PartitionInfo] = {}
    total_nodes = 0
    idle_nodes = 0
    alloc_nodes = 0
    down_nodes = 0
    mixed_nodes = 0
    total_cpus = 0
    alloc_cpus = 0
    cluster_name = ""

    for entry in data.get("sinfo", []):
        # Partition name
        pname = entry.get("partition", {})
        if isinstance(pname, dict):
            pname = pname.get("name", "unknown")
        pname = str(pname)

        # Cluster name
        if not cluster_name:
            cluster_name = str(entry.get("cluster", ""))

        nodes_info = entry.get("nodes", {})
        node_alloc = int(nodes_info.get("allocated", 0))
        node_idle = int(nodes_info.get("idle", 0))
        node_other = int(nodes_info.get("other", 0))
        node_total = int(nodes_info.get("total", 0))

        cpus_info = entry.get("cpus", {})
        cpu_total = 0
        cpu_avail = 0
        if isinstance(cpus_info, dict):
            cpu_total = int(cpus_info.get("total", 0))
            cpu_avail = int(cpus_info.get("idle", cpus_info.get("available", 0)))

        # Node state
        node_state = entry.get("node", {}).get("state", [])
        if isinstance(node_state, list):
            state_str = "+".join(node_state) if node_state else "UNKNOWN"
        else:
            state_str = str(node_state)

        # Aggregate to partition (may appear multiple times with different states)
        if pname in partitions:
            p = partitions[pname]
            partitions[pname] = PartitionInfo(
                name=pname,
                state=p.state,
                total_nodes=p.total_nodes + node_total,
                idle_nodes=p.idle_nodes + node_idle,
                alloc_nodes=p.alloc_nodes + node_alloc,
                other_nodes=p.other_nodes + node_other,
                total_cpus=p.total_cpus + cpu_total,
                avail_cpus=p.avail_cpus + cpu_avail,
                max_time=p.max_time,
            )
        else:
            # Partition availability
            part_avail = entry.get("partition", {})
            if isinstance(part_avail, dict):
                avail = part_avail.get("state", "up")
            else:
                avail = "up"

            max_time_raw = entry.get("time", {})
            if isinstance(max_time_raw, dict):
                max_time = _safe_str(max_time_raw.get("maximum", max_time_raw.get("limit")))
            else:
                max_time = _safe_str(max_time_raw)

            partitions[pname] = PartitionInfo(
                name=pname,
                state=str(avail).upper() if avail else "UP",
                total_nodes=node_total,
                idle_nodes=node_idle,
                alloc_nodes=node_alloc,
                other_nodes=node_other,
                total_cpus=cpu_total,
                avail_cpus=cpu_avail,
                max_time=max_time,
            )

        # Track node states for cluster-wide totals
        primary_state = node_state[0] if isinstance(node_state, list) and node_state else state_str
        primary_state = str(primary_state).upper()
        if "IDLE" in primary_state:
            idle_nodes += node_total
        elif "MIXED" in primary_state:
            mixed_nodes += node_total
        elif "ALLOC" in primary_state:
            alloc_nodes += node_total
        elif "DOWN" in primary_state or "DRAIN" in primary_state:
            down_nodes += node_total
        else:
            alloc_nodes += node_total  # default bucket

        total_nodes += node_total
        total_cpus += cpu_total
        alloc_cpus += cpu_total - cpu_avail

    return ClusterInfo(
        cluster_name=cluster_name or "unknown",
        partitions=list(partitions.values()),
        total_nodes=total_nodes,
        idle_nodes=idle_nodes,
        alloc_nodes=alloc_nodes,
        down_nodes=down_nodes,
        mixed_nodes=mixed_nodes,
        total_cpus=total_cpus,
        alloc_cpus=alloc_cpus,
    )


# ---------------------------------------------------------------------------
# seff
# ---------------------------------------------------------------------------

def get_job_efficiency(job_id: str) -> JobEfficiency | None:
    """Parse ``seff <job_id>`` plain-text output into a JobEfficiency."""
    try:
        result = subprocess.run(
            ["seff", job_id], capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            return None
    except (subprocess.TimeoutExpired, OSError):
        return None

    text = result.stdout
    cpu_eff = 0.0
    mem_eff = 0.0
    cpu_utilized = ""
    walltime = ""

    for line in text.splitlines():
        line = line.strip()
        if line.startswith("CPU Efficiency:"):
            m = re.search(r"([\d.]+)%", line)
            if m:
                cpu_eff = float(m.group(1))
        elif line.startswith("CPU Utilized:"):
            cpu_utilized = line.split(":", 1)[1].strip()
        elif line.startswith("Job Wall-clock time:"):
            walltime = line.split(":", 1)[1].strip()
        elif line.startswith("Memory Efficiency:"):
            m = re.search(r"([\d.]+)%", line)
            if m:
                mem_eff = float(m.group(1))

    return JobEfficiency(
        job_id=job_id,
        cpu_efficiency_pct=cpu_eff,
        mem_efficiency_pct=mem_eff,
        cpu_utilized=cpu_utilized,
        walltime=walltime,
    )
