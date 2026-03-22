from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class Job:
    job_id: str
    user: str
    account: str | None
    partition: str | None
    state: str
    num_cpus: int
    num_gpus: int
    req_mem_mb: float | None
    submit_time: float | None
    start_time: float | None
    end_time: float | None
    time_limit_s: int | None
    elapsed_s: int | None
    node_list: str | None
    exit_code: str | None
    cpu_time_s: float | None
    max_rss_mb: float | None
    reason: str | None
    cluster: str = ""


@dataclass(slots=True)
class PartitionInfo:
    name: str
    state: str
    total_nodes: int
    idle_nodes: int
    alloc_nodes: int
    other_nodes: int
    total_cpus: int
    avail_cpus: int
    max_time: str | None


@dataclass(slots=True)
class ClusterInfo:
    cluster_name: str
    partitions: list[PartitionInfo]
    total_nodes: int
    idle_nodes: int
    alloc_nodes: int
    down_nodes: int
    mixed_nodes: int
    total_cpus: int
    alloc_cpus: int


@dataclass(slots=True)
class JobEfficiency:
    job_id: str
    cpu_efficiency_pct: float
    mem_efficiency_pct: float
    cpu_utilized: str
    walltime: str
    # GPU fields (populated only when OSC tools are available)
    gpu_efficiency_pct: float | None = None
    gpu_mem_efficiency_pct: float | None = None
    gpu_utilization: str | None = None
    gpu_mem_utilized: str | None = None
    num_gpus: int | None = None


@dataclass(slots=True)
class UserUsage:
    account: str
    user: str
    raw_usage: int                                     # CPU-seconds (all-time)
    fairshare: float | None
    cpu_tres_mins: int                                 # from TRESRunMins
    gpu_tres_mins: int                                 # gres/gpu from TRESRunMins
    gpu_type_mins: dict[str, int] = field(default_factory=dict)  # {"a100": N}


@dataclass(slots=True)
class NodeUtilization:
    name: str
    state: str                    # ALLOCATED, MIXED, IDLE
    cpus_total: int
    cpus_alloc: int
    cpu_load: float               # OS load average
    load_ratio: float | None      # cpu_load / cpus_alloc (None if 0 alloc)
    mem_total_mb: int
    mem_alloc_mb: int
    gpus_total: int
    gpus_alloc: int
    gpu_type: str | None          # e.g. "a100"
    partitions: list[str] = field(default_factory=list)
    users: list[str] = field(default_factory=list)
