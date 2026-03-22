from __future__ import annotations

from dataclasses import dataclass


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
