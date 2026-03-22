"""Tests for per-node utilization monitoring."""

from __future__ import annotations

from slurmmon_cli.slurm import (
    expand_node_list,
    get_node_utilization,
    get_running_jobs_by_node,
    parse_gres_gpus,
)


class TestParseGresGpus:
    def test_basic(self):
        count, gtype = parse_gres_gpus("gpu:a100:2(S:2,7)")
        assert count == 2
        assert gtype == "a100"

    def test_multiple_entries(self):
        count, gtype = parse_gres_gpus("gpu:a100:2(S:2,7),gpu:a100:2(S:0,5)")
        assert count == 4
        assert gtype == "a100"

    def test_with_non_gpu_gres(self):
        count, gtype = parse_gres_gpus(
            "gpu:a100:2(S:2,7),nsight:no_consume:1,pfsdir:scratch:no_consume:1"
        )
        assert count == 2
        assert gtype == "a100"

    def test_zero_gpus(self):
        count, gtype = parse_gres_gpus("gpu:a100:0(IDX:N/A),nsight:0")
        assert count == 0

    def test_empty(self):
        count, gtype = parse_gres_gpus("")
        assert count == 0
        assert gtype is None

    def test_no_gpu_gres(self):
        count, gtype = parse_gres_gpus("nsight:no_consume:1")
        assert count == 0
        assert gtype is None


class TestExpandNodeList:
    def test_single_node(self):
        assert expand_node_list("a0101") == ["a0101"]

    def test_range(self):
        result = expand_node_list("a[0102-0104]")
        assert result == ["a0102", "a0103", "a0104"]

    def test_comma_list(self):
        result = expand_node_list("a[0101,0103]")
        assert result == ["a0101", "a0103"]

    def test_mixed_range_and_list(self):
        result = expand_node_list("a[0101-0103,0106]")
        assert result == ["a0101", "a0102", "a0103", "a0106"]

    def test_multiple_groups(self):
        result = expand_node_list("a[0001-0002],b[0001-0002]")
        assert result == ["a0001", "a0002", "b0001", "b0002"]

    def test_preserves_zero_padding(self):
        result = expand_node_list("c[0001-0003]")
        assert result == ["c0001", "c0002", "c0003"]

    def test_empty(self):
        assert expand_node_list("") == []
        assert expand_node_list(None) == []

    def test_real_world_example(self):
        result = expand_node_list("a[0120-0121,0130-0131]")
        assert result == ["a0120", "a0121", "a0130", "a0131"]


class TestGetNodeUtilization:
    def test_parses_nodes(self, mock_slurm):
        nodes = get_node_utilization()
        # Fixture has 4 nodes, but a0300 is DOWN (skipped)
        assert len(nodes) == 3
        names = {n.name for n in nodes}
        assert "a0101" in names
        assert "a0001" in names
        assert "a0200" in names
        assert "a0300" not in names  # DOWN, skipped

    def test_underutilized_node(self, mock_slurm):
        nodes = get_node_utilization()
        a0101 = next(n for n in nodes if n.name == "a0101")
        assert a0101.cpus_total == 128
        assert a0101.cpus_alloc == 114
        assert abs(a0101.cpu_load - 2.30) < 0.01
        assert a0101.load_ratio is not None
        assert a0101.load_ratio < 0.05  # ~2% utilization
        assert a0101.gpus_total == 2
        assert a0101.gpus_alloc == 1
        assert a0101.gpu_type == "a100"

    def test_well_utilized_node(self, mock_slurm):
        nodes = get_node_utilization()
        a0001 = next(n for n in nodes if n.name == "a0001")
        assert a0001.cpus_alloc == 96
        assert abs(a0001.cpu_load - 95.0) < 0.01
        assert a0001.load_ratio is not None
        assert a0001.load_ratio > 0.9  # ~99% utilization
        assert a0001.gpus_total == 4
        assert a0001.gpus_alloc == 4

    def test_idle_node(self, mock_slurm):
        nodes = get_node_utilization()
        a0200 = next(n for n in nodes if n.name == "a0200")
        assert a0200.cpus_alloc == 0
        assert a0200.load_ratio is None  # no allocation

    def test_partitions_parsed(self, mock_slurm):
        nodes = get_node_utilization()
        a0101 = next(n for n in nodes if n.name == "a0101")
        assert "nextgen" in a0101.partitions
        assert "longgpu" in a0101.partitions


class TestGetRunningJobsByNode:
    def test_maps_nodes_to_users(self, mock_slurm):
        node_users = get_running_jobs_by_node()
        # From fixture: alice on a0101, bob on a[0102-0104]
        assert "a0101" in node_users
        assert "alice" in node_users["a0101"]
        assert "a0102" in node_users
        assert "bob" in node_users["a0102"]

    def test_pending_jobs_excluded(self, mock_slurm):
        node_users = get_running_jobs_by_node()
        # charlie and dave are PENDING, should not appear
        all_users = set()
        for users in node_users.values():
            all_users.update(users)
        assert "charlie" not in all_users
        assert "dave" not in all_users
