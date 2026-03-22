"""Tests for slurm.py - JSON parsing and helpers."""

from __future__ import annotations

from slurmwatch.slurm import (
    extract_val,
    get_cluster_info,
    get_job_efficiency,
    get_queue,
    get_job_history,
    parse_mem_mb,
    parse_tres_gpus,
)


class TestExtractVal:
    def test_set_true(self):
        assert extract_val({"set": True, "infinite": False, "number": 42}) == 42

    def test_set_false(self):
        assert extract_val({"set": False, "infinite": False, "number": 0}) is None

    def test_infinite(self):
        assert extract_val({"set": True, "infinite": True, "number": 0}) == float("inf")

    def test_passthrough_int(self):
        assert extract_val(123) == 123

    def test_passthrough_none(self):
        assert extract_val(None) is None

    def test_passthrough_string(self):
        assert extract_val("hello") == "hello"

    def test_dict_without_set_key(self):
        assert extract_val({"foo": "bar"}) == {"foo": "bar"}


class TestParseMemMb:
    def test_gigabytes_string(self):
        assert parse_mem_mb("4G") == 4096.0

    def test_megabytes_string(self):
        assert parse_mem_mb("4096M") == 4096.0

    def test_plain_number_string(self):
        assert parse_mem_mb("4096") == 4096.0

    def test_kilobytes_string(self):
        assert parse_mem_mb("1024K") == 1.0

    def test_terabytes_string(self):
        assert parse_mem_mb("1T") == 1024 * 1024

    def test_int_value(self):
        assert parse_mem_mb(16384) == 16384.0

    def test_zero(self):
        assert parse_mem_mb(0) is None

    def test_none(self):
        assert parse_mem_mb(None) is None

    def test_wrapped(self):
        assert parse_mem_mb({"set": True, "infinite": False, "number": 8192}) == 8192.0


class TestParseTresGpus:
    def test_tres_string_with_gpu(self):
        assert parse_tres_gpus("cpu=8,mem=32G,node=1,billing=8,gres/gpu=4") == 4

    def test_tres_string_without_gpu(self):
        assert parse_tres_gpus("cpu=4,mem=16G,node=1,billing=4") == 0

    def test_tres_dict_with_gpu(self):
        tres = {
            "allocated": [
                {"type": "cpu", "name": "", "count": 32},
                {"type": "gres", "name": "gpu", "count": 2},
            ]
        }
        assert parse_tres_gpus(tres) == 2

    def test_tres_dict_without_gpu(self):
        tres = {
            "allocated": [
                {"type": "cpu", "name": "", "count": 4},
            ]
        }
        assert parse_tres_gpus(tres) == 0

    def test_empty_string(self):
        assert parse_tres_gpus("") == 0


class TestGetQueue:
    def test_parses_all_jobs(self, mock_slurm):
        jobs = get_queue()
        assert len(jobs) == 4

    def test_running_job_fields(self, mock_slurm):
        jobs = get_queue()
        alice = next(j for j in jobs if j.user == "alice")
        assert alice.job_id == "4349901"
        assert alice.state == "RUNNING"
        assert alice.num_cpus == 4
        assert alice.req_mem_mb == 16384.0
        assert alice.partition == "nextgen"
        assert alice.account == "pas2979"
        assert alice.submit_time is not None
        assert alice.start_time is not None
        assert alice.reason is None

    def test_pending_job_reason(self, mock_slurm):
        jobs = get_queue()
        charlie = next(j for j in jobs if j.user == "charlie")
        assert charlie.state == "PENDING"
        assert charlie.reason == "Resources"
        assert charlie.start_time is None

    def test_gpu_count(self, mock_slurm):
        jobs = get_queue()
        bob = next(j for j in jobs if j.user == "bob")
        assert bob.num_gpus == 2
        charlie = next(j for j in jobs if j.user == "charlie")
        assert charlie.num_gpus == 4

    def test_array_job_id(self, mock_slurm):
        jobs = get_queue()
        dave = next(j for j in jobs if j.user == "dave")
        assert dave.job_id == "4349960_3"

    def test_time_limit(self, mock_slurm):
        jobs = get_queue()
        alice = next(j for j in jobs if j.user == "alice")
        # 2880 minutes = 172800 seconds
        assert alice.time_limit_s == 172800


class TestGetJobHistory:
    def test_parses_all_jobs(self, mock_slurm):
        jobs = get_job_history()
        assert len(jobs) == 4

    def test_completed_job_fields(self, mock_slurm):
        jobs = get_job_history()
        alice = next(j for j in jobs if j.user == "alice")
        assert alice.job_id == "4349800"
        assert alice.state == "COMPLETED"
        assert alice.num_cpus == 4
        assert alice.elapsed_s == 86400
        assert alice.submit_time is not None
        assert alice.start_time is not None
        assert alice.end_time is not None
        assert alice.exit_code == "0:0"

    def test_failed_job_state(self, mock_slurm):
        jobs = get_job_history()
        charlie = next(j for j in jobs if j.user == "charlie")
        assert charlie.state == "FAILED"
        assert charlie.exit_code == "1:0"

    def test_gpu_from_tres(self, mock_slurm):
        jobs = get_job_history()
        bob = next(j for j in jobs if j.user == "bob")
        assert bob.num_gpus == 2

    def test_array_job(self, mock_slurm):
        jobs = get_job_history()
        dave = next(j for j in jobs if j.user == "dave")
        assert dave.job_id == "4349907_1"

    def test_rss_parsed(self, mock_slurm):
        jobs = get_job_history()
        alice = next(j for j in jobs if j.user == "alice")
        # 8589934592 bytes = 8192 MB
        assert alice.max_rss_mb is not None
        assert abs(alice.max_rss_mb - 8192.0) < 1.0


class TestGetClusterInfo:
    def test_returns_cluster_info(self, mock_slurm):
        info = get_cluster_info()
        assert info is not None
        assert info.cluster_name == "ascend"

    def test_partitions_parsed(self, mock_slurm):
        info = get_cluster_info()
        names = {p.name for p in info.partitions}
        assert "nextgen" in names
        assert "quad" in names
        assert "longgpu" in names
        assert "debug-nextgen" in names

    def test_nextgen_aggregated(self, mock_slurm):
        """nextgen appears twice in sinfo (MIXED + IDLE), should be merged."""
        info = get_cluster_info()
        nextgen = next(p for p in info.partitions if p.name == "nextgen")
        # 222 + 26 total nodes from two entries
        assert nextgen.total_nodes == 248
        assert nextgen.idle_nodes == 46  # 20 + 26


class TestGetJobEfficiency:
    def test_parses_seff_output(self, mock_slurm):
        eff = get_job_efficiency("4349800")
        assert eff is not None
        assert eff.job_id == "4349800"
        assert abs(eff.cpu_efficiency_pct - 89.66) < 0.01
        assert abs(eff.mem_efficiency_pct - 50.0) < 0.01
        assert eff.cpu_utilized == "86:06:40"
        assert eff.walltime == "24:00:00"
