"""Tests for GPU usage analysis from sshare data."""

from __future__ import annotations

import json
import time

from slurmmon_cli.storage.database import Database
from slurmmon_cli.analysis.gpu_usage import (
    top_gpu_users,
    top_cpu_users,
    top_gpu_accounts,
    top_gpu_requesters,
    gpu_usage_by_type,
    usage_delta,
)
from slurmmon_cli.storage.collector import _upsert_jobs, _collect_sshare
from slurmmon_cli.models import Job


def _seed_usage(db: Database, collected_at: float | None = None) -> None:
    """Seed user_usage table with test data."""
    now = collected_at or time.time()
    rows = [
        (now, "pas2979", "alice", 61915687, 0.51, 61915687, 669967,
         json.dumps({"a100": 669967})),
        (now, "pas1186", "bob", 45230100, 0.62, 45230100, 505800,
         json.dumps({"a100": 505800})),
        (now, "pas2979", "charlie", 1200000, 0.89, 1200000, 0, None),
        (now, "pas1186", "dave", 500000, 0.75, 500000, 120000,
         json.dumps({"a100": 80000, "v100": 40000})),
    ]
    db.conn.executemany(
        """INSERT INTO user_usage (
            collected_at, account, user, raw_usage, fairshare,
            cpu_tres_mins, gpu_tres_mins, gpu_type_mins
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    db.conn.commit()


def _seed_gpu_jobs(db: Database) -> None:
    """Seed jobs table with GPU jobs."""
    now = time.time()
    jobs = [
        Job(job_id="1", user="alice", account="pas2979", partition="longgpu",
            state="RUNNING", num_cpus=4, num_gpus=2, req_mem_mb=16384.0,
            submit_time=now - 1000, start_time=now - 500, end_time=None,
            time_limit_s=86400, elapsed_s=500, node_list="a0101",
            exit_code=None, cpu_time_s=None, max_rss_mb=None, reason=None),
        Job(job_id="2", user="alice", account="pas2979", partition="longgpu",
            state="PENDING", num_cpus=8, num_gpus=4, req_mem_mb=32768.0,
            submit_time=now - 500, start_time=None, end_time=None,
            time_limit_s=86400, elapsed_s=0, node_list=None,
            exit_code=None, cpu_time_s=None, max_rss_mb=None, reason="Resources"),
        Job(job_id="3", user="bob", account="pas1186", partition="longgpu",
            state="RUNNING", num_cpus=4, num_gpus=2, req_mem_mb=16384.0,
            submit_time=now - 800, start_time=now - 400, end_time=None,
            time_limit_s=86400, elapsed_s=400, node_list="a0102",
            exit_code=None, cpu_time_s=None, max_rss_mb=None, reason=None),
    ]
    _upsert_jobs(db, jobs, now)


class TestTopGpuUsers:
    def test_ranked_by_gpu_mins(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        _seed_usage(db)
        rows = top_gpu_users(db.conn)
        assert len(rows) == 3  # charlie has 0 GPU, excluded
        assert rows[0]["user"] == "alice"
        assert rows[1]["user"] == "bob"
        db.close()

    def test_top_limit(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        _seed_usage(db)
        rows = top_gpu_users(db.conn, top=1)
        assert len(rows) == 1
        db.close()

    def test_empty_db(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        assert top_gpu_users(db.conn) == []
        db.close()


class TestTopCpuUsers:
    def test_ranked_by_cpu_mins(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        _seed_usage(db)
        rows = top_cpu_users(db.conn)
        assert len(rows) == 4
        assert rows[0]["user"] == "alice"  # highest CPU
        db.close()


class TestTopGpuAccounts:
    def test_aggregated(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        _seed_usage(db)
        rows = top_gpu_accounts(db.conn)
        # pas2979: alice(669967) + charlie(0) = 669967 but charlie filtered (gpu>0)
        # pas1186: bob(505800) + dave(120000) = 625800
        assert len(rows) == 2
        # pas2979 has 669967, pas1186 has 625800
        assert rows[0]["account"] == "pas2979"
        db.close()


class TestTopGpuRequesters:
    def test_from_jobs(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        _seed_gpu_jobs(db)
        rows = top_gpu_requesters(db.conn)
        assert len(rows) == 2
        # alice: 2 running + 4 pending = 6 total
        alice = rows[0]
        assert alice["user"] == "alice"
        assert alice["gpus_running"] == 2
        assert alice["gpus_pending"] == 4
        assert alice["gpus_total"] == 6
        db.close()


class TestGpuUsageByType:
    def test_aggregates_types(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        _seed_usage(db)
        rows = gpu_usage_by_type(db.conn)
        types = {r["gpu_type"]: r["gpu_mins"] for r in rows}
        # a100: alice(669967) + bob(505800) + dave(80000) = 1255767
        assert types["a100"] == 1255767
        # v100: dave(40000)
        assert types["v100"] == 40000
        db.close()


class TestUsageDelta:
    def test_delta_between_snapshots(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        now = time.time()
        # Earlier snapshot (25 hours ago)
        _seed_usage(db, collected_at=now - 90000)
        # Latest snapshot with increased GPU usage for alice
        rows_latest = [
            (now, "pas2979", "alice", 62000000, 0.51, 62000000, 700000,
             json.dumps({"a100": 700000})),
            (now, "pas1186", "bob", 45300000, 0.62, 45300000, 510000,
             json.dumps({"a100": 510000})),
            (now, "pas2979", "charlie", 1200000, 0.89, 1200000, 0, None),
        ]
        db.conn.executemany(
            """INSERT INTO user_usage (
                collected_at, account, user, raw_usage, fairshare,
                cpu_tres_mins, gpu_tres_mins, gpu_type_mins
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            rows_latest,
        )
        db.conn.commit()

        rows = usage_delta(db.conn, hours=24)
        assert len(rows) >= 1
        # alice gained 700000-669967 = 30033 GPU-minutes
        alice = next(r for r in rows if r["user"] == "alice")
        assert alice["gpu_delta"] == 30033
        db.close()

    def test_empty_without_snapshots(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        assert usage_delta(db.conn) == []
        db.close()


class TestCollectSshare:
    def test_collect_stores_rows(self, mock_slurm, tmp_db):
        db = Database(tmp_db)
        db.connect()
        count = _collect_sshare(db, time.time())
        assert count == 3  # alice, bob, charlie (dave has zero usage)
        rows = db.conn.execute("SELECT COUNT(*) FROM user_usage").fetchone()[0]
        assert rows == 3
        db.close()
