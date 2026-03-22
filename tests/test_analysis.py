"""Tests for analysis modules against a seeded database."""

from __future__ import annotations

import time

from slurmmon_cli.models import Job
from slurmmon_cli.storage.database import Database
from slurmmon_cli.storage.collector import _upsert_jobs
from slurmmon_cli.analysis.users import user_summary, user_jobs
from slurmmon_cli.analysis.queue_time import wait_time_stats, wait_time_by_hour, wait_time_by_size
from slurmmon_cli.analysis.efficiency import job_efficiency, efficiency_summary, low_efficiency_jobs


def _make_job(**kw) -> Job:
    defaults = dict(
        job_id="1", user="alice", account="acc1", partition="nextgen",
        state="COMPLETED", num_cpus=4, num_gpus=0, req_mem_mb=16384.0,
        submit_time=1700000000.0, start_time=1700000300.0, end_time=1700086700.0,
        time_limit_s=172800, elapsed_s=86400, node_list="a0101",
        exit_code="0:0", cpu_time_s=310000.0, max_rss_mb=8192.0, reason=None,
    )
    defaults.update(kw)
    return Job(**defaults)


def _seed_db(db: Database) -> None:
    """Seed DB with representative jobs for testing."""
    now = time.time()
    jobs = [
        _make_job(job_id="1", user="alice", account="acc1", state="COMPLETED",
                  num_cpus=4, elapsed_s=86400, cpu_time_s=310000.0,
                  max_rss_mb=8192.0, req_mem_mb=16384.0,
                  submit_time=now - 90000, start_time=now - 89700),
        _make_job(job_id="2", user="alice", account="acc1", state="COMPLETED",
                  num_cpus=8, elapsed_s=3600, cpu_time_s=14400.0,
                  max_rss_mb=4096.0, req_mem_mb=32768.0,
                  submit_time=now - 50000, start_time=now - 49000),
        _make_job(job_id="3", user="bob", account="acc2", state="COMPLETED",
                  partition="quad", num_cpus=16, elapsed_s=7200,
                  cpu_time_s=57600.0, max_rss_mb=32768.0, req_mem_mb=65536.0,
                  submit_time=now - 40000, start_time=now - 38000),
        _make_job(job_id="4", user="bob", account="acc2", state="FAILED",
                  partition="quad", num_cpus=2, elapsed_s=600,
                  cpu_time_s=100.0, max_rss_mb=512.0, req_mem_mb=4096.0,
                  submit_time=now - 30000, start_time=now - 29500),
        _make_job(job_id="5", user="alice", account="acc1", state="RUNNING",
                  num_cpus=4, elapsed_s=500, cpu_time_s=None,
                  max_rss_mb=None, end_time=None,
                  submit_time=now - 1000, start_time=now - 500),
        _make_job(job_id="6", user="charlie", account="acc1", state="PENDING",
                  num_cpus=8, elapsed_s=0, cpu_time_s=None,
                  max_rss_mb=None, start_time=None, end_time=None,
                  submit_time=now - 500, reason="Resources"),
    ]
    _upsert_jobs(db, jobs, now)


class TestUserSummary:
    def test_returns_all_users(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        _seed_db(db)
        rows = user_summary(db.conn, top=10)
        users = {r["user"] for r in rows}
        assert "alice" in users
        assert "bob" in users
        assert "charlie" in users
        db.close()

    def test_counts_correct(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        _seed_db(db)
        rows = user_summary(db.conn, user="alice")
        assert len(rows) == 1
        r = rows[0]
        assert r["completed"] == 2
        assert r["running"] == 1
        assert r["total_jobs"] == 3
        db.close()

    def test_sort_by_cpus(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        _seed_db(db)
        rows = user_summary(db.conn, sort="cpus", top=10)
        # Alice has more CPU-hours than bob
        assert rows[0]["user"] == "alice"
        db.close()


class TestUserJobs:
    def test_filter_by_user(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        _seed_db(db)
        rows = user_jobs(db.conn, "bob")
        assert all(r["user"] == "bob" for r in rows)
        assert len(rows) == 2
        db.close()


class TestWaitTimeStats:
    def test_basic_stats(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        _seed_db(db)
        stats = wait_time_stats(db.conn)
        assert stats["count"] > 0
        assert stats["mean"] >= 0
        assert stats["median"] >= 0
        assert stats["p90"] >= stats["median"]
        db.close()

    def test_empty_db(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        stats = wait_time_stats(db.conn)
        assert stats["count"] == 0
        db.close()


class TestWaitTimeByHour:
    def test_returns_rows(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        _seed_db(db)
        rows = wait_time_by_hour(db.conn)
        assert len(rows) > 0
        assert all("hour" in r and "avg_wait" in r for r in rows)
        db.close()


class TestWaitTimeBySize:
    def test_returns_rows(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        _seed_db(db)
        rows = wait_time_by_size(db.conn)
        assert len(rows) > 0
        assert all("cpu_range" in r for r in rows)
        db.close()


class TestJobEfficiency:
    def test_single_job(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        _seed_db(db)
        d = job_efficiency(db.conn, "1")
        assert d is not None
        assert d["cpu_eff_pct"] is not None
        assert d["mem_eff_pct"] is not None
        # CPU eff: 310000 / (4 * 86400) = 89.7%
        assert abs(d["cpu_eff_pct"] - 89.7) < 1.0
        # Mem eff: 8192 / 16384 = 50%
        assert abs(d["mem_eff_pct"] - 50.0) < 0.1
        db.close()

    def test_missing_job(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        _seed_db(db)
        assert job_efficiency(db.conn, "nonexistent") is None
        db.close()


class TestEfficiencySummary:
    def test_summary(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        _seed_db(db)
        s = efficiency_summary(db.conn)
        assert s["total_jobs"] > 0
        assert s.get("avg_cpu_eff") is not None
        db.close()


class TestLowEfficiencyJobs:
    def test_finds_low_efficiency(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        _seed_db(db)
        rows = low_efficiency_jobs(db.conn, threshold_pct=90.0)
        # Job 4 (bob, FAILED) has very low CPU eff: 100/(2*600) = 8.3%
        ids = {r["job_id"] for r in rows}
        assert "4" in ids
        db.close()
