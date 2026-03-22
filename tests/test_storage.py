"""Tests for storage layer - schema, database, collector."""

from __future__ import annotations

import time

from slurmmon_cli.storage.database import Database
from slurmmon_cli.storage.schema import ensure_schema
from slurmmon_cli.storage.collector import (
    _upsert_jobs,
    _insert_snapshot,
    _update_partitions,
    collect_snapshot,
    prune_old_jobs,
)
from slurmmon_cli.models import ClusterInfo, Job, PartitionInfo


def _make_job(**overrides) -> Job:
    defaults = dict(
        job_id="100", user="testuser", account="acc1", partition="batch",
        state="RUNNING", num_cpus=4, num_gpus=0, req_mem_mb=4096.0,
        submit_time=1700000000.0, start_time=1700000100.0, end_time=None,
        time_limit_s=3600, elapsed_s=500, node_list="node01",
        exit_code=None, cpu_time_s=None, max_rss_mb=None, reason=None,
    )
    defaults.update(overrides)
    return Job(**defaults)


def _make_cluster_info() -> ClusterInfo:
    return ClusterInfo(
        cluster_name="test",
        partitions=[
            PartitionInfo(
                name="batch", state="UP", total_nodes=10, idle_nodes=3,
                alloc_nodes=6, other_nodes=1, total_cpus=80, avail_cpus=24,
                max_time="7-00:00:00",
            ),
        ],
        total_nodes=10, idle_nodes=3, alloc_nodes=6, down_nodes=0,
        mixed_nodes=1, total_cpus=80, alloc_cpus=56,
    )


class TestSchema:
    def test_creates_tables(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        tables = [
            r[0]
            for r in db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        ]
        assert "jobs" in tables
        assert "snapshots" in tables
        assert "partitions" in tables
        assert "metadata" in tables
        db.close()

    def test_schema_version(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        row = db.conn.execute(
            "SELECT value FROM metadata WHERE key='schema_version'"
        ).fetchone()
        assert row is not None
        assert row[0] == "1"
        db.close()

    def test_idempotent(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        ensure_schema(db.conn)  # second call should not fail
        db.close()


class TestUpsertJobs:
    def test_insert(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        job = _make_job()
        count = _upsert_jobs(db, [job], time.time())
        assert count == 1
        row = db.conn.execute("SELECT * FROM jobs WHERE job_id='100'").fetchone()
        assert row["user"] == "testuser"
        assert row["state"] == "RUNNING"
        db.close()

    def test_upsert_updates_state(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        now = time.time()
        _upsert_jobs(db, [_make_job(state="RUNNING")], now)
        _upsert_jobs(db, [_make_job(state="COMPLETED", end_time=now + 100)], now + 1)
        row = db.conn.execute("SELECT state, end_time FROM jobs WHERE job_id='100'").fetchone()
        assert row["state"] == "COMPLETED"
        assert row["end_time"] is not None
        db.close()

    def test_upsert_preserves_fields(self, tmp_db):
        """COALESCE should keep existing non-null values when new values are null."""
        db = Database(tmp_db)
        db.connect()
        now = time.time()
        _upsert_jobs(db, [_make_job(cpu_time_s=1000.0, max_rss_mb=2048.0)], now)
        _upsert_jobs(db, [_make_job(cpu_time_s=None, max_rss_mb=None)], now + 1)
        row = db.conn.execute("SELECT cpu_time_s, max_rss_mb FROM jobs WHERE job_id='100'").fetchone()
        assert row["cpu_time_s"] == 1000.0
        assert row["max_rss_mb"] == 2048.0
        db.close()


class TestInsertSnapshot:
    def test_insert(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        info = _make_cluster_info()
        _insert_snapshot(db, info, time.time(), running=5, pending=10)
        row = db.conn.execute("SELECT * FROM snapshots ORDER BY id DESC LIMIT 1").fetchone()
        assert row["running_jobs"] == 5
        assert row["pending_jobs"] == 10
        assert row["total_nodes"] == 10
        db.close()


class TestUpdatePartitions:
    def test_insert_and_update(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        info = _make_cluster_info()
        now = time.time()
        _update_partitions(db, info, now)
        row = db.conn.execute("SELECT * FROM partitions WHERE name='batch'").fetchone()
        assert row["state"] == "UP"
        assert row["total_nodes"] == 10

        # Update
        info.partitions[0] = PartitionInfo(
            name="batch", state="UP", total_nodes=12, idle_nodes=5,
            alloc_nodes=6, other_nodes=1, total_cpus=96, avail_cpus=40,
            max_time="7-00:00:00",
        )
        _update_partitions(db, info, now + 10)
        row = db.conn.execute("SELECT * FROM partitions WHERE name='batch'").fetchone()
        assert row["total_nodes"] == 12
        db.close()


class TestPrune:
    def test_prune_old_jobs(self, tmp_db):
        db = Database(tmp_db)
        db.connect()
        old_time = time.time() - 40 * 86400  # 40 days ago
        recent_time = time.time()
        _upsert_jobs(db, [_make_job(job_id="old1")], old_time)
        _upsert_jobs(db, [_make_job(job_id="recent1")], recent_time)
        pruned = prune_old_jobs(db, retention_days=30)
        assert pruned == 1
        remaining = db.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        assert remaining == 1
        db.close()


class TestCollectSnapshot:
    def test_full_cycle(self, mock_slurm, tmp_db):
        db = Database(tmp_db)
        db.connect()
        stats = collect_snapshot(db)
        assert stats["queue_jobs"] == 4
        assert stats["history_jobs"] == 4
        # Check jobs in DB
        job_count = db.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        assert job_count > 0
        # Check snapshot
        snap_count = db.conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        assert snap_count == 1
        # Check partitions
        part_count = db.conn.execute("SELECT COUNT(*) FROM partitions").fetchone()[0]
        assert part_count > 0
        db.close()
