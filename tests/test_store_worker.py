import hashlib
import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from jobforge.store import JobStore
from jobforge.worker import process_one


def test_worker_completes_echo_job(tmp_path):
    store = JobStore(tmp_path / "jobs.db")
    job = store.create_job("echo", {"message": "hello"})

    assert process_one(store) is True
    updated = store.get_job(job["id"])
    assert updated["status"] == "succeeded"
    assert updated["result"] == {"message": "hello"}
    assert updated["attempts"] == 1
    assert updated["started_at"] is not None
    assert updated["finished_at"] is not None


def test_worker_completes_sha256_job(tmp_path):
    store = JobStore(tmp_path / "jobs.db")
    store.create_job("sha256", {"text": "hello"})

    process_one(store)
    job = store.list_jobs(status="succeeded")[0]
    assert job["result"] == {"sha256": hashlib.sha256(b"hello").hexdigest()}


def test_empty_queue_does_not_claim_a_job(tmp_path):
    store = JobStore(tmp_path / "jobs.db")
    assert process_one(store) is False


def test_claiming_a_job_removes_it_from_queue(tmp_path):
    store = JobStore(tmp_path / "jobs.db")
    job = store.create_job("echo", {"message": "once"})

    claimed = store.claim_next()
    assert claimed["id"] == job["id"]
    assert claimed["status"] == "running"
    assert store.claim_next() is None


def test_worker_marks_unknown_task_failed(tmp_path):
    store = JobStore(tmp_path / "jobs.db")
    store.create_job("not-registered", {})

    assert process_one(store) is True
    job = store.list_jobs(status="failed")[0]
    assert job["error"].startswith("ValueError: unknown task")


def test_two_workers_cannot_claim_the_same_job(tmp_path):
    db_path = tmp_path / "jobs.db"
    store_a = JobStore(db_path)
    store_b = JobStore(db_path)
    job = store_a.create_job("echo", {"message": "one claim only"})

    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(lambda store: store.claim_next(), [store_a, store_b]))

    claimed = [claim for claim in claims if claim is not None]
    assert len(claimed) == 1
    assert claimed[0]["id"] == job["id"]


def test_store_requires_a_file_backed_database():
    with pytest.raises(ValueError, match="file-backed"):
        JobStore(":memory:")


def test_failed_attempt_is_requeued_and_can_succeed(tmp_path):
    store = JobStore(tmp_path / "jobs.db")
    job = store.create_job("echo", {"message": "retry me"}, max_attempts=2)
    claimed = store.claim_next()

    state = store.retry_or_fail(
        job["id"], claimed["claim_token"], "RuntimeError: temporary", retry_delay_seconds=0
    )
    assert state == "queued"
    assert store.get_job(job["id"])["error"] == "RuntimeError: temporary"

    assert process_one(store) is True
    updated = store.get_job(job["id"])
    assert updated["status"] == "succeeded"
    assert updated["attempts"] == 2
    assert updated["error"] is None


def test_retry_exhaustion_marks_job_failed(tmp_path):
    store = JobStore(tmp_path / "jobs.db")
    job = store.create_job("echo", {"message": "fail"}, max_attempts=1)
    claimed = store.claim_next()

    state = store.retry_or_fail(job["id"], claimed["claim_token"], "boom")

    updated = store.get_job(job["id"])
    assert state == "failed"
    assert updated["status"] == "failed"
    assert updated["error"] == "boom"
    assert updated["finished_at"] is not None


def test_expired_lease_is_reclaimed_with_a_new_token(tmp_path):
    store = JobStore(tmp_path / "jobs.db")
    job = store.create_job("echo", {"message": "recover me"}, max_attempts=2)
    first_claim = store.claim_next(lease_seconds=0.01)
    time.sleep(0.03)

    second_claim = store.claim_next(lease_seconds=30, retry_base_seconds=0)

    assert second_claim["id"] == job["id"]
    assert second_claim["attempts"] == 2
    assert second_claim["claim_token"] != first_claim["claim_token"]
    with pytest.raises(ValueError, match="lease expired"):
        store.mark_succeeded(job["id"], first_claim["claim_token"], {"message": "stale"})


def test_expired_lease_on_last_attempt_fails_job(tmp_path):
    store = JobStore(tmp_path / "jobs.db")
    job = store.create_job("echo", {"message": "do not retry"}, max_attempts=1)
    store.claim_next(lease_seconds=0.01)
    time.sleep(0.03)

    assert store.claim_next() is None
    updated = store.get_job(job["id"])
    assert updated["status"] == "failed"
    assert updated["error"] == "worker lease expired after maximum attempts"


def test_existing_database_is_migrated(tmp_path):
    path = tmp_path / "legacy.db"
    created = "2026-10-11T10:00:00.000000+00:00"
    with sqlite3.connect(path) as conn:
        conn.execute(
            """CREATE TABLE jobs (
                id TEXT PRIMARY KEY, task_name TEXT NOT NULL, payload TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
                result TEXT, error TEXT, attempts INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                started_at TEXT, finished_at TEXT
            )"""
        )
        conn.execute(
            "INSERT INTO jobs (id, task_name, payload, status, created_at, updated_at) "
            "VALUES (?, ?, ?, 'queued', ?, ?)",
            ("legacy-job", "echo", json.dumps({"message": "old"}), created, created),
        )

    store = JobStore(path)
    migrated = store.get_job("legacy-job")
    assert migrated["max_attempts"] == 3
    assert migrated["available_at"] == created
    assert migrated["lease_expires_at"] is None


def test_worker_retries_transient_exception(monkeypatch, tmp_path):
    import jobforge.worker as worker

    store = JobStore(tmp_path / "jobs.db")
    job = store.create_job("echo", {"message": "retry"}, max_attempts=2)
    original_retry = store.retry_or_fail
    monkeypatch.setattr(
        store,
        "retry_or_fail",
        lambda job_id, token, error: original_retry(
            job_id, token, error, retry_delay_seconds=0
        ),
    )
    calls = 0

    def flaky_task(task_name, payload):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary failure")
        return {"message": payload["message"]}

    monkeypatch.setattr(worker, "execute_task", flaky_task)
    assert worker.process_one(store) is True
    assert store.get_job(job["id"])["status"] == "queued"
    assert worker.process_one(store) is True
    updated = store.get_job(job["id"])
    assert updated["status"] == "succeeded"
    assert updated["attempts"] == 2
