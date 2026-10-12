"""SQLite persistence and atomic job state transitions."""

from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

Job = dict[str, Any]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _after(seconds: float, start: datetime | None = None) -> str:
    base = start or datetime.now(timezone.utc)
    return (base + timedelta(seconds=seconds)).isoformat(timespec="microseconds")


def _decode(row: sqlite3.Row | None) -> Job | None:
    if row is None:
        return None
    job = dict(row)
    job["payload"] = json.loads(job["payload"])
    job["result"] = json.loads(job["result"]) if job["result"] is not None else None
    return job


class JobStore:
    def __init__(self, path: str | Path = "jobforge.db") -> None:
        self.path = str(path)
        if self.path == ":memory:":
            raise ValueError("JobStore requires a file-backed SQLite database")
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    def initialize(self) -> None:
        with closing(self._connect()) as conn, conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    task_name TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN (
                        'queued', 'running', 'succeeded', 'failed'
                    )),
                    result TEXT,
                    error TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    available_at TEXT NOT NULL DEFAULT '',
                    lease_expires_at TEXT,
                    claim_token TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT
                )
                """
            )
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(jobs)")}
            migrations = {
                "max_attempts": "INTEGER NOT NULL DEFAULT 3",
                "available_at": "TEXT NOT NULL DEFAULT ''",
                "lease_expires_at": "TEXT",
                "claim_token": "TEXT",
            }
            for name, definition in migrations.items():
                if name not in columns:
                    conn.execute(f"ALTER TABLE jobs ADD COLUMN {name} {definition}")
            conn.execute("UPDATE jobs SET available_at = created_at WHERE available_at = ''")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS jobs_status_created_idx "
                "ON jobs(status, created_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS jobs_queue_idx "
                "ON jobs(status, available_at, created_at)"
            )

    def create_job(
        self,
        task_name: str,
        payload: dict[str, Any],
        max_attempts: int = 3,
    ) -> Job:
        if not 1 <= max_attempts <= 10:
            raise ValueError("max_attempts must be between 1 and 10")
        job_id = str(uuid.uuid4())
        now = _now()
        with closing(self._connect()) as conn, conn:
            conn.execute(
                """INSERT INTO jobs
                   (id, task_name, payload, status, attempts, max_attempts,
                    available_at, created_at, updated_at)
                   VALUES (?, ?, ?, 'queued', 0, ?, ?, ?, ?)""",
                (job_id, task_name, json.dumps(payload), max_attempts, now, now, now),
            )
        job = self.get_job(job_id)
        assert job is not None
        return job

    def get_job(self, job_id: str) -> Job | None:
        with closing(self._connect()) as conn, conn:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return _decode(row)

    def list_jobs(self, status: str | None = None, limit: int = 20) -> list[Job]:
        with closing(self._connect()) as conn, conn:
            if status is None:
                rows = conn.execute(
                    "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM jobs WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                    (status, limit),
                ).fetchall()
        return [_decode(row) for row in rows if row is not None]

    def claim_next(self, lease_seconds: float = 30, retry_base_seconds: float = 1) -> Job | None:
        """Recover expired leases, then atomically claim one available job."""
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than zero")
        if retry_base_seconds < 0:
            raise ValueError("retry_base_seconds must not be negative")

        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat(timespec="microseconds")
        with closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            expired = conn.execute(
                """SELECT id, attempts, max_attempts FROM jobs
                   WHERE status = 'running' AND lease_expires_at <= ?""",
                (now,),
            ).fetchall()
            for row in expired:
                if row["attempts"] >= row["max_attempts"]:
                    conn.execute(
                        """UPDATE jobs SET status = 'failed',
                           error = 'worker lease expired after maximum attempts',
                           updated_at = ?, finished_at = ?, lease_expires_at = NULL,
                           claim_token = NULL WHERE id = ? AND status = 'running'""",
                        (now, now, row["id"]),
                    )
                else:
                    delay = min(retry_base_seconds * (2 ** max(row["attempts"] - 1, 0)), 60)
                    conn.execute(
                        """UPDATE jobs SET status = 'queued',
                           error = 'worker lease expired; retry scheduled',
                           available_at = ?, updated_at = ?, finished_at = NULL,
                           lease_expires_at = NULL, claim_token = NULL
                           WHERE id = ? AND status = 'running'""",
                        (_after(delay, now_dt), now, row["id"]),
                    )

            row = conn.execute(
                """SELECT id FROM jobs
                   WHERE status = 'queued' AND available_at <= ? AND attempts < max_attempts
                   ORDER BY available_at ASC, created_at ASC LIMIT 1""",
                (now,),
            ).fetchone()
            if row is None:
                return None

            token = str(uuid.uuid4())
            lease_expires_at = _after(lease_seconds, now_dt)
            conn.execute(
                """UPDATE jobs SET status = 'running', attempts = attempts + 1,
                   started_at = ?, updated_at = ?, lease_expires_at = ?, claim_token = ?
                   WHERE id = ? AND status = 'queued'""",
                (now, now, lease_expires_at, token, row["id"]),
            )
            claimed = conn.execute("SELECT * FROM jobs WHERE id = ?", (row["id"],)).fetchone()
            return _decode(claimed)

    def mark_succeeded(self, job_id: str, claim_token: str, result: dict[str, Any]) -> None:
        now = _now()
        with closing(self._connect()) as conn, conn:
            cursor = conn.execute(
                """UPDATE jobs SET status = 'succeeded', result = ?, error = NULL,
                   updated_at = ?, finished_at = ?, lease_expires_at = NULL, claim_token = NULL
                   WHERE id = ? AND status = 'running' AND claim_token = ?
                     AND lease_expires_at > ?""",
                (json.dumps(result), now, now, job_id, claim_token, now),
            )
            if cursor.rowcount != 1:
                raise ValueError(f"job {job_id} is not owned by this worker or its lease expired")

    def mark_failed(self, job_id: str, claim_token: str, error: str) -> None:
        now = _now()
        with closing(self._connect()) as conn, conn:
            cursor = conn.execute(
                """UPDATE jobs SET status = 'failed', error = ?, updated_at = ?,
                   finished_at = ?, lease_expires_at = NULL, claim_token = NULL
                   WHERE id = ? AND status = 'running' AND claim_token = ?
                     AND lease_expires_at > ?""",
                (error[:1000], now, now, job_id, claim_token, now),
            )
            if cursor.rowcount != 1:
                raise ValueError(f"job {job_id} is not owned by this worker or its lease expired")

    def retry_or_fail(
        self,
        job_id: str,
        claim_token: str,
        error: str,
        retry_delay_seconds: float | None = None,
    ) -> str:
        """Schedule another attempt, or mark the job failed when attempts are exhausted."""
        if retry_delay_seconds is not None and retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must not be negative")
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat(timespec="microseconds")
        with closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT attempts, max_attempts FROM jobs
                   WHERE id = ? AND status = 'running' AND claim_token = ?
                     AND lease_expires_at > ?""",
                (job_id, claim_token, now),
            ).fetchone()
            if row is None:
                raise ValueError(f"job {job_id} is not owned by this worker or its lease expired")

            if row["attempts"] >= row["max_attempts"]:
                conn.execute(
                    """UPDATE jobs SET status = 'failed', error = ?, updated_at = ?,
                       finished_at = ?, lease_expires_at = NULL, claim_token = NULL
                       WHERE id = ?""",
                    (error[:1000], now, now, job_id),
                )
                return "failed"

            delay = retry_delay_seconds
            if delay is None:
                delay = min(2 ** max(row["attempts"] - 1, 0), 60)
            conn.execute(
                """UPDATE jobs SET status = 'queued', error = ?, available_at = ?,
                   updated_at = ?, finished_at = NULL, lease_expires_at = NULL,
                   claim_token = NULL WHERE id = ?""",
                (error[:1000], _after(delay, now_dt), now, job_id),
            )
            return "queued"
