"""Worker process for queued jobs."""

from __future__ import annotations

import argparse
import logging
import os
import time
from pathlib import Path

from jobforge.store import JobStore
from jobforge.tasks import execute_task

logger = logging.getLogger("jobforge.worker")


def process_one(store: JobStore, lease_seconds: float = 30) -> bool:
    job = store.claim_next(lease_seconds=lease_seconds)
    if job is None:
        return False

    job_id = job["id"]
    claim_token = job["claim_token"]
    logger.info(
        "running job %s (%s), attempt %s/%s",
        job_id,
        job["task_name"],
        job["attempts"],
        job["max_attempts"],
    )
    try:
        result = execute_task(job["task_name"], job["payload"])
    except ValueError as exc:
        store.mark_failed(job_id, claim_token, f"{type(exc).__name__}: {exc}")
        logger.exception("job %s failed permanently", job_id)
    except Exception as exc:
        state = store.retry_or_fail(job_id, claim_token, f"{type(exc).__name__}: {exc}")
        logger.exception("job %s errored; state=%s", job_id, state)
    else:
        store.mark_succeeded(job_id, claim_token, result)
        logger.info("job %s succeeded", job_id)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the JobForge background worker")
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(os.getenv("JOBFORGE_DB_PATH", "jobforge.db")),
    )
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument("--lease-seconds", type=float, default=30.0)
    parser.add_argument("--once", action="store_true", help="process at most one available job")
    args = parser.parse_args()
    if args.poll_interval <= 0:
        parser.error("--poll-interval must be greater than zero")
    if args.lease_seconds <= 0:
        parser.error("--lease-seconds must be greater than zero")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    store = JobStore(args.db)
    if args.once:
        process_one(store, lease_seconds=args.lease_seconds)
        return

    logger.info("worker started; database=%s lease=%.1fs", args.db, args.lease_seconds)
    try:
        while True:
            if not process_one(store, lease_seconds=args.lease_seconds):
                time.sleep(args.poll_interval)
    except KeyboardInterrupt:
        logger.info("worker stopped")


if __name__ == "__main__":
    main()
