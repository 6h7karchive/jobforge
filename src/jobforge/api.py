"""HTTP API for submitting and inspecting jobs."""

import os
from enum import StrEnum
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from jobforge.store import JobStore
from jobforge.tasks import validate_task


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class JobRequest(BaseModel):
    task_name: str
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = Field(default=3, ge=1, le=10)


class JobResponse(BaseModel):
    id: str
    task_name: str
    payload: dict[str, Any]
    status: JobStatus
    result: dict[str, Any] | None
    error: str | None
    attempts: int
    max_attempts: int
    available_at: str
    lease_expires_at: str | None
    created_at: str
    updated_at: str
    started_at: str | None
    finished_at: str | None


def create_app(store: JobStore | None = None) -> FastAPI:
    app = FastAPI(
        title="JobForge",
        version="0.1.0",
        description="A persistent SQLite-backed job queue",
    )
    app.state.store = store or JobStore(os.getenv("JOBFORGE_DB_PATH", "jobforge.db"))

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/jobs", response_model=JobResponse, status_code=status.HTTP_202_ACCEPTED)
    def submit_job(body: JobRequest, request: Request) -> dict[str, Any]:
        try:
            validate_task(body.task_name, body.payload)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return request.app.state.store.create_job(
            body.task_name, body.payload, max_attempts=body.max_attempts
        )

    @app.get("/jobs", response_model=list[JobResponse])
    def list_jobs(
        request: Request,
        job_status: JobStatus | None = Query(default=None, alias="status"),
        limit: int = Query(default=20, ge=1, le=100),
    ) -> list[dict[str, Any]]:
        value = job_status.value if job_status is not None else None
        return request.app.state.store.list_jobs(status=value, limit=limit)

    @app.get("/jobs/{job_id}", response_model=JobResponse)
    def get_job(job_id: str, request: Request) -> dict[str, Any]:
        job = request.app.state.store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job

    return app


app = create_app()
