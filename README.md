# JobForge

A persistent background-job queue built with Python, FastAPI and SQLite. Jobs are submitted over a REST API and executed by a separate worker process.

## Features
- REST API for submitting and inspecting jobs
- SQLite persistence
- Allowlisted task types (no arbitrary shell execution)
- Retry policy with exponential backoff
- Worker leases and claim tokens for recovery after worker crashes
- Pytest suite and GitHub Actions CI

## Requirements
Python 3.11+

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```
On Windows PowerShell: `.venv\Scripts\Activate.ps1`.

## Run
Start the API:
```bash
uvicorn jobforge.api:app --reload
```
In a second terminal start a worker:
```bash
python -m jobforge.worker --lease-seconds 30
```
Open http://127.0.0.1:8000/docs for the interactive API documentation.

## Tests
```bash
pytest
ruff check .
```

## Safety and delivery semantics
JobForge only supports registered task types such as `echo` and `sha256`; submitted jobs cannot execute arbitrary shell commands. Leases allow stale jobs to be reclaimed after a worker disappears, but they cannot prevent duplicate external side effects after a crash. Tasks with external side effects should be idempotent.

## License
MIT. See [LICENSE](LICENSE).
