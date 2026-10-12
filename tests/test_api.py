from fastapi.testclient import TestClient

from jobforge.api import create_app
from jobforge.store import JobStore


def client_for(tmp_path):
    return TestClient(create_app(JobStore(tmp_path / "api.db")))


def test_health_endpoint(tmp_path):
    response = client_for(tmp_path).get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_submit_and_fetch_job(tmp_path):
    client = client_for(tmp_path)
    response = client.post("/jobs", json={"task_name": "echo", "payload": {"message": "hi"}})
    assert response.status_code == 202
    created = response.json()
    assert created["status"] == "queued"
    assert created["payload"] == {"message": "hi"}
    assert created["max_attempts"] == 3
    assert created["available_at"]

    fetched = client.get(f"/jobs/{created['id']}")
    assert fetched.status_code == 200
    assert fetched.json()["id"] == created["id"]


def test_unknown_task_is_rejected(tmp_path):
    response = client_for(tmp_path).post("/jobs", json={"task_name": "shell", "payload": {}})
    assert response.status_code == 422


def test_invalid_payload_is_rejected(tmp_path):
    response = client_for(tmp_path).post("/jobs", json={"task_name": "echo", "payload": {}})
    assert response.status_code == 422


def test_list_jobs_can_filter_by_status(tmp_path):
    client = client_for(tmp_path)
    client.post("/jobs", json={"task_name": "echo", "payload": {"message": "hi"}})
    response = client.get("/jobs", params={"status": "queued"})
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert client.get("/jobs", params={"status": "succeeded"}).json() == []


def test_missing_job_returns_404(tmp_path):
    response = client_for(tmp_path).get("/jobs/not-a-real-id")
    assert response.status_code == 404


def test_submit_job_can_set_retry_limit(tmp_path):
    client = client_for(tmp_path)
    response = client.post(
        "/jobs",
        json={"task_name": "echo", "payload": {"message": "hi"}, "max_attempts": 5},
    )
    assert response.status_code == 202
    assert response.json()["max_attempts"] == 5


def test_retry_limit_is_bounded(tmp_path):
    response = client_for(tmp_path).post(
        "/jobs",
        json={"task_name": "echo", "payload": {"message": "hi"}, "max_attempts": 11},
    )
    assert response.status_code == 422
