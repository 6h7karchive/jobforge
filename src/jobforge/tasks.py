"""Built-in tasks. Only registered tasks can be submitted to the queue."""

import hashlib
from collections.abc import Callable
from typing import Any

Task = Callable[[dict[str, Any]], dict[str, Any]]


def _echo(payload: dict[str, Any]) -> dict[str, Any]:
    return {"message": payload["message"]}


def _sha256(payload: dict[str, Any]) -> dict[str, Any]:
    digest = hashlib.sha256(payload["text"].encode("utf-8")).hexdigest()
    return {"sha256": digest}


TASKS: dict[str, tuple[set[str], Task]] = {
    "echo": ({"message"}, _echo),
    "sha256": ({"text"}, _sha256),
}


def validate_task(task_name: str, payload: dict[str, Any]) -> None:
    task = TASKS.get(task_name)
    if task is None:
        raise ValueError(f"unknown task: {task_name}")

    required_keys, _ = task
    if set(payload) != required_keys:
        expected = ", ".join(sorted(required_keys))
        raise ValueError(f"task '{task_name}' requires exactly these fields: {expected}")

    value = payload[next(iter(required_keys))]
    if not isinstance(value, str):
        raise ValueError(f"task '{task_name}' fields must be strings")
    if len(value) > 10_000:
        raise ValueError("string inputs must be 10000 characters or fewer")


def execute_task(task_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    validate_task(task_name, payload)
    _, task = TASKS[task_name]
    return task(payload)
