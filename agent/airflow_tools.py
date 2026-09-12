import json
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
from datetime import datetime, timezone
import time


AIRFLOW_API_URL = os.getenv(
    "AIRFLOW_API_URL",
    "http://localhost:8080",
).rstrip("/")


def _get_token() -> str:
    """
    Get an Airflow JWT token.

    Priority:
    1. Use AIRFLOW_TOKEN if already provided.
    2. Otherwise log in using AIRFLOW_USERNAME / AIRFLOW_PASSWORD.
    """

    existing_token = os.getenv("AIRFLOW_TOKEN")

    if existing_token:
        return existing_token

    username = os.getenv("AIRFLOW_USERNAME")
    password = os.getenv("AIRFLOW_PASSWORD")

    if not username or not password:
        raise RuntimeError(
            "Airflow credentials are missing. "
            "Set AIRFLOW_TOKEN, or set both "
            "AIRFLOW_USERNAME and AIRFLOW_PASSWORD."
        )

    payload = json.dumps(
        {
            "username": username,
            "password": password,
        }
    ).encode("utf-8")

    request = Request(
        f"{AIRFLOW_API_URL}/auth/token",
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/json",
        },
    )

    try:
        with urlopen(request) as response:
            data = json.loads(response.read().decode("utf-8"))

    except HTTPError as exc:
        body = exc.read().decode("utf-8")
        raise RuntimeError(
            f"Airflow authentication failed: "
            f"HTTP {exc.code}: {body}"
        ) from exc

    return data["access_token"]


def _get_json(path: str) -> dict[str, Any]:
    """
    Send an authenticated GET request to Airflow.
    """

    token = _get_token()

    request = Request(
        f"{AIRFLOW_API_URL}{path}",
        method="GET",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
    )

    try:
        with urlopen(request) as response:
            return json.loads(
                response.read().decode("utf-8")
            )

    except HTTPError as exc:
        body = exc.read().decode("utf-8")

        raise RuntimeError(
            f"Airflow API request failed: "
            f"HTTP {exc.code}: {body}"
        ) from exc

    except URLError as exc:
        raise RuntimeError(
            f"Could not connect to Airflow at "
            f"{AIRFLOW_API_URL}: {exc.reason}"
        ) from exc



def _post_json(
    path: str,
    payload: dict | None = None,
) -> dict[str, Any]:
    """
    Send an authenticated POST request to Airflow.
    """

    token = _get_token()

    body = json.dumps(
        payload or {}
    ).encode("utf-8")

    request = Request(
        f"{AIRFLOW_API_URL}{path}",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )

    try:
        with urlopen(request) as response:
            return json.loads(
                response.read().decode("utf-8")
            )

    except HTTPError as exc:
        body = exc.read().decode("utf-8")

        raise RuntimeError(
            f"Airflow API request failed: "
            f"HTTP {exc.code}: {body}"
        ) from exc



def trigger_pipeline(
    dag_id: str,
) -> dict[str, Any]:
    """
    Trigger a new DAG run.

    Used by the evaluation harness,
    not exposed to the LLM agent.
    """

    encoded_dag_id = quote(
        dag_id,
        safe="",
    )

    logical_date = datetime.now(
        timezone.utc
    ).isoformat()

    result = _post_json(
        f"/api/v2/dags/{encoded_dag_id}/dagRuns",
        {
            "logical_date": logical_date,
        },
    )

    return {
        "dag_id": result.get("dag_id"),
        "dag_run_id": result.get("dag_run_id"),
        "state": result.get("state"),
        "logical_date": result.get("logical_date"),
        "start_date": result.get("start_date"),
    }


def get_pipeline_run(
    dag_id: str,
    dag_run_id: str,
) -> dict[str, Any]:
    """
    Get the current state of one specific DAG run.
    """

    encoded_dag_id = quote(
        dag_id,
        safe="",
    )

    encoded_run_id = quote(
        dag_run_id,
        safe="",
    )

    result = _get_json(
        f"/api/v2/dags/{encoded_dag_id}"
        f"/dagRuns/{encoded_run_id}"
    )

    return {
        "dag_id": result.get("dag_id"),
        "dag_run_id": result.get("dag_run_id"),
        "state": result.get("state"),
        "start_date": result.get("start_date"),
        "end_date": result.get("end_date"),
        "duration": result.get("duration"),
    }


def wait_for_pipeline(
    dag_id: str,
    dag_run_id: str,
    timeout_seconds: int = 60,
    poll_interval: int = 2,
) -> dict[str, Any]:
    """
    Wait until a DAG run reaches a terminal state.

    Terminal states:
    - success
    - failed

    Raises TimeoutError if the run does not finish
    within timeout_seconds.
    """

    terminal_states = {
        "success",
        "failed",
    }

    start_time = time.time()

    while True:

        run = get_pipeline_run(
            dag_id,
            dag_run_id,
        )

        state = run["state"]

        print(
            f"Pipeline state: {state}"
        )

        if state in terminal_states:
            return run

        elapsed = (
            time.time() - start_time
        )

        if elapsed >= timeout_seconds:
            raise TimeoutError(
                f"DAG run {dag_run_id} did not finish "
                f"within {timeout_seconds} seconds."
            )

        time.sleep(
            poll_interval
        )


def get_pipeline_runs(
    dag_id: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """
    Return recent runs for a DAG.

    The result is deliberately compact so it can later
    be passed to an LLM agent without unnecessary fields.
    """

    encoded_dag_id = quote(dag_id, safe="")

    data = _get_json(
        f"/api/v2/dags/{encoded_dag_id}/dagRuns"
    )

    runs = data.get("dag_runs", [])

    # Newest first.
    runs.sort(
        key=lambda run: run.get("start_date") or "",
        reverse=True,
    )

    return [
        {
            "dag_id": run.get("dag_id"),
            "dag_run_id": run.get("dag_run_id"),
            "state": run.get("state"),
            "run_type": run.get("run_type"),
            "start_date": run.get("start_date"),
            "end_date": run.get("end_date"),
            "duration": run.get("duration"),
        }
        for run in runs[:limit]
    ]


def get_task_instances(
    dag_id: str,
    dag_run_id: str,
) -> list[dict[str, Any]]:
    """
    Return task execution information for one DAG run.
    """

    encoded_dag_id = quote(dag_id, safe="")
    encoded_run_id = quote(dag_run_id, safe="")

    data = _get_json(
        f"/api/v2/dags/{encoded_dag_id}"
        f"/dagRuns/{encoded_run_id}"
        f"/taskInstances"
    )

    tasks = data.get("task_instances", [])

    return [
        {
            "task_id": task.get("task_id"),
            "state": task.get("state"),
            "try_number": task.get("try_number"),
            "start_date": task.get("start_date"),
            "end_date": task.get("end_date"),
            "duration": task.get("duration"),
        }
        for task in tasks
    ]


def get_task_logs(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int = 1,
) -> dict[str, Any]:
    """
    Return useful evidence from an Airflow task log.

    Instead of returning the entire raw Airflow log,
    extract errors and human-readable log events.
    """

    encoded_dag_id = quote(dag_id, safe="")
    encoded_run_id = quote(dag_run_id, safe="")
    encoded_task_id = quote(task_id, safe="")

    data = _get_json(
        f"/api/v2/dags/{encoded_dag_id}"
        f"/dagRuns/{encoded_run_id}"
        f"/taskInstances/{encoded_task_id}"
        f"/logs/{try_number}"
    )

    content = data.get("content", [])

    errors = []
    events = []

    for item in content:

        event = item.get("event")
        level = item.get("level")
        timestamp = item.get("timestamp")

        if event:
            events.append(
                {
                    "timestamp": timestamp,
                    "level": level,
                    "event": event,
                }
            )

        for error in item.get("error_detail", []):
            frames = error.get("frames", [])

            source = None

            if frames:
                last_frame = frames[-1]

                source = {
                    "filename": last_frame.get("filename"),
                    "line": last_frame.get("lineno"),
                    "function": last_frame.get("name"),
                }

            errors.append(
                {
                    "type": error.get("exc_type"),
                    "message": error.get("exc_value"),
                    "source": source,
                }
            )

    return {
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "task_id": task_id,
        "try_number": try_number,
        "errors": errors,
        "events": events,
    }


if __name__ == "__main__":

    DAG_ID = "orders_debug_pipeline"

    print("\n=== RECENT DAG RUNS ===")

    runs = get_pipeline_runs(DAG_ID)

    print(
        json.dumps(
            runs,
            indent=2,
        )
    )

    failed_runs = [
        run
        for run in runs
        if run["state"] == "failed"
    ]

    if not failed_runs:
        print("\nNo failed DAG runs found.")
        raise SystemExit(0)

    latest_failed_run = failed_runs[0]
    run_id = latest_failed_run["dag_run_id"]

    print("\n=== TASK INSTANCES ===")

    tasks = get_task_instances(
        DAG_ID,
        run_id,
    )

    print(
        json.dumps(
            tasks,
            indent=2,
        )
    )

    failed_tasks = [
        task
        for task in tasks
        if task["state"] == "failed"
    ]

    if not failed_tasks:
        print("\nNo failed tasks found.")
        raise SystemExit(0)

    failed_task = failed_tasks[0]

    print("\n=== FAILED TASK LOG ===")

    log = get_task_logs(
        DAG_ID,
        run_id,
        failed_task["task_id"],
        failed_task["try_number"],
    )

    print(
        json.dumps(
            log,
            indent=2,
        )
    )