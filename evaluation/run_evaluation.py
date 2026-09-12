import io
import json
import shutil
import sys
import time
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any


# =========================================================
# Project imports
# =========================================================

ROOT = Path(
    __file__
).resolve().parents[1]

AGENT_DIR = (
    ROOT / "agent"
)

sys.path.insert(
    0,
    str(AGENT_DIR),
)


from airflow_tools import (
    trigger_pipeline,
    wait_for_pipeline,
    get_task_instances,
    get_task_logs,
)

from dataset_config import (
    load_dataset_config,
)

from debug_agent_afc import (
    debug_pipeline,
)


# =========================================================
# Evaluation paths
# =========================================================

CASES_FILE = (
    ROOT
    / "evaluation"
    / "cases.json"
)

RESULTS_FILE = (
    ROOT
    / "evaluation"
    / "results.json"
)


# =========================================================
# Helpers
# =========================================================

def resolve_path(
    path_string: str,
) -> Path:
    """
    Resolve project-relative paths.
    """

    path = Path(
        path_string
    )

    if not path.is_absolute():
        path = ROOT / path

    return path


def load_cases() -> list[dict[str, Any]]:
    """
    Load evaluation cases.
    """

    with open(
        CASES_FILE,
        encoding="utf-8",
    ) as file:
        return json.load(file)


def evaluate_diagnosis(
    diagnosis: str,
    keyword_groups: list[list[str]],
) -> tuple[bool, list[dict[str, Any]]]:
    """
    Deterministic diagnosis grader.

    Each keyword group represents one concept.

    At least one keyword from every group
    must appear in the diagnosis.
    """

    diagnosis_lower = (
        diagnosis.lower()
    )

    group_results = []

    for group in keyword_groups:

        matched_keywords = [
            keyword
            for keyword in group
            if keyword.lower()
            in diagnosis_lower
        ]

        passed = bool(
            matched_keywords
        )

        group_results.append(
            {
                "keywords":
                    group,

                "matched":
                    matched_keywords,

                "passed":
                    passed,
            }
        )

    overall_passed = all(
        result["passed"]
        for result
        in group_results
    )

    return (
        overall_passed,
        group_results,
    )


# =========================================================
# Pipeline verification
# =========================================================

def verify_pipeline_failure(
    dag_id: str,
    dag_run_id: str,
    expected_failed_task: str,
    expected_log_keywords: list[str],
) -> dict[str, Any]:
    """
    Verify that the synthetic incident produced
    the intended Airflow failure.

    This separates:

    "the test fixture worked"

    from:

    "the Agent diagnosed it correctly"
    """

    task_instances = (
        get_task_instances(
            dag_id,
            dag_run_id,
        )
    )

    expected_task = next(
        (
            task
            for task in task_instances
            if task["task_id"]
            == expected_failed_task
        ),
        None,
    )

    if expected_task is None:
        return {
            "passed": False,
            "reason": (
                f"Expected task "
                f"'{expected_failed_task}' "
                f"was not found."
            ),
        }

    if (
        expected_task["state"]
        != "failed"
    ):
        return {
            "passed": False,
            "reason": (
                f"Expected task "
                f"'{expected_failed_task}' "
                f"to fail, but state was "
                f"'{expected_task['state']}'."
            ),
        }

    try_number = (
        expected_task.get(
            "try_number",
            1,
        )
        or 1
    )

    log_result = get_task_logs(
        dag_id,
        dag_run_id,
        expected_failed_task,
        try_number,
    )

    log_text = json.dumps(
        log_result,
        default=str,
    ).lower()

    missing_keywords = [
        keyword
        for keyword
        in expected_log_keywords
        if keyword.lower()
        not in log_text
    ]

    if missing_keywords:
        return {
            "passed": False,
            "reason": (
                "Expected incident log "
                "keywords were missing."
            ),
            "missing_log_keywords":
                missing_keywords,
        }

    return {
        "passed": True,
        "reason": (
            "Synthetic incident produced "
            "the expected pipeline failure."
        ),
    }


# =========================================================
# One evaluation case
# =========================================================

def run_case(
    case: dict[str, Any],
) -> dict[str, Any]:

    case_name = case[
        "name"
    ]

    dataset_name = case[
        "dataset"
    ]

    print(
        "\n"
        + "=" * 60
    )

    print(
        f"CASE: {case_name}"
    )

    print(
        f"DATASET: {dataset_name}"
    )

    print(
        "=" * 60
    )

    # -----------------------------------------------------
    # Load dataset-specific environment
    # -----------------------------------------------------

    config = load_dataset_config(
        dataset_name
    )

    dag_id = config[
        "dag_id"
    ]

    input_path = resolve_path(
        config["input"]["path"]
    )

    incident_fixture = resolve_path(
        case["fixture"]
    )

    healthy_fixture = resolve_path(
        case["healthy_fixture"]
    )

    dag_run_id = None

    started_at = time.time()

    agent_trace = ""

    try:

        # -------------------------------------------------
        # Install synthetic incident
        # -------------------------------------------------

        shutil.copyfile(
            incident_fixture,
            input_path,
        )

        print(
            f"Installed fixture: "
            f"{incident_fixture}"
        )

        # -------------------------------------------------
        # Trigger the dataset's configured DAG
        # -------------------------------------------------

        triggered_run = (
            trigger_pipeline(
                dag_id
            )
        )

        dag_run_id = triggered_run[
            "dag_run_id"
        ]

        print(
            f"Triggered DAG run: "
            f"{dag_run_id}"
        )

        # -------------------------------------------------
        # Wait for completion
        # -------------------------------------------------

        completed_run = (
            wait_for_pipeline(
                dag_id,
                dag_run_id,
            )
        )

        final_state = completed_run[
            "state"
        ]

        print(
            f"Final state: "
            f"{final_state}"
        )

        # -------------------------------------------------
        # Verify the incident itself.
        #
        # This is important:
        #
        # if our fixture is broken, the Agent should not
        # receive blame for a bad benchmark.
        # -------------------------------------------------

        setup_check = (
            verify_pipeline_failure(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                expected_failed_task=
                    case[
                        "expected_failed_task"
                    ],
                expected_log_keywords=
                    case.get(
                        "expected_log_keywords",
                        [],
                    ),
            )
        )

        if not setup_check[
            "passed"
        ]:

            return {
                "case":
                    case_name,

                "dataset":
                    dataset_name,

                "status":
                    "ERROR",

                "passed":
                    None,

                "error":
                    (
                        "Evaluation fixture did not "
                        "produce the expected failure."
                    ),

                "setup_check":
                    setup_check,

                "dag_run_id":
                    dag_run_id,
            }

        # -------------------------------------------------
        # Ask Agent to investigate THIS exact run.
        # -------------------------------------------------

        question = (
            f"Investigate Airflow DAG run "
            f"{dag_run_id} for the "
            f"{dataset_name} dataset. "
            "Determine the root cause of the failure "
            "using evidence from the available tools. "
            "Do not investigate a different DAG run."
        )

        trace_buffer = io.StringIO()

        with redirect_stdout(
            trace_buffer
        ):

            diagnosis = debug_pipeline(
                question=question,
                dataset_name=
                    dataset_name,
            )

        agent_trace = (
            trace_buffer.getvalue()
        )

        print(
            agent_trace
        )

        print(
            "\nAGENT DIAGNOSIS:"
        )

        print(
            diagnosis
        )

        # -------------------------------------------------
        # Grade diagnosis
        # -------------------------------------------------

        passed, keyword_results = (
            evaluate_diagnosis(
                diagnosis,
                case[
                    "diagnosis_keyword_groups"
                ],
            )
        )

        elapsed_seconds = (
            time.time()
            - started_at
        )

        return {
            "case":
                case_name,

            "dataset":
                dataset_name,

            "status":
                (
                    "PASS"
                    if passed
                    else "FAIL"
                ),

            "passed":
                passed,

            "dag_run_id":
                dag_run_id,

            "diagnosis":
                diagnosis,

            "keyword_results":
                keyword_results,

            "agent_trace":
                agent_trace,

            "elapsed_seconds":
                round(
                    elapsed_seconds,
                    2,
                ),
        }

    except Exception as exc:

        elapsed_seconds = (
            time.time()
            - started_at
        )

        return {
            "case":
                case_name,

            "dataset":
                dataset_name,

            "status":
                "ERROR",

            "passed":
                None,

            "dag_run_id":
                dag_run_id,

            "error":
                (
                    f"{type(exc).__name__}: "
                    f"{exc}"
                ),

            "agent_trace":
                agent_trace,

            "elapsed_seconds":
                round(
                    elapsed_seconds,
                    2,
                ),
        }

    finally:

        # -------------------------------------------------
        # Always restore the healthy dataset.
        # -------------------------------------------------

        shutil.copyfile(
            healthy_fixture,
            input_path,
        )

        print(
            f"Restored healthy fixture "
            f"for dataset: {dataset_name}"
        )


# =========================================================
# Main benchmark
# =========================================================

def main():

    cases = load_cases()

    results = []

    for case in cases:

        result = run_case(
            case
        )

        results.append(
            result
        )

        print(
            f"\nRESULT: "
            f"{result['status']}"
        )

    # -----------------------------------------------------
    # Summary
    # -----------------------------------------------------

    evaluated_results = [
        result
        for result in results
        if result["status"]
        in {
            "PASS",
            "FAIL",
        }
    ]

    passed_count = sum(
        1
        for result
        in evaluated_results
        if result["status"]
        == "PASS"
    )

    failed_count = sum(
        1
        for result
        in evaluated_results
        if result["status"]
        == "FAIL"
    )

    error_count = sum(
        1
        for result
        in results
        if result["status"]
        == "ERROR"
    )

    evaluated_count = len(
        evaluated_results
    )

    total_count = len(
        results
    )

    accuracy = (
        passed_count
        / evaluated_count
        if evaluated_count
        else 0
    )

    summary = {
        "total_cases":
            total_count,

        "evaluated_cases":
            evaluated_count,

        "passed":
            passed_count,

        "failed":
            failed_count,

        "errors":
            error_count,

        "completed_accuracy":
            round(
                accuracy,
                4,
            ),
    }

    output = {
        "summary":
            summary,

        "results":
            results,
    }

    with open(
        RESULTS_FILE,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            output,
            file,
            indent=2,
        )

    print(
        "\n"
        + "=" * 60
    )

    print(
        "EVALUATION SUMMARY"
    )

    print(
        "=" * 60
    )

    print(
        f"Total cases: "
        f"{total_count}"
    )

    print(
        f"Evaluated cases: "
        f"{evaluated_count}"
    )

    print(
        f"PASS: "
        f"{passed_count}"
    )

    print(
        f"FAIL: "
        f"{failed_count}"
    )

    print(
        f"ERROR: "
        f"{error_count}"
    )

    print(
        "Completed accuracy: "
        f"{passed_count}/"
        f"{evaluated_count}"
        f" = "
        f"{accuracy:.1%}"
        if evaluated_count
        else
        "Completed accuracy: N/A"
    )

    print(
        f"\nResults written to: "
        f"{RESULTS_FILE}"
    )


if __name__ == "__main__":
    main()