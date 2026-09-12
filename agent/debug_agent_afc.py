import argparse
import json
import os
from typing import Any

from google import genai
from google.genai import types

from airflow_tools import (
    get_pipeline_runs,
    get_task_instances,
    get_task_logs,
)

from database_tools import (
    get_table_schema,
    run_sql,
)

from data_tools import inspect_csv

from dataset_config import (
    load_dataset_config,
)

from contract_validator import (
    validate_against_contract,
)


# =========================================================
# Gemini configuration
# =========================================================

MODEL = os.getenv(
    "GEMINI_MODEL",
    "gemini-3.5-flash-lite",
)

MAX_REMOTE_CALLS = 10

client = genai.Client()


# =========================================================
# System prompt
# =========================================================

def build_system_prompt(
    config: dict[str, Any],
) -> str:
    """
    Build a dataset-specific system prompt from
    the generic dataset contract.

    The agent code itself does not need to know
    about orders, payments, customers, etc.
    """

    dataset_name = config["name"]
    dag_id = config["dag_id"]

    input_config = config["input"]
    database_config = config["database"]

    contract_json = json.dumps(
        config["columns"],
        indent=2,
    )

    return f"""
You are a data pipeline debugging agent.

Your job is to diagnose failures using real evidence
from the available tools.

You are currently debugging this dataset:

Dataset:
{dataset_name}

Airflow DAG:
{dag_id}

Input source:
- type: {input_config["type"]}
- path: {input_config["path"]}

Database:
- type: {database_config["type"]}
- schema: {database_config.get("schema", "public")}
- table: {database_config["table"]}

Declared data contract:

{contract_json}

Important rules:

1. Never guess the root cause.

2. Investigate the Airflow execution state first when
   diagnosing a pipeline failure.

3. Distinguish a task with state "failed" from tasks with
   state "upstream_failed".
   The task that actually failed is usually more relevant
   to the root cause.

4. Inspect logs from the task that actually failed.

5. Use the data tools when useful:
   - inspect_dataset observes the actual input data.
   - validate_data_contract deterministically compares the
     actual dataset against the declared contract.

6. Treat the declared data contract as the source of truth
   for expected schema and data-quality rules.

7. Do not infer business expectations when the contract
   already defines them.

8. Use PostgreSQL schema inspection or safe read-only SQL
   only when they provide useful additional evidence.

9. Every claimed root cause must be supported by evidence
   returned from tools.

10. Do not call tools merely because they exist.
    Avoid unnecessary investigation.

11. Stop calling tools once sufficient evidence exists.

12. Do not perform destructive database operations.

13. Before diagnosing a root cause, identify a specific
    Airflow DAG run and verify that it actually failed.

14. If the selected/latest run succeeded, do not invent
    a failure. State that no failure was observed for
    that run.

15. A configuration mismatch, schema difference, or
    suspicious condition is NOT sufficient to declare
    a root cause unless Airflow task state, logs, or
    other run-specific evidence connects it to the failure.

16. Current input data represents the dataset's present
    state. Do not assume it is identical to the input used
    by an older historical DAG run.

17. If the available evidence is insufficient to determine
    the root cause, explicitly say so instead of guessing.

Your final answer must contain exactly these sections:

Root Cause

Evidence

Recommended Action
"""


# =========================================================
# Config validation
# =========================================================

def validate_supported_environment(
    config: dict[str, Any],
) -> None:
    """
    V1.5 currently supports:

    - CSV input
    - PostgreSQL database

    The architecture is config-driven, but we still
    deliberately keep the supported technology scope small.
    """

    input_type = (
        config["input"]["type"]
        .strip()
        .lower()
    )

    database_type = (
        config["database"]["type"]
        .strip()
        .lower()
    )

    if input_type != "csv":
        raise ValueError(
            "This V1.5 agent currently supports "
            f"CSV inputs only. Received: {input_type}"
        )

    if database_type not in {
        "postgresql",
        "postgres",
    }:
        raise ValueError(
            "This V1.5 agent currently supports "
            "PostgreSQL only. "
            f"Received: {database_type}"
        )


# =========================================================
# Dataset-bound tool factory
# =========================================================

def build_tools(
    dataset_name: str,
    config: dict[str, Any],
):
    """
    Create tools bound to the selected dataset.

    Gemini does NOT choose:
    - DAG ID
    - CSV path
    - database table

    Those values come from the dataset contract.

    This prevents the model from accidentally inspecting
    unrelated resources and keeps the tools reusable.
    """

    dag_id = config["dag_id"]

    input_config = config["input"]
    database_config = config["database"]

    input_path = input_config["path"]

    encoding = input_config.get(
        "encoding",
        "utf-8",
    )

    delimiter = input_config.get(
        "delimiter",
        ",",
    )

    table_name = database_config[
        "table"
    ]

    # -----------------------------------------------------
    # Airflow tools
    # -----------------------------------------------------

    def tool_get_pipeline_runs(
        limit: int = 5,
    ):
        """
        Get recent Airflow runs for the current dataset's DAG.

        Args:
            limit: Maximum number of recent DAG runs to return.

        Returns:
            Recent DAG run metadata including run IDs and states.
        """

        print(
            "\n[TOOL] get_pipeline_runs"
        )
        print(
            f"dag_id={dag_id}, "
            f"limit={limit}"
        )

        result = get_pipeline_runs(
            dag_id,
            limit,
        )

        print(
            json.dumps(
                result,
                indent=2,
                default=str,
            )
        )

        return result

    def tool_get_task_instances(
        dag_run_id: str,
    ):
        """
        Get task states for one run of the current Airflow DAG.

        Use this to identify which task actually failed and
        which downstream tasks are only upstream_failed.

        Args:
            dag_run_id: The Airflow DAG run identifier.

        Returns:
            Task IDs, execution states, attempt numbers,
            and timing information.
        """

        print(
            "\n[TOOL] get_task_instances"
        )

        print(
            f"dag_id={dag_id}"
        )

        print(
            f"dag_run_id={dag_run_id}"
        )

        result = get_task_instances(
            dag_id,
            dag_run_id,
        )

        print(
            json.dumps(
                result,
                indent=2,
                default=str,
            )
        )

        return result

    def tool_get_task_logs(
        dag_run_id: str,
        task_id: str,
        try_number: int = 1,
    ):
        """
        Get Airflow logs and exception evidence for a task.

        Args:
            dag_run_id: The Airflow DAG run identifier.
            task_id: The task whose logs should be inspected.
            try_number: The task execution attempt number.

        Returns:
            Structured errors and relevant log events.
        """

        print(
            "\n[TOOL] get_task_logs"
        )

        print(
            f"dag_run_id={dag_run_id}"
        )

        print(
            f"task_id={task_id}"
        )

        print(
            f"try_number={try_number}"
        )

        result = get_task_logs(
            dag_id,
            dag_run_id,
            task_id,
            try_number,
        )

        print(
            json.dumps(
                result,
                indent=2,
                default=str,
            )
        )

        return result

    # -----------------------------------------------------
    # Data tools
    # -----------------------------------------------------

    def tool_inspect_dataset(
        sample_size: int = 5,
    ):
        """
        Inspect the actual input dataset without applying
        business expectations.

        Use this when raw observations such as columns,
        samples, null rates, uniqueness statistics, or
        structural CSV issues are useful.

        Args:
            sample_size: Maximum number of sample rows and
                sample values to return.

        Returns:
            A generic profile of the current dataset.
        """

        print(
            "\n[TOOL] inspect_dataset"
        )

        print(
            f"file_path={input_path}"
        )

        result = inspect_csv(
            file_path=input_path,
            sample_size=sample_size,
            encoding=encoding,
            delimiter=delimiter,
        )

        print(
            json.dumps(
                result,
                indent=2,
                default=str,
            )
        )

        return result

    def tool_validate_data_contract():
        """
        Compare the current input dataset against its
        declared data contract.

        Use this to identify deterministic violations such as:
        - missing required columns
        - invalid data types
        - disallowed nulls
        - excessive null rates
        - uniqueness violations
        - numeric range violations
        - malformed CSV structure

        Returns:
            Structured contract validation results.
        """

        print(
            "\n[TOOL] validate_data_contract"
        )

        print(
            f"dataset={dataset_name}"
        )

        result = validate_against_contract(
            dataset_name
        )

        print(
            json.dumps(
                result,
                indent=2,
                default=str,
            )
        )

        return result

    # -----------------------------------------------------
    # Database tools
    # -----------------------------------------------------

    def tool_get_target_table_schema():
        """
        Inspect the PostgreSQL schema and constraints for
        the current dataset's configured target table.

        Returns:
            Column types, nullability, defaults,
            and table constraints.
        """

        print(
            "\n[TOOL] get_target_table_schema"
        )

        print(
            f"table_name={table_name}"
        )

        result = get_table_schema(
            table_name
        )

        print(
            json.dumps(
                result,
                indent=2,
                default=str,
            )
        )

        return result

    def tool_run_sql(
        query: str,
        max_rows: int = 50,
    ):
        """
        Execute a safe read-only SQL query against PostgreSQL.

        Use this only when inspecting stored database data would
        materially help diagnose the pipeline failure.

        The underlying database tool enforces read-only safety,
        row limits, prohibited statements, and query timeout.

        Args:
            query: A read-only SELECT or WITH query.
            max_rows: Maximum number of rows to return.

        Returns:
            Query result rows and metadata.
        """

        print(
            "\n[TOOL] run_sql"
        )

        print(
            f"query={query}"
        )

        result = run_sql(
            query=query,
            max_rows=max_rows,
        )

        print(
            json.dumps(
                result,
                indent=2,
                default=str,
            )
        )

        return result

    return [
        tool_get_pipeline_runs,
        tool_get_task_instances,
        tool_get_task_logs,
        tool_inspect_dataset,
        tool_validate_data_contract,
        tool_get_target_table_schema,
        tool_run_sql,
    ]


# =========================================================
# Debugging agent
# =========================================================

def debug_pipeline(
    question: str,
    dataset_name: str = "orders",
) -> str:
    """
    Diagnose a pipeline failure for a configured dataset.

    Args:
        question:
            The user's debugging question.

        dataset_name:
            Name of the dataset config under configs/.

            Example:
            "orders" -> configs/orders.json

    Returns:
        Gemini's evidence-backed diagnosis.
    """

    config = load_dataset_config(
        dataset_name
    )

    validate_supported_environment(
        config
    )

    system_prompt = (
        build_system_prompt(
            config
        )
    )

    tools = build_tools(
        dataset_name,
        config,
    )

    response = client.models.generate_content(
        model=MODEL,
        contents=question,
        config=types.GenerateContentConfig(
            system_instruction=
                system_prompt,

            tools=tools,

            automatic_function_calling=
                types.AutomaticFunctionCallingConfig(
                    maximum_remote_calls=
                        MAX_REMOTE_CALLS,
                ),

            temperature=0.2,
        ),
    )

    if not response.text:
        raise RuntimeError(
            "Gemini returned no final text response."
        )

    return response.text


# =========================================================
# CLI
# =========================================================

def main():
    """
    Command-line entry point.

    Examples:

        python agent/debug_agent_afc.py orders

        python agent/debug_agent_afc.py orders \
            --question "Why did the latest pipeline run fail?"
    """

    parser = argparse.ArgumentParser(
        description=(
            "Debug a configured data pipeline "
            "using Gemini."
        )
    )

    parser.add_argument(
        "dataset",
        nargs="?",
        default="orders",
        help=(
            "Dataset config name. "
            "Example: orders -> configs/orders.json"
        ),
    )

    parser.add_argument(
        "--question",
        default=None,
        help=(
            "Optional debugging question."
        ),
    )

    parser.add_argument(
        "--run-id",
        default=None,
        help=(
            "Optional Airflow DAG run ID to investigate. "
            "If omitted, inspect the latest run."
        ),
    )

    args = parser.parse_args()

    dataset_name = args.dataset

    config = load_dataset_config(
        dataset_name
    )

    question = (
        f"Inspect the latest run of the "
        f"{config['name']} pipeline. "
        "Determine whether it succeeded or failed. "
        "If it failed, diagnose the root cause "
        "using run-specific evidence. "
        "If it succeeded, report that no failure "
        "was observed and do not invent a root cause."
    )

    print(
        "=" * 60
    )

    print(
        "DATA PIPELINE DEBUGGING AGENT"
    )

    print(
        "Gemini Automatic Function Calling"
    )

    print(
        "=" * 60
    )

    print(
        f"\nDataset: "
        f"{dataset_name}"
    )

    print(
        f"Model: "
        f"{MODEL}"
    )

    print(
        f"\nQuestion: "
        f"{question}"
    )

    answer = debug_pipeline(
        question=question,
        dataset_name=dataset_name,
    )

    print("\n")

    print(
        "=" * 60
    )

    print(
        "FINAL DIAGNOSIS"
    )

    print(
        "=" * 60
    )

    print(
        answer
    )


if __name__ == "__main__":
    main()