import json
import os

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

from data_tools import inspect_input_data


# ---------------------------------------------------------
# Gemini configuration
# ---------------------------------------------------------

MODEL = os.getenv(
    "GEMINI_MODEL",
    "gemini-3.5-flash-lite",
)

client = genai.Client()


SYSTEM_PROMPT = """
You are a data pipeline debugging agent.

Your job is to diagnose data pipeline failures using real evidence
from the available tools.

Environment:
- Airflow DAG: orders_debug_pipeline
- PostgreSQL table: orders
- Current pipeline input: data/input/orders.csv

Important rules:

1. Never guess the root cause.

2. Start by investigating the Airflow pipeline:
   - inspect recent DAG runs
   - identify the failed run
   - inspect task states
   - distinguish "failed" from "upstream_failed"

3. Inspect the failed task logs.

4. Do not stop merely because the log contains an error message.
   Whenever useful, verify the underlying cause using:
   - the current input dataset
   - PostgreSQL schema
   - safe read-only SQL queries

5. Database access is read-only.

6. Every claimed root cause must be supported by tool evidence.

7. Avoid unnecessary tool calls. Stop investigating when sufficient
   evidence exists.

8. Downstream tasks in "upstream_failed" state are not necessarily
   the root cause.

Your final answer must contain exactly these sections:

Root Cause

Evidence

Recommended Action
"""


# ---------------------------------------------------------
# Tool wrappers
#
# These wrappers are what Gemini sees.
# They call the Python tools we already built.
#
# We print every call so that we can observe the agent's behavior.
# ---------------------------------------------------------

def tool_get_pipeline_runs(
    dag_id: str,
    limit: int = 10,
):
    """
    Get recent Airflow DAG runs.

    Args:
        dag_id: The Airflow DAG ID.
        limit: Maximum number of recent runs to return.
    """

    print("\n[TOOL] get_pipeline_runs")
    print(f"dag_id={dag_id}, limit={limit}")

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
    dag_id: str,
    dag_run_id: str,
):
    """
    Get task execution states for one Airflow DAG run.

    Args:
        dag_id: The Airflow DAG ID.
        dag_run_id: The specific DAG run identifier.
    """

    print("\n[TOOL] get_task_instances")
    print(f"dag_id={dag_id}")
    print(f"dag_run_id={dag_run_id}")

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
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int,
):
    """
    Get logs and exception evidence for an Airflow task instance.

    Args:
        dag_id: The Airflow DAG ID.
        dag_run_id: The DAG run identifier.
        task_id: The Airflow task ID.
        try_number: The task execution attempt number.
    """

    print("\n[TOOL] get_task_logs")
    print(f"task_id={task_id}")

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


def tool_get_table_schema(
    table_name: str,
):
    """
    Inspect the PostgreSQL schema and constraints of a table.

    Args:
        table_name: Name of the PostgreSQL table.
    """

    print("\n[TOOL] get_table_schema")
    print(f"table_name={table_name}")

    result = get_table_schema(
        table_name,
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

    Use this to investigate stored pipeline data.

    Args:
        query: A read-only SELECT query.
        max_rows: Maximum number of rows returned.
    """

    print("\n[TOOL] run_sql")
    print(query)

    result = run_sql(
        query,
        max_rows,
    )

    print(
        json.dumps(
            result,
            indent=2,
            default=str,
        )
    )

    return result


def tool_inspect_input_data():
    """
    Inspect the current orders CSV input.

    Returns columns, row count, sample rows, null rates,
    duplicate order IDs, and customer ID format information.
    """

    print("\n[TOOL] inspect_input_data")

    result = inspect_input_data()

    print(
        json.dumps(
            result,
            indent=2,
            default=str,
        )
    )

    return result


TOOLS = [
    tool_get_pipeline_runs,
    tool_get_task_instances,
    tool_get_task_logs,
    tool_get_table_schema,
    tool_run_sql,
    tool_inspect_input_data,
]


# ---------------------------------------------------------
# Agent
# ---------------------------------------------------------

def debug_pipeline(question: str) -> str:

    response = client.models.generate_content(
        model=MODEL,
        contents=question,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
            tools=TOOLS,

            # Gemini's Python SDK automatically:
            # model -> function call -> execute Python
            # -> return result -> model continues
            automatic_function_calling=
                types.AutomaticFunctionCallingConfig(
                    maximum_remote_calls=10,
                ),

            temperature=0.2,
        ),
    )

    return response.text


# ---------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------

if __name__ == "__main__":

    question = (
        "Why did the orders pipeline fail? "
        "Investigate the latest failed run and determine "
        "the root cause using evidence."
    )

    print("=" * 60)
    print("DATA PIPELINE DEBUGGING AGENT")
    print("=" * 60)

    print(f"\nModel: {MODEL}")
    print(f"\nQuestion: {question}")

    answer = debug_pipeline(
        question
    )

    print("\n")
    print("=" * 60)
    print("FINAL DIAGNOSIS")
    print("=" * 60)

    print(answer)