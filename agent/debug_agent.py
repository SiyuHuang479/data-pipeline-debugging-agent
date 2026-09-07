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


# =========================================================
# Configuration
# =========================================================

MODEL = os.getenv(
    "GEMINI_MODEL",
    "gemini-3.5-flash-lite",
)

MAX_AGENT_STEPS = 10

client = genai.Client()


SYSTEM_PROMPT = """
You are a data pipeline debugging agent.

Your job is to diagnose data pipeline failures using real evidence
from the available tools.

Environment:
- Airflow DAG: orders_debug_pipeline
- PostgreSQL table: orders
- Current pipeline input: data/input/orders.csv

Rules:

1. Never guess the root cause.

2. Start by investigating the Airflow pipeline:
   - inspect recent DAG runs
   - identify the failed run
   - inspect task states
   - distinguish "failed" from "upstream_failed"

3. Inspect the failed task logs.

4. Do not stop merely because the log contains an error message.
   When useful, verify the underlying cause using:
   - the current input dataset
   - PostgreSQL schema
   - safe read-only SQL queries

5. Database access is read-only.

6. Every claimed root cause must be supported by tool evidence.

7. Avoid unnecessary tool calls.

8. Stop investigating when sufficient evidence exists.

9. A downstream task with state "upstream_failed" is not necessarily
   the root cause.

Final answer format:

Root Cause

Evidence

Recommended Action
"""


# =========================================================
# Tool declarations
#
# These tell Gemini what tools exist.
# They do NOT execute anything.
# =========================================================

get_pipeline_runs_declaration = types.FunctionDeclaration(
    name="get_pipeline_runs",
    description=(
        "Get recent Airflow runs for a DAG. "
        "Use this to determine whether and when a pipeline failed."
    ),
    parameters_json_schema={
        "type": "object",
        "properties": {
            "dag_id": {
                "type": "string",
                "description": "The Airflow DAG ID.",
            },
            "limit": {
                "type": "integer",
                "description": "Maximum number of recent runs.",
            },
        },
        "required": [
            "dag_id",
            "limit",
        ],
    },
)


get_task_instances_declaration = types.FunctionDeclaration(
    name="get_task_instances",
    description=(
        "Get the task execution states for a specific DAG run. "
        "Use this to identify which task actually failed."
    ),
    parameters_json_schema={
        "type": "object",
        "properties": {
            "dag_id": {
                "type": "string",
            },
            "dag_run_id": {
                "type": "string",
            },
        },
        "required": [
            "dag_id",
            "dag_run_id",
        ],
    },
)


get_task_logs_declaration = types.FunctionDeclaration(
    name="get_task_logs",
    description=(
        "Get logs and exception evidence for a specific "
        "Airflow task instance."
    ),
    parameters_json_schema={
        "type": "object",
        "properties": {
            "dag_id": {
                "type": "string",
            },
            "dag_run_id": {
                "type": "string",
            },
            "task_id": {
                "type": "string",
            },
            "try_number": {
                "type": "integer",
            },
        },
        "required": [
            "dag_id",
            "dag_run_id",
            "task_id",
            "try_number",
        ],
    },
)


get_table_schema_declaration = types.FunctionDeclaration(
    name="get_table_schema",
    description=(
        "Inspect the PostgreSQL schema and constraints "
        "for a table."
    ),
    parameters_json_schema={
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
            },
        },
        "required": [
            "table_name",
        ],
    },
)


run_sql_declaration = types.FunctionDeclaration(
    name="run_sql",
    description=(
        "Execute a safe read-only SQL query against PostgreSQL. "
        "Use this when database contents need investigation."
    ),
    parameters_json_schema={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
            },
            "max_rows": {
                "type": "integer",
            },
        },
        "required": [
            "query",
            "max_rows",
        ],
    },
)


inspect_input_data_declaration = types.FunctionDeclaration(
    name="inspect_input_data",
    description=(
        "Inspect the current orders CSV input. "
        "Returns columns, row count, sample rows, null rates, "
        "duplicate order IDs, and customer ID format information."
    ),
    parameters_json_schema={
        "type": "object",
        "properties": {},
    },
)


GEMINI_TOOLS = types.Tool(
    function_declarations=[
        get_pipeline_runs_declaration,
        get_task_instances_declaration,
        get_task_logs_declaration,
        get_table_schema_declaration,
        run_sql_declaration,
        inspect_input_data_declaration,
    ]
)


# =========================================================
# Tool execution layer
#
# Gemini requests a tool by name.
# OUR CODE decides what actually runs.
# =========================================================

def execute_tool(
    tool_name: str,
    arguments: dict,
):
    """
    Execute a tool requested by the model.
    """

    if tool_name == "get_pipeline_runs":
        return get_pipeline_runs(**arguments)

    if tool_name == "get_task_instances":
        return get_task_instances(**arguments)

    if tool_name == "get_task_logs":
        return get_task_logs(**arguments)

    if tool_name == "get_table_schema":
        return get_table_schema(**arguments)

    if tool_name == "run_sql":
        return run_sql(**arguments)

    if tool_name == "inspect_input_data":
        return inspect_input_data()

    raise ValueError(
        f"Unknown tool requested: {tool_name}"
    )


# =========================================================
# Manual agent loop
# =========================================================

def debug_pipeline(question: str) -> str:

    # Full conversation state.
    #
    # We manually maintain this instead of letting
    # Gemini's automatic function calling manage it.
    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_text(
                    text=question
                )
            ],
        )
    ]

    for step in range(
        1,
        MAX_AGENT_STEPS + 1,
    ):

        print("\n")
        print("=" * 60)
        print(f"AGENT STEP {step}")
        print("=" * 60)

        # ---------------------------------------------
        # THINK / PLAN
        #
        # Ask Gemini what to do next.
        # ---------------------------------------------

        response = client.models.generate_content(
            model=MODEL,
            contents=contents,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                tools=[GEMINI_TOOLS],

                # Important:
                # WE are handling function execution.
                automatic_function_calling=
                    types.AutomaticFunctionCallingConfig(
                        disable=True,
                    ),

                temperature=0.2,
            ),
        )

        # Preserve the model response in conversation history.
        #
        # This is important because the function call itself
        # becomes part of the conversation state.
        model_content = response.candidates[0].content

        contents.append(
            model_content
        )

        function_calls = (
            response.function_calls or []
        )

        # ---------------------------------------------
        # STOP CONDITION
        #
        # No tool requested -> model believes it has
        # enough evidence to produce final answer.
        # ---------------------------------------------

        if not function_calls:

            print("\nNo tool requested.")
            print("Agent investigation complete.")

            return response.text

        # ---------------------------------------------
        # ACT + OBSERVE
        #
        # Execute every requested tool.
        # ---------------------------------------------

        function_response_parts = []

        for function_call in function_calls:

            tool_name = function_call.name

            arguments = dict(
                function_call.args or {}
            )

            print(
                f"\n[ACTION] Tool requested: {tool_name}"
            )

            print(
                "Arguments:"
            )

            print(
                json.dumps(
                    arguments,
                    indent=2,
                    default=str,
                )
            )

            # -----------------------------------------
            # Execute tool
            # -----------------------------------------

            try:

                result = execute_tool(
                    tool_name,
                    arguments,
                )

                observation = {
                    "result": result,
                }

                print("\n[OBSERVATION]")

                print(
                    json.dumps(
                        result,
                        indent=2,
                        default=str,
                    )
                )

            except Exception as exc:

                observation = {
                    "error": type(exc).__name__,
                    "message": str(exc),
                }

                print("\n[TOOL ERROR]")

                print(
                    json.dumps(
                        observation,
                        indent=2,
                    )
                )

            # -----------------------------------------
            # Convert observation back into a Gemini
            # FunctionResponse.
            # -----------------------------------------

            function_response_parts.append(
                types.Part.from_function_response(
                    name=tool_name,
                    response=observation,
                )
            )

        # ---------------------------------------------
        # Add tool observations to conversation.
        #
        # On the next loop iteration, Gemini sees:
        #
        # model: "I want to call X"
        # tool:  "Here is the result of X"
        #
        # and decides what to do next.
        # ---------------------------------------------

        contents.append(
            types.Content(
                role="user",
                parts=function_response_parts,
            )       
        )

    # ---------------------------------------------
    # Safety guard:
    # prevent an infinite agent loop.
    # ---------------------------------------------

    raise RuntimeError(
        f"Agent exceeded maximum "
        f"{MAX_AGENT_STEPS} steps."
    )


# =========================================================
# CLI
# =========================================================

if __name__ == "__main__":

    question = (
        "Why did the orders pipeline fail? "
        "Investigate the latest failed run and determine "
        "the root cause using evidence."
    )

    print("=" * 60)
    print("DATA PIPELINE DEBUGGING AGENT")
    print("Manual Agent Loop")
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