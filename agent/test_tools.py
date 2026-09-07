import json

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


DAG_ID = "orders_debug_pipeline"


def pretty(title, data):
    print(f"\n{'=' * 10} {title} {'=' * 10}")
    print(
        json.dumps(
            data,
            indent=2,
            default=str,
        )
    )


def main():

    # --------------------------------
    # 1. Airflow: find recent runs
    # --------------------------------

    runs = get_pipeline_runs(DAG_ID)

    pretty(
        "PIPELINE RUNS",
        runs,
    )

    failed_runs = [
        run
        for run in runs
        if run["state"] == "failed"
    ]

    if not failed_runs:
        raise RuntimeError(
            "No failed DAG run found. "
            "Trigger an incident first."
        )

    latest_failed_run = failed_runs[0]

    run_id = latest_failed_run["dag_run_id"]

    # --------------------------------
    # 2. Airflow: inspect tasks
    # --------------------------------

    tasks = get_task_instances(
        DAG_ID,
        run_id,
    )

    pretty(
        "TASK INSTANCES",
        tasks,
    )

    failed_tasks = [
        task
        for task in tasks
        if task["state"] == "failed"
    ]

    if not failed_tasks:
        raise RuntimeError(
            "No failed task found."
        )

    failed_task = failed_tasks[0]

    # --------------------------------
    # 3. Airflow: get failure log
    # --------------------------------

    logs = get_task_logs(
        DAG_ID,
        run_id,
        failed_task["task_id"],
        failed_task["try_number"],
    )

    pretty(
        "FAILED TASK LOG",
        logs,
    )

    # --------------------------------
    # 4. PostgreSQL: inspect schema
    # --------------------------------

    schema = get_table_schema(
        "orders"
    )

    pretty(
        "ORDERS TABLE SCHEMA",
        schema,
    )

    # --------------------------------
    # 5. PostgreSQL: inspect data
    # --------------------------------

    db_result = run_sql(
        """
        SELECT
            COUNT(*) AS total_orders,
            COUNT(DISTINCT order_id) AS unique_orders,
            COUNT(*) FILTER (
                WHERE customer_id IS NULL
            ) AS null_customer_ids
        FROM orders
        """
    )

    pretty(
        "DATABASE DATA",
        db_result,
    )

    # --------------------------------
    # 6. Inspect current input CSV
    # --------------------------------

    input_data = inspect_input_data()

    pretty(
        "CURRENT INPUT DATA",
        input_data,
    )


if __name__ == "__main__":
    main()