import csv
import os
from typing import Any


DEFAULT_INPUT_FILE = os.getenv(
    "ORDERS_INPUT_FILE",
    "data/input/orders.csv",
)


def inspect_input_data(
    file_path: str = DEFAULT_INPUT_FILE,
    sample_size: int = 5,
) -> dict[str, Any]:
    """
    Inspect the current CSV input file.

    Returns:
    - columns
    - row count
    - sample rows
    - null counts/rates
    - duplicate order IDs
    - basic inferred value types
    """

    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"Input file not found: {file_path}"
        )

    with open(file_path, newline="") as f:
        reader = csv.DictReader(f)

        columns = reader.fieldnames or []
        rows = list(reader)

    if not rows:
        return {
            "file_path": file_path,
            "columns": columns,
            "row_count": 0,
            "sample_rows": [],
            "null_counts": {},
            "null_rates": {},
            "duplicate_order_ids": [],
        }

    # -----------------------------
    # Null statistics
    # -----------------------------

    null_counts = {
        column: 0
        for column in columns
    }

    for row in rows:
        for column in columns:
            value = row.get(column)

            if value is None or value.strip() == "":
                null_counts[column] += 1

    null_rates = {
        column: round(
            count / len(rows),
            4,
        )
        for column, count in null_counts.items()
    }

    # -----------------------------
    # Duplicate order IDs
    # -----------------------------

    duplicate_order_ids = []

    if "order_id" in columns:
        counts = {}

        for row in rows:
            order_id = row.get("order_id")

            if order_id:
                counts[order_id] = (
                    counts.get(order_id, 0) + 1
                )

        duplicate_order_ids = [
            order_id
            for order_id, count in counts.items()
            if count > 1
        ]

    # -----------------------------
    # Basic customer_id inspection
    # -----------------------------

    customer_id_examples = []

    if "customer_id" in columns:
        for row in rows:
            value = row.get("customer_id")

            if value:
                customer_id_examples.append(value)

            if len(customer_id_examples) >= sample_size:
                break

    customer_id_all_integer_like = None

    if "customer_id" in columns:
        non_null_values = [
            row["customer_id"]
            for row in rows
            if row.get("customer_id")
            and row["customer_id"].strip()
        ]

        customer_id_all_integer_like = all(
            value.isdigit()
            for value in non_null_values
        )

    return {
        "file_path": file_path,
        "columns": columns,
        "row_count": len(rows),
        "sample_rows": rows[:sample_size],
        "null_counts": null_counts,
        "null_rates": null_rates,
        "duplicate_order_ids": duplicate_order_ids,
        "customer_id_examples": customer_id_examples,
        "customer_id_all_integer_like":
            customer_id_all_integer_like,
    }


if __name__ == "__main__":

    import json

    result = inspect_input_data()

    print(
        json.dumps(
            result,
            indent=2,
        )
    )