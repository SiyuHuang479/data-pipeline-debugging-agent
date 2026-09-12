import csv
from collections import Counter
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(
    __file__
).resolve().parents[1]


def _resolve_file_path(
    file_path: str,
) -> Path:
    """
    Resolve a file path.

    Relative paths are interpreted relative to
    the project root instead of the current shell
    working directory.
    """

    path = Path(file_path)

    if not path.is_absolute():
        path = PROJECT_ROOT / path

    return path


def _is_null(
    value: Any,
) -> bool:
    """
    Decide whether a CSV value should be treated
    as null.

    CSV values are normally strings, but missing
    fields may also appear as None.
    """

    if value is None:
        return True

    if isinstance(value, str):
        return value.strip() == ""

    return False


def inspect_csv(
    file_path: str,
    sample_size: int = 5,
    encoding: str = "utf-8",
    delimiter: str = ",",
):
    """
    Generic CSV profiler.

    This function does NOT know anything about the
    expected schema or business rules.

    It only observes the actual CSV file and returns:

    - columns
    - row count
    - sample rows
    - null counts / rates
    - unique counts
    - duplicate counts
    - sample values
    - basic structural issues

    Expected schema validation belongs in a separate
    contract-validation layer.
    """

    path = _resolve_file_path(
        file_path
    )

    if not path.exists():
        raise FileNotFoundError(
            f"CSV file not found: {path}"
        )

    if not path.is_file():
        raise ValueError(
            f"Path is not a file: {path}"
        )

    if sample_size < 1:
        raise ValueError(
            "sample_size must be at least 1"
        )

    # ---------------------------------------------
    # Read CSV
    # ---------------------------------------------

    with open(
        path,
        newline="",
        encoding=encoding,
    ) as file:

        reader = csv.DictReader(
            file,
            delimiter=delimiter,
        )

        columns = (
            reader.fieldnames
            or []
        )

        # Detect duplicate header names.
        header_counts = Counter(
            columns
        )

        duplicate_columns = [
            column
            for column, count
            in header_counts.items()
            if count > 1
        ]

        # -----------------------------------------
        # Initialize column statistics
        # -----------------------------------------

        column_stats = {
            column: {
                "null_count": 0,
                "non_null_count": 0,
                "value_counts": Counter(),
                "sample_values": [],
            }
            for column in columns
        }

        row_count = 0

        sample_rows = []

        rows_with_extra_fields = 0
        rows_with_missing_fields = 0

        # -----------------------------------------
        # Stream through CSV rows
        # -----------------------------------------

        for row in reader:

            row_count += 1

            # csv.DictReader uses the key None when
            # a row contains more values than headers.
            extra_values = row.get(
                None
            )

            if extra_values:
                rows_with_extra_fields += 1

            # A short row may cause a declared column
            # to have value None.
            if any(
                row.get(column) is None
                for column in columns
            ):
                rows_with_missing_fields += 1

            # Keep only a small number of full rows
            # for evidence/debugging.
            if len(sample_rows) < sample_size:

                sample_row = {
                    column: row.get(column)
                    for column in columns
                }

                sample_rows.append(
                    sample_row
                )

            # -------------------------------------
            # Profile every column identically.
            #
            # No special handling for:
            # order_id
            # customer_id
            # payment_id
            # etc.
            # -------------------------------------

            for column in columns:

                value = row.get(
                    column
                )

                stats = column_stats[
                    column
                ]

                if _is_null(value):

                    stats[
                        "null_count"
                    ] += 1

                    continue

                cleaned_value = (
                    value.strip()
                    if isinstance(
                        value,
                        str,
                    )
                    else value
                )

                stats[
                    "non_null_count"
                ] += 1

                stats[
                    "value_counts"
                ][cleaned_value] += 1

                if (
                    len(
                        stats[
                            "sample_values"
                        ]
                    )
                    < sample_size
                ):

                    stats[
                        "sample_values"
                    ].append(
                        cleaned_value
                    )

    # ---------------------------------------------
    # Build final column profiles
    # ---------------------------------------------

    column_profiles = {}

    for column in columns:

        stats = column_stats[
            column
        ]

        value_counts = stats[
            "value_counts"
        ]

        unique_count = len(
            value_counts
        )

        duplicate_count = sum(
            count - 1
            for count
            in value_counts.values()
            if count > 1
        )

        null_count = stats[
            "null_count"
        ]

        null_rate = (
            null_count / row_count
            if row_count > 0
            else 0
        )

        column_profiles[
            column
        ] = {
            "null_count":
                null_count,

            "non_null_count":
                stats[
                    "non_null_count"
                ],

            "null_rate":
                round(
                    null_rate,
                    4,
                ),

            "unique_count":
                unique_count,

            "duplicate_count":
                duplicate_count,

            "sample_values":
                stats[
                    "sample_values"
                ],
        }

    # ---------------------------------------------
    # Return JSON-friendly profile
    # ---------------------------------------------

    return {
        "file_path": file_path,

        "row_count":
            row_count,

        "column_count":
            len(columns),

        "columns":
            columns,

        "sample_rows":
            sample_rows,

        "column_profiles":
            column_profiles,

        "structural_issues": {
            "duplicate_columns":
                duplicate_columns,

            "rows_with_extra_fields":
                rows_with_extra_fields,

            "rows_with_missing_fields":
                rows_with_missing_fields,
        },
    }


# -------------------------------------------------
# Temporary backward compatibility
#
# The old V1 Gemini agent still calls
# inspect_input_data() without arguments.
#
# We will remove this default when
# debug_agent_afc.py becomes config-driven.
# -------------------------------------------------

def inspect_input_data(
    file_path: str = "data/input/orders.csv",
    sample_size: int = 5,
):
    return inspect_csv(
        file_path=file_path,
        sample_size=sample_size,
    )


if __name__ == "__main__":

    import json

    result = inspect_csv(
        "data/input/orders.csv"
    )

    print(
        json.dumps(
            result,
            indent=2,
        )
    )