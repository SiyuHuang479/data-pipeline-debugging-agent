import csv
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from data_tools import inspect_csv
from dataset_config import load_dataset_config


PROJECT_ROOT = Path(
    __file__
).resolve().parents[1]


# =========================================================
# Path helpers
# =========================================================

def _resolve_file_path(
    file_path: str,
) -> Path:
    """
    Resolve paths relative to the project root.
    """

    path = Path(file_path)

    if not path.is_absolute():
        path = PROJECT_ROOT / path

    return path


def _is_null(
    value: Any,
) -> bool:
    """
    Use the same null definition as data_tools.py.
    """

    if value is None:
        return True

    if isinstance(value, str):
        return value.strip() == ""

    return False


# =========================================================
# Type validation
# =========================================================

def _is_integer(
    value: str,
    column_config: dict,
) -> bool:
    try:
        int(value)
        return True

    except (ValueError, TypeError):
        return False


def _is_decimal(
    value: str,
    column_config: dict,
) -> bool:
    try:
        Decimal(value)
        return True

    except (
        InvalidOperation,
        ValueError,
        TypeError,
    ):
        return False


def _is_date(
    value: str,
    column_config: dict,
) -> bool:

    date_format = column_config.get(
        "format",
        "%Y-%m-%d",
    )

    try:
        datetime.strptime(
            value,
            date_format,
        )

        return True

    except (
        ValueError,
        TypeError,
    ):
        return False


def _is_datetime(
    value: str,
    column_config: dict,
) -> bool:

    datetime_format = column_config.get(
        "format"
    )

    try:

        if datetime_format:
            datetime.strptime(
                value,
                datetime_format,
            )

        else:
            datetime.fromisoformat(
                value
            )

        return True

    except (
        ValueError,
        TypeError,
    ):
        return False


def _is_boolean(
    value: str,
    column_config: dict,
) -> bool:

    valid_values = {
        "true",
        "false",
        "1",
        "0",
        "yes",
        "no",
    }

    return (
        str(value)
        .strip()
        .lower()
        in valid_values
    )


def _is_string(
    value: str,
    column_config: dict,
) -> bool:
    return isinstance(
        value,
        str,
    )


TYPE_VALIDATORS = {
    "integer": _is_integer,

    "decimal": _is_decimal,

    # Treat "number" as decimal-like
    # for now.
    "number": _is_decimal,

    "date": _is_date,

    "datetime": _is_datetime,

    "boolean": _is_boolean,

    "string": _is_string,
}


def _validate_type(
    value: str,
    expected_type: str,
    column_config: dict,
) -> bool:
    """
    Validate one non-null value against
    a contract type.

    The contract is the source of truth.
    We do NOT infer the expected type.
    """

    validator = TYPE_VALIDATORS.get(
        expected_type
    )

    if validator is None:
        raise ValueError(
            f"Unsupported contract type: "
            f"{expected_type}"
        )

    return validator(
        value,
        column_config,
    )


# =========================================================
# Numeric range validation
# =========================================================

def _check_numeric_range(
    value: str,
    column_config: dict,
) -> str | None:
    """
    Check optional min/max constraints.

    Returns:
        None if valid.

        Otherwise, a human-readable
        violation reason.
    """

    try:
        numeric_value = Decimal(
            value
        )

    except (
        InvalidOperation,
        ValueError,
        TypeError,
    ):
        # Type validation will report this.
        return None

    # -----------------------------------------
    # Minimum
    # -----------------------------------------

    if "min_value" in column_config:

        minimum = Decimal(
            str(
                column_config[
                    "min_value"
                ]
            )
        )

        inclusive = column_config.get(
            "min_inclusive",
            True,
        )

        if inclusive:

            if numeric_value < minimum:
                return (
                    f"value {value} is below "
                    f"minimum {minimum}"
                )

        else:

            if numeric_value <= minimum:
                return (
                    f"value {value} must be "
                    f"greater than {minimum}"
                )

    # -----------------------------------------
    # Maximum
    # -----------------------------------------

    if "max_value" in column_config:

        maximum = Decimal(
            str(
                column_config[
                    "max_value"
                ]
            )
        )

        inclusive = column_config.get(
            "max_inclusive",
            True,
        )

        if inclusive:

            if numeric_value > maximum:
                return (
                    f"value {value} exceeds "
                    f"maximum {maximum}"
                )

        else:

            if numeric_value >= maximum:
                return (
                    f"value {value} must be "
                    f"less than {maximum}"
                )

    return None


# =========================================================
# Contract validation
# =========================================================

def validate_against_contract(
    dataset_name: str,
    max_examples: int = 5,
) -> dict[str, Any]:
    """
    Compare a dataset's actual CSV contents
    against its configured data contract.

    Responsibilities:

    - required columns
    - nullability
    - max null rate
    - uniqueness
    - data types
    - numeric ranges
    - basic CSV structural integrity
    """

    config = load_dataset_config(
        dataset_name
    )

    input_config = config[
        "input"
    ]

    file_path = input_config[
        "path"
    ]

    encoding = input_config.get(
        "encoding",
        "utf-8",
    )

    delimiter = input_config.get(
        "delimiter",
        ",",
    )

    expected_columns = config[
        "columns"
    ]

    # -----------------------------------------
    # First observe the actual dataset.
    # -----------------------------------------

    profile = inspect_csv(
        file_path=file_path,
        encoding=encoding,
        delimiter=delimiter,
    )

    actual_columns = set(
        profile["columns"]
    )

    # -----------------------------------------
    # Violation containers
    # -----------------------------------------

    violations = {
        "missing_columns": [],
        "type_violations": [],
        "nullability_violations": [],
        "null_rate_violations": [],
        "uniqueness_violations": [],
        "range_violations": [],
        "structural_violations": [],
    }

    # -----------------------------------------
    # Missing columns
    #
    # Columns are required by default.
    # A future contract may explicitly use:
    #
    # "required": false
    # -----------------------------------------

    for (
        column_name,
        column_config,
    ) in expected_columns.items():

        required = column_config.get(
            "required",
            True,
        )

        if (
            required
            and column_name
            not in actual_columns
        ):

            violations[
                "missing_columns"
            ].append(
                column_name
            )

    # -----------------------------------------
    # Unexpected columns are not automatically
    # considered violations.
    #
    # We report them as information.
    # -----------------------------------------

    unexpected_columns = sorted(
        actual_columns
        - set(
            expected_columns.keys()
        )
    )

    # -----------------------------------------
    # Profile-based rules
    # -----------------------------------------

    for (
        column_name,
        column_config,
    ) in expected_columns.items():

        if column_name not in actual_columns:
            continue

        column_profile = profile[
            "column_profiles"
        ][column_name]

        # -------------------------------------
        # Nullability
        # -------------------------------------

        nullable = column_config.get(
            "nullable",
            True,
        )

        null_count = column_profile[
            "null_count"
        ]

        null_rate = column_profile[
            "null_rate"
        ]

        if (
            not nullable
            and null_count > 0
        ):

            violations[
                "nullability_violations"
            ].append(
                {
                    "column":
                        column_name,

                    "null_count":
                        null_count,

                    "message":
                        (
                            f"{column_name} "
                            f"does not allow nulls"
                        ),
                }
            )

        # -------------------------------------
        # Max null rate
        # -------------------------------------

        if (
            "max_null_rate"
            in column_config
        ):

            max_null_rate = (
                column_config[
                    "max_null_rate"
                ]
            )

            if (
                null_rate
                > max_null_rate
            ):

                violations[
                    "null_rate_violations"
                ].append(
                    {
                        "column":
                            column_name,

                        "actual_null_rate":
                            null_rate,

                        "max_null_rate":
                            max_null_rate,
                    }
                )

        # -------------------------------------
        # Uniqueness
        # -------------------------------------

        if column_config.get(
            "unique",
            False,
        ):

            duplicate_count = (
                column_profile[
                    "duplicate_count"
                ]
            )

            if duplicate_count > 0:

                violations[
                    "uniqueness_violations"
                ].append(
                    {
                        "column":
                            column_name,

                        "duplicate_count":
                            duplicate_count,
                    }
                )

    # -----------------------------------------
    # Structural CSV issues
    # -----------------------------------------

    structural = profile[
        "structural_issues"
    ]

    if structural[
        "duplicate_columns"
    ]:

        violations[
            "structural_violations"
        ].append(
            {
                "type":
                    "duplicate_columns",

                "details":
                    structural[
                        "duplicate_columns"
                    ],
            }
        )

    if (
        structural[
            "rows_with_extra_fields"
        ]
        > 0
    ):

        violations[
            "structural_violations"
        ].append(
            {
                "type":
                    "rows_with_extra_fields",

                "count":
                    structural[
                        "rows_with_extra_fields"
                    ],
            }
        )

    if (
        structural[
            "rows_with_missing_fields"
        ]
        > 0
    ):

        violations[
            "structural_violations"
        ].append(
            {
                "type":
                    "rows_with_missing_fields",

                "count":
                    structural[
                        "rows_with_missing_fields"
                    ],
            }
        )

    # =====================================================
    # Value-level validation
    #
    # Re-read the file in streaming mode.
    #
    # This avoids storing every row in memory just so
    # we can validate types and numeric ranges.
    # =====================================================

    path = _resolve_file_path(
        file_path
    )

    type_errors = {}
    range_errors = {}

    with open(
        path,
        newline="",
        encoding=encoding,
    ) as file:

        reader = csv.DictReader(
            file,
            delimiter=delimiter,
        )

        for row_number, row in enumerate(
            reader,
            start=2,
        ):

            for (
                column_name,
                column_config,
            ) in expected_columns.items():

                if column_name not in actual_columns:
                    continue

                value = row.get(
                    column_name
                )

                # Null rules were already handled
                # by the profile-level checks.
                if _is_null(value):
                    continue

                cleaned_value = (
                    value.strip()
                    if isinstance(
                        value,
                        str,
                    )
                    else value
                )

                expected_type = (
                    column_config[
                        "type"
                    ]
                )

                # ---------------------------------
                # Type validation
                # ---------------------------------

                if not _validate_type(
                    cleaned_value,
                    expected_type,
                    column_config,
                ):

                    error = (
                        type_errors.setdefault(
                            column_name,
                            {
                                "column":
                                    column_name,

                                "expected_type":
                                    expected_type,

                                "invalid_count":
                                    0,

                                "examples":
                                    [],
                            },
                        )
                    )

                    error[
                        "invalid_count"
                    ] += 1

                    if (
                        len(
                            error[
                                "examples"
                            ]
                        )
                        < max_examples
                    ):

                        error[
                            "examples"
                        ].append(
                            {
                                "row":
                                    row_number,

                                "value":
                                    cleaned_value,
                            }
                        )

                    # Don't perform numeric range
                    # validation on a value whose
                    # type is already invalid.
                    continue

                # ---------------------------------
                # Numeric range validation
                # ---------------------------------

                if expected_type in {
                    "integer",
                    "decimal",
                    "number",
                }:

                    range_error = (
                        _check_numeric_range(
                            cleaned_value,
                            column_config,
                        )
                    )

                    if range_error:

                        error = (
                            range_errors.setdefault(
                                column_name,
                                {
                                    "column":
                                        column_name,

                                    "invalid_count":
                                        0,

                                    "examples":
                                        [],
                                },
                            )
                        )

                        error[
                            "invalid_count"
                        ] += 1

                        if (
                            len(
                                error[
                                    "examples"
                                ]
                            )
                            < max_examples
                        ):

                            error[
                                "examples"
                            ].append(
                                {
                                    "row":
                                        row_number,

                                    "value":
                                        cleaned_value,

                                    "reason":
                                        range_error,
                                }
                            )

    violations[
        "type_violations"
    ] = list(
        type_errors.values()
    )

    violations[
        "range_violations"
    ] = list(
        range_errors.values()
    )

    # -----------------------------------------
    # Final result
    # -----------------------------------------

    violation_count = sum(
        len(items)
        for items
        in violations.values()
    )

    return {
        "dataset":
            dataset_name,

        "file_path":
            file_path,

        "valid":
            violation_count == 0,

        "violation_count":
            violation_count,

        "unexpected_columns":
            unexpected_columns,

        "violations":
            violations,
    }


# =========================================================
# CLI test
# =========================================================

if __name__ == "__main__":

    result = validate_against_contract(
        "orders"
    )

    print(
        json.dumps(
            result,
            indent=2,
        )
    )