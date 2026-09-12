import json
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(
    __file__
).resolve().parents[1]

CONFIG_DIR = (
    PROJECT_ROOT
    / "configs"
)


def load_dataset_config(
    dataset_name: str,
) -> dict[str, Any]:
    """
    Load and validate the data contract
    for one dataset.
    """

    config_path = (
        CONFIG_DIR
        / f"{dataset_name}.json"
    )

    if not config_path.exists():
        raise FileNotFoundError(
            f"Dataset config not found: "
            f"{config_path}"
        )

    with open(
        config_path,
        encoding="utf-8",
    ) as f:
        config = json.load(f)

    # -----------------------------------------
    # Validate top-level fields
    # -----------------------------------------

    required_top_level_fields = {
        "name",
        "dag_id",
        "input",
        "database",
        "columns",
    }

    missing_top_level_fields = (
        required_top_level_fields
        - config.keys()
    )

    if missing_top_level_fields:
        raise ValueError(
            "Dataset config is missing top-level fields: "
            f"{sorted(missing_top_level_fields)}"
        )

    # -----------------------------------------
    # Validate input section
    # -----------------------------------------

    input_config = config["input"]

    required_input_fields = {
        "type",
        "path",
    }

    missing_input_fields = (
        required_input_fields
        - input_config.keys()
    )

    if missing_input_fields:
        raise ValueError(
            "Dataset config input section is missing fields: "
            f"{sorted(missing_input_fields)}"
        )

    # -----------------------------------------
    # Validate database section
    # -----------------------------------------

    database_config = config[
        "database"
    ]

    required_database_fields = {
        "type",
        "table",
    }

    missing_database_fields = (
        required_database_fields
        - database_config.keys()
    )

    if missing_database_fields:
        raise ValueError(
            "Dataset config database section is missing fields: "
            f"{sorted(missing_database_fields)}"
        )

    # -----------------------------------------
    # Validate columns section
    # -----------------------------------------

    columns = config["columns"]

    if not isinstance(
        columns,
        dict,
    ) or not columns:
        raise ValueError(
            "Dataset config 'columns' must be "
            "a non-empty object."
        )

    for column_name, column_config in columns.items():

        if not isinstance(
            column_config,
            dict,
        ):
            raise ValueError(
                f"Column config for "
                f"'{column_name}' must be an object."
            )

        if "type" not in column_config:
            raise ValueError(
                f"Column '{column_name}' "
                f"is missing required field 'type'."
            )

    return config


if __name__ == "__main__":

    config = load_dataset_config(
        "orders"
    )

    print(
        json.dumps(
            config,
            indent=2,
        )
    )