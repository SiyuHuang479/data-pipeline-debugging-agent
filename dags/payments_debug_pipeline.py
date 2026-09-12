import csv
import os
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg2

from airflow.sdk import dag, task


# =========================================================
# Pipeline configuration
# =========================================================

INPUT_FILE = os.getenv(
    "PAYMENTS_INPUT_PATH",
    "/opt/airflow/data/input/payments.csv",
)

TRANSFORMED_FILE = os.getenv(
    "PAYMENTS_TRANSFORMED_PATH",
    "/opt/airflow/data/processed/payments_transformed.csv",
)


DB_HOST = os.getenv(
    "ORDERS_DB_HOST",
    "orders-db",
)

DB_PORT = int(
    os.getenv(
        "ORDERS_DB_PORT",
        "5432",
    )
)

DB_NAME = os.getenv(
    "ORDERS_DB_NAME",
    "orders_db",
)

DB_USER = os.getenv(
    "ORDERS_DB_USER",
    "orders_user",
)

DB_PASSWORD = os.getenv(
    "ORDERS_DB_PASSWORD",
    "orders_password",
)


# =========================================================
# Payments DAG
# =========================================================

@dag(
    dag_id="payments_debug_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[
        "debugging-agent",
        "payments",
    ],
)
def payments_debug_pipeline():

    # =====================================================
    # 1. Extract
    # =====================================================

    @task
    def extract_payments() -> str:
        """
        Verify that the payments input file exists
        and contains at least one data row.
        """

        path = Path(
            INPUT_FILE
        )

        if not path.exists():
            raise FileNotFoundError(
                f"Payments input file not found: "
                f"{INPUT_FILE}"
            )

        if not path.is_file():
            raise ValueError(
                f"Payments input path is not a file: "
                f"{INPUT_FILE}"
            )

        with open(
            path,
            newline="",
            encoding="utf-8",
        ) as file:

            reader = csv.DictReader(
                file
            )

            rows = list(
                reader
            )

        if not rows:
            raise ValueError(
                "Payments input file contains no data rows"
            )

        print(
            f"Extracted {len(rows)} payment rows "
            f"from {INPUT_FILE}"
        )

        return INPUT_FILE

    # =====================================================
    # 2. Validate
    # =====================================================

    @task
    def validate_payments(
        input_file: str,
    ) -> str:
        """
        Validate payments-specific schema and data rules.

        Expected rules:

        - required columns exist
        - payment_id is non-null and unique
        - user_id is integer when present
        - user_id null rate <= 20%
        - payment_time is ISO datetime
        - amount is numeric and > 0
        - currency is non-null
        - method is non-null
        """

        required_columns = {
            "payment_id",
            "user_id",
            "payment_time",
            "amount",
            "currency",
            "method",
        }

        with open(
            input_file,
            newline="",
            encoding="utf-8",
        ) as file:

            reader = csv.DictReader(
                file
            )

            actual_columns = set(
                reader.fieldnames
                or []
            )

            # ---------------------------------------------
            # Required column validation
            # ---------------------------------------------

            missing_columns = (
                required_columns
                - actual_columns
            )

            if missing_columns:

                raise ValueError(
                    "Schema validation failed. "
                    "Missing columns: "
                    f"{sorted(missing_columns)}"
                )

            rows = list(
                reader
            )

        if not rows:

            raise ValueError(
                "Payments dataset contains no rows"
            )

        # ---------------------------------------------
        # Track values needed for dataset-level rules
        # ---------------------------------------------

        payment_ids = []

        null_user_id_count = 0

        # ---------------------------------------------
        # Row-level validation
        #
        # start=2 because row 1 is the CSV header.
        # ---------------------------------------------

        for row_number, row in enumerate(
            rows,
            start=2,
        ):

            # -----------------------------------------
            # payment_id
            # -----------------------------------------

            payment_id = (
                row.get(
                    "payment_id",
                    "",
                )
                .strip()
            )

            if not payment_id:

                raise ValueError(
                    "Invalid payment_id at "
                    f"CSV row {row_number}: "
                    "value is null or empty"
                )

            payment_ids.append(
                payment_id
            )

            # -----------------------------------------
            # user_id
            #
            # Null values are allowed, but the total
            # null rate must not exceed 20%.
            # -----------------------------------------

            user_id = (
                row.get(
                    "user_id",
                    "",
                )
                .strip()
            )

            if not user_id:

                null_user_id_count += 1

            else:

                try:
                    int(
                        user_id
                    )

                except ValueError as exc:

                    raise ValueError(
                        "Invalid user_id at "
                        f"CSV row {row_number}: "
                        f"{user_id}"
                    ) from exc

            # -----------------------------------------
            # payment_time
            # -----------------------------------------

            payment_time = (
                row.get(
                    "payment_time",
                    "",
                )
                .strip()
            )

            if not payment_time:

                raise ValueError(
                    "Invalid payment_time at "
                    f"CSV row {row_number}: "
                    "value is null or empty"
                )

            try:
                datetime.fromisoformat(
                    payment_time
                )

            except ValueError as exc:

                raise ValueError(
                    "Invalid payment_time at "
                    f"CSV row {row_number}: "
                    f"{payment_time}"
                ) from exc

            # -----------------------------------------
            # amount
            # -----------------------------------------

            amount = (
                row.get(
                    "amount",
                    "",
                )
                .strip()
            )

            if not amount:

                raise ValueError(
                    "Invalid amount at "
                    f"CSV row {row_number}: "
                    "value is null or empty"
                )

            try:
                amount_value = Decimal(
                    amount
                )

            except InvalidOperation as exc:

                raise ValueError(
                    "Invalid amount at "
                    f"CSV row {row_number}: "
                    f"{amount}"
                ) from exc

            if amount_value <= 0:

                raise ValueError(
                    "Invalid amount at "
                    f"CSV row {row_number}: "
                    f"{amount}. "
                    "Amount must be greater than 0."
                )

            # -----------------------------------------
            # currency
            # -----------------------------------------

            currency = (
                row.get(
                    "currency",
                    "",
                )
                .strip()
            )

            if not currency:

                raise ValueError(
                    "Invalid currency at "
                    f"CSV row {row_number}: "
                    "value is null or empty"
                )

            # -----------------------------------------
            # method
            # -----------------------------------------

            method = (
                row.get(
                    "method",
                    "",
                )
                .strip()
            )

            if not method:

                raise ValueError(
                    "Invalid method at "
                    f"CSV row {row_number}: "
                    "value is null or empty"
                )

        # ---------------------------------------------
        # payment_id uniqueness
        # ---------------------------------------------

        seen = set()

        duplicate_payment_ids = set()

        for payment_id in payment_ids:

            if payment_id in seen:
                duplicate_payment_ids.add(
                    payment_id
                )

            seen.add(
                payment_id
            )

        if duplicate_payment_ids:

            raise ValueError(
                "Data validation failed: "
                "duplicate payment_id values detected: "
                f"{sorted(duplicate_payment_ids)}"
            )

        # ---------------------------------------------
        # user_id null-rate validation
        # ---------------------------------------------

        null_rate = (
            null_user_id_count
            / len(rows)
        )

        max_null_rate = 0.20

        if null_rate > max_null_rate:

            raise ValueError(
                "Data validation failed: "
                "user_id null rate is "
                f"{null_rate:.1%}, "
                "exceeding 20% threshold"
            )

        print(
            "Payments validation passed."
        )

        print(
            f"Rows: {len(rows)}"
        )

        print(
            f"user_id null rate: "
            f"{null_rate:.1%}"
        )

        return input_file

    # =====================================================
    # 3. Transform
    # =====================================================

    @task
    def transform_payments(
        input_file: str,
    ) -> str:
        """
        Apply small deterministic transformations.

        This gives us a realistic transform step without
        making the demo unnecessarily complicated.
        """

        output_path = Path(
            TRANSFORMED_FILE
        )

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        transformed_rows = []

        with open(
            input_file,
            newline="",
            encoding="utf-8",
        ) as file:

            reader = csv.DictReader(
                file
            )

            for row in reader:

                user_id = (
                    row[
                        "user_id"
                    ].strip()
                )

                amount = Decimal(
                    row[
                        "amount"
                    ].strip()
                )

                transformed_rows.append(
                    {
                        "payment_id":
                            row[
                                "payment_id"
                            ].strip(),

                        "user_id":
                            user_id
                            if user_id
                            else "",

                        "payment_time":
                            row[
                                "payment_time"
                            ].strip(),

                        "amount":
                            f"{amount:.2f}",

                        "currency":
                            row[
                                "currency"
                            ]
                            .strip()
                            .upper(),

                        "method":
                            row[
                                "method"
                            ]
                            .strip()
                            .lower(),
                    }
                )

        fieldnames = [
            "payment_id",
            "user_id",
            "payment_time",
            "amount",
            "currency",
            "method",
        ]

        with open(
            output_path,
            "w",
            newline="",
            encoding="utf-8",
        ) as file:

            writer = csv.DictWriter(
                file,
                fieldnames=fieldnames,
            )

            writer.writeheader()

            writer.writerows(
                transformed_rows
            )

        print(
            f"Transformed "
            f"{len(transformed_rows)} "
            f"payment rows."
        )

        print(
            f"Output written to "
            f"{output_path}"
        )

        return str(
            output_path
        )

    # =====================================================
    # 4. Load
    # =====================================================

    @task
    def load_payments(
        transformed_file: str,
    ) -> int:
        """
        Load transformed payments into PostgreSQL.

        For this synthetic debugging project we use:

            TRUNCATE + INSERT

        so every DAG run starts with deterministic table
        contents.
        """

        connection = None

        try:

            connection = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
            )

            with connection.cursor() as cursor:

                cursor.execute(
                    "TRUNCATE TABLE payments;"
                )

                rows_loaded = 0

                with open(
                    transformed_file,
                    newline="",
                    encoding="utf-8",
                ) as file:

                    reader = csv.DictReader(
                        file
                    )

                    for row in reader:

                        user_id = (
                            row[
                                "user_id"
                            ].strip()
                        )

                        parsed_user_id = (
                            int(user_id)
                            if user_id
                            else None
                        )

                        payment_time = (
                            datetime.fromisoformat(
                                row[
                                    "payment_time"
                                ]
                            )
                        )

                        amount = Decimal(
                            row[
                                "amount"
                            ]
                        )

                        cursor.execute(
                            """
                            INSERT INTO payments (
                                payment_id,
                                user_id,
                                payment_time,
                                amount,
                                currency,
                                method
                            )
                            VALUES (
                                %s,
                                %s,
                                %s,
                                %s,
                                %s,
                                %s
                            );
                            """,
                            (
                                row[
                                    "payment_id"
                                ],
                                parsed_user_id,
                                payment_time,
                                amount,
                                row[
                                    "currency"
                                ],
                                row[
                                    "method"
                                ],
                            ),
                        )

                        rows_loaded += 1

            connection.commit()

            print(
                f"Loaded {rows_loaded} "
                f"rows into payments table."
            )

            return rows_loaded

        except Exception:

            if connection:
                connection.rollback()

            raise

        finally:

            if connection:
                connection.close()

    # =====================================================
    # Task dependencies
    # =====================================================

    extracted_file = (
        extract_payments()
    )

    validated_file = (
        validate_payments(
            extracted_file
        )
    )

    transformed_file = (
        transform_payments(
            validated_file
        )
    )

    load_payments(
        transformed_file
    )


payments_debug_pipeline()