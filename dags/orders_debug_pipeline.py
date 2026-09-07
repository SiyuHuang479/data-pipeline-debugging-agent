import csv
import os
from datetime import datetime

import pendulum
from airflow.sdk import dag, task


INPUT_FILE = "/opt/airflow/data/input/orders.csv"
STAGING_FILE = "/opt/airflow/data/staging/orders_clean.csv"

REQUIRED_COLUMNS = {
    "order_id",
    "customer_id",
    "order_date",
    "amount",
    "country",
}


@dag(
    dag_id="orders_debug_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-debugging-agent"],
)
def orders_debug_pipeline():

    @task
    def extract_orders():
        """Check that the input file exists and inspect basic metadata."""

        if not os.path.exists(INPUT_FILE):
            raise FileNotFoundError(
                f"Input file not found: {INPUT_FILE}"
            )

        with open(INPUT_FILE, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        print(f"Input file: {INPUT_FILE}")
        print(f"Extracted {len(rows)} orders")

        return {
            "file_path": INPUT_FILE,
            "row_count": len(rows),
        }

    @task
    def validate_orders(extract_result):
        """Validate schema and basic data-quality rules."""

        file_path = extract_result["file_path"]

        with open(file_path, newline="") as f:
            reader = csv.DictReader(f)

            columns = set(reader.fieldnames or [])

            # 1. Required columns
            missing_columns = REQUIRED_COLUMNS - columns

            if missing_columns:
                raise ValueError(
                    f"Schema validation failed. "
                    f"Missing columns: {sorted(missing_columns)}"
                )

            rows = list(reader)

        # 2. Empty dataset
        if not rows:
            raise ValueError("Validation failed: dataset is empty")

        order_ids = []
        null_customer_count = 0

        for row_number, row in enumerate(rows, start=2):

            # order_id must be integer
            try:
                order_id = int(row["order_id"])
                order_ids.append(order_id)
            except (ValueError, TypeError):
                raise ValueError(
                    f"Invalid order_id at CSV row {row_number}: "
                    f"{row['order_id']}"
                )

            # customer_id
            customer_id = row["customer_id"]

            if customer_id is None or customer_id.strip() == "":
                null_customer_count += 1
            else:
                try:
                    int(customer_id)
                except ValueError:
                    raise ValueError(
                        f"Invalid customer_id at CSV row {row_number}: "
                        f"{customer_id}"
                    )

            # amount
            try:
                amount = float(row["amount"])

                if amount <= 0:
                    raise ValueError

            except (ValueError, TypeError):
                raise ValueError(
                    f"Invalid amount at CSV row {row_number}: "
                    f"{row['amount']}"
                )

            # date
            try:
                datetime.strptime(
                    row["order_date"],
                    "%Y-%m-%d",
                )
            except ValueError:
                raise ValueError(
                    f"Invalid order_date at CSV row {row_number}: "
                    f"{row['order_date']}"
                )

        # 3. Duplicate order IDs
        if len(order_ids) != len(set(order_ids)):
            raise ValueError(
                "Data validation failed: duplicate order_id values detected"
            )

        # 4. Customer ID null-rate threshold
        null_rate = null_customer_count / len(rows)

        if null_rate > 0.20:
            raise ValueError(
                f"Data validation failed: customer_id null rate "
                f"is {null_rate:.1%}, exceeding 20% threshold"
            )

        print("Validation passed")
        print(f"Rows validated: {len(rows)}")
        print(f"customer_id null rate: {null_rate:.1%}")

        return file_path

    @task
    def transform_orders(file_path):
        """Normalize the data and create a clean staging CSV."""

        os.makedirs(
            os.path.dirname(STAGING_FILE),
            exist_ok=True,
        )

        cleaned_rows = []

        with open(file_path, newline="") as f:
            reader = csv.DictReader(f)

            for row in reader:
                cleaned_rows.append(
                    {
                        "order_id": int(row["order_id"]),
                        "customer_id": (
                            int(row["customer_id"])
                            if row["customer_id"].strip()
                            else None
                        ),
                        "order_date": row["order_date"],
                        "amount": round(float(row["amount"]), 2),
                        "country": row["country"].strip(),
                    }
                )

        with open(
            STAGING_FILE,
            "w",
            newline="",
        ) as f:

            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "order_id",
                    "customer_id",
                    "order_date",
                    "amount",
                    "country",
                ],
            )

            writer.writeheader()
            writer.writerows(cleaned_rows)

        print(
            f"Transformed {len(cleaned_rows)} rows "
            f"and wrote {STAGING_FILE}"
        )

        return STAGING_FILE

    @task
    def load_orders(staging_file):
        """Load clean orders into the business PostgreSQL database."""

        import psycopg2

        conn = psycopg2.connect(
            host=os.environ["ORDERS_DB_HOST"],
            port=os.environ["ORDERS_DB_PORT"],
            dbname=os.environ["ORDERS_DB_NAME"],
            user=os.environ["ORDERS_DB_USER"],
            password=os.environ["ORDERS_DB_PASSWORD"],
        )

        try:
            cur = conn.cursor()

            # Makes repeated MVP runs idempotent.
            cur.execute("TRUNCATE TABLE orders;")

            with open(staging_file, newline="") as f:
                reader = csv.DictReader(f)

                loaded_count = 0

                for row in reader:
                    cur.execute(
                        """
                        INSERT INTO orders (
                            order_id,
                            customer_id,
                            order_date,
                            amount,
                            country
                        )
                        VALUES (%s, %s, %s, %s, %s);
                        """,
                        (
                            int(row["order_id"]),
                            int(row["customer_id"]),
                            row["order_date"],
                            float(row["amount"]),
                            row["country"],
                        ),
                    )

                    loaded_count += 1

            conn.commit()

            print(
                f"Successfully loaded {loaded_count} "
                f"orders into PostgreSQL"
            )

        finally:
            cur.close()
            conn.close()

    extracted = extract_orders()
    validated = validate_orders(extracted)
    transformed = transform_orders(validated)
    load_orders(transformed)


orders_debug_pipeline()