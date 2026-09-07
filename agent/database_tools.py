import os
import re
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor


DB_HOST = os.getenv("ORDERS_DB_HOST", "localhost")
DB_PORT = int(os.getenv("ORDERS_DB_PORT", "5433"))
DB_NAME = os.getenv("ORDERS_DB_NAME", "orders_db")
DB_USER = os.getenv("ORDERS_DB_USER", "orders_user")
DB_PASSWORD = os.getenv("ORDERS_DB_PASSWORD", "orders_password")


def _connect():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def get_table_schema(table_name: str) -> dict[str, Any]:
    """
    Return columns, types, nullability, and constraints for a table.
    """

    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
        raise ValueError("Invalid table name")

    conn = _connect()

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            cur.execute(
                """
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s
                ORDER BY ordinal_position;
                """,
                (table_name,),
            )

            columns = cur.fetchall()

            cur.execute(
                """
                SELECT
                    tc.constraint_type,
                    kcu.column_name
                FROM information_schema.table_constraints tc
                LEFT JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                WHERE tc.table_schema = 'public'
                  AND tc.table_name = %s;
                """,
                (table_name,),
            )

            constraints = cur.fetchall()

        if not columns:
            raise ValueError(
                f"Table '{table_name}' does not exist"
            )

        return {
            "table": table_name,
            "columns": [dict(row) for row in columns],
            "constraints": [dict(row) for row in constraints],
        }

    finally:
        conn.close()


def run_sql(
    query: str,
    max_rows: int = 50,
) -> dict[str, Any]:
    """
    Execute a read-only SQL query.

    V1 safety rules:
    - SELECT only
    - no multiple statements
    - result capped at max_rows
    """

    cleaned = query.strip()

    if not cleaned:
        raise ValueError("Query cannot be empty")

    # Prevent multiple SQL statements.
    if ";" in cleaned.rstrip(";"):
        raise ValueError(
            "Multiple SQL statements are not allowed"
        )

    # Remove final semicolon for easier validation.
    cleaned = cleaned.rstrip(";").strip()

    first_word = cleaned.split(maxsplit=1)[0].upper()

    if first_word not in {"SELECT", "WITH"}:
        raise ValueError(
            "Only read-only SELECT queries are allowed"
        )

    dangerous_keywords = {
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "ALTER",
        "TRUNCATE",
        "CREATE",
        "GRANT",
        "REVOKE",
        "COPY",
    }

    upper_query = cleaned.upper()

    for keyword in dangerous_keywords:
        if re.search(rf"\b{keyword}\b", upper_query):
            raise ValueError(
                f"Disallowed SQL keyword: {keyword}"
            )

    conn = _connect()

    try:
        conn.set_session(readonly=True)

        with conn.cursor(
            cursor_factory=RealDictCursor
        ) as cur:

            # Avoid a generated query running forever.
            cur.execute(
                "SET LOCAL statement_timeout = '5s';"
            )

            cur.execute(cleaned)

            rows = cur.fetchmany(max_rows + 1)

            truncated = len(rows) > max_rows
            rows = rows[:max_rows]

            return {
                "query": cleaned,
                "row_count_returned": len(rows),
                "truncated": truncated,
                "rows": [dict(row) for row in rows],
            }

    finally:
        conn.close()


if __name__ == "__main__":

    import json

    print("\n=== ORDERS TABLE SCHEMA ===")

    schema = get_table_schema("orders")

    print(
        json.dumps(
            schema,
            indent=2,
            default=str,
        )
    )

    print("\n=== SAMPLE QUERY ===")

    result = run_sql(
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

    print(
        json.dumps(
            result,
            indent=2,
            default=str,
        )
    )