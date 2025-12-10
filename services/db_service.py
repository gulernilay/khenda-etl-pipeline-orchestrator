"""
db_service.py
=============

SQL Server database interaction layer using pyodbc.

This module provides:
- Robust SQL Server connection handling
- Fast bulk insert operations with executemany()
- Dynamic column-agnostic insert logic
- Reliable error handling for ETL pipelines
- Centralized DB service for all pipelines


"""

import pyodbc
from typing import List, Dict, Any
from config.settings import (
    DB_SERVER, DB_DATABASE, DB_USERNAME, DB_PASSWORD
)
from utils.logger import log


# ------------------------------------------------------------
# CONNECTION STRING (Trusted, Pooled, Fast)
# ------------------------------------------------------------
CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={DB_SERVER};"
    f"DATABASE={DB_DATABASE};"
    f"UID={DB_USERNAME};"
    f"PWD={DB_PASSWORD};"
    "TrustServerCertificate=yes;"
)


# ------------------------------------------------------------
# CONNECTION MANAGEMENT
# ------------------------------------------------------------
def _get_connection():
    """
    Opens a SQL Server connection using pyodbc.

    Returns:
        pyodbc.Connection: Active connection object.

    Raises:
        RuntimeError: On connection failure.
    """
    try:
        conn = pyodbc.connect(CONNECTION_STRING, autocommit=True)
        return conn
    except Exception as exc:
        raise RuntimeError(f"Database connection failed via pyodbc: {exc}")


# ------------------------------------------------------------
# GENERIC DYNAMIC INSERT
# ------------------------------------------------------------
def _insert_dynamic(table_name: str, rows: List[Dict[str, Any]]) -> int:
    """
    Dynamically inserts rows into a SQL Server table via pyodbc.
    Supports high-performance bulk insert with fast_executemany.

    Args:
        table_name (str): Fully qualified table name (schema.table).
        rows (list[dict]): Array of record dictionaries.

    Returns:
        int: Number of successfully inserted rows.

    Raises:
        RuntimeError: If insert operation fails.
    """

    if not rows:
        return 0

    columns = list(rows[0].keys())
    column_list = ", ".join([f"[{c}]" for c in columns])
    placeholders = ", ".join(["?"] * len(columns))

    sql = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"

    try:
        conn = _get_connection()
        cursor = conn.cursor()

        # Activate bulk optimization mode
        cursor.fast_executemany = True

        values = [tuple(row[col] for col in columns) for row in rows]

        cursor.executemany(sql, values)

        log(f"💾 Insert completed into {table_name} → {len(rows)} rows")

        cursor.close()
        conn.close()

        return len(rows)

    except Exception as exc:
        raise RuntimeError(
            f"Insert operation failed for table {table_name} via pyodbc: {exc}"
        )


# ------------------------------------------------------------
# TABLE-SPECIFIC ENTRY POINTS
# ------------------------------------------------------------
def insert_into_table_1(rows: List[Dict[str, Any]]) -> int:
    """
    Inserts data from ETL Pipeline 1 into SQL Table 1.

    Args:
        rows (list[dict])

    Returns:
        int: Number of rows inserted.
    """
    TABLE_NAME = "ChefsAI.dbo.Table1_ETL"
    return _insert_dynamic(TABLE_NAME, rows)


def insert_into_table_2(rows: List[Dict[str, Any]]) -> int:
    """
    Inserts data from ETL Pipeline 2 into khenda_hygiene table.

    Args:
        rows (list[dict])

    Returns:
        int: Number of rows inserted.
    """
    TABLE_NAME = "ChefsAI.dbo.khenda_hygiene"
    return _insert_dynamic(TABLE_NAME, rows)
