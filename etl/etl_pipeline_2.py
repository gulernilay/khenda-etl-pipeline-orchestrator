"""
etl_pipeline_1.py
=================

Production ETL pipeline for API Source 1.

This pipeline performs:
1. Extraction: Pull raw data from API Client 1
2. Transformation: Apply standardized cleaning & validation
3. Loading: Persist processed data into SQL Server

Core Objectives:
- Ensure robust data quality
- Prevent malformed payloads from reaching the database
- Provide detailed logging for audit & debugging
- Maintain idempotent ETL behavior (DB-side MERGE or INSERT logic)


"""

from services.api_client_1 import fetch_api_1_data
from services.db_service import insert_into_table_1
from etl.common_transforms import (
    clean_column_names,
    normalize_dates,
    drop_empty_rows,
    validate_schema
)
from utils.logger import log


def run_pipeline():
    """
    Execute ETL Pipeline 1 with full error isolation and logging.

    Returns:
        int: Number of rows successfully inserted into SQL Server.

    Raises:
        RuntimeError: If extraction, transformation or loading fails.
    """

    log(" [ETL1] Pipeline 1 started")

    try:
        # ------------------------------------------------------------
        # 1. EXTRACT
        # ------------------------------------------------------------
        raw_data = fetch_api_1_data()
        log(f" [ETL1] Extracted {len(raw_data)} raw records from API 1")

        # ------------------------------------------------------------
        # 2. TRANSFORM
        # ------------------------------------------------------------
        if not raw_data:
            log(" [ETL1] No data returned from API 1. Pipeline will end.")
            return 0

        cleaned = clean_column_names(raw_data)
        cleaned = drop_empty_rows(cleaned)
        cleaned = normalize_dates(cleaned)

        validate_schema(
            cleaned,
            required_fields=["id", "name", "created_at"]
        )

        log(f" [ETL1] Transformation phase completed. Total usable rows: {len(cleaned)}")

        # ------------------------------------------------------------
        # 3. LOAD
        # ------------------------------------------------------------
        inserted_count = insert_into_table_1(cleaned)
        log(f" [ETL1] Successfully inserted {inserted_count} rows into TABLE_1")

        log(" [ETL1] Pipeline 1 completed successfully")
        return inserted_count

    except Exception as exc:
        # Critical error, pipeline fails
        log(f" [ETL1] Pipeline 1 failed: {exc}")
        raise RuntimeError(f"ETL Pipeline 1 failed: {exc}")

