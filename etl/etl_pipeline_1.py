"""
etl_pipeline_2.py
=================

Production ETL pipeline for API Source 2.

Responsibilities:
    1. Extraction:
        - Fetch data from a paginated API endpoint
        - Handle throttling, retry, and network resilience
    2. Transformation:
        - Drop empty/invalid records
        - Normalize date fields
        - Validate mandatory schema fields
    3. Load:
        - Insert processed data into SQL Server target table

Design Principles:
    - Idempotency (database handles merge/insert logic)
    - Fail-fast but informative logging
    - Clear separation of extraction, transformation and load steps
    - Consistent transformation rules across pipelines

"""

from services.api_client_2 import fetch_api_2_data
from services.db_service import insert_into_table_2
from etl.common_transforms import (
    drop_empty_rows,
    normalize_dates,
    validate_schema,
    clean_column_names
)
from utils.logger import log


def run_pipeline(params: dict = None):
    """
    Execute ETL Pipeline 2 with API pagination-aware extraction.

    Args:
        params (dict, optional):
            Query parameters such as date filters:
                {"from": "2025-01-01", "to": "2025-01-31"}

    Returns:
        int: Number of successfully inserted rows.

    Raises:
        RuntimeError: For extraction or transformation failures.
    """

    log(" [ETL2] Pipeline 2 started")

    try:
        # ------------------------------------------------------------
        # 1. EXTRACT
        # ------------------------------------------------------------
        raw_data = fetch_api_2_data(params)
        log(f" [ETL2] Extracted {len(raw_data)} raw records from API 2")

        if not raw_data:
            log(" [ETL2] No data found from API 2. Pipeline ending cleanly.")
            return 0

        # ------------------------------------------------------------
        # 2. TRANSFORM
        # ------------------------------------------------------------
        cleaned = clean_column_names(raw_data)
        cleaned = drop_empty_rows(cleaned)
        cleaned = normalize_dates(cleaned)

        validate_schema(
            cleaned,
            required_fields=[
                "record_id",
                "status",
                "timestamp"
            ]
        )

        log(f" [ETL2] Transformation completed. Valid rows: {len(cleaned)}")

        # ------------------------------------------------------------
        # 3. LOAD
        # ------------------------------------------------------------
        inserted = insert_into_table_2(cleaned)
        log(f" [ETL2] Inserted {inserted} records into TABLE_2")

        log(" [ETL2] Pipeline 2 completed successfully")
        return inserted

    except Exception as exc:
        log(f" [ETL2] Pipeline 2 failed: {exc}")
        raise RuntimeError(f"ETL Pipeline 2 failed: {exc}")
