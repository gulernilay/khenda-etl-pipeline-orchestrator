"""
etl_pipeline_2.py
=================

Production ETL pipeline for API Source 2.

Responsibilities:
    1. Extraction:
        - Fetch data from a paginated API endpoint
        - Filter results by a date window (date_from → date_to)
    2. Transformation:
        - Drop empty/invalid records
        - Normalize date fields
        - Validate mandatory schema fields
    3. Load:
        - Insert processed data into SQL Server target table (khenda_hygiene)

Author: Chef Seasons – Data Engineering Team
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


def run_pipeline(date_from: str, date_to: str):
    """
    Execute ETL Pipeline 2 for a specific date range.

    Args:
        date_from (str): Start date (YYYY-MM-DD)
        date_to (str): End date (YYYY-MM-DD)

    Returns:
        int: Number of successfully inserted/updated rows.

    Raises:
        RuntimeError: For extraction or transformation failures.
    """

    log(f"🚀 [ETL2] Pipeline 2 started for window {date_from} → {date_to}")

    try:
        # ------------------------------------------------------------
        # 1. EXTRACT
        # ------------------------------------------------------------
        params = {
            "from": date_from,
            "to": date_to
        }

        raw_data = fetch_api_2_data(params=params)
        log(f"📥 [ETL2] Extracted {len(raw_data)} raw records from API 2")

        if not raw_data:
            log("⚠️ [ETL2] No data found from API 2 for this window. Pipeline ending cleanly.")
            return 0

        # ------------------------------------------------------------
        # 2. TRANSFORM
        # ------------------------------------------------------------
        cleaned = clean_column_names(raw_data)
        cleaned = drop_empty_rows(cleaned)
        cleaned = normalize_dates(cleaned)

        # hygiene tablosu için expected schema:
        validate_schema(
            cleaned,
            required_fields=[
                "id",
                "hygieneid",
                "datetime",
                "valid",
                "duration"
            ]
        )

        log(f"🔧 [ETL2] Transformation completed. Valid rows: {len(cleaned)}")

        # ------------------------------------------------------------
        # 3. LOAD
        # ------------------------------------------------------------
        inserted = insert_into_table_2(cleaned)
        log(f"💾 [ETL2] Inserted/updated approx. {inserted} records into khenda_hygiene")

        log("✅ [ETL2] Pipeline 2 completed successfully")
        return inserted

    except Exception as exc:
        log(f"❌ [ETL2] Pipeline 2 failed: {exc}", level="error")
        raise RuntimeError(f"ETL Pipeline 2 failed: {exc}")
