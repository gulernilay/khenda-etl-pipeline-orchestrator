"""
etl_pipeline_1.py
=================

Production ETL pipeline for API Source 1.

This pipeline performs:
1. Extraction:
    - Pull raw data from API Client 1
    - Filtered by a provided date window (date_from → date_to, last 7 days)
2. Transformation:
    - Apply standardized cleaning & validation
3. Loading:
    - Persist processed data into SQL Server via dynamic insert/upsert

Author: Chef Seasons – Data Engineering Team
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


def run_pipeline(date_from: str, date_to: str):
    """
    Execute ETL Pipeline 1 for a specific date range.

    The date range is expected to cover the last 7 calendar days
    including the current day. Example:

        date_from = "2025-12-01"
        date_to   = "2025-12-07"

    Args:
        date_from (str): Start date (YYYY-MM-DD)
        date_to (str): End date (YYYY-MM-DD)

    Returns:
        int: Number of rows successfully inserted/updated in SQL Server.

    Raises:
        RuntimeError: If extraction, transformation or loading fails.
    """

    log(f"🚀 [ETL1] Pipeline 1 started for window {date_from} → {date_to}")

    try:
        # ------------------------------------------------------------
        # 1. EXTRACT
        # ------------------------------------------------------------
        raw_data = fetch_api_1_data(date_from=date_from, date_to=date_to)
        log(f"📥 [ETL1] Extracted {len(raw_data)} raw records from API 1")

        # ------------------------------------------------------------
        # 2. TRANSFORM
        # ------------------------------------------------------------
        if not raw_data:
            log("⚠️ [ETL1] No data returned from API 1 for this window. Pipeline will end.")
            return 0

        cleaned = clean_column_names(raw_data)
        cleaned = drop_empty_rows(cleaned)
        cleaned = normalize_dates(cleaned)

        # Schema fields, API → DB mapping varsayımsal:
        validate_schema(
            cleaned,
            required_fields=[
                "id",
                "lineid",
                "isemrino",
                "tarih",
                "musteri",
                "urunkodu",
                "urunadi",
                "partino"
            ]
        )

        log(f"🔧 [ETL1] Transformation phase completed. Total usable rows: {len(cleaned)}")

        # ------------------------------------------------------------
        # 3. LOAD
        # ------------------------------------------------------------
        # Not: Güncelleme davranışı DB tarafında MERGE/UPSERT logic ile sağlanır.
        inserted_count = insert_into_table_1(cleaned)
        log(f"💾 [ETL1] Successfully inserted/updated approx. {inserted_count} rows into Pipeline 1 target table")

        log("✅ [ETL1] Pipeline 1 completed successfully")
        return inserted_count

    except Exception as exc:
        log(f"❌ [ETL1] Pipeline 1 failed: {exc}", level="error")
        raise RuntimeError(f"ETL Pipeline 1 failed: {exc}")
