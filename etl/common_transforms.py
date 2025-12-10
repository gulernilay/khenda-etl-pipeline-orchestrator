"""
common_transforms.py
====================

Shared transformation utilities for ETL pipelines.

This module contains reusable and production-grade transformation
functions that ensure consistency across pipelines.

Provided Features:
------------------
- Column name standardization
- Date normalization (ISO, timestamps → YYYY-MM-DD HH:MM:SS)
- Schema validation for required fields
- Empty row filtering
- Safe type conversion utilities
- Null fallback handler

"""

from datetime import datetime
from typing import List, Dict, Any
from utils.logger import log


# ------------------------------------------------------------
# COLUMN CLEANING
# ------------------------------------------------------------
def clean_column_names(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Standardizes column names for consistency across ETL pipelines.

    - Converts keys to lowercase
    - Replaces spaces with underscores
    - Strips leading/trailing whitespace

    Args:
        data (list[dict]): Raw API payload.

    Returns:
        list[dict]: List with normalized column names.
    """
    cleaned = []

    for row in data:
        new_row = {
            str(k).strip().lower().replace(" ", "_"): v
            for k, v in row.items()
        }
        cleaned.append(new_row)

    return cleaned


# ------------------------------------------------------------
# DATE NORMALIZATION
# ------------------------------------------------------------
def normalize_dates(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Normalize any ISO-like or timestamp-like strings to a consistent
    datetime format: `YYYY-MM-DD HH:MM:SS`.

    Handles formats such as:
        - 2025-01-05T12:33:00Z
        - 2025-01-05 12:33:00
        - 2025/01/05 12:33:00

    Args:
        data (list[dict]): Payload to process.

    Returns:
        list[dict]: Updated dataset with normalized date fields.
    """

    for row in data:
        for key, value in row.items():
            if isinstance(value, str) and any(token in value for token in ["-", "T", "/"]):
                try:
                    parsed = datetime.fromisoformat(value.replace("Z", "").replace("/", "-"))
                    row[key] = parsed.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    # ignore fields not parseable as dates
                    pass

    return data


# ------------------------------------------------------------
# EMPTY ROW FILTERING
# ------------------------------------------------------------
def drop_empty_rows(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove rows where all values are empty, null or blank.

    Useful for API sources returning optional footer records.

    Args:
        data (list[dict]): Raw API data.

    Returns:
        list[dict]: Filtered dataset.
    """

    filtered = [
        row for row in data
        if any(v not in (None, "", [], {}) for v in row.values())
    ]

    return filtered


# ------------------------------------------------------------
# SCHEMA VALIDATION
# ------------------------------------------------------------
def validate_schema(data: List[Dict[str, Any]], required_fields: List[str]):
    """
    Ensures that required fields exist in the dataset.

    Args:
        data (list[dict]): The dataset to validate.
        required_fields (list[str]): Mandatory fields that must appear.

    Raises:
        ValueError: If dataset is empty or required fields are missing.
    """

    if not data:
        raise ValueError("Schema validation failed: dataset is empty.")

    missing = [col for col in required_fields if col not in data[0].keys()]

    if missing:
        raise ValueError(
            f"Schema validation failed: missing required fields → {missing}"
        )

    log("✔ Schema validated successfully.")


# ------------------------------------------------------------
# SAFE TYPE CONVERSION
# ------------------------------------------------------------
def safe_int(value, default=0):
    """
    Safely convert a value to int, preventing exceptions.

    Args:
        value: Any input type.
        default: Fallback if conversion fails.

    Returns:
        int: Safely converted integer.
    """
    try:
        return int(value)
    except Exception:
        return default


def safe_float(value, default=0.0):
    """
    Safely convert a value to float.

    Args:
        value: Any input
        default: Returned if conversion fails.

    Returns:
        float
    """
    try:
        return float(value)
    except Exception:
        return default


# ------------------------------------------------------------
# NULL FALLBACK HANDLER
# ------------------------------------------------------------
def null_to_default(data: List[Dict[str, Any]], defaults: Dict[str, Any]):
    """
    Replace null/empty values using default fallback values per-column.

    Args:
        data (list[dict]): Dataset to update.
        defaults (dict): {"column_name": default_value}

    Returns:
        list[dict]
    """
    for row in data:
        for col, default_val in defaults.items():
            if col in row and (row[col] is None or row[col] == ""):
                row[col] = default_val

    return data
