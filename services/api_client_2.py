"""
api_client_2.py
===============

Production-grade API client for Source 2.

Features:
- Pagination support (page=1,2,3...)
- Date range filtering: ?from=YYYY-MM-DD&to=YYYY-MM-DD
- Retry mechanism for network/5xx errors
- Timeout protection
- Structured logging
- JSON parsing validation

Author: Chef Seasons – Data Engineering Team
"""

import requests
import time
from typing import Dict, List, Any, Optional
from config.settings import API_2_URL, API_2_TOKEN
from utils.logger import log


# -----------------------------
# CONFIG
# -----------------------------
TIMEOUT_SECONDS = 15
MAX_RETRY = 3
RETRY_DELAY_SECONDS = 2
PAGE_SIZE = 200  # API limitine göre ayarlanabilir


# -----------------------------
# HEADERS
# -----------------------------
def _build_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {API_2_TOKEN}",
        "Accept": "application/json",
        "User-Agent": "ChefSeasons-ETL-Agent/1.0"
    }


# -----------------------------
# MAIN FETCH FUNCTION
# -----------------------------
def fetch_api_2_data(params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Fetch data from API 2 with pagination and date filtering.

    Example params:
        {
            "from": "2025-12-01",
            "to": "2025-12-07"
        }

    Behavior:
        - Keeps requesting pages until API returns empty list.
        - Automatically merges all pages into a single dataset.

    Returns:
        list[dict]: Combined data from all pages.

    Raises:
        RuntimeError: On repeated failures.
    """

    headers = _build_headers()
    all_data: List[Dict[str, Any]] = []

    page = 1
    params = params.copy() if params else {}
    params["pageSize"] = PAGE_SIZE

    while True:
        params["page"] = page

        for attempt in range(1, MAX_RETRY + 1):
            try:
                log(f"🌐 [API2] Fetching page {page} with params {params}")

                response = requests.get(
                    API_2_URL,
                    headers=headers,
                    params=params,
                    timeout=TIMEOUT_SECONDS
                )

                # SUCCESS
                if response.status_code == 200:
                    try:
                        data = response.json()
                    except Exception:
                        raise RuntimeError("API 2 returned invalid JSON")

                    row_count = len(data)
                    log(f"📥 [API2] Page {page} returned {row_count} records.")

                    if row_count == 0:
                        log("📘 [API2] No more pages. Pagination completed.")
                        return all_data

                    all_data.extend(data)
                    break  # exit retry loop → go to next page

                # RETRYABLE ERRORS
                if response.status_code in (429, 500, 502, 503, 504):
                    log(f"⚠️ [API2] Retryable error {response.status_code}, waiting and retrying...")
                    time.sleep(RETRY_DELAY_SECONDS)
                    continue

                # NON-RETRYABLE
                raise RuntimeError(f"API 2 failed with HTTP {response.status_code}: {response.text}")

            except requests.Timeout:
                log(f"⏳ [API2] Timeout on page {page}, retrying...")
                time.sleep(RETRY_DELAY_SECONDS)
                continue

            except requests.RequestException as e:
                log(f"❌ [API2] Network error: {e}, retrying...")
                time.sleep(RETRY_DELAY_SECONDS)
                continue

        else:
            # retry loop exhausted
            raise RuntimeError(f"API 2: Maximum retry attempts exceeded on page {page}")

        # next page
        page += 1
