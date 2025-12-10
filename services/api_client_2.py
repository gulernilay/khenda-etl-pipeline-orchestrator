"""
api_client_2.py
===============

Production-grade API client for Source 2.

This module includes:
- Authenticated GET requests
- Query parameter support
- Pagination handling (if API returns paginated responses)
- Retry strategy for throttling and network issues
- Timeout control
- Safe JSON parsing and structured error reporting

"""

import requests
import time
from typing import List, Dict, Any, Optional
from config.settings import API_2_URL, API_2_TOKEN
from utils.logger import log


# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------
TIMEOUT_SECONDS = 20
MAX_RETRY = 3
RETRY_DELAY_SECONDS = 2


# ------------------------------------------------------------
# HEADER BUILDER
# ------------------------------------------------------------
def _build_headers() -> Dict[str, str]:
    """
    Construct request headers including authentication token.

    Returns:
        dict: Standard HTTP headers.
    """
    return {
        "Authorization": f"Bearer {API_2_TOKEN}",
        "Accept": "application/json",
        "User-Agent": "ChefSeasons-ETL-Agent/1.0"
    }


# ------------------------------------------------------------
# PAGINATION HANDLER
# ------------------------------------------------------------
def _extract_pagination_info(json_data: Dict[str, Any]) -> Optional[str]:
    """
    Extract next-page URL from API response.

    Expected API structure example:
    {
        "data": [...],
        "next": "https://api.example.com/resource?page=2"
    }

    Args:
        json_data (dict): Full API response.

    Returns:
        Optional[str]: URL to next page if exists.
    """
    if isinstance(json_data, dict) and "next" in json_data:
        return json_data["next"]

    return None


# ------------------------------------------------------------
# MAIN FETCH FUNCTION
# ------------------------------------------------------------
def fetch_api_2_data(params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """
    Fetch data from API Source 2 with support for pagination
    and retry-aware network safety.

    Args:
        params (dict, optional): Query parameters to attach.

    Returns:
        list[dict]: Combined data from all pages.

    Raises:
        RuntimeError: On final failure after retries.
    """

    url = API_2_URL
    headers = _build_headers()
    all_data: List[Dict[str, Any]] = []

    current_url = url
    current_params = params or {}

    while current_url:
        for attempt in range(1, MAX_RETRY + 1):
            try:
                log(f"🌐 [API2] Attempt {attempt}/{MAX_RETRY} → GET {current_url}")

                response = requests.get(
                    current_url,
                    headers=headers,
                    params=current_params,
                    timeout=TIMEOUT_SECONDS
                )

                # ----- SUCCESS CASE -----
                if response.status_code == 200:
                    try:
                        json_payload = response.json()
                    except Exception:
                        raise RuntimeError("API 2 returned non-JSON response.")

                    # Extract data array
                    if "data" not in json_payload:
                        raise RuntimeError("API 2 response missing `data` field.")

                    data_chunk = json_payload["data"]
                    all_data.extend(data_chunk)

                    log(f" [API2] Retrieved {len(data_chunk)} records (Total so far: {len(all_data)}).")

                    # Pagination handling
                    next_url = _extract_pagination_info(json_payload)

                    if next_url:
                        current_url = next_url
                        current_params = {}  # pagination URL already contains params
                        log(f" [API2] Next page detected: {next_url}")
                    else:
                        current_url = None

                    break  # success → exit retry loop

                # Retryable errors
                if response.status_code in (429, 500, 502, 503, 504):
                    log(f"️ [API2] HTTP {response.status_code}, retrying...")
                    time.sleep(RETRY_DELAY_SECONDS)
                    continue

                # Hard failure
                raise RuntimeError(
                    f"API 2 failed with HTTP {response.status_code}: {response.text}"
                )

            except requests.Timeout:
                log(" [API2] Timeout occurred, retrying...")
                time.sleep(RETRY_DELAY_SECONDS)
                continue

            except requests.RequestException as e:
                log(f" [API2] Network error: {e}, retrying...")
                time.sleep(RETRY_DELAY_SECONDS)
                continue

        else:
            # If loop completes without break → retry limit reached
            raise RuntimeError("API 2: Maximum retry attempts exceeded.")

    log(f" [API2] Completed fetching. Final record count: {len(all_data)}")
    return all_data
