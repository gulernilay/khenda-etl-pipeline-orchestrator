"""
api_client_1.py
===============

Production-grade API client for Source 1.

This module handles:
- Authenticated GET requests
- Retry logic (network failures, 429 & 5xx responses)
- Timeout control
- Response validation
- JSON parsing safety
- Error reporting with structured logging


"""

import requests
import time
from typing import List, Dict, Any
from config.settings import API_1_URL, API_1_TOKEN
from utils.logger import log


# ------------------------------------------------------------
# CONFIGURABLE PARAMETERS
# ------------------------------------------------------------
TIMEOUT_SECONDS = 15
MAX_RETRY = 3
RETRY_DELAY_SECONDS = 2


# ------------------------------------------------------------
# INTERNAL HELPER
# ------------------------------------------------------------
def _build_headers() -> Dict[str, str]:
    """
    Construct request headers for API 1.

    Returns:
        dict: Standard headers including authorization.
    """
    return {
        "Authorization": f"Bearer {API_1_TOKEN}",
        "Accept": "application/json",
        "User-Agent": "ChefSeasons-ETL-Agent/1.0"
    }


# ------------------------------------------------------------
# MAIN API CALL
# ------------------------------------------------------------
def fetch_api_1_data() -> List[Dict[str, Any]]:
    """
    Fetch data from API Source 1 with retry strategy.

    Workflow:
        1. Try HTTP GET
        2. If fails → retry (max=3)
        3. Validate status code
        4. Parse JSON response
        5. Return data as list of dicts

    Returns:
        List[Dict[str, Any]]: Parsed API response.

    Raises:
        RuntimeError: If maximum retry attempts fail.
    """

    url = API_1_URL
    headers = _build_headers()

    for attempt in range(1, MAX_RETRY + 1):
        try:
            log(f" [API1] Attempt {attempt}/{MAX_RETRY} → GET {url}")

            response = requests.get(
                url,
                headers=headers,
                timeout=TIMEOUT_SECONDS
            )

            # ---- STATUS CODE VALIDATION ----
            if response.status_code == 200:
                try:
                    data = response.json()
                except Exception:
                    raise RuntimeError("API 1 returned non-JSON response.")

                log(f" [API1] Successfully fetched {len(data)} records.")
                return data

            # Handle throttling & retryable errors
            if response.status_code in (429, 500, 502, 503, 504):
                log(f" [API1] Retryable error {response.status_code}, waiting...")
                time.sleep(RETRY_DELAY_SECONDS)
                continue

            # Non-retryable errors
            raise RuntimeError(
                f"API 1 failed with HTTP {response.status_code}: {response.text}"
            )

        except requests.Timeout:
            log(" [API1] Timeout occurred, retrying...")
            time.sleep(RETRY_DELAY_SECONDS)
            continue

        except requests.RequestException as e:
            log(f" [API1] Network error: {e}, retrying...")
            time.sleep(RETRY_DELAY_SECONDS)
            continue

    # Final failure after all retries
    raise RuntimeError("API 1: Maximum retry attempts exceeded.")