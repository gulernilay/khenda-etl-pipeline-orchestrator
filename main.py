"""
main.py
=======

Application entry point.

New Behavior:
- Default behavior: start daily scheduler (runs ETL at 22:00 every day)
- Manual run:
    python main.py p1   → run Pipeline 1 once for last 7 days
    python main.py p2   → run Pipeline 2 once for last 7 days

Author: Chef Seasons – Data Engineering Team
"""

import sys
import time

from services.scheduler import (
    start_scheduler,
    get_last_7_days_window,
)
from etl.etl_pipeline_1 import run_pipeline as run_p1
from etl.etl_pipeline_1 import run_pipeline as run_p2
from utils.logger import log


def main():
    """
    Main entry point for the ETL system.

    Usage:
        python main.py
            → Starts the scheduler (daily at 22:00)

        python main.py p1
            → Manually runs Pipeline 1 once for the last 7 days

        python main.py p2
            → Manually runs Pipeline 2 once for the last 7 days
    """
    log(" ETL Automation System Started")

    # Manual pipeline triggers
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        date_from, date_to = get_last_7_days_window()

        if arg == "p1":
            log(f" Manual trigger → Pipeline 1 for {date_from} → {date_to}")
            run_p1(date_from, date_to)
            return

        if arg == "p2":
            log(f" Manual trigger → Pipeline 2 for {date_from} → {date_to}")
            run_p2(date_from, date_to)
            return

        log(f" Unknown argument: {arg}. Starting scheduler instead...")

    # Default: start scheduler mode
    start_scheduler()

    log(" ETL system is now running in scheduler mode (daily at 22:00).")

    # Keep process alive so BackgroundScheduler can run
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log(" ETL system stopped by user.")


if __name__ == "__main__":
    main()
