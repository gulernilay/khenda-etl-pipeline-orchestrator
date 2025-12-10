"""
scheduler.py
============

Centralized scheduling system for periodic ETL execution.

New Requirements:
- Run ETL once per day at 22:00
- For each run, only fetch data for the last 7 calendar days including today.
  Example:
      If today is 2025-12-07, date window = 2025-12-01 → 2025-12-07


"""

from datetime import datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from etl.etl_pipeline_1 import run_pipeline as run_pipeline_1
from etl.etl_pipeline_1 import run_pipeline as run_pipeline_2
from utils.logger import log
from services.etl_monitor import ETLMonitor

monitor = ETLMonitor()

def get_last_7_days_window() -> tuple[str, str]:
    """
    Calculate the date window for the last 7 days including today.

    Example:
        If today = 2025-12-07
        -> start_date = 2025-12-01
        -> end_date   = 2025-12-07

    Returns:
        tuple[str, str]: (start_date, end_date) in 'YYYY-MM-DD' format.
    """
    today = datetime.today().date()
    start_date = today - timedelta(days=6)  # 7 gün = bugün + önceki 6 gün
    return (
        start_date.strftime("%Y-%m-%d"),
        today.strftime("%Y-%m-%d"),
    )


def run_pipeline_1_job():
    date_from, date_to = get_last_7_days_window()
    monitor.pipeline1.start()
    try:
        rows = run_pipeline_1(date_from, date_to)
        monitor.pipeline1.finish_success(rows)
    except Exception as exc:
        monitor.pipeline1.finish_failure(str(exc))


def run_pipeline_2_job():
    date_from, date_to = get_last_7_days_window()
    monitor.pipeline2.start()
    try:
        rows = run_pipeline_2(date_from, date_to)
        monitor.pipeline2.finish_success(rows)
    except Exception as exc:
        monitor.pipeline2.finish_failure(str(exc))


def start_scheduler():
    """
    Initialize and start the ETL job scheduler.

    Scheduled Jobs:
        - Pipeline 1 → Every day at 22:00
        - Pipeline 2 → Every day at 22:00

    Note:
        If you want to avoid running both at the exact same second,
        you can shift Pipeline 2 to 22:05 later.
    """
    log(" Initializing ETL scheduler with daily 22:00 jobs...")

    scheduler = BackgroundScheduler()

    # ---- Pipeline 1: every day at 22:00 ----
    scheduler.add_job(
        run_pipeline_1_job,
        CronTrigger(hour=22, minute=0),
        id="pipeline1_daily",
        name="Pipeline 1 - Daily ETL Run (last 7 days)",
        replace_existing=True,
    )

    # ---- Pipeline 2: every day at 22:00 ----
    scheduler.add_job(
        run_pipeline_2_job,
        CronTrigger(hour=22, minute=0),
        id="pipeline2_daily",
        name="Pipeline 2 - Daily ETL Run (last 7 days)",
        replace_existing=True,
    )

    scheduler.start()
    log(" Scheduler started. Daily ETL at 22:00 is now active.")

    return scheduler