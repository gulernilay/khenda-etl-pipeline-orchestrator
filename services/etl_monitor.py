"""
etl_monitor.py
==============

ETL monitoring and health reporting module.

This module:
- Tracks start/end timestamps for each pipeline
- Captures success row counts or error messages
- Builds a consolidated ETL report
- Sends daily e-mail summary using MailLogger

Author: Chef Seasons – Data Engineering Team
"""

from datetime import datetime
from utils.logger import log
from services.mail_logger import MailLogger


class PipelineStatus:
    """Stores runtime metrics for a single pipeline execution."""

    def __init__(self, name: str):
        self.name = name
        self.start_time: datetime = None
        self.end_time: datetime = None
        self.success: bool = False
        self.rows: int = 0
        self.error: str = ""

    def start(self):
        self.start_time = datetime.now()
        log(f"🔸 [{self.name}] Started at {self.start_time}")

    def finish_success(self, rows: int):
        self.end_time = datetime.now()
        self.success = True
        self.rows = rows
        log(f"🟢 [{self.name}] Completed successfully → {rows} rows processed.")

    def finish_failure(self, error_message: str):
        self.end_time = datetime.now()
        self.success = False
        self.error = error_message
        log(f"🔴 [{self.name}] Failed → {error_message}")


class ETLMonitor:
    """Tracks the status of both pipelines and sends summary reports."""

    def __init__(self):
        self.pipeline1 = PipelineStatus("Pipeline 1")
        self.pipeline2 = PipelineStatus("Pipeline 2")

    # -----------------------------
    # REPORT BUILDER
    # -----------------------------
    def build_report(self) -> str:
        """Generate HTML summary for status e-mail."""

        def row_html(p: PipelineStatus) -> str:
            status = (
                "<span style='color:green;'>SUCCESS</span>"
                if p.success
                else "<span style='color:red;'>FAILED</span>"
            )

            duration = (
                f"{(p.end_time - p.start_time).seconds} sec"
                if p.start_time and p.end_time
                else "N/A"
            )

            return f"""
                <tr>
                    <td><b>{p.name}</b></td>
                    <td>{status}</td>
                    <td>{p.rows if p.success else '-'}</td>
                    <td>{duration}</td>
                    <td>{p.error if not p.success else "-"}</td>
                </tr>
            """

        html = f"""
        <h2>Daily ETL Execution Report</h2>
        <p>Date: <b>{datetime.now().strftime('%Y-%m-%d')}</b></p>

        <table border="1" cellpadding="6" cellspacing="0" style="border-collapse: collapse;">
            <tr>
                <th>Pipeline</th>
                <th>Status</th>
                <th>Rows Processed</th>
                <th>Duration</th>
                <th>Error</th>
            </tr>
            {row_html(self.pipeline1)}
            {row_html(self.pipeline2)}
        </table>

        <p style="margin-top:20px;">Chef Seasons ETL Automation System</p>
        """

        return html

    # -----------------------------
    # SEND REPORT
    # -----------------------------
    def send_report(self):
        """Send ETL status email."""
        report_html = self.build_report()
        MailLogger.start("/etl/daily-report")
        MailLogger.add(report_html)
        MailLogger.send(subject="Daily ETL Status Report")
        log("📧 ETL daily status report sent.")