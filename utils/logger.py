"""
logger.py
=========

Centralized logging utility for the ETL system.

The final version will:
- Write logs to rotating file handlers
- Stream logs to console
- Possibly integrate with MailLogger

Author: Chef Seasons – Data Engineering Team
"""

def log(message: str):
    """
    Log placeholder function.

    Args:
        message (str): Message to be printed/logged.

    Current version prints only to console.
    """
    print(message)