"""
settings.py
===========

Loads and exposes environment variables needed by the ETL system.
This file acts as a central configuration hub.

Future additions:
    - Environment validation
    - Structured settings (class-based config)
    - Default fallback values

"""

from dotenv import load_dotenv
import os

load_dotenv()

API_1_URL = os.getenv("API_1_URL")
API_1_TOKEN = os.getenv("API_1_TOKEN")

API_2_URL = os.getenv("API_2_URL")
API_2_TOKEN = os.getenv("API_2_TOKEN")

DB_SERVER = os.getenv("DB_SERVER")
DB_DATABASE = os.getenv("DB_DATABASE")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")