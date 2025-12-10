# khenda-etl-pipeline-orchestrator

Automated ETL pipelines that fetch data from multiple APIs daily and load it into SQL Server with full logging, retry, and scheduling support.

## Project Structure

- `etl/` – Transformation utilities and ETL pipeline modules
- `services/` – API clients, database connector and scheduler
- `utils/` – Logging and helper utilities
- `config/` – Centralized environment and configuration management
- `sql/` – SQL scripts for table creation and merges
- `main.py` – Application entry point

## Getting Started

1. Create a virtual environment
2. Install required packages:

pip install -r requirements.txt 3. Copy `.env.example` to `.env` and fill in values  
4. Run the application:
