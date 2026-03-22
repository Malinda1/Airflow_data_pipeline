"""
weather_pipeline_dag.py
-----------------------
Airflow DAG for the automated weather data pipeline.

Schedule : Once per day (at midnight UTC)
Tasks    : 
    1. validate_api_key      → Check API key exists before doing anything
    2. extract_weather_data  → Fetch weather from OpenWeatherMap API
    3. log_summary           → Log a summary of extracted data

Author   : You
Version  : 1.0.0
"""

import os
import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Make sure Airflow can find our scripts/ folder
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ---------------------------------------------------------------------------
# Logger for this DAG file
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default arguments applied to every task in this DAG
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner"           : "data_engineer",
    "depends_on_past" : False,
    "start_date"      : datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry"  : False,
    "retries"         : 3,
    "retry_delay"     : timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
dag = DAG(
    dag_id="weather_data_pipeline",
    default_args=DEFAULT_ARGS,
    description="Extracts weather data from OpenWeatherMap API once per day",
    schedule_interval="0 0 * * *",      # Every day at midnight UTC (cron)
    catchup=False,                       # Do NOT run missed past schedules
    max_active_runs=1,                   # Only one run at a time
    tags=["weather", "extraction", "pipeline"],
)


# ---------------------------------------------------------------------------
# Task 1 — Validate API Key
# ---------------------------------------------------------------------------
def validate_api_key(**context) -> None:
    """
    Checks that the OPENWEATHER_API_KEY environment variable is set.
    Fails the task immediately if it is missing — so we don't waste
    time running the extraction with a bad config.
    """
    logger.info("=" * 60)
    logger.info("TASK 1 — Validating API key configuration...")
    logger.info("=" * 60)

    api_key = os.getenv("OPENWEATHER_API_KEY")

    if not api_key:
        logger.error("OPENWEATHER_API_KEY is not set in environment variables.")
        raise EnvironmentError(
            "OPENWEATHER_API_KEY is missing. "
            "Set it in your .env file or Docker environment."
        )

    if len(api_key) < 10:
        logger.error("OPENWEATHER_API_KEY looks invalid — too short.")
        raise ValueError("OPENWEATHER_API_KEY appears to be invalid.")

    logger.info("API key found and looks valid.")
    logger.info("Validation passed. Proceeding to extraction.")


# ---------------------------------------------------------------------------
# Task 2 — Extract Weather Data
# ---------------------------------------------------------------------------
def run_weather_extraction(**context) -> None:
    """
    Calls the extract_weather_data() function, fetches weather for all
    cities, and pushes a summary to XCom so the next task can log it.

    XCom pushed keys:
        - row_count    : number of cities successfully extracted
        - columns      : list of column names in the DataFrame
        - sample_cities: first 3 city names from the result
    """
    logger.info("=" * 60)
    logger.info("TASK 2 — Starting weather data extraction...")
    logger.info("=" * 60)

    # Import here to avoid Airflow import issues at DAG parse time
    from scripts.extract_weather import extract_weather_data

    df = extract_weather_data()

    if df.empty:
        logger.error("Extraction returned an empty DataFrame.")
        raise ValueError(
            "Weather extraction failed — no data returned. "
            "Check API key and network connectivity."
        )

    logger.info(f"Extraction successful. Rows extracted : {len(df)}")
    logger.info(f"Columns available : {list(df.columns)}")
    logger.info(f"Cities extracted  : {df['city'].tolist()}")

    # Push summary data to XCom for the next task to use
    context["ti"].xcom_push(key="row_count",     value=len(df))
    context["ti"].xcom_push(key="columns",       value=list(df.columns))
    context["ti"].xcom_push(key="sample_cities", value=df["city"].tolist()[:3])

    logger.info("Data pushed to XCom. Extraction task complete.")


# ---------------------------------------------------------------------------
# Task 3 — Log Summary
# ---------------------------------------------------------------------------
def log_pipeline_summary(**context) -> None:
    """
    Pulls XCom values from Task 2 and logs a clean pipeline summary.
    This acts as the final confirmation that the pipeline ran correctly.
    """
    logger.info("=" * 60)
    logger.info("TASK 3 — Pipeline execution summary")
    logger.info("=" * 60)

    ti = context["ti"]

    row_count     = ti.xcom_pull(task_ids="extract_weather_data", key="row_count")
    columns       = ti.xcom_pull(task_ids="extract_weather_data", key="columns")
    sample_cities = ti.xcom_pull(task_ids="extract_weather_data", key="sample_cities")

    logger.info(f"Pipeline run date    : {context['ds']}")
    logger.info(f"Total cities fetched : {row_count}")
    logger.info(f"Sample cities        : {sample_cities}")
    logger.info(f"Total columns        : {len(columns)}")
    logger.info(f"Column names         : {columns}")
    logger.info("=" * 60)
    logger.info("Pipeline completed SUCCESSFULLY.")
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# Register Tasks inside the DAG
# ---------------------------------------------------------------------------
with dag:

    task_validate_key = PythonOperator(
        task_id="validate_api_key",
        python_callable=validate_api_key,
        provide_context=True,
    )

    task_extract_data = PythonOperator(
        task_id="extract_weather_data",
        python_callable=run_weather_extraction,
        provide_context=True,
    )

    task_log_summary = PythonOperator(
        task_id="log_pipeline_summary",
        python_callable=log_pipeline_summary,
        provide_context=True,
    )

    # ---------------------------------------------------------------------------
    # Task Dependencies — defines execution order
    # validate_api_key → extract_weather_data → log_pipeline_summary
    # ---------------------------------------------------------------------------
    task_validate_key >> task_extract_data >> task_log_summary