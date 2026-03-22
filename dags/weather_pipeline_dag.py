"""
weather_pipeline_dag.py
-----------------------
Airflow DAG for the automated weather data pipeline.

Schedule : Once per day (midnight UTC)
Tasks    :
    1. validate_api_key      → Check API key exists
    2. extract_weather_data  → Fetch weather from OpenWeatherMap
    3. store_weather_data    → Save records to PostgreSQL
    4. log_summary           → Log pipeline execution summary
"""

import os
import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Fix Python path — point directly to /opt/airflow where scripts/ lives
# ---------------------------------------------------------------------------
AIRFLOW_HOME = "/opt/airflow"
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default arguments
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
    description="Extracts and stores weather data from OpenWeatherMap daily",
    schedule_interval="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["weather", "extraction", "storage", "pipeline"],
)


# ---------------------------------------------------------------------------
# Task 1 — Validate API Key
# ---------------------------------------------------------------------------
def validate_api_key(**context) -> None:
    logger.info("=" * 60)
    logger.info("TASK 1 — Validating API key configuration...")
    logger.info("=" * 60)

    api_key = os.getenv("OPENWEATHER_API_KEY")

    if not api_key:
        raise EnvironmentError("OPENWEATHER_API_KEY is missing.")

    if len(api_key) < 10:
        raise ValueError("OPENWEATHER_API_KEY appears to be invalid.")

    logger.info("API key validated successfully.")


# ---------------------------------------------------------------------------
# Task 2 — Extract Weather Data
# ---------------------------------------------------------------------------
def run_weather_extraction(**context) -> None:
    logger.info("=" * 60)
    logger.info("TASK 2 — Starting weather data extraction...")
    logger.info("=" * 60)

    # Import here — after sys.path is fixed
    from scripts.extract_weather import extract_weather_data

    df = extract_weather_data()

    if df.empty:
        raise ValueError("Extraction returned empty DataFrame.")

    logger.info(f"Extracted {len(df)} city records successfully.")

    context["ti"].xcom_push(key="weather_data",  value=df.to_json(orient="records"))
    context["ti"].xcom_push(key="row_count",     value=len(df))
    context["ti"].xcom_push(key="sample_cities", value=df["city"].tolist()[:3])

    logger.info("Weather data pushed to XCom.")


# ---------------------------------------------------------------------------
# Task 3 — Store Weather Data in PostgreSQL
# ---------------------------------------------------------------------------
def store_weather_data(**context) -> None:
    logger.info("=" * 60)
    logger.info("TASK 3 — Storing weather data in PostgreSQL...")
    logger.info("=" * 60)

    import pandas as pd
    from database.repository import create_tables, save_weather_records

    ti           = context["ti"]
    weather_json = ti.xcom_pull(task_ids="extract_weather_data", key="weather_data")

    if not weather_json:
        raise ValueError("No weather data found in XCom.")

    df = pd.read_json(weather_json, orient="records")
    logger.info(f"Pulled {len(df)} records from XCom.")

    create_tables()

    result = save_weather_records(df)

    logger.info(
        f"Storage complete — "
        f"Inserted: {result['inserted']} | "
        f"Skipped: {result['skipped']} | "
        f"Failed: {result['failed']}"
    )

    ti.xcom_push(key="inserted", value=result["inserted"])
    ti.xcom_push(key="skipped",  value=result["skipped"])


# ---------------------------------------------------------------------------
# Task 4 — Log Pipeline Summary
# ---------------------------------------------------------------------------
def log_pipeline_summary(**context) -> None:
    logger.info("=" * 60)
    logger.info("TASK 4 — Pipeline execution summary")
    logger.info("=" * 60)

    ti            = context["ti"]
    row_count     = ti.xcom_pull(task_ids="extract_weather_data", key="row_count")
    sample_cities = ti.xcom_pull(task_ids="extract_weather_data", key="sample_cities")
    inserted      = ti.xcom_pull(task_ids="store_weather_data",   key="inserted")
    skipped       = ti.xcom_pull(task_ids="store_weather_data",   key="skipped")

    logger.info(f"Pipeline run date      : {context['ds']}")
    logger.info(f"Cities extracted       : {row_count}")
    logger.info(f"Sample cities          : {sample_cities}")
    logger.info(f"Records inserted to DB : {inserted}")
    logger.info(f"Records skipped (dup)  : {skipped}")
    logger.info("=" * 60)
    logger.info("Pipeline completed SUCCESSFULLY.")
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# Register Tasks
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

    task_store_data = PythonOperator(
        task_id="store_weather_data",
        python_callable=store_weather_data,
        provide_context=True,
    )

    task_log_summary = PythonOperator(
        task_id="log_pipeline_summary",
        python_callable=log_pipeline_summary,
        provide_context=True,
    )

    # Task execution order
    task_validate_key >> task_extract_data >> task_store_data >> task_log_summary