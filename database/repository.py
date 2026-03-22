"""
repository.py
-------------
All database operations for weather data.
Follows the Repository Pattern — keeps DB logic separate from business logic.
"""

import pandas as pd
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from database.connection import get_engine, get_session
from database.models import Base, WeatherRecord
from scripts.utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Table Management
# ---------------------------------------------------------------------------

def create_tables() -> None:
    """
    Creates all database tables if they do not already exist.
    Safe to call multiple times — will not overwrite existing tables.
    """
    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created (or already exist).")


# ---------------------------------------------------------------------------
# Write Operations
# ---------------------------------------------------------------------------

def save_weather_records(df: pd.DataFrame) -> dict:
    """
    Saves all rows from the DataFrame into the weather_records table.

    Skips duplicate rows (same city + extracted_at) instead of failing.

    Args:
        df (pd.DataFrame): Extracted weather data from extract_weather.py

    Returns:
        dict: Summary with keys 'inserted', 'skipped', 'failed'
    """
    if df.empty:
        logger.warning("Received empty DataFrame. Nothing to save.")
        return {"inserted": 0, "skipped": 0, "failed": 0}

    session : Session = get_session()
    inserted = 0
    skipped  = 0
    failed   = 0

    logger.info(f"Saving {len(df)} weather records to database...")

    try:
        for _, row in df.iterrows():
            record = WeatherRecord(
                city                = row.get("city"),
                country             = row.get("country"),
                latitude            = row.get("latitude"),
                longitude           = row.get("longitude"),
                temperature_c       = row.get("temperature_c"),
                feels_like_c        = row.get("feels_like_c"),
                temp_min_c          = row.get("temp_min_c"),
                temp_max_c          = row.get("temp_max_c"),
                humidity_pct        = row.get("humidity_pct"),
                pressure_hpa        = row.get("pressure_hpa"),
                visibility_m        = row.get("visibility_m"),
                cloudiness_pct      = row.get("cloudiness_pct"),
                wind_speed_mps      = row.get("wind_speed_mps"),
                wind_direction_deg  = row.get("wind_direction_deg"),
                weather_condition   = row.get("weather_condition"),
                weather_description = row.get("weather_description"),
                extracted_at        = row.get("extracted_at"),
            )

            try:
                session.add(record)
                session.commit()
                inserted += 1
                logger.info(f"Inserted record for city: {record.city}")

            except IntegrityError:
                # Duplicate row — skip it cleanly
                session.rollback()
                skipped += 1
                logger.warning(
                    f"Skipped duplicate record for city: {row.get('city')} "
                    f"at {row.get('extracted_at')}"
                )

            except SQLAlchemyError as e:
                session.rollback()
                failed += 1
                logger.error(f"Failed to insert record for {row.get('city')}: {e}")

    finally:
        session.close()

    logger.info(
        f"Save complete — Inserted: {inserted} | "
        f"Skipped: {skipped} | Failed: {failed}"
    )
    return {"inserted": inserted, "skipped": skipped, "failed": failed}


# ---------------------------------------------------------------------------
# Read Operations
# ---------------------------------------------------------------------------

def fetch_all_weather_records() -> pd.DataFrame:
    """
    Fetches all records from weather_records table as a DataFrame.

    Returns:
        pd.DataFrame: All stored weather records.
    """
    session = get_session()
    try:
        records = session.query(WeatherRecord).all()

        if not records:
            logger.info("No records found in weather_records table.")
            return pd.DataFrame()

        data = [
            {
                "id"                 : r.id,
                "city"               : r.city,
                "country"            : r.country,
                "temperature_c"      : r.temperature_c,
                "feels_like_c"       : r.feels_like_c,
                "humidity_pct"       : r.humidity_pct,
                "pressure_hpa"       : r.pressure_hpa,
                "weather_condition"  : r.weather_condition,
                "weather_description": r.weather_description,
                "wind_speed_mps"     : r.wind_speed_mps,
                "cloudiness_pct"     : r.cloudiness_pct,
                "latitude"           : r.latitude,
                "longitude"          : r.longitude,
                "extracted_at"       : r.extracted_at,
                "created_at"         : r.created_at,
            }
            for r in records
        ]

        df = pd.DataFrame(data)
        logger.info(f"Fetched {len(df)} records from database.")
        return df

    except SQLAlchemyError as e:
        logger.error(f"Failed to fetch records: {e}")
        return pd.DataFrame()

    finally:
        session.close()


def fetch_latest_records() -> pd.DataFrame:
    """
    Fetches only the most recent extraction batch.

    Returns:
        pd.DataFrame: Latest weather records only.
    """
    session = get_session()
    try:
        # Get the most recent extracted_at timestamp
        latest = (
            session.query(WeatherRecord.extracted_at)
            .order_by(WeatherRecord.extracted_at.desc())
            .first()
        )

        if not latest:
            logger.info("No records found.")
            return pd.DataFrame()

        records = (
            session.query(WeatherRecord)
            .filter(WeatherRecord.extracted_at == latest[0])
            .all()
        )

        data = [
            {
                "city"              : r.city,
                "country"           : r.country,
                "temperature_c"     : r.temperature_c,
                "humidity_pct"      : r.humidity_pct,
                "weather_condition" : r.weather_condition,
                "wind_speed_mps"    : r.wind_speed_mps,
                "extracted_at"      : r.extracted_at,
            }
            for r in records
        ]

        df = pd.DataFrame(data)
        logger.info(f"Fetched {len(df)} latest records (extracted_at={latest[0]}).")
        return df

    except SQLAlchemyError as e:
        logger.error(f"Failed to fetch latest records: {e}")
        return pd.DataFrame()

    finally:
        session.close()