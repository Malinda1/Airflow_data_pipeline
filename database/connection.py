"""
connection.py
-------------
Manages PostgreSQL connection using environment variables.
Uses hardcoded /opt/airflow path for reliable imports inside Docker.
"""

import os
import sys

# ---------------------------------------------------------------------------
# Fix Python path for Airflow Docker environment
# ---------------------------------------------------------------------------
AIRFLOW_HOME = "/opt/airflow"
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from scripts.utils.logger import get_logger

logger = get_logger(__name__)


def get_db_url() -> str:
    user     = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host     = os.getenv("POSTGRES_HOST")
    port     = os.getenv("POSTGRES_PORT", "5432")
    db       = os.getenv("POSTGRES_DB")

    missing = [
        name for name, val in {
            "POSTGRES_USER"    : user,
            "POSTGRES_PASSWORD": password,
            "POSTGRES_HOST"    : host,
            "POSTGRES_DB"      : db,
        }.items() if not val
    ]

    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {missing}"
        )

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    logger.info(f"Database URL built for host: {host}:{port}/{db}")
    return url


def get_engine():
    url    = get_db_url()
    engine = create_engine(
        url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        echo=False,
    )
    logger.info("SQLAlchemy engine created successfully.")
    return engine


def get_session() -> Session:
    engine       = get_engine()
    SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    return SessionLocal()