"""
connection.py
-------------
Manages the PostgreSQL database connection using SQLAlchemy.
Reads credentials from environment variables — never hardcoded.
"""

import os
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session
from scripts.utils.logger import get_logger

logger = get_logger(__name__)


def get_db_url() -> str:
    """
    Builds the PostgreSQL connection URL from environment variables.

    Returns:
        str: SQLAlchemy-compatible connection URL.

    Raises:
        EnvironmentError: If any required env variable is missing.
    """
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


def get_engine() -> Engine:
    """
    Creates and returns a SQLAlchemy engine.

    Returns:
        Engine: SQLAlchemy engine connected to PostgreSQL.
    """
    url = get_db_url()
    engine = create_engine(
        url,
        pool_pre_ping=True,       # Checks connection health before using it
        pool_size=5,              # Max 5 persistent connections
        max_overflow=10,          # Allow 10 extra connections under load
        echo=False,               # Set True to log all SQL queries (debug only)
    )
    logger.info("SQLAlchemy engine created successfully.")
    return engine


def get_session() -> Session:
    """
    Creates and returns a new SQLAlchemy session.

    Returns:
        Session: Active database session.
    """
    engine        = get_engine()
    SessionLocal  = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    return SessionLocal()