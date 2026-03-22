"""
local_connection.py
-------------------
PostgreSQL connection for LOCAL use only (Jupyter Notebook).
Connects to the PostgreSQL container exposed on localhost:5432.

Compatible with SQLAlchemy 2.x and pandas 2.x.
DO NOT use this inside Airflow DAGs — use connection.py for that.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv

# Load .env file
load_dotenv()


def get_local_db_url() -> str:
    """
    Builds connection URL using localhost instead of 'postgres'
    hostname — because Jupyter runs outside Docker network.

    Returns:
        str: SQLAlchemy connection URL for local use.
    """
    user     = os.getenv("POSTGRES_USER",     "airflow")
    password = os.getenv("POSTGRES_PASSWORD", "airflow")
    host     = "localhost"
    port     = "5432"
    db       = os.getenv("POSTGRES_DB", "airflow")

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def get_local_engine():
    """
    Creates SQLAlchemy 2.x engine for local Jupyter connection.

    Returns:
        Engine: Connected SQLAlchemy engine.
    """
    url    = get_local_db_url()
    engine = create_engine(
        url,
        pool_pre_ping=True,
        echo=False,
    )
    return engine


def get_local_session() -> Session:
    """
    Returns a local database session.

    Returns:
        Session: Active SQLAlchemy session.
    """
    engine       = get_local_engine()
    SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    return SessionLocal()


def read_sql(query: str) -> pd.DataFrame:
    """
    Executes a SQL query and returns a pandas DataFrame.
    Handles SQLAlchemy 2.x + pandas 2.x compatibility automatically.

    Args:
        query (str): SQL SELECT query to execute.

    Returns:
        pd.DataFrame: Query results as a DataFrame.
    """
    engine = get_local_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    return df


def test_connection() -> bool:
    """
    Tests if the local database connection works.

    Returns:
        bool: True if connected, False otherwise.
    """
    try:
        engine = get_local_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(" Database connection successful!")
        return True
    except Exception as e:
        print(f" Database connection failed: {e}")
        return False