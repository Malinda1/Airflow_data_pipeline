"""
models.py
---------
Defines the database table schema using SQLAlchemy ORM.
Each class maps to one table in PostgreSQL.
"""

from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, Float, String,
    DateTime, UniqueConstraint
)
from sqlalchemy.orm import DeclarativeBase


# ---------------------------------------------------------------------------
# Base class — all models inherit from this
# ---------------------------------------------------------------------------
class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# WeatherRecord — maps to the 'weather_records' table
# ---------------------------------------------------------------------------
class WeatherRecord(Base):
    """
    Stores one weather snapshot per city per extraction run.

    Meaningful attributes (satisfies Task 4 requirement):
        - city, country          → location identity
        - temperature_c          → primary weather metric
        - humidity_pct           → moisture level
        - weather_condition      → sky condition (Clear, Rain, etc.)
        - wind_speed_mps         → wind measurement
        - extracted_at           → timestamp of data extraction
        - created_at             → timestamp of database insertion
    """

    __tablename__ = "weather_records"

    # Primary key
    id                  = Column(Integer, primary_key=True, autoincrement=True)

    # Location fields
    city                = Column(String(100),  nullable=False)
    country             = Column(String(10),   nullable=True)
    latitude            = Column(Float,        nullable=True)
    longitude           = Column(Float,        nullable=True)

    # Temperature fields
    temperature_c       = Column(Float,        nullable=True)
    feels_like_c        = Column(Float,        nullable=True)
    temp_min_c          = Column(Float,        nullable=True)
    temp_max_c          = Column(Float,        nullable=True)

    # Atmospheric fields
    humidity_pct        = Column(Integer,      nullable=True)
    pressure_hpa        = Column(Integer,      nullable=True)
    visibility_m        = Column(Integer,      nullable=True)
    cloudiness_pct      = Column(Integer,      nullable=True)

    # Wind fields
    wind_speed_mps      = Column(Float,        nullable=True)
    wind_direction_deg  = Column(Integer,      nullable=True)

    # Weather condition fields
    weather_condition   = Column(String(100),  nullable=True)
    weather_description = Column(String(200),  nullable=True)

    # Timestamp fields
    extracted_at        = Column(String(30),   nullable=True)   # From API
    created_at          = Column(                               # DB insert time
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # Prevent duplicate rows for same city in same extraction run
    __table_args__ = (
        UniqueConstraint("city", "extracted_at", name="uq_city_extracted_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<WeatherRecord("
            f"city={self.city!r}, "
            f"temp={self.temperature_c}°C, "
            f"condition={self.weather_condition!r}, "
            f"extracted_at={self.extracted_at!r}"
            f")>"
        )