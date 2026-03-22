"""
extract_weather.py
------------------
Extracts current weather data from OpenWeatherMap API
for a list of cities and returns a structured DataFrame.

Author  : You
Version : 1.0.0
"""

import os
import logging
import requests
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load environment variables from .env file
# ---------------------------------------------------------------------------
load_dotenv()

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
from scripts.utils.logger import get_logger
logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
API_KEY   = os.getenv("OPENWEATHER_API_KEY")
BASE_URL  = "https://api.openweathermap.org/data/2.5/weather"
UNITS     = "metric"          # Celsius. Use "imperial" for Fahrenheit
CITIES    = [
    "London",
    "New York",
    "Tokyo",
    "Paris",
    "Sydney",
    "Dubai",
    "Colombo",
    "Singapore",
    "Berlin",
    "Toronto",
]

# ---------------------------------------------------------------------------
# Core Functions
# ---------------------------------------------------------------------------

def fetch_weather_for_city(city: str) -> dict | None:
    """
    Fetches current weather data for a single city.

    Args:
        city (str): Name of the city.

    Returns:
        dict: Parsed weather data or None if request fails.
    """
    if not API_KEY:
        logger.error("OPENWEATHER_API_KEY is missing. Check your .env file.")
        raise EnvironmentError("OPENWEATHER_API_KEY not set.")

    params = {
        "q"     : city,
        "appid" : API_KEY,
        "units" : UNITS,
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()             # Raises HTTPError for 4xx/5xx
        raw = response.json()
        logger.info(f"Successfully fetched data for: {city}")
        return parse_weather(raw)

    except requests.exceptions.HTTPError as e:
        logger.warning(f"HTTP error for '{city}': {e}")
    except requests.exceptions.ConnectionError:
        logger.error("Connection error. Check your internet connection.")
    except requests.exceptions.Timeout:
        logger.warning(f"Request timed out for city: '{city}'")
    except requests.exceptions.RequestException as e:
        logger.error(f"Unexpected request error for '{city}': {e}")

    return None


def parse_weather(raw: dict) -> dict:
    """
    Parses the raw API response into a clean flat dictionary.

    Args:
        raw (dict): Raw JSON response from OpenWeatherMap API.

    Returns:
        dict: Flat structured dictionary with selected fields.
    """
    return {
        "city"              : raw.get("name"),
        "country"           : raw.get("sys", {}).get("country"),
        "temperature_c"     : raw.get("main", {}).get("temp"),
        "feels_like_c"      : raw.get("main", {}).get("feels_like"),
        "temp_min_c"        : raw.get("main", {}).get("temp_min"),
        "temp_max_c"        : raw.get("main", {}).get("temp_max"),
        "humidity_pct"      : raw.get("main", {}).get("humidity"),
        "pressure_hpa"      : raw.get("main", {}).get("pressure"),
        "weather_condition" : raw.get("weather", [{}])[0].get("main"),
        "weather_description": raw.get("weather", [{}])[0].get("description"),
        "wind_speed_mps"    : raw.get("wind", {}).get("speed"),
        "wind_direction_deg": raw.get("wind", {}).get("deg"),
        "cloudiness_pct"    : raw.get("clouds", {}).get("all"),
        "visibility_m"      : raw.get("visibility"),
        "latitude"          : raw.get("coord", {}).get("lat"),
        "longitude"         : raw.get("coord", {}).get("lon"),
        "extracted_at"      : datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }


def extract_weather_data(cities: list[str] = CITIES) -> pd.DataFrame:
    """
    Extracts weather data for all cities and returns a clean DataFrame.

    Args:
        cities (list[str]): List of city names to fetch data for.

    Returns:
        pd.DataFrame: Structured DataFrame with one row per city.
    """
    logger.info(f"Starting weather extraction for {len(cities)} cities...")

    results = []

    for city in cities:
        data = fetch_weather_for_city(city)
        if data:
            results.append(data)

    if not results:
        logger.error("No data was extracted. Check API key and city names.")
        return pd.DataFrame()

    df = pd.DataFrame(results)

    logger.info(f"Extraction complete. {len(df)} cities extracted successfully.")
    return df


# ---------------------------------------------------------------------------
# Entry point — run this script directly for testing
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    df = extract_weather_data()

    if not df.empty:
        print("\n========== EXTRACTED WEATHER DATA ==========")
        print(df.to_string(index=False))
        print(f"\nTotal rows : {len(df)}")
        print(f"Total cols : {len(df.columns)}")
        print("Columns    :", list(df.columns))
    else:
        print("No data extracted. Check logs above for errors.")