"""
test_extract_weather.py
-----------------------
Unit tests for the weather extraction script.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from scripts.extract_weather import (
    parse_weather,
    extract_weather_data,
    fetch_weather_for_city,
)

# ---------------------------------------------------------------------------
# Sample raw API response for mocking
# ---------------------------------------------------------------------------
MOCK_RAW_RESPONSE = {
    "name": "London",
    "sys": {"country": "GB"},
    "main": {
        "temp": 15.5,
        "feels_like": 14.0,
        "temp_min": 13.0,
        "temp_max": 17.0,
        "humidity": 72,
        "pressure": 1012,
    },
    "weather": [{"main": "Clouds", "description": "overcast clouds"}],
    "wind": {"speed": 4.5, "deg": 230},
    "clouds": {"all": 90},
    "visibility": 10000,
    "coord": {"lat": 51.51, "lon": -0.13},
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_parse_weather_returns_correct_fields():
    result = parse_weather(MOCK_RAW_RESPONSE)
    assert result["city"]               == "London"
    assert result["country"]            == "GB"
    assert result["temperature_c"]      == 15.5
    assert result["humidity_pct"]       == 72
    assert result["weather_condition"]  == "Clouds"
    assert "extracted_at"               in result


def test_parse_weather_handles_missing_fields():
    result = parse_weather({})
    assert result["city"]          is None
    assert result["temperature_c"] is None


@patch("scripts.extract_weather.requests.get")
def test_fetch_weather_for_city_success(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = MOCK_RAW_RESPONSE
    mock_response.raise_for_status   = MagicMock()
    mock_get.return_value            = mock_response

    result = fetch_weather_for_city("London")
    assert result is not None
    assert result["city"] == "London"


@patch("scripts.extract_weather.requests.get")
def test_fetch_weather_for_city_http_error(mock_get):
    mock_get.side_effect = __import__("requests").exceptions.HTTPError("404 Not Found")
    result = fetch_weather_for_city("FakeCity")
    assert result is None


@patch("scripts.extract_weather.fetch_weather_for_city")
def test_extract_weather_data_returns_dataframe(mock_fetch):
    mock_fetch.return_value = parse_weather(MOCK_RAW_RESPONSE)
    df = extract_weather_data(cities=["London"])
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert "city" in df.columns


@patch("scripts.extract_weather.fetch_weather_for_city")
def test_extract_weather_data_empty_on_all_failures(mock_fetch):
    mock_fetch.return_value = None
    df = extract_weather_data(cities=["FakeCity"])
    assert df.empty