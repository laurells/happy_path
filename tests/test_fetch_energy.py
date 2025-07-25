import pytest
import pandas as pd
from datetime import datetime, timedelta
from pipeline.fetch_energy import fetch_energy_data, validate_energy_data
import os
import tempfile

class DummyResponse:
    def __init__(self, status_code=200, json_data=None, content=None):
        self.status_code = status_code
        self._json = json_data or {}
        self.content = content or b''
    
    def json(self):
        return self._json
    
    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")

@pytest.fixture
def city_config():
    return {"name": "TestCity", "eia_region_code": "TEST"}

def test_fetch_energy_data_api_success(monkeypatch, city_config, tmp_path):
    # Change to tmp directory to avoid creating files in project
    os.chdir(tmp_path)
    
    # Mock requests.get to return a successful API response with proper structure
    def mock_get(url, params=None, timeout=None):
        return DummyResponse(200, json_data={
            "response": {
                "data": [
                    {
                        "period": "2024-07-01", 
                        "type": "D",  # Daily data
                        "value": 123.4, 
                        "respondent": "TEST",
                        "timezone-description": "Eastern"
                    },
                    {
                        "period": "2024-07-02", 
                        "type": "D",  # Daily data
                        "value": 234.5, 
                        "respondent": "TEST",
                        "timezone-description": "Eastern"
                    },
                ],
                "total": 2
            }
        })
    
    monkeypatch.setattr("requests.get", mock_get)
    
    # Mock os.makedirs to avoid file system operations
    monkeypatch.setattr("os.makedirs", lambda *args, **kwargs: None)
    
    # Mock pandas DataFrame.to_csv to avoid file operations
    monkeypatch.setattr("pandas.DataFrame.to_csv", lambda self, *args, **kwargs: None)
    
    start_date = "2024-07-01"
    end_date = "2024-07-07"
    api_key = "dummy"
    
    result = fetch_energy_data(city_config, start_date, end_date, api_key)
    
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["city"] == "TestCity"
    assert result[0]["energy_mwh"] == 123.4
    assert result[0]["region_code"] == "TEST"
    assert result[0]["data_source"] == "EIA_API"
    assert result[1]["energy_mwh"] == 234.5

def test_fetch_energy_data_api_with_timezone_selection(monkeypatch, city_config, tmp_path):
    # Change to tmp directory
    os.chdir(tmp_path)
    
    # Test timezone selection logic for New York
    city_config_ny = {"name": "New York", "eia_region_code": "NYIS"}
    
    # Mock API response with multiple timezone entries for same date
    def mock_get(url, params=None, timeout=None):
        return DummyResponse(200, json_data={
            "response": {
                "data": [
                    {
                        "period": "2024-07-01", 
                        "type": "D",
                        "value": 100.0, 
                        "respondent": "NYIS",
                        "timezone-description": "Eastern"
                    },
                    {
                        "period": "2024-07-01", 
                        "type": "D",
                        "value": 200.0, 
                        "respondent": "NYIS",
                        "timezone-description": "Central"
                    },
                ],
                "total": 2
            }
        })
    
    monkeypatch.setattr("requests.get", mock_get)
    monkeypatch.setattr("os.makedirs", lambda *args, **kwargs: None)
    monkeypatch.setattr("pandas.DataFrame.to_csv", lambda self, *args, **kwargs: None)
    
    result = fetch_energy_data(city_config_ny, "2024-07-01", "2024-07-01", "dummy")
    
    # Should select the Eastern timezone entry for New York
    assert len(result) == 1
    assert result[0]["energy_mwh"] == 100.0  # Eastern timezone value
    assert result[0]["timezone"] == "Eastern"

def test_fetch_energy_data_pagination(monkeypatch, city_config, tmp_path):
    # Test pagination logic
    os.chdir(tmp_path)
    
    call_count = {"count": 0}
    
    def mock_get(url, params=None, timeout=None):
        call_count["count"] += 1
        offset = params.get("offset", 0)
        
        if offset == 0:
            # First page
            return DummyResponse(200, json_data={
                "response": {
                    "data": [
                        {
                            "period": "2024-07-01", 
                            "type": "D",
                            "value": 100.0, 
                            "respondent": "TEST",
                            "timezone-description": "Eastern"
                        }
                    ] * 500,  # Full page
                    "total": 750
                }
            })
        else:
            # Second page
            return DummyResponse(200, json_data={
                "response": {
                    "data": [
                        {
                            "period": "2024-07-02", 
                            "type": "D",
                            "value": 200.0, 
                            "respondent": "TEST",
                            "timezone-description": "Eastern"
                        }
                    ] * 250,  # Partial page
                    "total": 750
                }
            })
    
    monkeypatch.setattr("requests.get", mock_get)
    monkeypatch.setattr("os.makedirs", lambda *args, **kwargs: None)
    monkeypatch.setattr("pandas.DataFrame.to_csv", lambda self, *args, **kwargs: None)
    
    result = fetch_energy_data(city_config, "2024-07-01", "2024-07-02", "dummy")
    
    # Should have made 2 API calls for pagination
    assert call_count["count"] == 2
    # Should have data from both pages (but deduplicated by date)
    assert len(result) == 2  # One for each unique date

def test_fetch_energy_data_all_fail(monkeypatch, city_config, tmp_path):
    # Both API and backup fail
    os.chdir(tmp_path)
    
    def mock_get(url, params=None, timeout=None):
        raise Exception("Network failure")
    
    monkeypatch.setattr("requests.get", mock_get)
    monkeypatch.setattr("os.makedirs", lambda *args, **kwargs: None)
    monkeypatch.setattr("pandas.DataFrame.to_csv", lambda self, *args, **kwargs: None)
    
    start_date = "2024-07-01"
    end_date = "2024-07-07"
    api_key = "dummy"
    
    result = fetch_energy_data(city_config, start_date, end_date, api_key)
    assert result == []

def test_fetch_energy_data_rate_limiting(monkeypatch, city_config, tmp_path):
    # Test rate limiting handling
    os.chdir(tmp_path)
    
    call_count = {"count": 0}
    
    def mock_get(url, params=None, timeout=None):
        call_count["count"] += 1
        if call_count["count"] == 1:
            # First call gets rate limited
            return DummyResponse(429)
        else:
            # Second call succeeds
            return DummyResponse(200, json_data={
                "response": {
                    "data": [
                        {
                            "period": "2024-07-01", 
                            "type": "D",
                            "value": 123.4, 
                            "respondent": "TEST",
                            "timezone-description": "Eastern"
                        }
                    ],
                    "total": 1
                }
            })
    
    monkeypatch.setattr("requests.get", mock_get)
    monkeypatch.setattr("time.sleep", lambda x: None)  # Skip actual sleep
    monkeypatch.setattr("os.makedirs", lambda *args, **kwargs: None)
    monkeypatch.setattr("pandas.DataFrame.to_csv", lambda self, *args, **kwargs: None)
    
    result = fetch_energy_data(city_config, "2024-07-01", "2024-07-01", "dummy")
    
    # Should have retried and succeeded
    assert len(result) == 1
    assert result[0]["energy_mwh"] == 123.4
    assert call_count["count"] == 2

def test_validate_energy_data():
    # Test validation function
    
    # Valid data
    valid_data = [
        {"city": "TestCity", "energy_mwh": 100.0, "timezone": "Eastern"},
        {"city": "TestCity", "energy_mwh": 200.0, "timezone": "Eastern"}
    ]
    assert validate_energy_data(valid_data, "TestCity") == True
    
    # Empty data
    assert validate_energy_data([], "TestCity") == False
    
    # Wrong city
    wrong_city_data = [
        {"city": "WrongCity", "energy_mwh": 100.0, "timezone": "Eastern"}
    ]
    assert validate_energy_data(wrong_city_data, "TestCity") == False
    
    # Multiple cities
    multi_city_data = [
        {"city": "TestCity", "energy_mwh": 100.0, "timezone": "Eastern"},
        {"city": "OtherCity", "energy_mwh": 200.0, "timezone": "Eastern"}
    ]
    assert validate_energy_data(multi_city_data, "TestCity") == False
    
    # Multiple timezones (should still pass but log warning)
    multi_tz_data = [
        {"city": "TestCity", "energy_mwh": 100.0, "timezone": "Eastern"},
        {"city": "TestCity", "energy_mwh": 200.0, "timezone": "Central"}
    ]
    assert validate_energy_data(multi_tz_data, "TestCity") == True

def test_fetch_energy_data_no_valid_records(monkeypatch, city_config, tmp_path):
    # Test when API returns data but no valid records after filtering
    os.chdir(tmp_path)
    
    def mock_get(url, params=None, timeout=None):
        if "api.eia.gov" in url:
            return DummyResponse(200, json_data={
                "response": {
                    "data": [
                        {
                            "period": "2024-07-01", 
                            "type": "M",  # Monthly data (not daily)
                            "value": 123.4, 
                            "respondent": "WRONG_REGION",  # Wrong region
                            "timezone-description": "Eastern"
                        }
                    ],
                    "total": 1
                }
            })
        else:
            # Backup also fails
            raise Exception("Backup fail")
    
    monkeypatch.setattr("requests.get", mock_get)
    monkeypatch.setattr("os.makedirs", lambda *args, **kwargs: None)
    monkeypatch.setattr("pandas.DataFrame.to_csv", lambda self, *args, **kwargs: None)
    
    result = fetch_energy_data(city_config, "2024-07-01", "2024-07-01", "dummy")
    
    # Should return empty list since no valid records after filtering
    assert result == []