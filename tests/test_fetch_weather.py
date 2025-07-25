import pytest
import os
import tempfile
import shutil
from unittest.mock import Mock, patch, MagicMock
from pipeline.fetch_weather import fetch_weather_data

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
    return {"name": "TestCity", "noaa_station_id": "GHCND:FAKE123456"}

@pytest.fixture
def temp_data_dir():
    """Create a temporary data directory for tests"""
    temp_dir = tempfile.mkdtemp()
    data_dir = os.path.join(temp_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    
    # Change to temp directory for test
    original_cwd = os.getcwd()
    os.chdir(temp_dir)
    
    yield data_dir
    
    # Cleanup
    os.chdir(original_cwd)
    shutil.rmtree(temp_dir)

def test_fetch_weather_data_api_success(monkeypatch, city_config):
    """Test successful API response"""
    def mock_get(url, headers=None, params=None, timeout=None):
        return DummyResponse(200, json_data={
            "results": [
                {"date": "2024-07-01T00:00:00", "datatype": "TMAX", "value": 30.0, "station": "GHCND:FAKE123456"},
                {"date": "2024-07-01T00:00:00", "datatype": "TMIN", "value": 20.0, "station": "GHCND:FAKE123456"},
                {"date": "2024-07-02T00:00:00", "datatype": "TMAX", "value": 31.0, "station": "GHCND:FAKE123456"},
                {"date": "2024-07-02T00:00:00", "datatype": "TMIN", "value": 21.0, "station": "GHCND:FAKE123456"},
            ]
        })
    
    monkeypatch.setattr("requests.get", mock_get)
    
    # Mock pandas to_csv to avoid file operations
    mock_df = Mock()
    mock_df.to_csv = Mock()
    monkeypatch.setattr("pandas.DataFrame", Mock(return_value=mock_df))
    
    start_date = "2024-07-01"
    end_date = "2024-07-02"
    api_token = "dummy"
    
    result = fetch_weather_data(city_config, start_date, end_date, api_token)
    
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["city"] == "TestCity"
    assert result[0]["tmax_f"] > result[0]["tmin_f"]
    # Check temperature conversion (30C = 86F, 20C = 68F)
    assert abs(result[0]["tmax_f"] - 86.0) < 0.1
    assert abs(result[0]["tmin_f"] - 68.0) < 0.1

    
def test_fetch_weather_data_all_fail(monkeypatch, city_config):
    """Test when both API and backup fail"""
    def mock_get(url, headers=None, params=None, timeout=None):
        raise Exception("Network failure")
    
    monkeypatch.setattr("requests.get", mock_get)
    
    start_date = "2024-07-01"
    end_date = "2024-07-02"
    api_token = "dummy"
    
    result = fetch_weather_data(city_config, start_date, end_date, api_token)
    assert result == []

def test_fetch_weather_data_rate_limited_then_success(monkeypatch, city_config):
    """Test rate limiting with eventual success"""
    call_count = [0]
    
    def mock_get(url, headers=None, params=None, timeout=None):
        call_count[0] += 1
        if call_count[0] <= 2:
            return DummyResponse(429)  # Rate limited
        else:
            return DummyResponse(200, json_data={
                "results": [
                    {"date": "2024-07-01T00:00:00", "datatype": "TMAX", "value": 25.0, "station": "GHCND:FAKE123456"},
                    {"date": "2024-07-01T00:00:00", "datatype": "TMIN", "value": 15.0, "station": "GHCND:FAKE123456"},
                ]
            })
    
    monkeypatch.setattr("requests.get", mock_get)
    monkeypatch.setattr("time.sleep", lambda x: None)  # Skip sleep delays
    
    # Mock pandas to_csv to avoid file operations
    mock_df = Mock()
    mock_df.to_csv = Mock()
    monkeypatch.setattr("pandas.DataFrame", Mock(return_value=mock_df))
    
    start_date = "2024-07-01"
    end_date = "2024-07-01"
    api_token = "dummy"
    
    result = fetch_weather_data(city_config, start_date, end_date, api_token)
    
    assert isinstance(result, list)
    assert len(result) == 1
    assert call_count[0] == 3  # Two rate limited calls, then success

def test_fetch_weather_data_station_id_filtering(monkeypatch, city_config):
    """Test that data from wrong station IDs is filtered out"""
    def mock_get(url, headers=None, params=None, timeout=None):
        return DummyResponse(200, json_data={
            "results": [
                {"date": "2024-07-01T00:00:00", "datatype": "TMAX", "value": 25.0, "station": "GHCND:FAKE123456"},
                {"date": "2024-07-01T00:00:00", "datatype": "TMIN", "value": 15.0, "station": "GHCND:FAKE123456"},
                {"date": "2024-07-01T00:00:00", "datatype": "TMAX", "value": 99.0, "station": "GHCND:WRONG123456"},  # Wrong station
                {"date": "2024-07-01T00:00:00", "datatype": "TMIN", "value": 99.0, "station": "GHCND:WRONG123456"},  # Wrong station
            ]
        })
    
    monkeypatch.setattr("requests.get", mock_get)
    
    # Mock pandas to_csv to avoid file operations
    mock_df = Mock()
    mock_df.to_csv = Mock()
    monkeypatch.setattr("pandas.DataFrame", Mock(return_value=mock_df))
    
    start_date = "2024-07-01"
    end_date = "2024-07-01"
    api_token = "dummy"
    
    result = fetch_weather_data(city_config, start_date, end_date, api_token)
    
    assert isinstance(result, list)
    assert len(result) == 1
    # Should only have data from correct station (25C = 77F, 15C = 59F)
    assert abs(result[0]["tmax_f"] - 77.0) < 0.1
    assert abs(result[0]["tmin_f"] - 59.0) < 0.1