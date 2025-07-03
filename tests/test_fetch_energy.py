import pytest
import pandas as pd
from pipeline.fetch_energy import fetch_energy_data
from datetime import datetime, timedelta

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

def test_fetch_energy_data_api_success(monkeypatch, city_config):
    # Mock requests.get to return a successful API response
    def mock_get(url, params=None, timeout=None):
        return DummyResponse(200, json_data={
            "response": {
                "data": [
                    {"period": "2024-07-01", "frequency": "daily", "value": 123.4},
                    {"period": "2024-07-02", "frequency": "daily", "value": 234.5},
                ]
            }
        })
    monkeypatch.setattr("requests.get", mock_get)
    start_date = "2024-07-01"
    end_date = "2024-07-07"
    api_key = "dummy"
    result = fetch_energy_data(city_config, start_date, end_date, api_key)
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["city"] == "TestCity"
    assert result[0]["energy_mwh"] == 123.4

def test_fetch_energy_data_api_fail_backup_success(monkeypatch, tmp_path, city_config):
    # Mock requests.get: first call (API) fails, second call (backup) returns CSV
    calls = {"count": 0}
    def mock_get(url, params=None, timeout=None):
        if "api.eia.gov" in url:
            calls["count"] += 1
            raise Exception("API fail")
        # Simulate backup CSV download
        csv = "date,value,frequency\n2024-07-01,111.1,daily\n2024-07-02,222.2,daily"
        return DummyResponse(200, content=csv.encode())
    monkeypatch.setattr("requests.get", mock_get)
    # Patch pd.read_csv to read from the downloaded backup file
    real_read_csv = pd.read_csv
    def fake_read_csv(path, *a, **kw):
        if str(path).startswith("data/eia_backup_"):
            return real_read_csv(path, *a, **kw)
        return real_read_csv(path, *a, **kw)
    monkeypatch.setattr(pd, "read_csv", fake_read_csv)
    # Ensure data dir exists
    import os
    os.makedirs("data", exist_ok=True)
    start_date = "2024-07-01"
    end_date = "2024-07-07"
    api_key = "dummy"
    result = fetch_energy_data(city_config, start_date, end_date, api_key)
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["energy_mwh"] == 111.1
    assert result[1]["energy_mwh"] == 222.2

def test_fetch_energy_data_all_fail(monkeypatch, city_config):
    # Both API and backup fail
    def mock_get(url, params=None, timeout=None):
        raise Exception("fail")
    monkeypatch.setattr("requests.get", mock_get)
    start_date = "2024-07-01"
    end_date = "2024-07-07"
    api_key = "dummy"
    result = fetch_energy_data(city_config, start_date, end_date, api_key)
    assert result == [] 