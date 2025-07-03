import pytest
import os
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
    return {"name": "TestCity", "noaa_station_id": "FAKE:STATION"}

def test_fetch_weather_data_api_success(monkeypatch, city_config):
    # Mock requests.get to return a successful API response
    def mock_get(url, headers=None, params=None, timeout=None):
        return DummyResponse(200, json_data={
            "results": [
                {"date": "2024-07-01T00:00:00", "datatype": "TMAX", "value": 300},
                {"date": "2024-07-01T00:00:00", "datatype": "TMIN", "value": 200},
                {"date": "2024-07-02T00:00:00", "datatype": "TMAX", "value": 310},
                {"date": "2024-07-02T00:00:00", "datatype": "TMIN", "value": 210},
            ]
        })
    monkeypatch.setattr("requests.get", mock_get)
    start_date = "2024-07-01"
    end_date = "2024-07-02"
    api_token = "dummy"
    result = fetch_weather_data(city_config, start_date, end_date, api_token)
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["city"] == "TestCity"
    assert result[0]["tmax_f"] > result[0]["tmin_f"]

def test_fetch_weather_data_api_fail_backup_success(monkeypatch, tmp_path, city_config):
    # Mock requests.get: first call (API) fails, second call (backup) returns tar.gz with .dly
    import tarfile
    import io
    calls = {"count": 0}
    def mock_get(url, headers=None, params=None, timeout=None):
        if "ncei.noaa.gov" in url:
            calls["count"] += 1
            raise Exception("API fail")
        # Simulate backup tar.gz download
        # Create a fake .dly file in tar.gz
        dly_content = (
            "FAKE:STATION202407TMAX  0300    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999\n"
            "FAKE:STATION202407TMIN  0200    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999\n"
        )
        tar_bytes = io.BytesIO()
        with tarfile.open(fileobj=tar_bytes, mode="w:gz") as tar:
            info = tarfile.TarInfo(name="FAKE_STATION.dly")
            info.size = len(dly_content.encode())
            tar.addfile(info, io.BytesIO(dly_content.encode()))
        tar_bytes.seek(0)
        return DummyResponse(200, content=tar_bytes.read())
    monkeypatch.setattr("requests.get", mock_get)
    # Patch tarfile.open to read from the fake tar.gz
    real_tarfile_open = tarfile.open
    def fake_tarfile_open(name=None, mode="r:gz", fileobj=None, **kwargs):
        if name == "data/ghcnd_all.tar.gz" or (name is None and fileobj is not None):
            return real_tarfile_open(fileobj=io.BytesIO(mock_get(None).content), mode=mode)
        return real_tarfile_open(name, mode=mode, fileobj=fileobj, **kwargs)
    monkeypatch.setattr("tarfile.open", fake_tarfile_open)
    # Patch os.path.exists and os.path.getmtime to always re-download
    monkeypatch.setattr(os.path, "exists", lambda path: False)
    monkeypatch.setattr(os.path, "getmtime", lambda path: 0)
    # Patch open for .dly file to read from the extracted file
    real_open = open
    def fake_open(path, *a, **kw):
        if str(path).endswith(".dly"):
            return real_open(path, *a, **kw)
        return real_open(path, *a, **kw)
    monkeypatch.setattr("builtins.open", fake_open)
    # Ensure data dir exists
    os.makedirs("data", exist_ok=True)
    # Write the expected .dly file to the data/ directory
    dly_path = os.path.join("data", "FAKE_STATION.dly")
    with open(dly_path, "w") as f:
        f.write(
            "FAKE:STATION202407TMAX  0300    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999\n"
            "FAKE:STATION202407TMIN  0200    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999    9999\n"
        )
    start_date = "2024-07-01"
    end_date = "2024-07-01"
    api_token = "dummy"
    result = fetch_weather_data(city_config, start_date, end_date, api_token)
    assert isinstance(result, list)
    assert len(result) >= 1
    assert result[0]["city"] == "TestCity"

def test_fetch_weather_data_all_fail(monkeypatch, city_config):
    # Both API and backup fail
    def mock_get(url, headers=None, params=None, timeout=None):
        raise Exception("fail")
    monkeypatch.setattr("requests.get", mock_get)
    start_date = "2024-07-01"
    end_date = "2024-07-02"
    api_token = "dummy"
    result = fetch_weather_data(city_config, start_date, end_date, api_token)
    assert result == [] 