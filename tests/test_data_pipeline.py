import pytest
import numpy as np
from pipeline.data_pipeline import nan_to_none
import os
from datetime import datetime

# Test nan_to_none for various structures
@pytest.mark.parametrize("input_obj, expected", [
    (np.nan, None),
    (1.0, 1.0),
    ("foo", "foo"),
    ([1, np.nan, 2], [1, None, 2]),
    ({'a': np.nan, 'b': 2}, {'a': None, 'b': 2}),
    ([{'x': np.nan}, {'x': 1}], [{'x': None}, {'x': 1}]),
    ({'a': [np.nan, 3]}, {'a': [None, 3]}),
    ({'a': {'b': np.nan}}, {'a': {'b': None}}),
    ([], []),
    ({}, {}),
])
def test_nan_to_none(input_obj, expected):
    assert nan_to_none(input_obj) == expected

def test_run_pipeline_smoke(monkeypatch, tmp_path):
    import pipeline.data_pipeline as dp
    import builtins
    
    # Save the real open before monkeypatching
    real_open = builtins.open
    
    # Mock config/cities.yaml
    cities_yaml = """cities:
  - name: TestCity
    noaa_station_id: TEST123
    eia_region_code: TEST
"""
    config_path = tmp_path / "cities.yaml"
    config_path.write_text(cities_yaml)
    
    def open_patch(f, *a, **kw):
        if f == "config/cities.yaml":
            return real_open(config_path, *a, **kw)
        return real_open(f, *a, **kw)
    
    monkeypatch.setattr("builtins.open", open_patch)
    
    # Mock environment variables
    monkeypatch.setenv("NOAA_API_TOKEN", "dummy")
    monkeypatch.setenv("EIA_API_KEY", "dummy")
    
    # Mock fetch_weather_data and fetch_energy_data
    monkeypatch.setattr(dp, "fetch_weather_data", lambda *a, **kw: [
        {"date": "2024-07-01", "city": "TestCity", "tmax_f": 80.0, "tmin_f": 60.0}
    ])
    monkeypatch.setattr(dp, "fetch_energy_data", lambda *a, **kw: [
        {"date": "2024-07-01", "city": "TestCity", "energy_mwh": 100.0}
    ])
    
    # Mock validation functions
    monkeypatch.setattr(dp, "validate_weather_data", lambda *a, **kw: {"is_valid": True, "issues": []})
    monkeypatch.setattr(dp, "validate_energy_data", lambda *a, **kw: True)
    
    # Mock run_data_quality_checks to return a valid report structure
    def mock_quality_checks(df, report_date):
        return {
            "run_date": report_date,
            "missing_values": {"summary": {}},
            "outliers": {
                "temperature": {"count": 0, "records": []}, 
                "energy": {"count": 0, "records": []}
            },
            "freshness": {
                "latest_data_date": report_date, 
                "current_date": report_date, 
                "is_stale": False, 
                "days_since_update": 0
            },
            "consistency": {
                "duplicates": 0, 
                "date_range": {"start": report_date, "end": report_date}, 
                "cities_missing": []
            },
            "details": {}
        }
    
    monkeypatch.setattr(dp, "run_data_quality_checks", mock_quality_checks)
    
    # Mock generate_quality_report function
    def mock_generate_quality_report(quality_report, report_path):
        # Create the report file
        with open(report_path, 'w') as f:
            f.write("Mock quality report\n")
    
    monkeypatch.setattr(dp, "generate_quality_report", mock_generate_quality_report)
    
    # Mock timezone to return a fixed date
    class MockTimezone:
        def __init__(self, tz_name):
            self.tz_name = tz_name
    
    class MockDatetime:
        @staticmethod
        def now(tz):
            class MockNow:
                def strftime(self, fmt):
                    return "2024-07-01"
            return MockNow()
    
    monkeypatch.setattr(dp, "timezone", MockTimezone)
    monkeypatch.setattr(dp, "datetime", MockDatetime)
    
    # Change working directory to tmp_path
    old_cwd = os.getcwd()
    os.chdir(tmp_path)
    
    # Create required subdirectories
    (tmp_path / "data").mkdir()
    (tmp_path / "reports").mkdir()
    
    try:
        # Run pipeline
        start_date = "2024-07-01"
        end_date = "2024-07-01"
        dp.run_pipeline(start_date, end_date)
        
        # Check that files are created with the correct names (as per the actual code)
        assert (tmp_path / "data/merged_data.csv").exists()
        assert (tmp_path / "reports/quality_report.txt").exists()
        assert (tmp_path / "reports/quality_report.json").exists()
        
        # Verify the CSV file has expected content
        import pandas as pd
        df = pd.read_csv(tmp_path / "data/merged_data.csv")
        assert len(df) == 1
        assert df.iloc[0]["city"] == "TestCity"
        assert df.iloc[0]["tmax_f"] == 80.0
        assert df.iloc[0]["energy_mwh"] == 100.0
        
    finally:
        os.chdir(old_cwd)