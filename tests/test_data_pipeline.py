import pytest
import numpy as np
from pipeline.data_pipeline import nan_to_none
import os

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

# Note: run_pipeline is not unit-testable without heavy mocking due to side effects (file I/O, API calls)
# Here is a placeholder for a smoke test using monkeypatch and tmp_path if you want to test file creation logic.
# You can expand this with more mocks for fetch_weather_data, fetch_energy_data, etc.

def test_run_pipeline_smoke(monkeypatch, tmp_path):
    import pipeline.data_pipeline as dp
    import builtins
    import sys
    # Save the real open before monkeypatching
    real_open = builtins.open
    # Mock config/cities.yaml
    cities_yaml = "cities:\n  - name: TestCity"
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
    monkeypatch.setattr(dp, "fetch_weather_data", lambda *a, **kw: [{"date": "2024-07-01", "city": "TestCity", "tmax_f": 80.0}])
    monkeypatch.setattr(dp, "fetch_energy_data", lambda *a, **kw: [{"date": "2024-07-01", "city": "TestCity", "energy_mwh": 100.0}])
    # Mock run_data_quality_checks to return a valid report structure
    monkeypatch.setattr(dp, "run_data_quality_checks", lambda df, d: {
        "run_date": d,
        "missing_values": {"summary": {}},
        "outliers": {"temperature": {"count": 0, "records": []}, "energy": {"count": 0, "records": []}},
        "freshness": {"latest_data_date": d, "current_date": d, "is_stale": False, "days_since_update": 0},
        "consistency": {"duplicates": 0, "date_range": {"start": d, "end": d}, "cities_missing": []},
        "details": {}
    })
    # Patch out timezone to UTC for deterministic output
    monkeypatch.setattr(dp, "timezone", lambda tz: None)
    # Run pipeline
    start_date = "2024-07-01"
    end_date = "2024-07-01"
    # Change working directory to tmp_path
    old_cwd = os.getcwd()
    os.chdir(tmp_path)
    # Create required subdirectories
    (tmp_path / "data").mkdir()
    (tmp_path / "reports").mkdir()
    try:
        dp.run_pipeline(start_date, end_date)
        # Check that merged and report files are created
        assert (tmp_path / f"data/merged_{start_date}_to_{end_date}.csv").exists()
        assert (tmp_path / f"reports/quality_report_{start_date}_to_{end_date}.txt").exists()
        assert (tmp_path / f"reports/quality_{start_date}_to_{end_date}.json").exists()
    finally:
        os.chdir(old_cwd) 