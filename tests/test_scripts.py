import pytest
import sys
from datetime import date, timedelta

@pytest.mark.parametrize("script_name, expected_days", [
    ("scripts/fetch_historical.py", 90),
    ("scripts/run_daily_pipeline.py", 1),
])
def test_scripts_run(monkeypatch, script_name, expected_days):
    # Patch run_pipeline to record calls
    called = {}
    def fake_run_pipeline(start_date, end_date):
        called["start_date"] = start_date
        called["end_date"] = end_date
    monkeypatch.setattr("pipeline.data_pipeline.run_pipeline", fake_run_pipeline)
    # Patch load_dotenv, logging.basicConfig, yaml.safe_load
    monkeypatch.setattr("dotenv.load_dotenv", lambda *a, **kw: None)
    monkeypatch.setattr("logging.basicConfig", lambda *a, **kw: None)
    monkeypatch.setattr("yaml.safe_load", lambda f: {"cities": ["TestCity"]})
    # Patch open to return a dummy config
    import builtins
    real_open = builtins.open
    def fake_open(f, *a, **kw):
        if f == "config/cities.yaml":
            from io import StringIO
            return StringIO("cities:\n  - TestCity\n")
        return real_open(f, *a, **kw)
    monkeypatch.setattr("builtins.open", fake_open)
    # Patch sys.argv and __name__
    monkeypatch.setattr(sys, "argv", [script_name])
    # Run the script as __main__
    import runpy
    runpy.run_path(script_name, run_name="__main__")
    # Check that run_pipeline was called with correct date range
    assert "start_date" in called and "end_date" in called
    start = date.fromisoformat(called["start_date"])
    end = date.fromisoformat(called["end_date"])
    assert (end - start).days == expected_days - 1 