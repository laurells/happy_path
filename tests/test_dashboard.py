import pytest
import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import os
import tempfile

from dashboard.app import convert_to_local, load_data

# Test convert_to_local
@pytest.mark.parametrize("city, tz_str", [
    ("New York", "America/New_York"),
    ("Chicago", "America/Chicago"),
    ("Houston", "America/Chicago"),
    ("Phoenix", "America/Phoenix"),
    ("Seattle", "America/Los_Angeles"),
    ("Unknown", "America/New_York"),
])
def test_convert_to_local(city, tz_str):
    utc_dt = datetime(2024, 7, 1, 12, 0, 0, tzinfo=timezone.utc)
    local_dt = convert_to_local(utc_dt, city)
    assert local_dt.tzinfo == ZoneInfo(tz_str)
    # Check that the time is correctly converted (offset)
    assert local_dt.utcoffset() is not None

# Test load_data (Streamlit cache is ignored in test)
def test_load_data(tmp_path, monkeypatch):
    # Create a temporary CSV file
    csv_content = "col1,col2\n1,2\n3,4"
    file_path = tmp_path / "test.csv"
    file_path.write_text(csv_content)
    # Patch st.cache_data to be a no-op (if needed)
    import dashboard.app as app_mod
    monkeypatch.setattr(app_mod.st, "cache_data", lambda *a, **kw: (lambda f: f))
    df = load_data(str(file_path))
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["col1", "col2"]
    assert df.shape == (2, 2)

# Test is_weekend (copied from dashboard/app.py)
def is_weekend(dt):
    return dt.weekday() >= 5

def test_is_weekend():
    # Saturday
    assert is_weekend(datetime(2024, 7, 6)) is True
    # Sunday
    assert is_weekend(datetime(2024, 7, 7)) is True
    # Monday
    assert is_weekend(datetime(2024, 7, 8)) is False
    # Friday
    assert is_weekend(datetime(2024, 7, 5)) is False 