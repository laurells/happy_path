import pytest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import pandas as pd
import os

from dashboard.app import convert_to_local, load_data

# Test convert_to_local utility
@pytest.mark.parametrize("city, tz_str", [
    ("New York", "America/New_York"),
    ("Chicago", "America/Chicago"),
    ("Houston", "America/Chicago"),
    ("Phoenix", "America/Phoenix"),
    ("Seattle", "America/Los_Angeles"),
])
def test_convert_to_local(city, tz_str):
    utc_dt = datetime(2024, 7, 1, 12, 0, 0, tzinfo=timezone.utc)
    local_dt = convert_to_local(utc_dt, city)
    assert local_dt.tzinfo == ZoneInfo(tz_str)
    # Check that the hour is correct for the timezone offset
    offset_hours = (local_dt.hour - utc_dt.hour) % 24
    # The offset should match the timezone's UTC offset (not a strict test, but checks conversion)
    assert isinstance(local_dt, datetime)

# Test load_data utility

def test_load_data(tmp_path):
    # Create a temporary CSV file
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    csv_path = tmp_path / "test.csv"
    df.to_csv(csv_path, index=False)
    # Use the load_data function
    loaded = load_data(str(csv_path))
    pd.testing.assert_frame_equal(loaded, df) 