import pytest
import pandas as pd
import os
from pipeline.data_quality import run_data_quality_checks, generate_quality_report
from datetime import datetime, timedelta

CITY_NAMES = ["New York", "Chicago", "Houston", "Phoenix", "Seattle"]

def make_test_df():
    today = datetime.now().date()
    return pd.DataFrame([
        {"date": today, "city": "New York", "tmax_f": 85, "tmin_f": 70, "energy_mwh": 100},
        {"date": today, "city": "Chicago", "tmax_f": 95, "tmin_f": 80, "energy_mwh": -10},  # energy outlier
        {"date": today, "city": "Houston", "tmax_f": 140, "tmin_f": 90, "energy_mwh": 120}, # temp outlier
        {"date": today, "city": "Phoenix", "tmax_f": None, "tmin_f": 75, "energy_mwh": 110}, # missing tmax_f
        {"date": today, "city": "Seattle", "tmax_f": 80, "tmin_f": None, "energy_mwh": 90},  # missing tmin_f
        {"date": today, "city": "New York", "tmax_f": 85, "tmin_f": 70, "energy_mwh": 100}, # duplicate
    ])

def test_run_data_quality_checks_all():
    df = make_test_df()
    report_date = datetime.now().strftime('%Y-%m-%d')
    report = run_data_quality_checks(df, report_date)
    # Check keys
    assert set(report.keys()) >= {"run_date", "missing_values", "outliers", "freshness", "consistency", "details"}
    # Missing values
    assert report["missing_values"]["summary"]["tmax_f"] == 1
    assert report["missing_values"]["summary"]["tmin_f"] == 1
    # Outliers
    assert report["outliers"]["energy"]["count"] == 1
    assert report["outliers"]["temperature"]["count"] == 1
    # Freshness
    assert report["freshness"]["is_stale"] in (True, False)
    # Consistency
    assert report["consistency"]["duplicates"] == 1
    assert set(report["consistency"]["cities_missing"]) == set()
    # Details
    assert any("date" in rec and "city" in rec for rec in report["details"]["missing_tmax_f"])

def test_run_data_quality_checks_missing_city():
    # Remove Seattle
    df = make_test_df().query('city != "Seattle"')
    report_date = datetime.now().strftime('%Y-%m-%d')
    report = run_data_quality_checks(df, report_date)
    assert "Seattle" in report["consistency"]["cities_missing"]

def test_generate_quality_report(tmp_path):
    # Minimal valid report
    report = {
        "run_date": "2024-07-01",
        "missing_values": {"summary": {"tmax_f": 1, "tmin_f": 0, "energy_mwh": 2}},
        "outliers": {"temperature": {"count": 1, "records": []}, "energy": {"count": 2, "records": []}},
        "freshness": {"latest_data_date": "2024-07-01", "current_date": "2024-07-01", "is_stale": False, "days_since_update": 0},
        "consistency": {"duplicates": 0, "date_range": {"start": "2024-07-01", "end": "2024-07-01"}, "cities_missing": ["Houston"]},
        "details": {"missing_tmax_f": [{"date": "2024-07-01", "city": "Houston", "tmax_f": None}]}
    }
    out_path = tmp_path / "quality_report.txt"
    generate_quality_report(report, str(out_path))
    assert out_path.exists()
    content = out_path.read_text()
    assert "Data Quality Report - 2024-07-01" in content
    assert "tmax_f: 1 missing" in content
    assert "Temperature Outliers: 1" in content
    assert "Energy Outliers: 2" in content
    assert "Missing Cities: Houston" in content
    assert "2024-07-01 | Houston" in content 