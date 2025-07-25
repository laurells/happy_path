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

def test_run_data_quality_checks_all(monkeypatch):
    # Mock the load_city_names function to return our expected cities
    import pipeline.data_quality as dq
    monkeypatch.setattr(dq, "load_city_names", lambda *args: CITY_NAMES)
    
    df = make_test_df()
    report_date = datetime.now().strftime('%Y-%m-%d')
    report = run_data_quality_checks(df, report_date)
    
    # Check top-level structure (new comprehensive format)
    assert set(report.keys()) >= {"metadata", "issues", "summary"}
    
    # Check metadata
    assert report["metadata"]["run_date"] == report_date
    assert report["metadata"]["total_records"] == len(df)
    
    # Check missing values (now under issues)
    missing_issues = report["issues"]
    assert "missing_tmax_f" in missing_issues
    assert "missing_tmin_f" in missing_issues
    assert missing_issues["missing_tmax_f"]["count"] == 1
    assert missing_issues["missing_tmin_f"]["count"] == 1
    
    # Check outliers (now under issues)
    assert "energy_outliers" in missing_issues
    assert "temperature_outliers" in missing_issues
    assert missing_issues["energy_outliers"]["count"] >= 1  # negative energy
    assert missing_issues["temperature_outliers"]["count"] >= 1  # temp > 130
    
    # Check freshness (now under issues)
    assert "data_freshness" in missing_issues
    assert missing_issues["data_freshness"]["count"] >= 0  # days since update
    
    # Check consistency (now under issues)
    assert "duplicates" in missing_issues
    assert "missing_cities" in missing_issues
    assert missing_issues["duplicates"]["count"] == 2  # duplicate records
    assert missing_issues["missing_cities"]["count"] == 0  # all cities present
    
    # Check summary
    assert "quality_score" in report["summary"]
    assert 0 <= report["summary"]["quality_score"] <= 100

def test_run_data_quality_checks_missing_city(monkeypatch):
    # Mock the load_city_names function to return our expected cities
    import pipeline.data_quality as dq
    monkeypatch.setattr(dq, "load_city_names", lambda *args: CITY_NAMES)
    
    # Remove Seattle
    df = make_test_df().query('city != "Seattle"')
    report_date = datetime.now().strftime('%Y-%m-%d')
    report = run_data_quality_checks(df, report_date)
    
    # Check that Seattle is reported as missing
    missing_cities_issue = report["issues"]["missing_cities"]
    assert missing_cities_issue["count"] == 1
    missing_city_records = missing_cities_issue["records"]
    assert any(record["missing_city"] == "Seattle" for record in missing_city_records)

def test_generate_quality_report(tmp_path, monkeypatch):
    # Mock the load_city_names function
    import pipeline.data_quality as dq
    monkeypatch.setattr(dq, "load_city_names", lambda *args: CITY_NAMES)
    
    # Create a report in the new comprehensive format
    report = {
        "metadata": {
            "run_date": "2024-07-01 10:30:00",
            "total_records": 5,
            "date_range": {"start": "2024-07-01", "end": "2024-07-01"},
            "cities_analyzed": ["New York", "Chicago", "Houston", "Phoenix"],
            "thresholds_used": {
                "temp_max_threshold": 130.0,
                "temp_min_threshold": -50.0,
                "energy_min_threshold": 0.0,
                "energy_outlier_std_multiplier": 3.0,
                "freshness_days_threshold": 2,
                "missing_data_critical_pct": 10.0,
                "missing_data_warning_pct": 5.0,
                "duplicate_critical_pct": 1.0
            }
        },
        "issues": {
            "missing_tmax_f": {
                "issue_type": "Missing tmax_f",
                "severity": "low",
                "count": 1,
                "percentage": 20.0,
                "description": "1 missing values (20.00%) in tmax_f",
                "records": [{"date": "2024-07-01", "city": "Houston", "tmax_f": None}],
                "recommendation": "Monitor: Track tmax_f missing data trends."
            },
            "temperature_outliers": {
                "issue_type": "Temperature Outliers",
                "severity": "high",
                "count": 1,
                "percentage": 20.0,
                "description": "1 temperature records with extreme or illogical values",
                "records": [{"date": "2024-07-01", "city": "Phoenix", "tmax_f": 140, "tmin_f": 90}],
                "recommendation": "Review sensor calibration and data collection processes"
            },
            "energy_outliers": {
                "issue_type": "Energy Outliers",
                "severity": "high",
                "count": 2,
                "percentage": 40.0,
                "description": "2 energy records with negative or extreme values",
                "records": [{"date": "2024-07-01", "city": "Chicago", "energy_mwh": -10}],
                "recommendation": "Investigate data collection and meter reading processes"
            },
            "missing_cities": {
                "issue_type": "Missing Cities",
                "severity": "medium",
                "count": 1,
                "percentage": 20.0,
                "description": "1 expected cities missing from dataset",
                "records": [{"missing_city": "Seattle"}],
                "recommendation": "Check data source coverage and collection processes"
            }
        },
        "summary": {
            "total_issues": 5,
            "critical_issues": 0,
            "high_issues": 2,
            "medium_issues": 1,
            "low_issues": 1,
            "quality_score": 75.5
        }
    }
    
    out_path = tmp_path / "quality_report.txt"
    generate_quality_report(report, str(out_path))
    
    assert out_path.exists()
    content = out_path.read_text()
    
    # Check for key sections in the comprehensive report format
    assert "COMPREHENSIVE DATA QUALITY REPORT" in content
    assert "Generated: 2024-07-01 10:30:00" in content
    assert "EXECUTIVE SUMMARY" in content
    assert "Overall Quality Score: 75.5/100" in content
    assert "Total Records Analyzed: 5" in content
    assert "ISSUE SUMMARY" in content
    assert "Critical Issues: 0" in content
    assert "High Priority: 2" in content
    assert "Medium Priority: 1" in content
    assert "Low Priority: 1" in content
    assert "DETAILED FINDINGS" in content
    assert "MISSING TMAX_F" in content
    assert "TEMPERATURE OUTLIERS" in content
    assert "ENERGY OUTLIERS" in content
    assert "MISSING CITIES" in content
    assert "Severity: High" in content
    assert "missing_city: Seattle" in content
    assert "CONFIGURATION" in content

def test_empty_dataframe(monkeypatch):
    # Mock the load_city_names function
    import pipeline.data_quality as dq
    monkeypatch.setattr(dq, "load_city_names", lambda *args: CITY_NAMES)
    
    # Test with empty DataFrame
    df = pd.DataFrame()
    report_date = datetime.now().strftime('%Y-%m-%d')
    
    with pytest.raises(ValueError, match="Input DataFrame is empty"):
        run_data_quality_checks(df, report_date)

def test_minimal_valid_dataframe(monkeypatch):
    # Mock the load_city_names function
    import pipeline.data_quality as dq
    monkeypatch.setattr(dq, "load_city_names", lambda *args: CITY_NAMES)
    
    # Test with minimal valid DataFrame
    df = pd.DataFrame([
        {"date": "2024-07-01", "city": "New York", "tmax_f": 80, "tmin_f": 60, "energy_mwh": 100}
    ])
    report_date = datetime.now().strftime('%Y-%m-%d')
    report = run_data_quality_checks(df, report_date)
    
    # Should complete without errors
    assert report["metadata"]["total_records"] == 1
    assert report["summary"]["quality_score"] >= 0