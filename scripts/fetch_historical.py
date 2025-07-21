import os
import logging
from pipeline.data_pipeline import run_pipeline
import yaml
from dotenv import load_dotenv
from datetime import date, timedelta, datetime, timezone
import pytz

"""
Script to fetch and process 90 days of historical weather and energy data for all configured cities.
Runs the main pipeline and logs the process for monitoring and debugging.
"""

if __name__ == "__main__":
    # Load environment variables (API keys)
    load_dotenv()
    
    # Set up logging to a file for historical fetches
    logging.basicConfig(filename="logs/fetch_historical.log", level=logging.INFO)
    
    # Load city configuration
    with open("config/cities.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    # DEBUG: Check what date we're getting
    print(f"System date.today(): {date.today()}")
    print(f"System datetime.now(): {datetime.now()}")
    
    
    # Calculate date range: last 90 days (including today)
    end_date = datetime.now(timezone.utc).date()

    start_date = end_date - timedelta(days=89)
    
    print(f"Date range: {start_date.isoformat()} to {end_date.isoformat()}")
    
    # Also log this information
    logging.info(f"Starting pipeline with date range: {start_date.isoformat()} to {end_date.isoformat()}")
    logging.info(f"System date: {date.today()}")
    
    # Run the pipeline for the full date range
    run_pipeline(start_date=start_date.isoformat(), end_date=end_date.isoformat())

    # Run data quality check and generate report
    import pandas as pd
    from pipeline.data_quality import run_data_quality_checks, generate_quality_report
    from datetime import datetime

    df = pd.read_csv("data/merged_data.csv")
    report_date = datetime.now().strftime('%Y-%m-%d')
    quality_report = run_data_quality_checks(df, report_date)
    generate_quality_report(quality_report, "reports/quality_report.txt")
    with open("reports/quality_report.json", "w") as f:
        import json
        json.dump(quality_report, f, indent=2, default=str)