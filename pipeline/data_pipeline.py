import logging
from typing import List, Dict
import pandas as pd
import yaml
import os
from dotenv import load_dotenv
from pipeline.fetch_weather import fetch_weather_data
from pipeline.fetch_energy import fetch_energy_data
from pipeline.data_quality import run_data_quality_checks, generate_quality_report
from datetime import datetime
import json
import numpy as np
from pytz import timezone

"""
Main data pipeline orchestration module.
Handles fetching, merging, and quality checking of weather and energy data.
"""

def nan_to_none(obj):
    """
    Recursively convert all numpy NaN values in a nested structure to None.
    Useful for JSON serialization and data quality reporting.
    """
    if isinstance(obj, float) and np.isnan(obj):
        return None
    if isinstance(obj, dict):
        return {k: nan_to_none(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [nan_to_none(x) for x in obj]
    return obj

def run_pipeline(start_date: str, end_date: str):
    """
    Orchestrate fetching, merging, and saving weather and energy data for all configured cities.
    Also runs data quality checks and saves reports.
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
    """
    # Load environment variables (API keys)
    load_dotenv()
    # Load city configuration from YAML
    with open("config/cities.yaml", "r") as f:
        config = yaml.safe_load(f)
    noaa_token = os.getenv("NOAA_API_TOKEN")
    eia_key = os.getenv("EIA_API_KEY")
    all_weather = []
    all_energy = []
    # Fetch data for each city
    for city in config["cities"]:
        weather = fetch_weather_data(city, start_date, end_date, noaa_token)
        energy = fetch_energy_data(city, start_date, end_date, eia_key)
        all_weather.extend(weather)
        all_energy.extend(energy)
    # Convert to DataFrames
    df_weather = pd.DataFrame(all_weather)
    df_energy = pd.DataFrame(all_energy)
    # Merge weather and energy data on date and city
    if not df_weather.empty and not df_energy.empty:
        df = pd.merge(df_weather, df_energy, on=["date", "city"], how="outer")
    elif not df_weather.empty:
        df = df_weather
    elif not df_energy.empty:
        df = df_energy
    else:
        logging.error("No data fetched for any city.")
        return
    # Save merged data to CSV for dashboard and analysis
    out_path = f"data/merged_{start_date}_to_{end_date}.csv"
    df.to_csv(out_path, index=False)
    logging.info(f"Saved merged data to {out_path}") 

    # Use New York timezone for all reporting (business standard)
    ny_tz = timezone('America/New_York')
    now_local = datetime.now(ny_tz)
    report_date = now_local.strftime('%Y-%m-%d')

    # Run data quality checks and generate reports
    quality_report = run_data_quality_checks(df, report_date)
    report_path = f"reports/quality_report_{start_date}_to_{end_date}.txt"
    generate_quality_report(quality_report, report_path)
    
    # Save JSON version of the quality report for dashboard use
    quality_report = nan_to_none(quality_report)
    json_path = f"reports/quality_{start_date}_to_{end_date}.json"
    with open(json_path, 'w') as f:
        json.dump(quality_report, f, indent=2, default=str)
    
    logging.info(f"Data quality report generated: {report_path}")