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

"""
Main data pipeline orchestration module.
"""

def nan_to_none(obj):
    if isinstance(obj, float) and np.isnan(obj):
        return None
    if isinstance(obj, dict):
        return {k: nan_to_none(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [nan_to_none(x) for x in obj]
    return obj

def run_pipeline(start_date: str, end_date: str):
    """
    Orchestrate fetching, merging, and saving weather and energy data.
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
    """
    load_dotenv()
    with open("config/cities.yaml", "r") as f:
        config = yaml.safe_load(f)
    noaa_token = os.getenv("NOAA_API_TOKEN")
    eia_key = os.getenv("EIA_API_KEY")
    all_weather = []
    all_energy = []
    for city in config["cities"]:
        weather = fetch_weather_data(city, start_date, end_date, noaa_token)
        energy = fetch_energy_data(city, start_date, end_date, eia_key)
        all_weather.extend(weather)
        all_energy.extend(energy)
    df_weather = pd.DataFrame(all_weather)
    df_energy = pd.DataFrame(all_energy)
    # Merge on date and city
    if not df_weather.empty and not df_energy.empty:
        df = pd.merge(df_weather, df_energy, on=["date", "city"], how="outer")
    elif not df_weather.empty:
        df = df_weather
    elif not df_energy.empty:
        df = df_energy
    else:
        logging.error("No data fetched for any city.")
        return
    # Save merged data
    out_path = f"data/merged_{start_date}_to_{end_date}.csv"
    df.to_csv(out_path, index=False)
    logging.info(f"Saved merged data to {out_path}") 

     # Run quality checks
    quality_report = run_data_quality_checks(df, datetime.now().strftime('%Y-%m-%d'))
    report_path = f"reports/quality_report_{start_date}_to_{end_date}.txt"
    generate_quality_report(quality_report, report_path)
    
    # Save JSON version for dashboard
    quality_report = nan_to_none(quality_report)
    json_path = f"reports/quality_{start_date}_to_{end_date}.json"
    with open(json_path, 'w') as f:
        json.dump(quality_report, f, indent=2, default=str)
    
    logging.info(f"Data quality report generated: {report_path}")