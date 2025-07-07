import logging
from typing import List, Dict
import pandas as pd
import yaml
import os
from dotenv import load_dotenv
from pipeline.fetch_weather import fetch_weather_data, validate_weather_data
from pipeline.fetch_energy import fetch_energy_data, validate_energy_data
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
    
    if not noaa_token:
        logging.error("NOAA_API_TOKEN not found in environment variables")
        return
    
    if not eia_key:
        logging.error("EIA_API_KEY not found in environment variables")
        return
    
    all_weather = []
    all_energy = []
    pipeline_stats = {
        "cities_processed": 0,
        "weather_records": 0,
        "energy_records": 0,
        "validation_failures": [],
        "processing_errors": []
    }
    
    # Fetch data for each city
    for city in config["cities"]:
        city_name = city["name"]
        logging.info(f"Processing data for {city_name}")
        
        try:
            # Fetch weather data
            weather = fetch_weather_data(city, start_date, end_date, noaa_token)
            weather_validation = validate_weather_data(weather, city_name, start_date, end_date)
            
            if not weather_validation["is_valid"]:
                pipeline_stats["validation_failures"].append({
                    "city": city_name,
                    "data_type": "weather",
                    "issues": weather_validation["issues"]
                })
                logging.warning(f"Weather data validation failed for {city_name}")
            
            # Fetch energy data
            energy = fetch_energy_data(city, start_date, end_date, eia_key)
            energy_validation = validate_energy_data(energy, city_name)
            
            if not energy_validation:
                pipeline_stats["validation_failures"].append({
                    "city": city_name,
                    "data_type": "energy",
                    "issues": ["Energy data validation failed"]
                })
                logging.warning(f"Energy data validation failed for {city_name}")
            
            # Add to collections
            all_weather.extend(weather)
            all_energy.extend(energy)
            
            pipeline_stats["cities_processed"] += 1
            pipeline_stats["weather_records"] += len(weather)
            pipeline_stats["energy_records"] += len(energy)
            
            logging.info(f"Completed {city_name}: {len(weather)} weather records, {len(energy)} energy records")
            
        except Exception as e:
            error_msg = f"Failed to process {city_name}: {str(e)}"
            logging.error(error_msg)
            pipeline_stats["processing_errors"].append({
                "city": city_name,
                "error": str(e)
            })
    
    # Convert to DataFrames
    df_weather = pd.DataFrame(all_weather)
    df_energy = pd.DataFrame(all_energy)
    
    # Log data summary
    logging.info(f"Total weather records: {len(df_weather)}")
    logging.info(f"Total energy records: {len(df_energy)}")
    
    # CRITICAL FIX: Remove the problematic timezone-based filtering
    # The energy data should already be correctly filtered by region in fetch_energy_data
    # No additional filtering needed here as it was causing incorrect data aggregation
    
    # Validate that we have data for each expected city
    if not df_weather.empty:
        weather_cities = set(df_weather["city"].unique())
        logging.info(f"Weather data available for cities: {weather_cities}")
    
    if not df_energy.empty:
        energy_cities = set(df_energy["city"].unique())
        logging.info(f"Energy data available for cities: {energy_cities}")
        
        # Check for region code consistency
        if "region_code" in df_energy.columns:
            region_summary = df_energy.groupby("city")["region_code"].nunique()
            for city_name, region_count in region_summary.items():
                if region_count > 1:
                    logging.warning(f"Multiple region codes found for {city_name}: {region_count}")
    
    # Merge weather and energy data on date and city
    if not df_weather.empty and not df_energy.empty:
        # Before merging, check for date/city overlaps
        weather_keys = set(df_weather[["date", "city"]].apply(tuple, axis=1))
        energy_keys = set(df_energy[["date", "city"]].apply(tuple, axis=1))
        
        common_keys = weather_keys.intersection(energy_keys)
        logging.info(f"Common date/city combinations: {len(common_keys)}")
        
        if len(common_keys) == 0:
            logging.warning("No common date/city combinations found between weather and energy data")
        
        # Perform the merge
        df = pd.merge(df_weather, df_energy, on=["date", "city"], how="outer")
        
        # Add merge quality indicators
        df["has_weather"] = df["tmax_f"].notna() | df["tmin_f"].notna()
        df["has_energy"] = df["energy_mwh"].notna()
        df["complete_record"] = df["has_weather"] & df["has_energy"]
        
        # Log merge statistics
        total_records = len(df)
        complete_records = df["complete_record"].sum()
        weather_only = df["has_weather"].sum() - complete_records
        energy_only = df["has_energy"].sum() - complete_records
        
        logging.info(f"Merge results - Total: {total_records}, Complete: {complete_records}, Weather-only: {weather_only}, Energy-only: {energy_only}")
        
    elif not df_weather.empty:
        df = df_weather
        df["has_weather"] = True
        df["has_energy"] = False
        df["complete_record"] = False
        logging.warning("Only weather data available")
    elif not df_energy.empty:
        df = df_energy
        df["has_weather"] = False
        df["has_energy"] = True
        df["complete_record"] = False
        logging.warning("Only energy data available")
    else:
        logging.error("No data fetched for any city.")
        return
    
    # Sort by city and date for consistent output
    df = df.sort_values(["city", "date"]).reset_index(drop=True)
    
    # FIXED: Use consistent filename instead of date-range-based naming
    # This prevents creating new files every time the pipeline runs
    out_path = "data/merged_data.csv"
    
    # Check if existing data file exists and merge with new data
    if os.path.exists(out_path):
        logging.info(f"Existing data file found: {out_path}")
        existing_df = pd.read_csv(out_path)
        existing_df['date'] = pd.to_datetime(existing_df['date'])
        df['date'] = pd.to_datetime(df['date'])
        
        # Remove overlapping records from existing data
        # Keep only records that are NOT in the new date range
        new_start = pd.to_datetime(start_date)
        new_end = pd.to_datetime(end_date)
        
        # Filter existing data to exclude the date range we're updating
        existing_filtered = existing_df[
            ~((existing_df['date'] >= new_start) & (existing_df['date'] <= new_end))
        ]
        
        logging.info(f"Existing data: {len(existing_df)} records")
        logging.info(f"New data: {len(df)} records")
        logging.info(f"Existing data after filtering: {len(existing_filtered)} records")
        
        # Combine existing and new data
        combined_df = pd.concat([existing_filtered, df], ignore_index=True)
        combined_df = combined_df.sort_values(['city', 'date']).reset_index(drop=True)
        
        # Remove any remaining duplicates (just in case)
        combined_df = combined_df.drop_duplicates(subset=['date', 'city'], keep='last')
        
        logging.info(f"Combined data: {len(combined_df)} records")
        df = combined_df
    else:
        logging.info(f"Creating new data file: {out_path}")
    
    # Save merged data to CSV for dashboard and analysis
    df.to_csv(out_path, index=False)
    logging.info(f"Saved merged data to {out_path}")
    
    # Save pipeline statistics
    pipeline_stats["final_record_count"] = len(df)
    pipeline_stats["cities_with_data"] = df["city"].nunique()
    pipeline_stats["date_range"] = {
        "start": df["date"].min() if not df.empty else None,
        "end": df["date"].max() if not df.empty else None
    }
    
    # Use New York timezone for all reporting (business standard)
    ny_tz = timezone('America/New_York')
    now_local = datetime.now(ny_tz)
    report_date = now_local.strftime('%Y-%m-%d')
    
    # Run data quality checks and generate reports
    quality_report = run_data_quality_checks(df, report_date)
    
    # Add pipeline statistics to quality report
    quality_report["pipeline_stats"] = pipeline_stats
    
    # FIXED: Use consistent report filenames
    report_path = "reports/quality_report.txt"
    generate_quality_report(quality_report, report_path)
    
    # Save JSON version of the quality report for dashboard use
    quality_report = nan_to_none(quality_report)
    json_path = "reports/quality_report.json"
    with open(json_path, 'w') as f:
        json.dump(quality_report, f, indent=2, default=str)
    
    logging.info(f"Data quality report generated: {report_path}")
    
    # Final pipeline summary
    logging.info("=" * 50)
    logging.info("PIPELINE SUMMARY")
    logging.info("=" * 50)
    logging.info(f"Cities processed: {pipeline_stats['cities_processed']}")
    logging.info(f"Weather records: {pipeline_stats['weather_records']}")
    logging.info(f"Energy records: {pipeline_stats['energy_records']}")
    logging.info(f"Final merged records: {pipeline_stats['final_record_count']}")
    
    if pipeline_stats["validation_failures"]:
        logging.warning(f"Validation failures: {len(pipeline_stats['validation_failures'])}")
        for failure in pipeline_stats["validation_failures"]:
            logging.warning(f"  - {failure['city']} ({failure['data_type']}): {failure['issues']}")
    
    if pipeline_stats["processing_errors"]:
        logging.error(f"Processing errors: {len(pipeline_stats['processing_errors'])}")
        for error in pipeline_stats["processing_errors"]:
            logging.error(f"  - {error['city']}: {error['error']}")
    
    logging.info("Pipeline completed successfully")


def validate_pipeline_config(config_path: str = "config/cities.yaml") -> bool:
    """
    Validate the pipeline configuration before running.
    """
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        
        required_fields = ["name", "noaa_station_id", "eia_region_code"]
        
        for city in config.get("cities", []):
            for field in required_fields:
                if field not in city:
                    logging.error(f"Missing required field '{field}' for city configuration")
                    return False
        
        logging.info(f"Configuration validation passed for {len(config.get('cities', []))} cities")
        return True
        
    except Exception as e:
        logging.error(f"Configuration validation failed: {e}")
        return False