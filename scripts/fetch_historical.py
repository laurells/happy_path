import os
import logging
from pipeline.data_pipeline import run_pipeline
import yaml
from dotenv import load_dotenv
from datetime import date, timedelta, datetime
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
    
    # Use a more explicit approach to get today's date
    # You can also force a specific timezone if needed
    eastern = pytz.timezone('US/Eastern')
    now_eastern = datetime.now(eastern)
    today_eastern = now_eastern.date()
    
    print(f"Today in Eastern timezone: {today_eastern}")
    
    # Calculate date range: last 90 days (including today)
    end_date = today_eastern  # Use the timezone-aware date
    start_date = end_date - timedelta(days=89)
    
    print(f"Date range: {start_date.isoformat()} to {end_date.isoformat()}")
    
    # Also log this information
    logging.info(f"Starting pipeline with date range: {start_date.isoformat()} to {end_date.isoformat()}")
    logging.info(f"System date: {date.today()}, Eastern date: {today_eastern}")
    
    # Run the pipeline for the full date range
    run_pipeline(start_date=start_date.isoformat(), end_date=end_date.isoformat())