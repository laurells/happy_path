import os
import logging
from pipeline.data_pipeline import run_pipeline
import yaml
from dotenv import load_dotenv
from datetime import date, timedelta

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
    # Calculate date range: last 90 days (including today)
    end_date = date.today()
    start_date = end_date - timedelta(days=89)
    # Run the pipeline for the full date range
    run_pipeline(start_date=start_date.isoformat(), end_date=end_date.isoformat()) 