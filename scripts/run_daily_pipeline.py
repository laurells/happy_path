import os
import logging
from pipeline.data_pipeline import run_pipeline
import yaml
from dotenv import load_dotenv

"""
Script to fetch and process today's weather and energy data for all configured cities.
Runs the main pipeline and logs the process for monitoring and debugging.
"""

if __name__ == "__main__":
    # Load environment variables (API keys)
    load_dotenv()
    # Set up logging to a file for daily pipeline runs
    logging.basicConfig(filename="logs/daily_pipeline.log", level=logging.INFO)
    # Load city configuration
    with open("config/cities.yaml", "r") as f:
        config = yaml.safe_load(f)
    # Get today's date
    from datetime import date
    today = date.today().isoformat()
    # Run the pipeline for today only
    run_pipeline(start_date=today, end_date=today) 