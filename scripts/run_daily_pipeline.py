import os
import logging
from pipeline.data_pipeline import run_pipeline
import yaml
from dotenv import load_dotenv

if __name__ == "__main__":
    load_dotenv()
    logging.basicConfig(filename="logs/daily_pipeline.log", level=logging.INFO)
    with open("config/cities.yaml", "r") as f:
        config = yaml.safe_load(f)
    # Example: run for today
    from datetime import date
    today = date.today().isoformat()
    run_pipeline(start_date=today, end_date=today) 