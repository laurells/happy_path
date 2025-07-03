import os
import logging
from pipeline.data_pipeline import run_pipeline
import yaml
from dotenv import load_dotenv
from datetime import date, timedelta

if __name__ == "__main__":
    load_dotenv()
    logging.basicConfig(filename="logs/fetch_historical.log", level=logging.INFO)
    with open("config/cities.yaml", "r") as f:
        config = yaml.safe_load(f)
    end_date = date.today()
    start_date = end_date - timedelta(days=89)
    run_pipeline(start_date=start_date.isoformat(), end_date=end_date.isoformat()) 