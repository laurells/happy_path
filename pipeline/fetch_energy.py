import requests
import logging
import os
import pandas as pd
from typing import List, Dict
from datetime import datetime
import time

"""
Module for fetching energy consumption data from EIA API.
"""

EIA_BASE_URL = "https://api.eia.gov/v2/electricity/rto/daily-region-data/data/"


def fetch_energy_data(city_config: Dict, start_date: str, end_date: str, api_key: str) -> List[Dict]:
    """
    Fetch daily energy consumption data for a city/region from EIA.
    If the API fails, attempt to download and parse the latest CSV from the EIA backup site.
    Implements retry logic for rate limiting and transient errors.
    Args:
        city_config: Dict with city info (name, region code, etc.)
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        api_key: EIA API key
    Returns:
        List of dicts with energy data (date, usage, city, etc.)
    """
    params = {
        "api_key": api_key,
        "frequency": "daily",
        "data[0]": "value",
        "facets[respondent][]": city_config["eia_region_code"],
        "start": start_date,
        "end": end_date,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "offset": 0,
        "length": 500
    }
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(EIA_BASE_URL, params=params, timeout=30)
            if response.status_code == 429:
                logging.warning(f"EIA API rate limited for {city_config['name']} (attempt {attempt+1}/{max_retries})")
                time.sleep(2 ** attempt)
                continue
            if 500 <= response.status_code < 600:
                logging.warning(f"EIA API server error {response.status_code} for {city_config['name']} (attempt {attempt+1}/{max_retries})")
                time.sleep(2 ** attempt)
                continue
            response.raise_for_status()
            data = response.json().get("response", {}).get("data", [])
            results = []
            for entry in data:
                results.append({
                    "date": entry["period"],
                    "city": city_config["name"],
                    "energy_mwh": entry.get("value", None)
                })
            logging.info(f"EIA API used for {city_config['name']}")
            return results
        except Exception as e:
            logging.error(f"EIA fetch failed for {city_config['name']} (attempt {attempt+1}/{max_retries}): {e}")
            time.sleep(2 ** attempt)
    # If all retries fail, use backup
    logging.error(f"EIA API failed after {max_retries} attempts for {city_config['name']}. Falling back to backup.")
    # Try backup CSV download
    try:
        backup_url = f"https://www.eia.gov/electricity/data/browser/csv.php?region={city_config['eia_region_code']}&type=consumption"
        backup_csv = f"data/eia_backup_{city_config['eia_region_code']}.csv"
        # Download the CSV
        r = requests.get(backup_url, timeout=30)
        r.raise_for_status()
        with open(backup_csv, 'wb') as f:
            f.write(r.content)
        # Parse the CSV
        df = pd.read_csv(backup_csv)
        # Try to find the date and value columns
        date_col = [col for col in df.columns if 'date' in col.lower() or 'period' in col.lower()]
        value_col = [col for col in df.columns if 'consumption' in col.lower() or 'value' in col.lower() or 'mwh' in col.lower()]
        if not date_col or not value_col:
            raise ValueError('Could not find date or value columns in backup CSV')
        df = df[[date_col[0], value_col[0]]].rename(columns={date_col[0]: 'date', value_col[0]: 'energy_mwh'})
        # Filter by date range
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        mask = (df['date'] >= start_date) & (df['date'] <= end_date)
        df = df.loc[mask]
        results = []
        for _, row in df.iterrows():
            results.append({
                "date": row['date'],
                "city": city_config["name"],
                "energy_mwh": row['energy_mwh']
            })
        logging.info(f"EIA BACKUP CSV used for {city_config['name']}")
        return results
    except Exception as e2:
        logging.error(f"EIA backup fetch failed for {city_config['name']}: {e2}")
        return [] 