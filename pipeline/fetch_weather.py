import requests
import logging
import os
import pandas as pd
import gzip
from typing import List, Dict
from datetime import datetime
import time

"""
Module for fetching weather data from NOAA API.
"""

NOAA_BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"


def fetch_weather_data(city_config: Dict, start_date: str, end_date: str, api_token: str) -> List[Dict]:
    """
    Fetch daily high/low temperature data for a city from NOAA.
    If the API fails, attempt to download and parse the latest GHCND daily file from NOAA FTP/HTTP.
    Implements retry logic for rate limiting and transient errors.
    Args:
        city_config: Dict with city info (name, station id, etc.)
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        api_token: NOAA API token
    Returns:
        List of dicts with weather data (date, tmax, tmin, city, etc.)
    """
    headers = {"token": api_token}
    params = {
        "datasetid": "GHCND",
        "stationid": city_config["noaa_station_id"],
        "startdate": start_date,
        "enddate": end_date,
        "datatypeid": "TMAX,TMIN",
        "units": "metric",
        "limit": 1000
    }
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(NOAA_BASE_URL, headers=headers, params=params, timeout=30)
            if response.status_code == 429:
                logging.warning(f"NOAA API rate limited for {city_config['name']} (attempt {attempt+1}/{max_retries})")
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
            if 500 <= response.status_code < 600:
                logging.warning(f"NOAA API server error {response.status_code} for {city_config['name']} (attempt {attempt+1}/{max_retries})")
                time.sleep(2 ** attempt)
                continue
            response.raise_for_status()
            data = response.json().get("results", [])
            # Organize by date
            daily = {}
            for entry in data:
                date_str = entry["date"][:10]
                if date_str not in daily:
                    daily[date_str] = {"date": date_str, "city": city_config["name"]}
                if entry["datatype"] == "TMAX":
                    # NOAA returns tenths of deg C; convert to F
                    daily[date_str]["tmax_f"] = round((entry["value"] / 10) * 9/5 + 32, 1)
                elif entry["datatype"] == "TMIN":
                    daily[date_str]["tmin_f"] = round((entry["value"] / 10) * 9/5 + 32, 1)
            # Fill missing values with None
            for d in daily.values():
                d.setdefault("tmax_f", None)
                d.setdefault("tmin_f", None)
            logging.info(f"NOAA API used for {city_config['name']}")
            return list(daily.values())
        except Exception as e:
            logging.error(f"NOAA fetch failed for {city_config['name']} (attempt {attempt+1}/{max_retries}): {e}")
            time.sleep(2 ** attempt)
    # If all retries fail, use backup
    logging.error(f"NOAA API failed after {max_retries} attempts for {city_config['name']}. Falling back to backup.")
    # Try backup GHCND file
    try:
        # Download the latest GHCND daily file (compressed)
        ghcnd_url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd_all.tar.gz"
        local_tar = "data/ghcnd_all.tar.gz"
        local_txt = f"data/{city_config['noaa_station_id'].replace(':','_')}.dly"
        # Download tar.gz if not already present or older than 1 day
        if not os.path.exists(local_tar) or (time.time() - os.path.getmtime(local_tar) > 86400):
            r = requests.get(ghcnd_url, timeout=120)
            r.raise_for_status()
            with open(local_tar, 'wb') as f:
                f.write(r.content)
        # Extract the station file from the tar.gz
        import tarfile
        with tarfile.open(local_tar, 'r:gz') as tar:
            member_name = city_config['noaa_station_id'].replace(':','_') + '.dly'
            try:
                tar.extract(member_name, path='data')
            except Exception:
                logging.error(f"Station file {member_name} not found in GHCND archive.")
                return []
        # Parse the .dly file for TMAX and TMIN
        def parse_ghcnd_dly(filepath, start_date, end_date):
            # GHCND .dly format: https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt
            records = []
            with open(filepath, 'r') as f:
                for line in f:
                    station = line[0:11]
                    year = line[11:15]
                    month = line[15:17]
                    element = line[17:21]
                    for day in range(1,32):
                        value = line[21+(day-1)*8:26+(day-1)*8].strip()
                        if value == '' or value == '-9999':
                            continue
                        date_str = f"{year}-{month}-{day:02d}"
                        if date_str < start_date or date_str > end_date:
                            continue
                        if element in ['TMAX','TMIN']:
                            val_f = round((int(value)/10)*9/5+32,1)
                            rec = next((r for r in records if r['date']==date_str), None)
                            if not rec:
                                rec = {'date': date_str, 'city': city_config['name']}
                                records.append(rec)
                            if element == 'TMAX':
                                rec['tmax_f'] = val_f
                            elif element == 'TMIN':
                                rec['tmin_f'] = val_f
            # Fill missing values with None
            for r in records:
                r.setdefault('tmax_f', None)
                r.setdefault('tmin_f', None)
            return records
        records = parse_ghcnd_dly(local_txt, start_date, end_date)
        logging.info(f"NOAA BACKUP GHCND used for {city_config['name']}")
        return records
    except Exception as e2:
        logging.error(f"NOAA backup fetch failed for {city_config['name']}: {e2}")
        return [] 