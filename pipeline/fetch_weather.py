import requests
import logging
import os
import pandas as pd
import gzip
from typing import List, Dict
from datetime import datetime, timedelta
import time

"""
Module for fetching weather data from NOAA API.
Handles both real-time API calls and backup GHCND archive parsing for robustness.
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
        "limit": 1000  # Increased limit to handle larger date ranges
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
            
            # Validate that we got data for the correct station
            if data:
                stations_in_response = set(entry.get("station") for entry in data)
                expected_station = city_config["noaa_station_id"]
                if expected_station not in stations_in_response:
                    logging.warning(f"Expected station {expected_station} not found in response for {city_config['name']}")
                    logging.warning(f"Stations in response: {stations_in_response}")
            
            # Save raw NOAA data before any transformation
            raw_df = pd.DataFrame(data)
            # FIXED: Use consistent filename for raw data
            raw_path = f"data/weather_raw_{city_config['name'].replace(' ', '_')}.csv"
            raw_df.to_csv(raw_path, index=False)
            
            # Organize by date - filter by station ID for safety
            daily = {}
            records_processed = 0
            records_filtered = 0
            
            for entry in data:
                # Double-check station ID matches
                if entry.get("station") != city_config["noaa_station_id"]:
                    records_filtered += 1
                    continue
                
                date_str = entry["date"][:10]
                if date_str not in daily:
                    daily[date_str] = {
                        "date": date_str, 
                        "city": city_config["name"],
                        "station_id": entry.get("station")
                    }
                
                if entry["datatype"] == "TMAX":
                    # NOAA API returns degrees C; convert to F
                    daily[date_str]["tmax_f"] = round(entry["value"] * 9/5 + 32, 1)
                elif entry["datatype"] == "TMIN":
                    daily[date_str]["tmin_f"] = round(entry["value"] * 9/5 + 32, 1)
                
                records_processed += 1
            
            # Fill missing values with None and add data quality indicators
            for d in daily.values():
                d.setdefault("tmax_f", None)
                d.setdefault("tmin_f", None)
                # Add data quality flag
                d["has_both_temps"] = d["tmax_f"] is not None and d["tmin_f"] is not None
            
            logging.info(f"NOAA API used for {city_config['name']}: {records_processed} records processed, {records_filtered} filtered out")
            logging.info(f"Retrieved {len(daily)} days of weather data")
            
            # Validate date range coverage
            if daily:
                actual_start = min(daily.keys())
                actual_end = max(daily.keys())
                logging.info(f"Weather data covers {actual_start} to {actual_end}")
                
                # Check for gaps in data
                expected_days = (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days + 1
                actual_days = len(daily)
                if actual_days < expected_days * 0.9:  # Less than 90% coverage
                    logging.warning(f"Low weather data coverage: {actual_days}/{expected_days} days")
            
            return list(daily.values())
            
        except Exception as e:
            logging.error(f"NOAA fetch failed for {city_config['name']} (attempt {attempt+1}/{max_retries}): {e}")
            time.sleep(2 ** attempt)
    
    # If all retries fail, use backup
    logging.error(f"NOAA API failed after {max_retries} attempts for {city_config['name']}. Falling back to backup.")
    
    # Try backup GHCND file
    try:
        # Download the latest GHCND daily file (compressed)
        ghcnd_url = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd_all.tar.gz"
        local_tar = "data/ghcnd_all.tar.gz"
        
        # Extract just the station ID without the prefix for file naming
        station_id_clean = city_config['noaa_station_id'].split(':')[-1] if ':' in city_config['noaa_station_id'] else city_config['noaa_station_id']
        local_txt = f"data/{station_id_clean}.dly"
        
        # Download tar.gz if not already present or older than 1 day
        if not os.path.exists(local_tar) or (time.time() - os.path.getmtime(local_tar) > 86400):
            logging.info(f"Downloading GHCND archive for {city_config['name']}...")
            r = requests.get(ghcnd_url, timeout=120)
            r.raise_for_status()
            with open(local_tar, 'wb') as f:
                f.write(r.content)
        
        # Extract the station file from the tar.gz
        import tarfile
        with tarfile.open(local_tar, 'r:gz') as tar:
            member_name = station_id_clean + '.dly'
            try:
                tar.extract(member_name, path='data')
            except Exception:
                logging.error(f"Station file {member_name} not found in GHCND archive for {city_config['name']}")
                return []
        
        # Parse the .dly file for TMAX and TMIN
        def parse_ghcnd_dly(filepath, start_date, end_date, expected_station_id):
            """Parse GHCND .dly file with proper validation"""
            # GHCND .dly format: https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt
            records = []
            lines_processed = 0
            
            with open(filepath, 'r') as f:
                for line in f:
                    lines_processed += 1
                    if len(line) < 21:  # Skip malformed lines
                        continue
                        
                    station = line[0:11].strip()
                    year = line[11:15]
                    month = line[15:17]
                    element = line[17:21]
                    
                    # Validate station ID matches
                    if station != expected_station_id:
                        continue
                    
                    # Only process temperature data
                    if element not in ['TMAX', 'TMIN']:
                        continue
                    
                    for day in range(1, 32):
                        try:
                            value_start = 21 + (day-1) * 8
                            value_end = 26 + (day-1) * 8
                            
                            if value_end > len(line):
                                break
                                
                            value = line[value_start:value_end].strip()
                            if value == '' or value == '-9999':
                                continue
                            
                            # Validate date
                            try:
                                date_str = f"{year}-{month}-{day:02d}"
                                datetime.strptime(date_str, "%Y-%m-%d")  # Validate date format
                            except ValueError:
                                continue  # Skip invalid dates like Feb 30
                            
                            if date_str < start_date or date_str > end_date:
                                continue
                            
                            val_f = round((int(value) / 10) * 9/5 + 32, 1)
                            rec = next((r for r in records if r['date'] == date_str), None)
                            if not rec:
                                rec = {
                                    'date': date_str, 
                                    'city': city_config['name'],
                                    'station_id': station
                                }
                                records.append(rec)
                            
                            if element == 'TMAX':
                                rec['tmax_f'] = val_f
                            elif element == 'TMIN':
                                rec['tmin_f'] = val_f
                                
                        except (ValueError, IndexError) as e:
                            continue  # Skip malformed data
            
            # Fill missing values with None and add quality indicators
            for r in records:
                r.setdefault('tmax_f', None)
                r.setdefault('tmin_f', None)
                r["has_both_temps"] = r["tmax_f"] is not None and r["tmin_f"] is not None
            
            logging.info(f"Parsed {lines_processed} lines from GHCND file, extracted {len(records)} weather records")
            return records
        
        records = parse_ghcnd_dly(local_txt, start_date, end_date, station_id_clean)
        logging.info(f"NOAA BACKUP GHCND used for {city_config['name']}: {len(records)} records")
        return records
        
    except Exception as e2:
        logging.error(f"NOAA backup fetch failed for {city_config['name']}: {e2}")
        return []


def validate_weather_data(data: List[Dict], city_name: str, start_date: str, end_date: str) -> Dict:
    """
    Validate weather data quality and coverage for energy forecasting.
    Returns a dict with validation results.
    """
    validation_results = {
        "is_valid": True,
        "issues": [],
        "stats": {}
    }
    
    if not data:
        validation_results["is_valid"] = False
        validation_results["issues"].append("No weather data returned")
        return validation_results
    
    # Check date coverage
    dates_in_data = set(record["date"] for record in data)
    expected_dates = set()
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    current_dt = start_dt
    
    while current_dt <= end_dt:
        expected_dates.add(current_dt.strftime("%Y-%m-%d"))
        current_dt += timedelta(days=1)
    
    missing_dates = expected_dates - dates_in_data
    coverage_pct = (len(dates_in_data) / len(expected_dates)) * 100
    
    validation_results["stats"]["coverage_percent"] = coverage_pct
    validation_results["stats"]["missing_days"] = len(missing_dates)
    validation_results["stats"]["total_days"] = len(expected_dates)
    
    if coverage_pct < 90:
        validation_results["is_valid"] = False
        validation_results["issues"].append(f"Low date coverage: {coverage_pct:.1f}%")
    
    # Check temperature data quality
    records_with_both_temps = sum(1 for record in data if record.get("has_both_temps"))
    temp_completeness = (records_with_both_temps / len(data)) * 100
    
    validation_results["stats"]["temp_completeness_percent"] = temp_completeness
    
    if temp_completeness < 80:
        validation_results["is_valid"] = False
        validation_results["issues"].append(f"Low temperature data completeness: {temp_completeness:.1f}%")
    
    # Check for consistent city/station
    cities = set(record.get("city") for record in data)
    stations = set(record.get("station_id") for record in data if record.get("station_id"))
    
    if len(cities) > 1:
        validation_results["is_valid"] = False
        validation_results["issues"].append(f"Multiple cities in data: {cities}")
    
    if len(stations) > 1:
        validation_results["is_valid"] = False
        validation_results["issues"].append(f"Multiple weather stations in data: {stations}")
    
    validation_results["stats"]["unique_cities"] = len(cities)
    validation_results["stats"]["unique_stations"] = len(stations)
    
    # Log validation results
    if validation_results["is_valid"]:
        logging.info(f"Weather data validation passed for {city_name}")
    else:
        logging.warning(f"Weather data validation failed for {city_name}: {validation_results['issues']}")
    
    return validation_results