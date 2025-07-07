import requests
import logging
import os
import pandas as pd
from typing import List, Dict
from datetime import datetime, timedelta
import time

"""
Module for fetching energy consumption data from EIA API.
Handles both real-time API calls and backup CSV download for robustness.
"""

EIA_BASE_URL = "https://api.eia.gov/v2/electricity/rto/daily-region-data/data/"

CITY_TIMEZONE = {
    "New York": "America/New_York",
    "Chicago": "America/Chicago",
    "Houston": "America/Chicago",
    "Phoenix": "America/Phoenix",        # Doesn't observe daylight saving
    "Seattle": "America/Los_Angeles"
}

def fetch_energy_data(city_config: Dict, start_date: str, end_date: str, api_key: str) -> List[Dict]:
    """
    Fetch daily energy consumption data for a city/region from EIA.
    If the API fails, attempt to download and parse the latest CSV from the EIA backup site.
    Implements retry logic for rate limiting and transient errors.
    Fetches data for the full requested date range.
    
    Args:
        city_config: Dict with city info (name, region code, etc.)
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        api_key: EIA API key
    Returns:
        List of dicts with energy data (date, usage, city, etc.)
    """
    # Use the full requested date range
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    params = {
        "api_key": api_key,
        "frequency": "daily",
        "data[0]": "value",
        "facets[respondent][]": city_config["eia_region_code"],
        "start": start_dt.isoformat(),
        "end": end_dt.isoformat(),
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "offset": 0,
        "length": 500
    }
    
    # Implement pagination to handle large date ranges
    all_data = []
    offset = 0
    max_retries = 3
    
    while True:
        current_data = []  # Initialize for this iteration
        
        for attempt in range(max_retries):
            try:
                # Update offset for pagination
                params["offset"] = offset
                
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
                
                response_data = response.json()
                current_data = response_data.get("response", {}).get("data", [])
                total_count = response_data.get("response", {}).get("total", 0)
                try:
                    total_count = int(total_count)
                except (ValueError, TypeError):
                    total_count = 0
                
                all_data.extend(current_data)
                
                # Check if we've got all the data
                if len(all_data) >= total_count or len(current_data) < 500:
                    break
                    
                offset += 500
                logging.info(f"EIA API pagination for {city_config['name']}: fetched {len(all_data)} records, total available: {total_count}")
                break  # Success, exit retry loop
                
            except Exception as e:
                logging.error(f"EIA fetch failed for {city_config['name']} (attempt {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
        
        # If we've got all data or no more data, break out of pagination loop
        if len(all_data) >= total_count or len(current_data) < 500:
            break
        
        # If all retries failed and we got no data this iteration, break
        if not current_data:
            break
    
    # Save raw EIA data before any filtering for debugging
    os.makedirs("data", exist_ok=True)
    raw_df = pd.DataFrame(all_data)
    raw_path = f"data/eia_raw_{city_config['name'].replace(' ', '_')}.csv"
    raw_df.to_csv(raw_path, index=False)
    
    results = []
    target_region = city_config["eia_region_code"]
    
    # Log what we're filtering for
    logging.info(f"Filtering for {city_config['name']}: region={target_region}")
    
    # Group records by date to identify duplicates
    date_groups = {}
    for entry in all_data:
        if (
            entry.get("value") is not None
            and entry.get("type") == "D"  # Daily data
            and entry.get("respondent") == target_region
        ):
            date_key = entry["period"]
            if date_key not in date_groups:
                date_groups[date_key] = []
            date_groups[date_key].append(entry)
    
    # Process each date and handle duplicates
    for date_key, entries in date_groups.items():
        selected_entry = None
        
        if len(entries) == 1:
            # Only one record for this date, use it
            selected_entry = entries[0]
        else:
            # Multiple records - select the correct timezone for this city
            timezone_list = [entry.get("timezone-description", "Unknown") for entry in entries]
            logging.info(f"Multiple timezone records for {city_config['name']} on {date_key}: {timezone_list}")
            
            # Get the expected timezone for this city
            expected_timezone = CITY_TIMEZONE.get(city_config['name'])
            if expected_timezone:
                # Map timezone names to EIA format
                timezone_mapping = {
                    "America/New_York": "Eastern",
                    "America/Chicago": "Central", 
                    "America/Phoenix": "Mountain",
                    "America/Los_Angeles": "Pacific"
                }
                expected_eia_timezone = timezone_mapping.get(expected_timezone, expected_timezone)
                
                # Try to find the entry with the correct timezone
                for entry in entries:
                    if entry.get("timezone-description") == expected_eia_timezone:
                        selected_entry = entry
                        logging.info(f"Selected {expected_eia_timezone} timezone for {city_config['name']} on {date_key}")
                        break
            
            # If no matching timezone found, use the first entry and log warning
            if not selected_entry:
                selected_entry = entries[0]
                logging.warning(f"No matching timezone found for {city_config['name']}, using first available: {selected_entry.get('timezone-description')}")
        
        if selected_entry:
            results.append({
                "date": selected_entry["period"],
                "city": city_config["name"],
                "energy_mwh": float(selected_entry["value"]),
                "region_code": selected_entry.get("respondent"),
                "timezone": selected_entry.get("timezone-description"),
                "data_source": "EIA_API"
            })
    
    logging.info(f"EIA API used for {city_config['name']}: {len(results)} records found")
    
    # Log timezone distribution for debugging
    if results:
        timezone_counts = {}
        for record in results:
            tz = record.get("timezone", "Unknown")
            timezone_counts[tz] = timezone_counts.get(tz, 0) + 1
        logging.info(f"Timezone distribution for {city_config['name']}: {timezone_counts}")
    
    # If we got data, return it
    if results:
        return results
    
    # If no data was retrieved, try backup
    logging.error(f"EIA API returned no data for {city_config['name']}. Falling back to backup.")
    return _fetch_backup_data(city_config, start_date, end_date)


def _fetch_backup_data(city_config: Dict, start_date: str, end_date: str) -> List[Dict]:
    """
    Fetch data from EIA backup CSV with improved timezone filtering.
    """
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
        
        # Try to find the required columns
        date_col = next((col for col in df.columns if 'date' in col.lower() or 'period' in col.lower()), None)
        value_col = next((col for col in df.columns if 'consumption' in col.lower() or 'value' in col.lower() or 'mwh' in col.lower()), None)
        freq_col = next((col for col in df.columns if 'frequency' in col.lower()), None)
        region_col = next((col for col in df.columns if 'respondent' in col.lower() or 'region' in col.lower()), None)
        timezone_col = next((col for col in df.columns if 'timezone' in col.lower()), None)
        
        if not date_col or not value_col:
            raise ValueError('Could not find date or value columns in backup CSV')
        
        # Select relevant columns and rename for consistency
        cols_to_keep = [date_col, value_col]
        if freq_col:
            cols_to_keep.append(freq_col)
        if region_col:
            cols_to_keep.append(region_col)
        if timezone_col:
            cols_to_keep.append(timezone_col)
            
        df = df[cols_to_keep].rename(columns={
            date_col: 'date', 
            value_col: 'energy_mwh'
        })
        
        # Convert date and filter by date range
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        mask = (df['date'] >= start_dt.isoformat()) & (df['date'] <= end_dt.isoformat())
        
        # Filter by frequency if available
        if freq_col:
            mask = mask & (df[freq_col] == 'daily')
        
        # Filter by region if available
        if region_col:
            mask = mask & (df[region_col] == city_config["eia_region_code"])
        
        df = df.loc[mask]
        
        results = []
        for _, row in df.iterrows():
            if pd.notnull(row['energy_mwh']):
                results.append({
                    "date": row['date'],
                    "city": city_config["name"],
                    "energy_mwh": row['energy_mwh'],
                    "region_code": city_config["eia_region_code"],
                    "timezone": row.get(timezone_col, "Unknown"),
                    "data_source": "EIA_BACKUP_CSV"
                })
        
        logging.info(f"EIA BACKUP CSV used for {city_config['name']}: {len(results)} records found")
        return results
        
    except Exception as e:
        logging.error(f"EIA backup fetch failed for {city_config['name']}: {e}")
        return []


def validate_energy_data(data: List[Dict], city_name: str) -> bool:
    """
    Validate that the returned energy data is actually for the requested city/region.
    Also checks for timezone consistency.
    """
    if not data:
        logging.warning(f"No energy data returned for {city_name}")
        return False
    
    # Check if all records are for the same city
    cities = set(record.get("city") for record in data)
    if len(cities) > 1:
        logging.warning(f"Multiple cities found in data for {city_name}: {cities}")
        return False
    
    # Check if we have the expected city
    if city_name not in cities:
        logging.warning(f"Expected city {city_name} not found in returned data")
        return False
    
    # Check timezone consistency (should be consistent since we filter by timezone)
    timezones = set(record.get("timezone") for record in data if record.get("timezone"))
    
    if len(timezones) > 1:
        logging.warning(f"Multiple timezones found in data for {city_name}: {timezones}")
        # Don't fail validation for this, just log the warning
    else:
        logging.info(f"Timezone consistency check passed for {city_name}: {list(timezones)}")
    
    logging.info(f"Energy data validation passed for {city_name}: {len(data)} records")
    return True