import os
import yaml
import requests
from datetime import datetime, timedelta

# Load city configs from YAML
with open('config/cities.yaml', 'r') as f:
    config = yaml.safe_load(f)

city = config['cities'][0]  # Just the first city

yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

noaa_token = "VnnwQxuubJrkxqtNGTAmKWHxplJwTAZU"
eia_api_key = "SbmgOqeAknBlODMQVHr3GUGchFQw8z393MkRIz95"

if not noaa_token or not eia_api_key:
    print("Please set NOAA_API_TOKEN and EIA_API_KEY environment variables.")
    exit(1)

print(f"\n=== {city['name']} ({yesterday}) ===")

# NOAA Weather API (Daily summary for station)
noaa_url = f"https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
noaa_headers = {"token": noaa_token}
noaa_params = {
    "datasetid": "GHCND",
    "datatypeid": ["TMAX", "TMIN"],
    "stationid": city['noaa_station_id'],
    "startdate": yesterday,
    "enddate": yesterday,
    "units": "metric",
    "limit": 1000
}
try:
    r = requests.get(noaa_url, headers=noaa_headers, params=noaa_params, timeout=30)
    r.raise_for_status()
    print("Weather API response:")
    print(r.json())
except Exception as e:
    print("Weather API error:", e)

# EIA Energy API (Daily region data)
eia_url = "https://api.eia.gov/v2/electricity/rto/daily-region-data/data/"
eia_params = {
    "api_key": eia_api_key,
    "frequency": "daily",
    "data[0]": "value",
    "facets[respondent][]": city['eia_region_code'],
    "start": yesterday,
    "end": yesterday,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": 0,
    "length": 500
}
try:
    r = requests.get(eia_url, params=eia_params, timeout=30)
    r.raise_for_status()
    print("Energy API response:")
    print(r.json())
except Exception as e:
    print("Energy API error:", e) 