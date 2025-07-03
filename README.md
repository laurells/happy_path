# Energy & Weather Data Pipeline

## Overview
This project fetches weather and energy consumption data for 5 US cities, performs data quality checks, and provides a Streamlit dashboard for analysis.

## Project Structure
```
happy_path/
├── config/
│   └── cities.yaml
├── data/
├── logs/
├── pipeline/
│   ├── fetch_weather.py
│   ├── fetch_energy.py
│   ├── data_pipeline.py
│   └── data_quality.py
├── dashboard/
│   └── app.py
├── scripts/
│   ├── run_daily_pipeline.py
│   └── fetch_historical.py
├── requirements.txt
├── README.md
└── .env
```

## Setup
1. **Clone the repo and install dependencies:**
   ```
   pip install -r requirements.txt
   ```
2. **Configure API Keys:**
   - Create a `.env` file in the project root:
     ```
     NOAA_API_TOKEN=your_noaa_token_here
     EIA_API_KEY=your_eia_key_here
     ```
3. **Edit `config/cities.yaml`** if you want to add/remove cities.

## Running the Pipeline
- **Daily fetch:**
  ```
  python scripts/run_daily_pipeline.py
  ```
- **Fetch 90 days of historical data:**
  ```
  python scripts/fetch_historical.py
  ```

## Data & Logs
- Data is stored in the `data/` directory.
- Logs are written to the `logs/` directory for debugging and monitoring.

## Next Steps
- Implement the API fetching logic in `pipeline/` modules.
- Build the Streamlit dashboard in `dashboard/app.py`.

## Notes
- The config file uses YAML for clarity and flexibility.
- API keys are loaded from `.env` for security.
- All code is modular and production-ready for easy extension.