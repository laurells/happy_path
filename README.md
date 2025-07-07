# Energy & Weather Data Pipeline

## Overview
This project fetches weather and energy consumption data for 5 major US cities, performs robust data quality checks, and provides a production-ready Streamlit dashboard for analytics. It is designed for business value, with automated data fetching, backup logic, error handling, and clear reporting.

## Project Structure
```
happy_path/
├── config/
│   └── cities.yaml         # City/region codes and station IDs
├── data/                   # Merged and raw data files
├── logs/                   # Pipeline and fetch logs
├── pipeline/
│   ├── fetch_weather.py    # NOAA API + backup logic
│   ├── fetch_energy.py     # EIA API + backup logic
│   ├── data_pipeline.py    # Orchestration and merging
│   └── data_quality.py     # Data quality checks and reporting
├── dashboard/
│   └── app.py              # Streamlit dashboard
├── scripts/
│   ├── run_daily_pipeline.py
│   └── fetch_historical.py
├── requirements.txt        # (Optional) pip requirements
├── pyproject.toml          # Modern dependency management
├── README.md
└── .env                    # API keys (not tracked)
```

## Setup
### 1. Clone the repo
```
git clone <repo-url>
cd happy_path
```

### 2. Install dependencies
#### Option A: Modern (Recommended)
- Install [uv](https://github.com/astral-sh/uv):
  ```
  pip install uv
  ```
- Install all dependencies from `pyproject.toml`:
  ```
  uv pip install -r requirements.txt  # for pip-style
  # OR
  uv pip install --system  # for pyproject.toml (recommended)
  ```

#### Option B: Classic pip
```
pip install -r requirements.txt
```

### 3. Configure API Keys
- Create a `.env` file in the project root:
  ```
  NOAA_API_TOKEN=your_noaa_token_here
  EIA_API_KEY=your_eia_key_here
  ```

### 4. Edit `config/cities.yaml` if you want to add/remove cities.

## Running the Pipeline

### Manual Execution
- **Daily fetch:**
  ```
  python scripts/run_daily_pipeline.py
  ```
- **Fetch 90 days of historical data:**
  ```
  python scripts/fetch_historical.py
  ```
- **Custom date range:**
  ```
  python scripts/run_daily_pipeline.py --start-date 2024-01-01 --end-date 2024-01-31
  ```

### Automated Execution (Windows)

#### Option 1: PowerShell Setup (Recommended)
```powershell
# Run as Administrator for system-wide task
.\scripts\setup_automation.ps1 -RunAsAdmin

# Or run as current user
.\scripts\setup_automation.ps1

# Custom schedule (e.g., 8 AM daily)
.\scripts\setup_automation.ps1 -Time "08:00"

# Remove existing task and recreate
.\scripts\setup_automation.ps1 -RemoveExisting
```

#### Option 2: Batch Script Setup
```cmd
# Run as Administrator
.\scripts\setup_automation.bat
```

#### Monitoring Automation
```powershell
# Check pipeline status
.\scripts\monitor_pipeline.ps1

# Check with detailed logs
.\scripts\monitor_pipeline.ps1 -ShowLogs

# Check Task Scheduler status
.\scripts\monitor_pipeline.ps1 -CheckTaskStatus
```

#### Managing the Automated Task
```powershell
# View task status
Get-ScheduledTask -TaskName "EnergyWeatherPipeline"

# Run task immediately
Start-ScheduledTask -TaskName "EnergyWeatherPipeline"

# Delete task
Unregister-ScheduledTask -TaskName "EnergyWeatherPipeline"

# View recent logs
Get-Content logs\pipeline_*.log | Select-Object -Last 50
```

### Production Features
- **Automated Data Fetching:**
  - Weather: NOAA API (with backup to GHCND daily archive if API fails)
  - Energy: EIA API (with backup to EIA CSV if API fails)
  - Logs which source (API or backup) was used for each city
- **Robust Error Handling:**
  - Retries on transient errors/rate limits
  - Graceful fallback to backup sources
  - All errors and source choices are logged in `logs/`
- **Data Quality Reporting:**
  - Checks for missing values, outliers (e.g., negative energy, implausible temps), data freshness, and duplicates
  - Generates both JSON (`reports/quality_*.json`) and human-readable TXT reports
  - Outliers and missing values are filtered in the dashboard visualizations
- **Automation & Monitoring:**
  - Windows Task Scheduler integration for daily automated runs
  - Comprehensive logging with timestamps and exit codes
  - Monitoring scripts to check pipeline status and data freshness
  - Configurable schedules and error handling for production environments

## Streamlit Dashboard
- **Run the dashboard:**
  ```
  streamlit run dashboard/app.py
  ```
- **Features:**
  1. **Geographic Overview:** Interactive US map with city stats (current temp, today's energy, % change from yesterday), color-coded (red/green/gray), with title and timestamp. If today's data is missing, the most recent available day is shown.
  2. **Time Series Analysis:** Dual-axis line chart (temp & energy), city selector, weekend shading, proper labels/legend.
  3. **Correlation Analysis:** Scatter plot (temp vs energy), regression line, equation, R², correlation coefficient, tooltips. Outliers are filtered.
  4. **Usage Patterns Heatmap:** Heatmap of average energy by temp range & day of week, blue-to-red color scale, city filter, cell annotations, color bar. Outlier filtering for temperature and energy is applied.
  5. **Data Quality Tab:** Visualizes missing values, outliers, and data freshness over time. Summarizes latest report and trends.

- **Interpreting Data Quality Reports:**
  - **Missing Values:** Number of missing temperature or energy values per city/date.
  - **Outliers:** Implausible values (e.g., negative energy, extreme temps) are flagged and excluded from analysis.
  - **Freshness:** Indicates if the latest data is stale (e.g., due to source delays).
  - **Duplicates:** Number of duplicate records by date/city.

## Data & Logs
- Data is stored in the `data/` directory (merged and raw files).
- Logs are written to the `logs/` directory for debugging and monitoring.
- Data quality reports are in `reports/`.

## Extending & Maintenance
- All code is modular and production-ready for easy extension.
- To add new cities, update `config/cities.yaml` and rerun the pipeline.
- To update dependencies, edit `pyproject.toml` and run `uv pip install --system`.

## Notes
- The config file uses YAML for clarity and flexibility.
- API keys are loaded from `.env` for security.
- The pipeline and dashboard handle missing or delayed data gracefully, always showing the most recent available data.