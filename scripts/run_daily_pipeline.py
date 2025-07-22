import os
import logging
import sys
from datetime import date, timedelta
from pipeline.data_pipeline import run_pipeline
import yaml
import argparse
from dotenv import load_dotenv

"""
Automated script to fetch and process daily weather and energy data for all configured cities.
Designed for automated execution via Windows Task Scheduler, cron, or manual runs.
Features:
- Fetches data for the last 90 days to ensure no gaps
- Comprehensive logging for monitoring
- Error handling and exit codes for automation
- Configurable date ranges
"""

def setup_logging():
    """Set up comprehensive logging for automated pipeline runs."""
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)
    
    # Create a unique log file for each run
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"logs/daily_pipeline_{timestamp}.log"
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)  # Also log to console
        ]
    )
    
    return log_filename

def run_automated_pipeline(start_date=None, end_date=None):
    """
    Run the automated pipeline with robust error handling.
    
    Args:
        start_date: Start date in YYYY-MM-DD format (default: days_back days ago)
        end_date: End date in YYYY-MM-DD format (default: today)
        days_back: Number of days to look back if start_date not provided
    """
    try:
        # Load environment variables (API keys)
        load_dotenv()
        
        # Validate API keys
        noaa_token = os.getenv('NOAA_API_TOKEN')
        eia_key = os.getenv('EIA_API_KEY')
        
        if not noaa_token:
            logging.error("NOAA_API_TOKEN not found in environment variables")
            return False
        if not eia_key:
            logging.error("EIA_API_KEY not found in environment variables")
            return False
        
        # Set default dates if not provided
        if not end_date:
            end_date = date.today().isoformat()
        if not start_date:
            start_date = (date.today() - timedelta(days=89)).isoformat()
        
        logging.info(f"Starting automated pipeline for {start_date} to {end_date}")
        
        # Load city configuration
        with open("config/cities.yaml", "r") as f:
            config = yaml.safe_load(f)
        
        logging.info(f"Loaded configuration for {len(config['cities'])} cities")
        
        # Run the pipeline
        run_pipeline(start_date=start_date, end_date=end_date)
        
        logging.info("Pipeline completed successfully")
        return True
        
    except Exception as e:
        logging.error(f"Pipeline failed with error: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    log_file = setup_logging()
    logging.info("="*50)
    logging.info("DAILY PIPELINE STARTED")
    logging.info("="*50)
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run the daily data pipeline')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Run the pipeline
    success = run_automated_pipeline(
        start_date=args.start_date,
        end_date=args.end_date
    )
    
    # Exit with appropriate code for automation
    if success:
        logging.info("Pipeline completed successfully - exiting with code 0")
        sys.exit(0)
    else:
        logging.error("Pipeline failed - exiting with code 1")
        sys.exit(1) 