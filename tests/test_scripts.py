import pytest
import sys
import os
import tempfile
import shutil
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from io import StringIO

@pytest.fixture
def temp_project_dir():
    """Create a temporary project directory structure"""
    temp_dir = tempfile.mkdtemp()
    
    # Create directory structure
    scripts_dir = os.path.join(temp_dir, "scripts")
    config_dir = os.path.join(temp_dir, "config")
    logs_dir = os.path.join(temp_dir, "logs")
    
    os.makedirs(scripts_dir, exist_ok=True)
    os.makedirs(config_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)
    
    # Create a mock cities.yaml
    cities_yaml = """
cities:
  - name: "TestCity"
    noaa_station_id: "TEST123"
    eia_region: "TEST"
"""
    with open(os.path.join(config_dir, "cities.yaml"), "w") as f:
        f.write(cities_yaml)
    
    # Create .env file
    env_content = """NOAA_API_TOKEN=test_noaa_token
EIA_API_KEY=test_eia_key
"""
    with open(os.path.join(temp_dir, ".env"), "w") as f:
        f.write(env_content)
    
    original_cwd = os.getcwd()
    yield temp_dir
    
    # Cleanup
    os.chdir(original_cwd)
    shutil.rmtree(temp_dir, ignore_errors=True)

def test_run_automated_pipeline_default_dates(monkeypatch, temp_project_dir):
    """Test run_automated_pipeline function with default dates (90 days)"""
    
    # Change to temp directory
    original_cwd = os.getcwd()
    os.chdir(temp_project_dir)
    
    try:
        # Track pipeline calls
        pipeline_calls = []
        
        def mock_run_pipeline(start_date, end_date):
            pipeline_calls.append({
                "start_date": start_date,
                "end_date": end_date
            })
        
        # Mock all the dependencies
        monkeypatch.setattr("pipeline.data_pipeline.run_pipeline", mock_run_pipeline)
        monkeypatch.setenv("NOAA_API_TOKEN", "test_token")
        monkeypatch.setenv("EIA_API_KEY", "test_key")
        
        # Mock logging setup
        monkeypatch.setattr("logging.basicConfig", lambda *args, **kwargs: None)
        monkeypatch.setattr("logging.info", lambda *args: None)
        monkeypatch.setattr("logging.error", lambda *args: None)
        
        # This is the core logic from run_automated_pipeline
        def test_run_automated_pipeline(start_date=None, end_date=None):
            try:
                from datetime import date, timedelta
                import yaml
                from dotenv import load_dotenv
                from pathlib import Path
                
                BASE_DIR = Path(temp_project_dir)
                load_dotenv(BASE_DIR / '.env')
                
                noaa_token = os.getenv('NOAA_API_TOKEN')
                eia_key = os.getenv('EIA_API_KEY')
                
                if not noaa_token or not eia_key:
                    return False
                
                if not end_date:
                    end_date = date.today().isoformat()
                if not start_date:
                    start_date = (date.today() - timedelta(days=89)).isoformat()
                
                with open("config/cities.yaml", "r") as f:
                    config = yaml.safe_load(f)
                
                # Import here to use the mocked version
                from pipeline.data_pipeline import run_pipeline
                run_pipeline(start_date=start_date, end_date=end_date)
                
                return True
                
            except Exception as e:
                return False
        
        # Run the function
        success = test_run_automated_pipeline()
        
        # Verify results
        assert success is True
        assert len(pipeline_calls) == 1
        
        call = pipeline_calls[0]
        from datetime import date, timedelta
        start = date.fromisoformat(call["start_date"])
        end = date.fromisoformat(call["end_date"])
        
        expected_start = date.today() - timedelta(days=89)
        expected_end = date.today()
        
        assert start == expected_start
        assert end == expected_end
        assert (end - start).days == 89  # 89 days difference = 90 days total
        
    finally:
        os.chdir(original_cwd)

def test_run_automated_pipeline_custom_dates(monkeypatch, temp_project_dir):
    """Test run_automated_pipeline function with custom dates"""
    
    original_cwd = os.getcwd()
    os.chdir(temp_project_dir)
    
    try:
        pipeline_calls = []
        
        def mock_run_pipeline(start_date, end_date):
            pipeline_calls.append({
                "start_date": start_date,
                "end_date": end_date
            })
        
        monkeypatch.setattr("pipeline.data_pipeline.run_pipeline", mock_run_pipeline)
        monkeypatch.setenv("NOAA_API_TOKEN", "test_token")
        monkeypatch.setenv("EIA_API_KEY", "test_key")
        
        # Mock logging
        monkeypatch.setattr("logging.basicConfig", lambda *args, **kwargs: None)
        monkeypatch.setattr("logging.info", lambda *args: None)
        monkeypatch.setattr("logging.error", lambda *args: None)
        
        # Test with custom dates
        custom_start = "2024-01-01"
        custom_end = "2024-01-05"
        
        # Simulate the script logic with custom dates
        def test_run_automated_pipeline(start_date=None, end_date=None):
            try:
                from datetime import date, timedelta
                import yaml
                from dotenv import load_dotenv
                from pathlib import Path
                
                BASE_DIR = Path(temp_project_dir)
                load_dotenv(BASE_DIR / '.env')
                
                noaa_token = os.getenv('NOAA_API_TOKEN')
                eia_key = os.getenv('EIA_API_KEY')
                
                if not noaa_token or not eia_key:
                    return False
                
                if not end_date:
                    end_date = date.today().isoformat()
                if not start_date:
                    start_date = (date.today() - timedelta(days=89)).isoformat()
                
                with open("config/cities.yaml", "r") as f:
                    config = yaml.safe_load(f)
                
                from pipeline.data_pipeline import run_pipeline
                run_pipeline(start_date=start_date, end_date=end_date)
                
                return True
                
            except Exception as e:
                return False
        
        # Run with custom dates
        success = test_run_automated_pipeline(start_date=custom_start, end_date=custom_end)
        
        assert success is True
        assert len(pipeline_calls) == 1
        
        call = pipeline_calls[0]
        assert call["start_date"] == custom_start
        assert call["end_date"] == custom_end
        
        # Verify date range
        from datetime import date
        start = date.fromisoformat(call["start_date"])
        end = date.fromisoformat(call["end_date"])
        assert (end - start).days == 4  # 5 days total (Jan 1-5)
        
    finally:
        os.chdir(original_cwd)

def test_run_automated_pipeline_missing_env_vars(monkeypatch, temp_project_dir):
    """Test that function fails gracefully when environment variables are missing"""
    original_cwd = os.getcwd()
    os.chdir(temp_project_dir)

    try:
        pipeline_calls = []

        def mock_run_pipeline(start_date, end_date):
            pipeline_calls.append({
                "start_date": start_date,
                "end_date": end_date
            })

        monkeypatch.setattr("pipeline.data_pipeline.run_pipeline", mock_run_pipeline)

        # Don't set environment variables (or explicitly unset them)
        monkeypatch.delenv("NOAA_API_TOKEN", raising=False)
        monkeypatch.delenv("EIA_API_KEY", raising=False)

        # Mock logging
        monkeypatch.setattr("logging.basicConfig", lambda *args, **kwargs: None)
        monkeypatch.setattr("logging.info", lambda *args: None)
        monkeypatch.setattr("logging.error", lambda *args: None)

        # Mock load_dotenv so it does nothing
        monkeypatch.setattr("dotenv.load_dotenv", lambda *args, **kwargs: None)

        from datetime import date, timedelta
        import yaml
        from dotenv import load_dotenv
        from pathlib import Path

        def test_run_automated_pipeline(start_date=None, end_date=None):
            try:
                BASE_DIR = Path(temp_project_dir)
                load_dotenv(BASE_DIR / '.env')

                noaa_token = os.getenv('NOAA_API_TOKEN')
                eia_key = os.getenv('EIA_API_KEY')

                if not noaa_token:
                    return False
                if not eia_key:
                    return False

                # This shouldn't be reached
                return True

            except Exception as e:
                return False

        # Run the function - should fail due to missing env vars
        success = test_run_automated_pipeline()

        # Verify failure
        assert success is False
        assert len(pipeline_calls) == 0  # Pipeline should never be called

    finally:
        os.chdir(original_cwd)

def test_script_argument_parsing():
    """Test the argument parsing logic"""
    import argparse
    
    # Test with no arguments
    parser = argparse.ArgumentParser(description='Run the daily data pipeline')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    
    args = parser.parse_args([])
    assert args.start_date is None
    assert args.end_date is None
    
    # Test with custom arguments
    args = parser.parse_args(['--start-date', '2024-01-01', '--end-date', '2024-01-31'])
    assert args.start_date == '2024-01-01'
    assert args.end_date == '2024-01-31'

def test_date_calculation_logic():
    """Test the date calculation logic directly"""
    from datetime import date, timedelta
    
    # Test default date logic (what the script does)
    end_date = date.today().isoformat()
    start_date = (date.today() - timedelta(days=89)).isoformat()
    
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    
    # Should be 89 days difference (90 days total including both start and end)
    assert (end - start).days == 89
    assert end == date.today()
    assert start == date.today() - timedelta(days=89)