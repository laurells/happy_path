@echo off
cd /d "C:\Users\USER\Desktop\happy_path\scripts\.."
python scripts/run_daily_pipeline.py
if errorlevel 1 (
    echo Pipeline failed with error code %errorlevel%
    exit /b 1
)
echo Pipeline completed successfully
