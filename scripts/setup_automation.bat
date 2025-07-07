@echo off
REM Setup script for automated daily pipeline execution on Windows
REM This script creates a Windows Task Scheduler job to run the pipeline daily

echo Setting up automated pipeline execution...
echo.

REM Get the current directory (where this script is located)
set "SCRIPT_DIR=%~dp0"
set "PROJECT_DIR=%SCRIPT_DIR%.."

REM Create the batch file that will be executed by Task Scheduler
echo Creating execution script...
(
echo @echo off
echo cd /d "%PROJECT_DIR%"
echo python scripts/run_daily_pipeline.py
echo if errorlevel 1 ^(
echo     echo Pipeline failed with error code %%errorlevel%%
echo     exit /b 1
echo ^)
echo echo Pipeline completed successfully
) > "%PROJECT_DIR%\scripts\run_pipeline_task.bat"

echo Execution script created: scripts/run_pipeline_task.bat
echo.

REM Create the Task Scheduler command
echo Creating Windows Task Scheduler job...
echo.

REM Get current user for the task
for /f "tokens=2 delims==" %%a in ('wmic useraccount where "name='%USERNAME%'" get name /value') do set "CURRENT_USER=%%a"

REM Create the scheduled task
schtasks /create /tn "EnergyWeatherPipeline" /tr "\"%PROJECT_DIR%\scripts\run_pipeline_task.bat\"" /sc daily /st 06:00 /ru "%CURRENT_USER%" /f

if %errorlevel% equ 0 (
    echo.
    echo SUCCESS: Automated pipeline scheduled!
    echo.
    echo Task Details:
    echo - Name: EnergyWeatherPipeline
    echo - Schedule: Daily at 6:00 AM
    echo - Command: %PROJECT_DIR%\scripts\run_pipeline_task.bat
    echo - User: %CURRENT_USER%
    echo.
    echo To manage the task:
    echo - View: schtasks /query /tn "EnergyWeatherPipeline"
    echo - Delete: schtasks /delete /tn "EnergyWeatherPipeline" /f
    echo - Run now: schtasks /run /tn "EnergyWeatherPipeline"
    echo.
    echo To change the schedule, delete and recreate the task with different parameters.
    echo Example: schtasks /create /tn "EnergyWeatherPipeline" /tr "\"%PROJECT_DIR%\scripts\run_pipeline_task.bat\"" /sc daily /st 08:00 /ru "%CURRENT_USER%" /f
) else (
    echo.
    echo ERROR: Failed to create scheduled task.
    echo You may need to run this script as Administrator.
    echo.
    echo Manual setup instructions:
    echo 1. Open Task Scheduler ^(taskschd.msc^)
    echo 2. Create Basic Task
    echo 3. Name: EnergyWeatherPipeline
    echo 4. Trigger: Daily at 6:00 AM
    echo 5. Action: Start a program
    echo 6. Program: %PROJECT_DIR%\scripts\run_pipeline_task.bat
    echo 7. Finish
)

echo.
echo Setup complete!
pause 