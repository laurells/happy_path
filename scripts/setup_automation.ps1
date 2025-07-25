# PowerShell script for setting up automated pipeline execution
# This provides more advanced automation features than the batch script

param(
    [string]$Schedule = "Daily",
    [string]$Time = "06:00",
    [string]$TaskName = "EnergyWeatherPipeline",
    [switch]$RunAsAdmin,
    [switch]$RemoveExisting
)

Write-Host "Setting up automated pipeline execution..." -ForegroundColor Green
Write-Host ""

# Get the project directory
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectDir = Split-Path -Parent $ScriptDir

Write-Host "Project directory: $ProjectDir" -ForegroundColor Yellow

# Check if Python is available
try {
    $pythonVersion = python --version 2>&1
    Write-Host "Python found: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "ERROR: Python not found in PATH" -ForegroundColor Red
    Write-Host "Please ensure Python is installed and added to PATH" -ForegroundColor Red
    exit 1
}

# Check if .env file exists
$envFile = Join-Path $ProjectDir ".env"
if (-not (Test-Path $envFile)) {
    Write-Host "WARNING: .env file not found at $envFile" -ForegroundColor Yellow
    Write-Host "Please create .env file with NOAA_API_TOKEN and EIA_API_KEY" -ForegroundColor Yellow
}

# Create the execution script
$executionScript = Join-Path $ProjectDir "scripts\run_pipeline_task.ps1"
$executionScriptContent = @"
# Automated pipeline execution script
# Generated by setup_automation.ps1

param(
    [string]`$LogLevel = "INFO"
)

# Set error action preference
`$ErrorActionPreference = "Stop"

# Change to project directory
Set-Location "$ProjectDir"

# Set up logging
`$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
`$logFile = "logs\pipeline_`$timestamp.log"

# Ensure logs directory exists
if (-not (Test-Path "logs")) {
    New-Item -ItemType Directory -Path "logs" -Force | Out-Null
}

# Function to write to both console and log
function Write-Log {
    param(`$Message, `$Level = "INFO")
    `$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    `$logMessage = "`$timestamp - `$Level - `$Message"
    Write-Host `$logMessage
    Add-Content -Path `$logFile -Value `$logMessage
}

Write-Log "Starting automated pipeline execution" "INFO"
Write-Log "Log file: `$logFile" "INFO"

try {
    # Run the pipeline
    `$result = python scripts/run_daily_pipeline.py
    
    if (`$LASTEXITCODE -eq 0) {
        Write-Log "Pipeline completed successfully" "INFO"
        exit 0
    } else {
        Write-Log "Pipeline failed with exit code `$LASTEXITCODE" "ERROR"
        exit 1
    }
} catch {
    Write-Log "Pipeline execution error: `$(`$_.Exception.Message)" "ERROR"
    exit 1
}
"@

Write-Host "Creating execution script..." -ForegroundColor Yellow
Set-Content -Path $executionScript -Value $executionScriptContent -Encoding UTF8
Write-Host "Execution script created: $executionScript" -ForegroundColor Green

# Remove existing task if requested
if ($RemoveExisting) {
    Write-Host "Removing existing task '$TaskName'..." -ForegroundColor Yellow
    try {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
        Write-Host "Existing task removed" -ForegroundColor Green
    } catch {
        Write-Host "No existing task to remove" -ForegroundColor Yellow
    }
}

# Remove existing task if it exists
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

# Create the scheduled task
Write-Host "Creating Windows Task Scheduler job..." -ForegroundColor Yellow

try {
    # Define the action
    $action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-ExecutionPolicy Bypass -File `"$executionScript`""
    
    # Define the trigger
    $trigger = New-ScheduledTaskTrigger -Daily -At $Time
    
    # Define the settings
    $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable
    
    # Define the principal (user account)
    if ($RunAsAdmin) {
        $principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
    } else {
        $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive
    }
    
    # Create the task
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description "Automated energy and weather data pipeline"
    
    Write-Host ""
    Write-Host "SUCCESS: Automated pipeline scheduled!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Task Details:" -ForegroundColor Cyan
    Write-Host "- Name: $TaskName" -ForegroundColor White
    Write-Host "- Schedule: $Schedule at $Time" -ForegroundColor White
    Write-Host "- Command: $executionScript" -ForegroundColor White
    Write-Host "- User: $($principal.UserId)" -ForegroundColor White
    Write-Host ""
    Write-Host "To manage the task:" -ForegroundColor Cyan
    Write-Host "- View: Get-ScheduledTask -TaskName '$TaskName'" -ForegroundColor White
    Write-Host "- Delete: Unregister-ScheduledTask -TaskName '$TaskName'" -ForegroundColor White
    Write-Host "- Run now: Start-ScheduledTask -TaskName '$TaskName'" -ForegroundColor White
    Write-Host "- View logs: Get-Content logs\pipeline_*.log | Select-Object -Last 50" -ForegroundColor White
    
} catch {
    Write-Host ""
    Write-Host "ERROR: Failed to create scheduled task" -ForegroundColor Red
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host ""
    Write-Host "Manual setup instructions:" -ForegroundColor Yellow
    Write-Host "1. Open Task Scheduler (taskschd.msc)" -ForegroundColor White
    Write-Host "2. Create Basic Task" -ForegroundColor White
    Write-Host "3. Name: $TaskName" -ForegroundColor White
    Write-Host "4. Trigger: $Schedule at $Time" -ForegroundColor White
    Write-Host "5. Action: Start a program" -ForegroundColor White
    Write-Host "6. Program: powershell.exe" -ForegroundColor White
    Write-Host "7. Arguments: -ExecutionPolicy Bypass -File `"$executionScript`"" -ForegroundColor White
    Write-Host "8. Finish" -ForegroundColor White
}

Write-Host ""
Write-Host "Setup complete!" -ForegroundColor Green 