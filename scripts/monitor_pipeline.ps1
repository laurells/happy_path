# Pipeline monitoring script
# Use this to check the status of automated pipeline runs

param(
    [int]$DaysBack = 7,
    [switch]$ShowLogs,
    [switch]$CheckTaskStatus
)

Write-Host "Energy & Weather Pipeline Monitor" -ForegroundColor Green
Write-Host "=" * 40 -ForegroundColor Green
Write-Host ""

# Check Task Scheduler status
if ($CheckTaskStatus) {
    Write-Host "Checking Task Scheduler status..." -ForegroundColor Yellow
    try {
        $task = Get-ScheduledTask -TaskName "EnergyWeatherPipeline" -ErrorAction SilentlyContinue
        if ($task) {
            Write-Host "Task Status: $($task.State)" -ForegroundColor Green
            Write-Host "Last Run: $($task.LastRunTime)" -ForegroundColor Cyan
            Write-Host "Next Run: $($task.NextRunTime)" -ForegroundColor Cyan
        } else {
            Write-Host "Task 'EnergyWeatherPipeline' not found" -ForegroundColor Red
            Write-Host "Run setup_automation.ps1 to create the task" -ForegroundColor Yellow
        }
    } catch {
        Write-Host "Error checking task status: $($_.Exception.Message)" -ForegroundColor Red
    }
    Write-Host ""
}

# Check recent log files
Write-Host "Checking recent pipeline logs..." -ForegroundColor Yellow
$logFiles = Get-ChildItem "logs\pipeline_*.log" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 5

if ($logFiles) {
    Write-Host "Recent log files:" -ForegroundColor Cyan
    foreach ($log in $logFiles) {
        $status = "UNKNOWN"
        $color = "Gray"
        
        # Check if pipeline completed successfully
        $content = Get-Content $log.FullName -Tail 10
        if ($content -match "Pipeline completed successfully") {
            $status = "SUCCESS"
            $color = "Green"
        } elseif ($content -match "Pipeline failed") {
            $status = "FAILED"
            $color = "Red"
        }
        
        Write-Host "  $($log.Name) - $status ($($log.LastWriteTime))" -ForegroundColor $color
    }
    
    if ($ShowLogs) {
        Write-Host ""
        Write-Host "Latest log content:" -ForegroundColor Yellow
        Write-Host "-" * 50 -ForegroundColor Gray
        Get-Content $logFiles[0].FullName -Tail 20 | ForEach-Object { Write-Host $_ -ForegroundColor White }
    }
} else {
    Write-Host "No pipeline log files found" -ForegroundColor Red
    Write-Host "Run the pipeline manually first: python scripts/run_daily_pipeline.py" -ForegroundColor Yellow
}

Write-Host ""

# Check data freshness
Write-Host "Checking data freshness..." -ForegroundColor Yellow
$dataFiles = Get-ChildItem "data\merged_*.csv" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1

if ($dataFiles) {
    $lastUpdate = $dataFiles[0].LastWriteTime
    $daysSinceUpdate = (Get-Date) - $lastUpdate
    $daysSinceUpdate = $daysSinceUpdate.Days
    
    if ($daysSinceUpdate -eq 0) {
        Write-Host "Data is current (updated today)" -ForegroundColor Green
    } elseif ($daysSinceUpdate -eq 1) {
        Write-Host "Data is recent (updated yesterday)" -ForegroundColor Yellow
    } else {
        Write-Host "Data is stale (updated $daysSinceUpdate days ago)" -ForegroundColor Red
    }
    
    Write-Host "Last data update: $lastUpdate" -ForegroundColor Cyan
} else {
    Write-Host "No merged data files found" -ForegroundColor Red
}

Write-Host ""

# Check API keys
Write-Host "Checking API configuration..." -ForegroundColor Yellow
$envFile = ".env"
if (Test-Path $envFile) {
    $envContent = Get-Content $envFile
    $noaaToken = $envContent | Where-Object { $_ -match "NOAA_API_TOKEN" }
    $eiaKey = $envContent | Where-Object { $_ -match "EIA_API_KEY" }
    
    if ($noaaToken -and $eiaKey) {
        Write-Host "API keys configured" -ForegroundColor Green
    } else {
        Write-Host "Missing API keys in .env file" -ForegroundColor Red
    }
} else {
    Write-Host ".env file not found" -ForegroundColor Red
}

Write-Host ""
Write-Host "Monitor complete!" -ForegroundColor Green 