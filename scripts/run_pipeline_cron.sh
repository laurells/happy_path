#!/bin/bash
# Shell script to run the daily pipeline and log output

cd /path/to/your/project
python3 scripts/run_daily_pipeline.py >> logs/cron_pipeline.log 2>&1