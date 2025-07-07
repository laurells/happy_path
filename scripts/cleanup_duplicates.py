#!/usr/bin/env python3
"""
Script to clean up duplicate records in existing data files.
This addresses the issue where EIA API returns multiple timezone records for the same date.
"""

import pandas as pd
import glob
import os
import logging
from datetime import datetime

# City timezone mapping for proper filtering
CITY_TIMEZONE = {
    "New York": "Eastern",
    "Chicago": "Central", 
    "Houston": "Central",
    "Phoenix": "Mountain",
    "Seattle": "Pacific"
}

def cleanup_merged_data():
    """
    Clean up duplicate records in merged data files.
    """
    # Find all merged data files
    merged_files = glob.glob("data/merged_*.csv")
    
    if not merged_files:
        print("No merged data files found to clean up.")
        return
    
    print(f"Found {len(merged_files)} merged data files to process...")
    
    for file_path in merged_files:
        print(f"\nProcessing: {file_path}")
        
        # Read the data
        df = pd.read_csv(file_path)
        
        # Check for duplicates
        duplicates = df.duplicated(subset=['date', 'city'], keep=False)
        duplicate_count = duplicates.sum()
        
        if duplicate_count == 0:
            print(f"  No duplicates found in {file_path}")
            continue
        
        print(f"  Found {duplicate_count} duplicate records")
        
        # Create backup
        backup_path = file_path.replace('.csv', '_backup.csv')
        df.to_csv(backup_path, index=False)
        print(f"  Created backup: {backup_path}")
        
        # Remove duplicates based on timezone preference
        cleaned_records = []
        
        for (date, city), group in df.groupby(['date', 'city']):
            if len(group) == 1:
                # No duplicates for this date/city
                cleaned_records.append(group.iloc[0])
            else:
                # Duplicates found - select the correct timezone
                expected_timezone = CITY_TIMEZONE.get(city)
                
                if expected_timezone and 'timezone' in group.columns:
                    # Try to find the record with the correct timezone
                    matching_records = group[group['timezone'] == expected_timezone]
                    
                    if not matching_records.empty:
                        selected_record = matching_records.iloc[0]
                        print(f"    Selected {expected_timezone} timezone for {city} on {date}")
                    else:
                        # Fall back to first record
                        selected_record = group.iloc[0]
                        print(f"    No {expected_timezone} timezone found for {city} on {date}, using first available")
                else:
                    # No timezone info, use first record
                    selected_record = group.iloc[0]
                    print(f"    No timezone info for {city} on {date}, using first available")
                
                cleaned_records.append(selected_record)
        
        # Create cleaned dataframe
        cleaned_df = pd.DataFrame(cleaned_records)
        
        # Save cleaned data
        cleaned_df.to_csv(file_path, index=False)
        print(f"  Saved cleaned data: {len(cleaned_df)} records (removed {duplicate_count} duplicates)")

def cleanup_raw_eia_data():
    """
    Clean up raw EIA data files to show the filtering process.
    """
    eia_files = glob.glob("data/eia_raw_*.csv")
    
    if not eia_files:
        print("No raw EIA data files found.")
        return
    
    print(f"\nFound {len(eia_files)} raw EIA data files...")
    
    for file_path in eia_files:
        print(f"\nAnalyzing: {file_path}")
        
        df = pd.read_csv(file_path)
        
        # Check for timezone distribution
        if 'timezone-description' in df.columns:
            timezone_counts = df['timezone-description'].value_counts()
            print(f"  Timezone distribution:")
            for tz, count in timezone_counts.items():
                print(f"    {tz}: {count} records")
        
        # Check for duplicates by date and respondent
        if 'period' in df.columns and 'respondent' in df.columns:
            duplicates = df.duplicated(subset=['period', 'respondent'], keep=False)
            duplicate_count = duplicates.sum()
            print(f"  Duplicate records by date/respondent: {duplicate_count}")

if __name__ == "__main__":
    print("Energy Data Cleanup Script")
    print("=" * 40)
    
    # Clean up merged data
    cleanup_merged_data()
    
    # Analyze raw EIA data
    cleanup_raw_eia_data()
    
    print("\nCleanup complete!") 