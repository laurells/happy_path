import pandas as pd
import logging
from datetime import datetime, timedelta
import os

"""
Module for data quality checks and reporting.
"""

def run_data_quality_checks(data: pd.DataFrame, report_date: str) -> dict:
    """
    Run comprehensive data quality checks on the merged dataset.
    Returns a dictionary with quality metrics and detailed issue reports.
    """
    # Ensure numeric types for checks
    data['energy_mwh'] = pd.to_numeric(data['energy_mwh'], errors='coerce')
    data['tmax_f'] = pd.to_numeric(data['tmax_f'], errors='coerce')
    data['tmin_f'] = pd.to_numeric(data['tmin_f'], errors='coerce')
    
    # Convert to datetime if needed
    if not isinstance(data['date'], pd.DatetimeIndex):
        data['date'] = pd.to_datetime(data['date'])
    
    # Initialize report
    report = {
        'run_date': report_date,
        'missing_values': {},
        'outliers': {},
        'freshness': {},
        'details': {}
    }
    
    # 1. Missing Value Analysis
    missing_counts = data.isnull().sum()
    report['missing_values']['summary'] = missing_counts.to_dict()
    
    # Detailed missing records
    for column in ['tmax_f', 'tmin_f', 'energy_mwh']:
        missing_df = data[data[column].isnull()][['date', 'city', column]]
        report['details'][f'missing_{column}'] = missing_df.to_dict('records')
    
    # 2. Outlier Detection
    # Temperature outliers
    temp_outliers = data[
        (data['tmax_f'] > 130) | 
        (data['tmin_f'] < -50) |
        (data['tmax_f'] < data['tmin_f'])
    ]
    report['outliers']['temperature'] = {
        'count': len(temp_outliers),
        'records': temp_outliers[['date', 'city', 'tmax_f', 'tmin_f']].to_dict('records')
    }
    
    # Energy outliers
    energy_outliers = data[data['energy_mwh'] < 0]
    report['outliers']['energy'] = {
        'count': len(energy_outliers),
        'records': energy_outliers[['date', 'city', 'energy_mwh']].to_dict('records')
    }
    
    # 3. Data Freshness Check
    latest_date = data['date'].max()
    freshness_threshold = datetime.now() - timedelta(days=2)
    
    report['freshness'] = {
        'latest_data_date': latest_date.strftime('%Y-%m-%d'),
        'current_date': datetime.now().strftime('%Y-%m-%d'),
        'is_stale': latest_date < freshness_threshold,
        'days_since_update': (datetime.now() - latest_date).days
    }
    
    # 4. Data Consistency Checks
    report['consistency'] = {
        'duplicates': data.duplicated(subset=['date', 'city']).sum(),
        'date_range': {
            'start': data['date'].min().strftime('%Y-%m-%d'),
            'end': data['date'].max().strftime('%Y-%m-%d')
        },
        'cities_missing': list(set(CITY_NAMES) - set(data['city'].unique()))
    }
    
    return report

def generate_quality_report(report: dict, output_path: str):
    """Generate human-readable quality report and save to file"""
    # Ensure the reports directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        # Header
        f.write(f"Data Quality Report - {report['run_date']}\n")
        f.write("="*50 + "\n\n")
        
        # Missing Values
        f.write("Missing Values Analysis:\n")
        f.write("-"*30 + "\n")
        for col, count in report['missing_values']['summary'].items():
            f.write(f"{col}: {count} missing\n")
        
        # Outliers
        f.write("\nOutlier Detection:\n")
        f.write("-"*30 + "\n")
        f.write(f"Temperature Outliers: {report['outliers']['temperature']['count']}\n")
        f.write(f"Energy Outliers: {report['outliers']['energy']['count']}\n")
        
        # Freshness
        f.write("\nData Freshness:\n")
        f.write("-"*30 + "\n")
        freshness = report['freshness']
        f.write(f"Latest Data Date: {freshness['latest_data_date']}\n")
        f.write(f"Current Date: {freshness['current_date']}\n")
        f.write(f"Stale Data: {'YES' if freshness['is_stale'] else 'NO'}\n")
        f.write(f"Days Since Update: {freshness['days_since_update']}\n")
        
        # Consistency
        f.write("\nData Consistency:\n")
        f.write("-"*30 + "\n")
        f.write(f"Duplicate Records: {report['consistency']['duplicates']}\n")
        f.write(f"Date Range: {report['consistency']['date_range']['start']} to {report['consistency']['date_range']['end']}\n")
        if report['consistency']['cities_missing']:
            f.write(f"Missing Cities: {', '.join(report['consistency']['cities_missing'])}\n")
        
        # Detailed findings
        f.write("\nDetailed Findings:\n")
        f.write("-"*30 + "\n")
        for issue_type, records in report['details'].items():
            if records:
                f.write(f"\n{issue_type.replace('_', ' ').title()}:\n")
                for record in records:
                    f.write(f"  - {record['date']} | {record['city']}\n")

# City reference
CITY_NAMES = ["New York", "Chicago", "Houston", "Phoenix", "Seattle"]