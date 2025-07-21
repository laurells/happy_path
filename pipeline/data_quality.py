import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from pathlib import Path
import yaml
import json
from dataclasses import dataclass, asdict
from enum import Enum

"""
Enhanced module for comprehensive data quality checks and reporting.
Provides robust functions to analyze missing values, outliers, data freshness, 
consistency, and statistical anomalies in merged datasets.
"""

# Configure logging
logger = logging.getLogger(__name__)

# --- ENUMS AND DATACLASSES (keep for structure) ---
class DataQualitySeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class QualityThresholds:
    temp_max_threshold: float = 130.0
    temp_min_threshold: float = -50.0
    energy_min_threshold: float = 0.0
    energy_outlier_std_multiplier: float = 3.0
    freshness_days_threshold: int = 2
    missing_data_critical_pct: float = 10.0
    missing_data_warning_pct: float = 5.0
    duplicate_critical_pct: float = 1.0

@dataclass
class DataQualityIssue:
    issue_type: str
    severity: DataQualitySeverity
    count: int
    percentage: float
    description: str
    records: List[Dict[str, Any]]
    recommendation: str

# --- MODULE-LEVEL DEFAULTS ---
DEFAULT_THRESHOLDS = QualityThresholds()
DEFAULT_CONFIG_PATH = "config/cities.yaml"

# --- UTILITY FUNCTIONS ---
def load_city_names(config_path=DEFAULT_CONFIG_PATH):
    try:
        path = Path(config_path)
        if path.exists():
            with open(path, 'r') as f:
                config = yaml.safe_load(f)
            cities = [city['name'] for city in config.get('cities', [])]
            logger.info(f"Loaded {len(cities)} cities from config")
            return cities
        else:
            logger.warning(f"Config file not found: {config_path}")
    except Exception as e:
        logger.error(f"Error loading cities from config: {e}")
    fallback_cities = ["New York", "Chicago", "Houston", "Phoenix", "Seattle"]
    logger.info(f"Using fallback cities: {fallback_cities}")
    return fallback_cities

def prepare_data(data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        raise ValueError("Input DataFrame is empty")
    data_copy = data.copy()
    required_columns = ['date', 'city', 'energy_mwh', 'tmax_f', 'tmin_f']
    for col in required_columns:
        if col not in data_copy.columns:
            logger.warning(f"Missing column '{col}', creating with NaN values")
            data_copy[col] = np.nan
    try:
        data_copy['date'] = pd.to_datetime(data_copy['date'])
        data_copy['energy_mwh'] = pd.to_numeric(data_copy['energy_mwh'], errors='coerce')
        data_copy['tmax_f'] = pd.to_numeric(data_copy['tmax_f'], errors='coerce')
        data_copy['tmin_f'] = pd.to_numeric(data_copy['tmin_f'], errors='coerce')
    except Exception as e:
        logger.error(f"Error converting data types: {e}")
        raise
    return data_copy

def check_missing_values(data: pd.DataFrame, thresholds: QualityThresholds) -> Dict[str, DataQualityIssue]:
    missing_issues = {}
    total_records = len(data)
    for column in ['tmax_f', 'tmin_f', 'energy_mwh']:
        missing_mask = data[column].isnull()
        missing_count = missing_mask.sum()
        missing_pct = (missing_count / total_records) * 100
        if missing_pct >= thresholds.missing_data_critical_pct:
            severity = DataQualitySeverity.CRITICAL
        elif missing_pct >= thresholds.missing_data_warning_pct:
            severity = DataQualitySeverity.MEDIUM
        else:
            severity = DataQualitySeverity.LOW
        missing_records = data[missing_mask][['date', 'city', column]].head(10)
        missing_issues[f'missing_{column}'] = DataQualityIssue(
            issue_type=f"Missing {column}",
            severity=severity,
            count=missing_count,
            percentage=missing_pct,
            description=f"{missing_count} missing values ({missing_pct:.2f}%) in {column}",
            records=missing_records.to_dict('records'),
            recommendation=get_missing_data_recommendation(column, missing_pct)
        )
    return missing_issues

def check_outliers(data: pd.DataFrame, thresholds: QualityThresholds) -> Dict[str, DataQualityIssue]:
    outlier_issues = {}
    temp_outlier_mask = (
        (data['tmax_f'] > thresholds.temp_max_threshold) |
        (data['tmin_f'] < thresholds.temp_min_threshold) |
        (data['tmax_f'] < data['tmin_f'])
    )
    temp_outliers = data[temp_outlier_mask]
    outlier_issues['temperature_outliers'] = DataQualityIssue(
        issue_type="Temperature Outliers",
        severity=DataQualitySeverity.HIGH if len(temp_outliers) > 0 else DataQualitySeverity.LOW,
        count=len(temp_outliers),
        percentage=(len(temp_outliers) / len(data)) * 100,
        description=f"{len(temp_outliers)} temperature records with extreme or illogical values",
        records=temp_outliers[['date', 'city', 'tmax_f', 'tmin_f']].head(10).to_dict('records'),
        recommendation="Review sensor calibration and data collection processes"
    )
    energy_negative_mask = data['energy_mwh'] < thresholds.energy_min_threshold
    energy_outliers = data[energy_negative_mask]
    if not data['energy_mwh'].isna().all():
        Q1 = data['energy_mwh'].quantile(0.25)
        Q3 = data['energy_mwh'].quantile(0.75)
        IQR = Q3 - Q1
        energy_statistical_outliers = data[
            (data['energy_mwh'] < Q1 - 1.5 * IQR) |
            (data['energy_mwh'] > Q3 + 1.5 * IQR)
        ]
        total_energy_outliers = pd.concat([energy_outliers, energy_statistical_outliers]).drop_duplicates()
    else:
        total_energy_outliers = energy_outliers
    outlier_issues['energy_outliers'] = DataQualityIssue(
        issue_type="Energy Outliers",
        severity=DataQualitySeverity.HIGH if len(total_energy_outliers) > 0 else DataQualitySeverity.LOW,
        count=len(total_energy_outliers),
        percentage=(len(total_energy_outliers) / len(data)) * 100,
        description=f"{len(total_energy_outliers)} energy records with negative or extreme values",
        records=total_energy_outliers[['date', 'city', 'energy_mwh']].head(10).to_dict('records'),
        recommendation="Investigate data collection and meter reading processes"
    )
    return outlier_issues

def check_data_freshness(data: pd.DataFrame, thresholds: QualityThresholds) -> DataQualityIssue:
    if data['date'].isna().all():
        return DataQualityIssue(
            issue_type="Data Freshness",
            severity=DataQualitySeverity.CRITICAL,
            count=0,
            percentage=0.0,
            description="No valid dates found in dataset",
            records=[],
            recommendation="Check data ingestion pipeline"
        )
    latest_date = data['date'].max()
    current_date = datetime.now()
    days_since_update = (current_date - latest_date).days
    if days_since_update > thresholds.freshness_days_threshold * 2:
        severity = DataQualitySeverity.CRITICAL
    elif days_since_update > thresholds.freshness_days_threshold:
        severity = DataQualitySeverity.MEDIUM
    else:
        severity = DataQualitySeverity.LOW
    return DataQualityIssue(
        issue_type="Data Freshness",
        severity=severity,
        count=days_since_update,
        percentage=0.0,
        description=f"Latest data is {days_since_update} days old (threshold: {thresholds.freshness_days_threshold} days)",
        records=[{
            'latest_data_date': latest_date.strftime('%Y-%m-%d'),
            'current_date': current_date.strftime('%Y-%m-%d'),
            'days_behind': days_since_update
        }],
        recommendation=get_freshness_recommendation(days_since_update)
    )

def check_data_consistency(data: pd.DataFrame, city_names: List[str], thresholds: QualityThresholds) -> Dict[str, DataQualityIssue]:
    consistency_issues = {}
    duplicate_mask = data.duplicated(subset=['date', 'city'], keep=False)
    duplicates = data[duplicate_mask]
    duplicate_pct = (len(duplicates) / len(data)) * 100
    consistency_issues['duplicates'] = DataQualityIssue(
        issue_type="Duplicate Records",
        severity=DataQualitySeverity.HIGH if duplicate_pct >= thresholds.duplicate_critical_pct else DataQualitySeverity.LOW,
        count=len(duplicates),
        percentage=duplicate_pct,
        description=f"{len(duplicates)} duplicate records found ({duplicate_pct:.2f}%)",
        records=duplicates[['date', 'city']].head(10).to_dict('records'),
        recommendation="Implement deduplication in data pipeline"
    )
    data_cities = set(data['city'].dropna().unique())
    expected_cities = set(city_names)
    missing_cities = expected_cities - data_cities
    consistency_issues['missing_cities'] = DataQualityIssue(
        issue_type="Missing Cities",
        severity=DataQualitySeverity.MEDIUM if missing_cities else DataQualitySeverity.LOW,
        count=len(missing_cities),
        percentage=(len(missing_cities) / len(expected_cities)) * 100,
        description=f"{len(missing_cities)} expected cities missing from dataset",
        records=[{'missing_city': city} for city in missing_cities],
        recommendation="Check data source coverage and collection processes"
    )
    date_range = pd.date_range(start=data['date'].min(), end=data['date'].max(), freq='D')
    actual_dates = set(data['date'].dt.date)
    expected_dates = set(date_range.date)
    missing_dates = expected_dates - actual_dates
    consistency_issues['date_gaps'] = DataQualityIssue(
        issue_type="Date Gaps",
        severity=DataQualitySeverity.MEDIUM if len(missing_dates) > 7 else DataQualitySeverity.LOW,
        count=len(missing_dates),
        percentage=(len(missing_dates) / len(expected_dates)) * 100,
        description=f"{len(missing_dates)} missing dates in time series",
        records=[{'missing_date': str(date)} for date in sorted(list(missing_dates))[:10]],
        recommendation="Investigate data collection gaps and implement backfill process"
    )
    return consistency_issues

def get_missing_data_recommendation(column: str, percentage: float) -> str:
    if percentage >= 50:
        return f"Critical: {column} has >50% missing data. Consider data source replacement."
    elif percentage >= 20:
        return f"High priority: Implement imputation strategy for {column}."
    elif percentage >= 5:
        return f"Monitor: Track {column} missing data trends."
    else:
        return f"Acceptable: {column} missing data within normal range."

def get_freshness_recommendation(days_behind: int) -> str:
    if days_behind > 7:
        return "Critical: Data pipeline may be broken. Investigate immediately."
    elif days_behind > 3:
        return "High priority: Check data ingestion schedule and processes."
    elif days_behind > 1:
        return "Monitor: Data update frequency may need adjustment."
    else:
        return "Good: Data freshness within acceptable range."

# --- MAIN ENTRY POINT ---
def run_comprehensive_quality_checks(data: pd.DataFrame, report_date: Optional[str] = None, thresholds: QualityThresholds = None, config_path: str = None) -> Dict[str, Any]:
    if thresholds is None:
        thresholds = DEFAULT_THRESHOLDS
    if config_path is None:
        config_path = DEFAULT_CONFIG_PATH
    city_names = load_city_names(config_path)
    if report_date is None:
        report_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Starting comprehensive data quality checks for {len(data)} records")
    try:
        prepared_data = prepare_data(data)
        report = {
            'metadata': {
                'run_date': report_date,
                'total_records': len(prepared_data),
                'date_range': {
                    'start': prepared_data['date'].min().strftime('%Y-%m-%d') if not prepared_data['date'].isna().all() else None,
                    'end': prepared_data['date'].max().strftime('%Y-%m-%d') if not prepared_data['date'].isna().all() else None
                },
                'cities_analyzed': sorted(prepared_data['city'].dropna().unique().tolist()),
                'thresholds_used': asdict(thresholds)
            },
            'issues': {},
            'summary': {
                'total_issues': 0,
                'critical_issues': 0,
                'high_issues': 0,
                'medium_issues': 0,
                'low_issues': 0
            }
        }
        all_issues = {}
        all_issues.update(check_missing_values(prepared_data, thresholds))
        all_issues.update(check_outliers(prepared_data, thresholds))
        all_issues['data_freshness'] = check_data_freshness(prepared_data, thresholds)
        all_issues.update(check_data_consistency(prepared_data, city_names, thresholds))
        report['issues'] = {k: asdict(v) for k, v in all_issues.items()}
        for issue in all_issues.values():
            report['summary']['total_issues'] += issue.count
            severity_key = f"{issue.severity.value}_issues"
            report['summary'][severity_key] += 1
        critical_weight = report['summary']['critical_issues'] * 4
        high_weight = report['summary']['high_issues'] * 3
        medium_weight = report['summary']['medium_issues'] * 2
        low_weight = report['summary']['low_issues'] * 1
        total_weight = critical_weight + high_weight + medium_weight + low_weight
        max_possible_weight = len(all_issues) * 4
        report['summary']['quality_score'] = max(0, 100 - (total_weight / max_possible_weight * 100)) if max_possible_weight > 0 else 100
        logger.info(f"Quality checks completed. Score: {report['summary']['quality_score']:.1f}/100")
        return report
    except Exception as e:
        logger.error(f"Error during quality checks: {e}")
        raise

def generate_comprehensive_report(report: Dict[str, Any], output_path: Union[str, Path]) -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(f"COMPREHENSIVE DATA QUALITY REPORT\n")
        f.write(f"Generated: {report['metadata']['run_date']}\n")
        f.write("=" * 60 + "\n\n")
        f.write("EXECUTIVE SUMMARY\n")
        f.write("-" * 30 + "\n")
        summary = report['summary']
        f.write(f"Overall Quality Score: {summary['quality_score']:.1f}/100\n")
        f.write(f"Total Records Analyzed: {report['metadata']['total_records']:,}\n")
        f.write(f"Date Range: {report['metadata']['date_range']['start']} to {report['metadata']['date_range']['end']}\n")
        f.write(f"Cities Covered: {len(report['metadata']['cities_analyzed'])}\n\n")
        f.write("ISSUE SUMMARY\n")
        f.write("-" * 30 + "\n")
        f.write(f"Critical Issues: {summary['critical_issues']}\n")
        f.write(f"High Priority: {summary['high_issues']}\n")
        f.write(f"Medium Priority: {summary['medium_issues']}\n")
        f.write(f"Low Priority: {summary['low_issues']}\n\n")
        f.write("DETAILED FINDINGS\n")
        f.write("-" * 30 + "\n")
        for issue_key, issue_data in report['issues'].items():
            severity_icon = ''
            f.write(f"\n{issue_data['issue_type'].upper()}\n")
            f.write(f"   Severity: {issue_data['severity'].value.title()}\n")
            f.write(f"   Count: {issue_data['count']:,}\n")
            if issue_data['percentage'] > 0:
                f.write(f"   Percentage: {issue_data['percentage']:.2f}%\n")
            f.write(f"   Description: {issue_data['description']}\n")
            f.write(f"   Recommendation: {issue_data['recommendation']}\n")
            if issue_data['records'] and len(issue_data['records']) > 0:
                f.write(f"   Sample Records:\n")
                for i, record in enumerate(issue_data['records'][:3], 1):
                    record_str = " | ".join([f"{k}: {v}" for k, v in record.items()])
                    f.write(f"     {i}. {record_str}\n")
        f.write(f"\nCONFIGURATION\n")
        f.write("-" * 30 + "\n")
        thresholds = report['metadata']['thresholds_used']
        for key, value in thresholds.items():
            f.write(f"{key.replace('_', ' ').title()}: {value}\n")

def export_to_json(report: Dict[str, Any], output_path: Union[str, Path]) -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)

def run_data_quality_checks(data: pd.DataFrame, report_date: str) -> dict:
    return run_comprehensive_quality_checks(data, report_date)

def generate_quality_report(report: dict, output_path: str):
    generate_comprehensive_report(report, output_path)