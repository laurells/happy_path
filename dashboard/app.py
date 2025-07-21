import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import glob
import os
import json
from sklearn.linear_model import LinearRegression
from datetime import date, timedelta, datetime, UTC
# from pytz import timezone  # Deprecated: use zoneinfo instead
from zoneinfo import ZoneInfo  # Modern timezone handling
from pathlib import Path
import logging
from typing import List, Dict, Any, Optional

# City coordinates, timezones, and mapping info for dashboard
CITY_COORDS = {
    "New York": {"lat": 40.7128, "lon": -74.0060, "tz": "America/New_York"},
    "Chicago": {"lat": 41.8781, "lon": -87.6298, "tz": "America/Chicago"},
    "Houston": {"lat": 29.7604, "lon": -95.3698, "tz": "America/Chicago"},
    "Phoenix": {"lat": 33.4484, "lon": -112.0740, "tz": "America/Phoenix"},
    "Seattle": {"lat": 47.6062, "lon": -122.3321, "tz": "America/Los_Angeles"},
}

# Utility function for timezone conversion using zoneinfo
# Converts a UTC datetime to the local time for a given city
# This replaces the old pytz-based approach for modern Python (3.9+)
def convert_to_local(utc_dt, city):
    tz_mapping = {
        "New York": "America/New_York",
        "Chicago": "America/Chicago",
        "Houston": "America/Chicago",
        "Phoenix": "America/Phoenix",
        "Seattle": "America/Los_Angeles",
    }
    return utc_dt.astimezone(ZoneInfo(tz_mapping.get(city, "America/New_York")))

# Cache data loading for 1 hour to improve dashboard performance
@st.cache_data(ttl=3600)
def load_data(filepath):
    """Load a CSV file as a DataFrame, with Streamlit caching."""
    return pd.read_csv(filepath)

# --- Data Quality Dashboard Module-Level Variables ---
REPORTS_PATH = Path("reports")
QUALITY_THRESHOLDS = {
    'missing_critical': 10.0,
    'missing_warning': 5.0,
    'outlier_critical': 5.0,
    'freshness_critical': 2,
    'freshness_warning': 1
}

# --- Data Quality Dashboard Functions ---
def load_quality_reports() -> pd.DataFrame:
    try:
        report_pattern = str(REPORTS_PATH / "quality_*.json")
        report_files = glob.glob(report_pattern)
        if not report_files:
            st.error("No quality reports found. Please run the data pipeline first.")
            return pd.DataFrame()
        reports = []
        for file in sorted(report_files):
            try:
                with open(file, 'r') as f:
                    report = json.load(f)
                report_data = _extract_report_data(report, file)
                if report_data:
                    reports.append(report_data)
            except Exception as e:
                logger.error(f"Error loading report {file}: {e}")
                continue
        if not reports:
            st.error("Failed to load any valid quality reports.")
            return pd.DataFrame()
        df_reports = pd.DataFrame(reports)
        df_reports['date'] = pd.to_datetime(df_reports['date'])
        df_reports = df_reports.sort_values('date')
        return df_reports
    except Exception as e:
        logger.error(f"Error in load_quality_reports: {e}")
        st.error(f"Error loading reports: {str(e)}")
        return pd.DataFrame()

def _extract_report_data(report: Dict[str, Any], file_path: str) -> Optional[Dict[str, Any]]:
    try:
        if 'metadata' in report:
            return {
                'file_path': file_path,
                'date': report['metadata']['run_date'],
                'total_records': report['metadata']['total_records'],
                'quality_score': report['summary']['quality_score'],
                'critical_issues': report['summary']['critical_issues'],
                'high_issues': report['summary']['high_issues'],
                'medium_issues': report['summary']['medium_issues'],
                'low_issues': report['summary']['low_issues'],
                'missing_tmax': _get_issue_count(report, 'missing_tmax_f'),
                'missing_tmin': _get_issue_count(report, 'missing_tmin_f'),
                'missing_energy': _get_issue_count(report, 'missing_energy_mwh'),
                'temp_outliers': _get_issue_count(report, 'temperature_outliers'),
                'energy_outliers': _get_issue_count(report, 'energy_outliers'),
                'duplicates': _get_issue_count(report, 'duplicates'),
                'missing_cities': _get_issue_count(report, 'missing_cities'),
                'date_gaps': _get_issue_count(report, 'date_gaps'),
                'days_since_update': _get_freshness_days(report),
                'is_stale': _is_data_stale(report),
                'full_report': report
            }
        else:
            return {
                'file_path': file_path,
                'date': report.get('run_date', ''),
                'total_records': 0,
                'quality_score': 0,
                'critical_issues': 0,
                'high_issues': 0,
                'medium_issues': 0,
                'low_issues': 0,
                'missing_tmax': report.get('missing_values', {}).get('summary', {}).get('tmax_f', 0),
                'missing_tmin': report.get('missing_values', {}).get('summary', {}).get('tmin_f', 0),
                'missing_energy': report.get('missing_values', {}).get('summary', {}).get('energy_mwh', 0),
                'temp_outliers': report.get('outliers', {}).get('temperature', {}).get('count', 0),
                'energy_outliers': report.get('outliers', {}).get('energy', {}).get('count', 0),
                'duplicates': 0,
                'missing_cities': 0,
                'date_gaps': 0,
                'days_since_update': report.get('freshness', {}).get('days_since_update', 0),
                'is_stale': report.get('freshness', {}).get('is_stale', False),
                'full_report': report
            }
    except Exception as e:
        logger.error(f"Error extracting report data: {e}")
        return None

def _get_issue_count(report: Dict[str, Any], issue_key: str) -> int:
    try:
        return report['issues'].get(issue_key, {}).get('count', 0)
    except:
        return 0

def _get_freshness_days(report: Dict[str, Any]) -> int:
    try:
        return report['issues'].get('data_freshness', {}).get('count', 0)
    except:
        return 0

def _is_data_stale(report: Dict[str, Any]) -> bool:
    days = _get_freshness_days(report)
    return days > QUALITY_THRESHOLDS['freshness_critical']

def render_header():
    st.markdown("""
    <div style="text-align: center; padding: 1rem; background: linear-gradient(90deg, #1f4037 0%, #99f2c8 100%); border-radius: 10px; margin-bottom: 2rem;">
        <h1 style="color: white; margin: 0;">Energy Data Quality Monitoring Dashboard</h1>
        <p style="color: white; margin: 0.5rem 0 0 0;">Real-time monitoring of data pipeline health and quality</p>
    </div>
    """, unsafe_allow_html=True)

def render_sidebar_controls(df_reports: pd.DataFrame) -> Dict[str, Any]:
    """Render sidebar controls and filters."""
    st.sidebar.header("Dashboard Controls")
    
    # Date range filter
    if not df_reports.empty:
        min_date = df_reports['date'].min().date()
        max_date = df_reports['date'].max().date()
        
        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        if len(date_range) == 2:
            start_date, end_date = date_range
            filtered_df = df_reports[
                (df_reports['date'].dt.date >= start_date) & 
                (df_reports['date'].dt.date <= end_date)
            ]
        else:
            filtered_df = df_reports
    else:
        filtered_df = df_reports
    
    # Auto-refresh option
    auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)
    if auto_refresh:
        st.rerun()
    
    # Quality threshold controls
    st.sidebar.subheader("Quality Thresholds")
    
    missing_threshold = st.sidebar.slider(
        "Missing Data Critical %", 
        min_value=1, max_value=50, 
        value=int(QUALITY_THRESHOLDS['missing_critical'])
    )
    
    freshness_threshold = st.sidebar.slider(
        "Data Freshness (days)", 
        min_value=1, max_value=7, 
        value=QUALITY_THRESHOLDS['freshness_critical']
    )
    
    return {
        'filtered_df': filtered_df,
        'missing_threshold': missing_threshold,
        'freshness_threshold': freshness_threshold
    }

def render_kpi_metrics(latest_report: pd.Series, df_reports: pd.DataFrame):
    """Render key performance indicator metrics."""
    st.subheader("Key Performance Indicators")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    # Data Quality Score
    with col1:
        score = latest_report.get('quality_score', 0)
        st.metric(
            label=f"Quality Score",
            value=f"{score:.1f}%",
            delta=_calculate_score_delta(df_reports) if len(df_reports) > 1 else None
        )
    
    # Total Issues
    with col2:
        total_issues = (latest_report['critical_issues'] + latest_report['high_issues'] + 
                      latest_report['medium_issues'] + latest_report['low_issues'])
        st.metric(
            label=f"Total Issues",
            value=total_issues,
            delta=_calculate_issue_delta(df_reports) if len(df_reports) > 1 else None
        )
    
    # Missing Data
    with col3:
        missing_total = (latest_report['missing_tmax'] + latest_report['missing_tmin'] + 
                       latest_report['missing_energy'])
        if isinstance(missing_total, (int, float)) and pd.notnull(missing_total):
            missing_value_str = f"{int(missing_total):,}"
        else:
            missing_value_str = "N/A"
        st.metric(
            label=f"Missing Values",
            value=missing_value_str,
            delta=_calculate_missing_delta(df_reports) if len(df_reports) > 1 else None
        )
    
    # Outliers
    with col4:
        outliers_total = latest_report['temp_outliers'] + latest_report['energy_outliers']
        st.metric(
            label=f"Data Outliers",
            value=f"{outliers_total:,}",
            delta=_calculate_outlier_delta(df_reports) if len(df_reports) > 1 else None
        )
    
    # Data Freshness
    with col5:
        days_old = latest_report['days_since_update']
        freshness_status = "Fresh" if days_old <= 1 else "Aging" if days_old <= 2 else "Stale"
        st.metric(
            label=f"Data Freshness",
            value=freshness_status,
            delta=f"{days_old} days old"
        )

def render_quality_trends(df_reports: pd.DataFrame):
    """Render quality trend visualizations."""
    st.subheader("Data Quality Trends")
    
    if df_reports.empty:
        st.warning("No trend data available")
        return
    
    tab1, tab2, tab3, tab4 = st.tabs(["Quality Score", "Missing Values", "Outliers", "Freshness"])
    
    with tab1:
        _render_quality_score_chart(df_reports)
    
    with tab2:
        _render_missing_values_chart(df_reports)
    
    with tab3:
        _render_outliers_chart(df_reports)
    
    with tab4:
        _render_freshness_chart(df_reports)

def _render_quality_score_chart(df_reports: pd.DataFrame):
    """Render quality score trend chart."""
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df_reports['date'],
        y=df_reports['quality_score'],
        mode='lines+markers',
        name='Quality Score',
        line=dict(color='#2E86AB', width=3),
        marker=dict(size=8, color='#2E86AB'),
        fill='tonexty',
        fillcolor='rgba(46, 134, 171, 0.1)'
    ))
    
    # Add quality thresholds
    fig.add_hline(y=90, line_dash="dash", line_color="green", 
                 annotation_text="Excellent (90%)")
    fig.add_hline(y=70, line_dash="dash", line_color="orange", 
                 annotation_text="Good (70%)")
    
    fig.update_layout(
        title="Data Quality Score Over Time",
        xaxis_title="Date",
        yaxis_title="Quality Score (%)",
        yaxis=dict(range=[0, 100]),
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)

def _render_missing_values_chart(df_reports: pd.DataFrame):
    """Render missing values trend chart."""
    fig = go.Figure()
    
    # Absolute counts
    fig.add_trace(go.Scatter(
        x=df_reports['date'], y=df_reports['missing_tmax'],
        name='Max Temperature', line=dict(color='#FF6B6B')
    ))
    
    fig.add_trace(go.Scatter(
        x=df_reports['date'], y=df_reports['missing_tmin'],
        name='Min Temperature', line=dict(color='#4ECDC4')
    ))
    
    fig.add_trace(go.Scatter(
        x=df_reports['date'], y=df_reports['missing_energy'],
        name='Energy', line=dict(color='#45B7D1')
    ))
    
    # Percentage (if total_records is available)
    if 'total_records' in df_reports.columns:
        missing_tmax = pd.to_numeric(df_reports['missing_tmax'], errors='coerce')
        total_records = pd.to_numeric(df_reports['total_records'], errors='coerce')
        percent_missing_tmax = (missing_tmax / total_records * 100)
        fig.add_trace(go.Scatter(
            x=df_reports['date'],
            y=percent_missing_tmax,
            name='Max Temp %', line=dict(color='#FF6B6B', dash='dot')
        ))
    
    fig.update_layout(title="Missing Values Analysis", hovermode='x unified')
    st.plotly_chart(fig, use_container_width=True)

def _render_outliers_chart(df_reports: pd.DataFrame):
    """Render outliers trend chart."""
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df_reports['date'], 
        y=df_reports['temp_outliers'],
        name='Temperature Outliers',
        marker_color='#FF6B6B'
    ))
    
    fig.add_trace(go.Bar(
        x=df_reports['date'], 
        y=df_reports['energy_outliers'],
        name='Energy Outliers',
        marker_color='#4ECDC4'
    ))
    
    fig.update_layout(
        title="Data Outliers Over Time",
        xaxis_title="Date",
        yaxis_title="Number of Outliers",
        hovermode='x unified'
    )
    st.plotly_chart(fig, use_container_width=True)

def _render_freshness_chart(df_reports: pd.DataFrame):
    """Render data freshness chart."""
    # Color code bars based on freshness
    colors = df_reports['days_since_update'].apply(
        lambda x: '#28a745' if x <= 1 else '#ffc107' if x <= 2 else '#dc3545'
    )
    
    fig = go.Figure(data=[
        go.Bar(
            x=df_reports['date'],
            y=df_reports['days_since_update'],
            marker_color=colors,
            name='Days Since Update',
            text=df_reports['days_since_update'],
            textposition='auto'
        )
    ])
    
    # Add freshness thresholds
    fig.add_hline(y=1, line_dash="dash", line_color="green", 
                 annotation_text="Fresh (≤1 day)")
    fig.add_hline(y=2, line_dash="dash", line_color="orange", 
                 annotation_text="Warning (≤2 days)")
    
    fig.update_layout(
        title="Data Freshness Over Time",
        xaxis_title="Date",
        yaxis_title="Days Since Latest Data",
        hovermode='x unified'
    )
    st.plotly_chart(fig, use_container_width=True)

def render_detailed_analysis(latest_report: pd.Series):
    """Render detailed analysis section."""
    st.subheader("Detailed Quality Analysis")
    
    if 'full_report' not in latest_report or not latest_report['full_report']:
        st.warning("Detailed analysis not available for this report.")
        return
    
    report = latest_report['full_report']
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Temperature Issues")
        if 'issues' in report:
            temp_issues = report['issues'].get('temperature_outliers', {})
            if temp_issues.get('records'):
                temp_df = pd.DataFrame(temp_issues['records'])
                st.dataframe(temp_df, use_container_width=True)
                
                # Temperature outlier chart
                if len(temp_df) > 0:
                    fig = px.scatter(
                        temp_df, x='tmax_f', y='tmin_f', 
                        color='city', title="Temperature Outliers",
                        hover_data=['date']
                    )
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.success("No temperature outliers found")
        else:
            st.info("Temperature analysis not available")
    
    with col2:
        st.markdown("### Energy Issues")
        if 'issues' in report:
            energy_issues = report['issues'].get('energy_outliers', {})
            if energy_issues.get('records'):
                energy_df = pd.DataFrame(energy_issues['records'])
                st.dataframe(energy_df, use_container_width=True)
                
                # Energy outlier chart
                if len(energy_df) > 0:
                    fig = px.histogram(
                        energy_df, x='energy_mwh', 
                        title="Energy Outlier Distribution",
                        nbins=20
                    )
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.success("No energy outliers found")
        else:
            st.info("Energy analysis not available")

def render_documentation():
    """Render comprehensive documentation."""
    with st.expander("Understanding Data Quality Metrics", expanded=False):
        st.markdown("""
        ### Data Quality Framework
        
        #### Quality Score (0-100%)
        - **90-100%**: Excellent data quality
        - **70-89%**: Good data quality with minor issues
        - **50-69%**: Fair data quality requiring attention
        - **<50%**: Poor data quality needing immediate action
        
        #### Missing Values Analysis
        - **Critical (>10%)**: Significant data gaps affecting analysis
        - **Warning (5-10%)**: Moderate gaps requiring monitoring
        - **Acceptable (<5%)**: Normal operational range
        
        #### Outlier Detection
        - **Temperature**: Flags impossible values (>130°F, <-50°F) or logical errors (max < min)
        - **Energy**: Identifies negative consumption values and statistical anomalies
        - **Impact**: Outliers can skew forecasts and indicate sensor malfunctions
        
        #### Data Freshness
        - **Fresh (≤1 day)**: Current and actionable
        - **Aging (1-2 days)**: Still usable but monitor pipeline
        - **Stale (>2 days)**: May impact forecast accuracy
        
        #### Data Consistency
        - **Duplicates**: Checks for redundant records that could bias analysis
        - **Coverage**: Ensures all expected cities and dates are present
        - **Completeness**: Validates data pipeline integrity
        """)

def render_alerts_section(latest_report: pd.Series):
    """Render alerts and recommendations."""
    st.subheader("Alerts & Recommendations")
    
    alerts = []
    
    # Quality score alerts
    score = latest_report.get('quality_score', 100)
    if score < 50:
        alerts.append(("Critical", f"Quality score is {score:.1f}% - Immediate action required"))
    elif score < 70:
        alerts.append(("Warning", f"Quality score is {score:.1f}% - Monitor closely"))
    
    # Missing data alerts
    total_missing = (latest_report['missing_tmax'] + latest_report['missing_tmin'] + 
                    latest_report['missing_energy'])
    if isinstance(total_missing, (int, float)) and pd.notnull(total_missing):
        if total_missing > 1000:
            alerts.append(("Critical", f"{int(total_missing):,} missing values detected"))
        elif total_missing > 100:
            alerts.append(("Warning", f"{int(total_missing):,} missing values - investigate data sources"))
    
    # Freshness alerts
    days_old = latest_report['days_since_update']
    if days_old > 3:
        alerts.append(("Critical", f"Data is {days_old} days old - check pipeline"))
    elif days_old > 1:
        alerts.append(("Warning", f"Data is {days_old} days old - monitor updates"))
    
    # Outlier alerts
    total_outliers = latest_report['temp_outliers'] + latest_report['energy_outliers']
    if total_outliers > 50:
        alerts.append(("Warning", f"{total_outliers} outliers detected - review data sources"))
    
    if alerts:
        for alert_type, message in alerts:
            if "Critical" in alert_type:
                st.error(f"{alert_type}: {message}")
            else:
                st.warning(f"{alert_type}: {message}")
    else:
        st.success("No critical issues detected - data quality is within acceptable ranges")

def _calculate_score_delta(df_reports: pd.DataFrame) -> Optional[float]:
    """Calculate quality score delta from previous report."""
    if len(df_reports) < 2:
        return None
    current_score = df_reports.iloc[-1]['quality_score']
    previous_score = df_reports.iloc[-2]['quality_score']
    return round(current_score - previous_score, 1)

def _calculate_issue_delta(df_reports: pd.DataFrame) -> Optional[int]:
    """Calculate total issues delta from previous report."""
    if len(df_reports) < 2:
        return None
    current_issues = (df_reports.iloc[-1]['critical_issues'] + df_reports.iloc[-1]['high_issues'] + 
                     df_reports.iloc[-1]['medium_issues'] + df_reports.iloc[-1]['low_issues'])
    previous_issues = (df_reports.iloc[-2]['critical_issues'] + df_reports.iloc[-2]['high_issues'] + 
                      df_reports.iloc[-2]['medium_issues'] + df_reports.iloc[-2]['low_issues'])
    return int(current_issues - previous_issues)

def _calculate_missing_delta(df_reports: pd.DataFrame) -> Optional[int]:
    """Calculate missing values delta from previous report."""
    if len(df_reports) < 2:
        return None
    current_missing = (df_reports.iloc[-1]['missing_tmax'] + df_reports.iloc[-1]['missing_tmin'] + 
                      df_reports.iloc[-1]['missing_energy'])
    previous_missing = (df_reports.iloc[-2]['missing_tmax'] + df_reports.iloc[-2]['missing_tmin'] + 
                       df_reports.iloc[-2]['missing_energy'])
    return int(current_missing - previous_missing)

def _calculate_outlier_delta(df_reports: pd.DataFrame) -> Optional[int]:
    """Calculate outliers delta from previous report."""
    if len(df_reports) < 2:
        return None
    current_outliers = df_reports.iloc[-1]['temp_outliers'] + df_reports.iloc[-1]['energy_outliers']
    previous_outliers = df_reports.iloc[-2]['temp_outliers'] + df_reports.iloc[-2]['energy_outliers']
    return int(current_outliers - previous_outliers)


# Main dashboard page: all analytics and visualizations
# -----------------------------------------------------
def show_main_dashboard():
    st.title("US Energy & Weather Data Dashboard")

    # FIXED: Use consistent data file path
    data_file = "data/merged_data.csv"
    if os.path.exists(data_file):
        df = load_data(data_file)
        df["date"] = pd.to_datetime(df["date"])
    else:
        st.error("No data file found. Please run the pipeline first.")
        st.stop()

    # --- Timezone handling for 'last checked' ---
    # Use timezone-aware UTC datetime (Python 3.11+)
    utc_now = datetime.now(UTC)
    if len(df["city"].unique()) == 1:
        city = df["city"].unique()[0]
    else:
        city = "New York"
    local_now = convert_to_local(utc_now, city)
    st.caption(f"Last checked: {local_now.strftime('%Y-%m-%d %H:%M %Z')}")

    # Sidebar filters for date range and city selection
    default_start = df["date"].min().date()
    default_end = df["date"].max().date()
    date_range = st.sidebar.date_input(
        "Date range", [default_start, default_end], min_value=default_start, max_value=default_end)

    # Robust handling for date_range
    if (isinstance(date_range, (tuple, list)) and len(date_range) == 2):
        start_date, end_date = date_range
    else:
        st.warning("Please select a valid start and end date for the date range.")
        return

    st.sidebar.subheader("Cities")
    all_cities = list(CITY_COORDS.keys())

    # Radio for All Cities or Select Cities
    city_mode = st.sidebar.radio(
        "City Selection Mode",
        ["All Cities", "Select Cities"],
        index=0,
        key="city_mode"
    )

    if city_mode == "All Cities":
        cities = all_cities
    else:
        # Show checkboxes for each city
        checked_cities = []
        for city in all_cities:
            if st.sidebar.checkbox(city, value=False, key=f"city_{city}"):
                checked_cities.append(city)
        # If none checked, default to first city
        if not checked_cities:
            st.sidebar.info(f"At least one city must be selected. Showing: {all_cities[0]}")
            checked_cities = [all_cities[0]]
        cities = checked_cities
        # If all cities are checked, show a message
        if len(checked_cities) == len(all_cities):
            st.sidebar.info("All cities selected.")

    # Filter data based on sidebar selections
    mask = (
        (df["date"] >= pd.to_datetime(start_date)) &
        (df["date"] <= pd.to_datetime(end_date)) &
        (df["city"].isin(cities))
    )
    df_filt = df[mask].copy()

    # Show the most recent data date in the filtered set
    st.caption(f"Last updated: {df['date'].max().date()}")

    # --- Visualization 1: Geographic Overview ---
    st.subheader("1. Geographic Overview")
    
    # Find the latest and previous day in the filtered data
    latest_day = df_filt["date"].max()
    prev_day = latest_day - pd.Timedelta(days=1)
    
    # Create map data for each city
    map_data = []
    
    for city in cities:
        city_data = df_filt[df_filt["city"] == city].sort_values("date")
        
        # Find the most recent day with energy data for this city
        city_data_nonan = city_data.dropna(subset=["energy_mwh"])
        
        if city_data_nonan.empty:
            temp = energy = energy_yest = pct_change = None
            day_used = None
        else:
            day_used = city_data_nonan["date"].max()
            row_today = city_data_nonan[city_data_nonan["date"] == day_used]
            
            # Get current temperature and energy
            temp = row_today["tmax_f"].dropna().iloc[0] if not row_today["tmax_f"].dropna().empty else None
            energy = row_today["energy_mwh"].dropna().iloc[0] if not row_today["energy_mwh"].dropna().empty else None
            
            # Find previous day with energy data
            prev_days = city_data_nonan[city_data_nonan["date"] < day_used]
            if not prev_days.empty:
                prev_day_actual = prev_days["date"].max()
                row_yest = prev_days[prev_days["date"] == prev_day_actual]
                energy_yest = row_yest["energy_mwh"].dropna().iloc[0] if not row_yest["energy_mwh"].dropna().empty else None
            else:
                energy_yest = None
            
            # Calculate percentage change
            pct_change = None
            if energy is not None and energy_yest is not None and energy_yest != 0:
                pct_change = 100 * (energy - energy_yest) / energy_yest
        
        # Determine color and size based on energy usage and change
        if energy is None or energy_yest is None:
            color = "gray"
            marker_size = 15
            status = "No Data"
        elif pct_change > 5:  # Significant increase
            color = "red"
            marker_size = 20
            status = "High Usage"
        elif pct_change < -5:  # Significant decrease
            color = "green"
            marker_size = 20
            status = "Low Usage"
        else:  # Minor change
            color = "green"
            marker_size = 15
            status = "Low Usage"
        
        map_data.append({
            "city": city,
            "lat": CITY_COORDS[city]["lat"],
            "lon": CITY_COORDS[city]["lon"],
            "Current Temp (°F)": f"{temp:.1f}" if temp is not None else "N/A",
            "Today's Energy (MWh)": f"{energy:.1f}" if energy is not None else "N/A",
            "% Change from Yesterday": f"{pct_change:+.1f}%" if pct_change is not None else "N/A",
            "color": color,
            "marker_size": marker_size,
            "status": status,
            "Day Used": str(day_used.date()) if day_used is not None else "N/A",
            "temp_val": temp,
            "energy_val": energy,
            "pct_change_val": pct_change
        })
    
    # Create DataFrame for map
    map_df = pd.DataFrame(map_data)
    
    # Improved error handling for empty or malformed map_df
    if map_df.empty or "status" not in map_df.columns:
        st.warning("No data available for the selected filters. Please adjust your filters or check your data.")
    else:
        # Create custom scatter plot for better control
        fig = go.Figure()
        
        # Add markers for each status type
        for status in ["High Usage", "Low Usage", "No Data"]:
            status_data = map_df[map_df["status"] == status]
            if not status_data.empty:
                fig.add_trace(go.Scattermapbox(
                    lat=status_data["lat"],
                    lon=status_data["lon"],
                    mode="markers",
                    marker=dict(
                        size=status_data["marker_size"],
                        color=status_data["color"],
                        opacity=0.8,
                        sizemode="diameter"
                    ),
                    text=status_data["city"],
                    hovertemplate="<b>%{text}</b><br>" +
                                "Temperature: " + status_data["Current Temp (°F)"] + "<br>" +
                                "Energy Usage: " + status_data["Today's Energy (MWh)"] + "<br>" +
                                "Change from Yesterday: " + status_data["% Change from Yesterday"] + "<br>" +
                                "Data Date: " + status_data["Day Used"] + "<br>" +
                                "Status: " + status_data["status"] +
                                "<extra></extra>",
                    name=status
                ))
        
        # Update layout
        fig.update_layout(
            mapbox=dict(
                style="open-street-map",
                zoom=3,
                center=dict(lat=39.8283, lon=-98.5795)  # Center of USA
            ),
            height=500,
            margin=dict(r=0, t=60, l=0, b=0),
            title=dict(
                text=f"Energy Usage Overview - Temperature & Daily Changes<br>" +
                     f"<sup>Last updated: {latest_day.date()}</sup>",
                x=0.0,
                font=dict(size=16)
            ),
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Add summary statistics below the map
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            high_usage_count = len(map_df[map_df["status"] == "High Usage"])
            st.metric("Cities with High Usage", high_usage_count)
        
        with col2:
            low_usage_count = len(map_df[map_df["status"] == "Low Usage"])
            st.metric("Cities with Low Usage", low_usage_count)
        
        with col3:
            avg_temp = map_df[map_df["temp_val"].notna()]["temp_val"].mean()
            st.metric("Average Temperature", f"{avg_temp:.1f}°F" if not pd.isna(avg_temp) else "N/A")
        
        with col4:
            # Average energy usage for the most recent day across all cities
            latest_day = df_filt["date"].max()
            latest_day_df = df_filt[df_filt["date"] == latest_day]
            avg_energy_latest_day = latest_day_df["energy_mwh"].mean()
            st.metric("Avg Energy (Latest Day)", f"{avg_energy_latest_day:.1f} MWh" if not pd.isna(avg_energy_latest_day) else "N/A")
        
        with col5:
            # Average energy usage for all days and all cities in the filtered data
            avg_energy_all_days = df_filt["energy_mwh"].mean()
            st.metric("Avg Energy (All Days)", f"{avg_energy_all_days:.1f} MWh" if not pd.isna(avg_energy_all_days) else "N/A")
        
        # Add detailed table
        with st.expander("City Details"):
            display_df = map_df[["city", "Current Temp (°F)", "Today's Energy (MWh)", 
                               "% Change from Yesterday", "Day Used", "status"]].copy()
            display_df.columns = ["City", "Temperature", "Energy Usage", "Daily Change", "Data Date", "Status"]
            st.dataframe(display_df, use_container_width=True)

    # --- Visualization 2: Time Series Analysis ---
    st.subheader("2. Time Series Analysis")

    if df_filt.empty or "tmax_f" not in df_filt.columns or "energy_mwh" not in df_filt.columns:
        st.warning("No data available for time series analysis with the selected filters.")
    else:
        # Use sidebar city selection for time series
        if len(cities) == 1:
            df_ts = df_filt[df_filt["city"] == cities[0]].copy()
            selected_city = cities[0]
        else:
            df_ts = df_filt.copy()
            selected_city = "All Cities"

        # Sort by date to ensure proper line connections
        df_ts = df_ts.sort_values("date")

        # Helper to shade weekends
        def is_weekend(dt):
            return dt.weekday() >= 5

        # Create dual-axis line chart
        fig_ts = go.Figure()

        # Add temperature trace
        fig_ts.add_trace(go.Scatter(
            x=df_ts["date"], 
            y=df_ts["tmax_f"], 
            name="Max Temp (°F)", 
            yaxis="y1", 
            mode="lines", 
            line=dict(color="royalblue", width=2),
            hovertemplate="<b>Date:</b> %{x}<br><b>Max Temp:</b> %{y:.1f}°F<extra></extra>"
        ))

        # Add energy trace
        fig_ts.add_trace(go.Scatter(
            x=df_ts["date"], 
            y=df_ts["energy_mwh"], 
            name="Energy (MWh)", 
            yaxis="y2", 
            mode="lines", 
            line=dict(color="firebrick", dash="dot", width=2),
            hovertemplate="<b>Date:</b> %{x}<br><b>Energy:</b> %{y:.1f} MWh<extra></extra>"
        ))

        # Shade weekends more efficiently
        date_range = pd.date_range(start=df_ts["date"].min(), end=df_ts["date"].max(), freq='D')
        weekend_dates = [d for d in date_range if is_weekend(d)]

        for weekend_date in weekend_dates:
            fig_ts.add_vrect(
                x0=weekend_date, 
                x1=weekend_date + pd.Timedelta(days=1), 
                fillcolor="lightgray", 
                opacity=0.15, 
                line_width=0,
                layer="below"
            )

        # Update layout
        fig_ts.update_layout(
            title=f"Temperature vs Energy Consumption - {selected_city}" if selected_city != "All Cities" else "Temperature vs Energy Consumption - All Cities",
            yaxis=dict(
                title=dict(
                    text="Max Temperature (°F)",
                    font=dict(color="royalblue")
                ),
                side="left",
                tickfont=dict(color="royalblue")
            ),
            yaxis2=dict(
                overlaying="y",
                side="right",
                title=dict(
                    text="Energy (MWh)",
                    font=dict(color="firebrick")
                ),
                tickfont=dict(color="firebrick")
            ),
            xaxis_title="Date",
            legend=dict(
                orientation="h", 
                yanchor="bottom", 
                y=1.02, 
                xanchor="right", 
                x=1
            ),
            height=450,
            hovermode='x unified',
            showlegend=True,
            plot_bgcolor='white',
            margin=dict(t=80)
        )

        # Add grid for better readability
        fig_ts.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
        fig_ts.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')

        st.plotly_chart(fig_ts, use_container_width=True)

        # Add summary statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Avg Max Temp", f"{df_ts['tmax_f'].mean():.1f}°F")
        with col2:
            st.metric("Avg Energy", f"{df_ts['energy_mwh'].mean():.1f} MWh")
        with col3:
            correlation = df_ts['tmax_f'].corr(df_ts['energy_mwh'])
            st.metric("Correlation", f"{correlation:.3f}")

        # Add trend analysis
        if len(df_ts) > 7: 
            st.write("**Trend Analysis:**")
        
        # Simple 7-day moving averages
        df_ts['temp_ma7'] = df_ts['tmax_f'].rolling(window=7, center=True).mean()
        df_ts['energy_ma7'] = df_ts['energy_mwh'].rolling(window=7, center=True).mean()
        
        # Show trend direction
        temp_trend = "increasing" if df_ts['temp_ma7'].iloc[-1] > df_ts['temp_ma7'].iloc[0] else "decreasing"
        energy_trend = "increasing" if df_ts['energy_ma7'].iloc[-1] > df_ts['energy_ma7'].iloc[0] else "decreasing"
        
        st.write(f"- Temperature trend: {temp_trend}")
        st.write(f"- Energy consumption trend: {energy_trend}")

    # --- Visualization 3: Correlation Analysis ---
    st.subheader("3. Correlation Analysis")

    if df_filt.empty or "tmax_f" not in df_filt.columns or "energy_mwh" not in df_filt.columns:
        st.warning("No data available for correlation analysis with the selected filters.")
    else:
        # Remove rows with missing values for correlation
        corr_df = df_filt.dropna(subset=["tmax_f", "energy_mwh"])
        if len(cities) == 1:
            # Only one city selected: show individual analysis
            plot_df = corr_df[corr_df["city"] == cities[0]].copy()
            title_suffix = f" - {cities[0]}"
            analysis_type = "Individual Cities"
        else:
            # Multiple cities: show all cities combined
            plot_df = corr_df.copy()
            title_suffix = " - All Cities"
            analysis_type = "All Cities Combined"
        if not plot_df.empty:
            # Calculate regression statistics
            X = plot_df[["tmax_f"]].values
            y = plot_df["energy_mwh"].values
            reg = LinearRegression().fit(X, y)
            y_pred = reg.predict(X)
            r2 = reg.score(X, y)
            corr_coef = np.corrcoef(plot_df["tmax_f"], plot_df["energy_mwh"])[0,1]
            
            # Calculate additional statistics
            n_points = len(plot_df)
            slope = reg.coef_[0]
            intercept = reg.intercept_
            
            # Create equation string
            sign = "+" if intercept >= 0 else "-"
            eqn = f"y = {slope:.2f}x {sign} {abs(intercept):.2f}"
            
            # Create scatter plot
            fig_corr = px.scatter(
                plot_df, 
                x="tmax_f", 
                y="energy_mwh", 
                color="city" if analysis_type == "All Cities Combined" else None,
                hover_data=["date"],
                labels={"tmax_f": "Max Temperature (°F)", "energy_mwh": "Energy Consumption (MWh)"},
                title=f"Temperature vs Energy Consumption{title_suffix}",
                color_discrete_sequence=px.colors.qualitative.Set1
            )
            
            # Add regression line
            fig_corr.add_trace(go.Scatter(
                x=plot_df["tmax_f"], 
                y=y_pred, 
                mode="lines", 
                name="Regression Line",
                line=dict(color="black", dash="dash", width=3),
                hovertemplate="<b>Regression Line</b><br>Temp: %{x:.1f}°F<br>Predicted Energy: %{y:.1f} MWh<extra></extra>"
            ))
            
            # Add confidence interval (optional)
            if st.checkbox("Show Confidence Interval", value=False):
                from scipy import stats
                
                # Calculate prediction intervals
                y_mean = np.mean(y)
                ss_res = np.sum((y - y_pred) ** 2)
                ss_tot = np.sum((y - y_mean) ** 2)
                
                # Standard error of prediction
                mse = ss_res / (n_points - 2)
                se = np.sqrt(mse)
                
                # 95% confidence interval
                t_val = stats.t.ppf(0.975, n_points - 2)
                margin = t_val * se
                
                # Sort for smooth confidence bands
                sorted_indices = np.argsort(plot_df["tmax_f"])
                x_sorted = plot_df["tmax_f"].iloc[sorted_indices]
                y_pred_sorted = y_pred[sorted_indices]
                
                fig_corr.add_trace(go.Scatter(
                    x=x_sorted, 
                    y=y_pred_sorted + margin,
                    mode="lines",
                    line=dict(color="rgba(0,0,0,0)"),
                    showlegend=False,
                    hoverinfo="skip"
                ))
                
                fig_corr.add_trace(go.Scatter(
                    x=x_sorted, 
                    y=y_pred_sorted - margin,
                    mode="lines",
                    line=dict(color="rgba(0,0,0,0)"),
                    fill='tonexty',
                    fillcolor="rgba(128,128,128,0.2)",
                    name="95% Confidence Interval",
                    hoverinfo="skip"
                ))
            
            # Add statistics annotation
            stats_text = f"""
            <b>Statistics:</b><br>
            {eqn}<br>
            R² = {r2:.3f}<br>
            r = {corr_coef:.3f}<br>
            n = {n_points}
            """
            
            fig_corr.add_annotation(
                x=0.05, y=0.95, 
                xref="paper", yref="paper",
                text=stats_text,
                showarrow=False,
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor="gray",
                borderwidth=1,
                font=dict(size=12)
            )
            
            # Update layout
            fig_corr.update_layout(
                height=500,
                hovermode='closest',
                showlegend=True,
                legend=dict(
                    orientation="v",
                    yanchor="top",
                    y=0.99,
                    xanchor="left",
                    x=1.01
                )
            )
            
            # Add grid
            fig_corr.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
            fig_corr.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
            
            st.plotly_chart(fig_corr, use_container_width=True)
            
            # Statistical interpretation
            st.write("**Statistical Interpretation:**")
            
            # Correlation strength interpretation
            if abs(corr_coef) >= 0.8:
                strength = "very strong"
            elif abs(corr_coef) >= 0.6:
                strength = "strong"
            elif abs(corr_coef) >= 0.4:
                strength = "moderate"
            elif abs(corr_coef) >= 0.2:
                strength = "weak"
            else:
                strength = "very weak"
            
            direction = "positive" if corr_coef > 0 else "negative"
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Correlation Strength", strength.title())
            with col2:
                st.metric("Direction", direction.title())
            with col3:
                st.metric("R-squared", f"{r2:.3f}")
            with col4:
                st.metric("Sample Size", f"{n_points:,}")
            
            # Detailed interpretation
            if analysis_type == "All Cities Combined":
                st.write(f"The analysis shows a **{strength} {direction}** correlation between temperature and energy consumption across all cities.")
            else:
                st.write(f"For {cities[0]}, there is a **{strength} {direction}** correlation between temperature and energy consumption.")
            
            st.write(f"The R-squared value of {r2:.3f} indicates that {r2*100:.1f}% of the variance in energy consumption can be explained by maximum temperature.")
            
            # Practical interpretation
            if corr_coef > 0:
                st.write(f"**Practical meaning:** For every 1°F increase in maximum temperature, energy consumption increases by approximately {slope:.2f} MWh on average.")
            else:
                st.write(f"**Practical meaning:** For every 1°F increase in maximum temperature, energy consumption decreases by approximately {abs(slope):.2f} MWh on average.")
            
            # City-specific analysis for all cities
            if analysis_type == "All Cities Combined" and len(cities) > 1:
                st.write("**City-Specific Correlations:**")
                city_corr_data = []
                for city in cities:
                    city_data = corr_df[corr_df["city"] == city]
                    if len(city_data) > 3:  # Need at least 4 points for meaningful correlation
                        city_corr = city_data["tmax_f"].corr(city_data["energy_mwh"])
                        city_r2 = LinearRegression().fit(
                            city_data[["tmax_f"]], city_data["energy_mwh"]
                        ).score(city_data[["tmax_f"]], city_data["energy_mwh"])
                        city_corr_data.append({
                            "City": city,
                            "Correlation": city_corr,
                            "R²": city_r2,
                            "Data Points": len(city_data)
                        })
                if city_corr_data:
                    city_corr_df = pd.DataFrame(city_corr_data)
                    st.dataframe(city_corr_df.round(3), use_container_width=True)
                else:
                    st.info("No city-specific data available for correlation analysis.")
        else:
            # Handle case where selected city has no data
            st.info("Not enough data for correlation analysis. Please ensure your dataset contains both temperature and energy consumption values for the selected city.")

    # --- Visualization 4: Usage Patterns Heatmap ---
    st.subheader("4. Usage Patterns Heatmap")

    if df_filt.empty or "tmax_f" not in df_filt.columns or "energy_mwh" not in df_filt.columns:
        st.warning("No data available for heatmap with the selected filters.")
    else:
        # Create temperature bins
        bins = [-100, 50, 60, 70, 80, 90, 1000]
        labels = ["<50°F", "50-60°F", "60-70°F", "70-80°F", "80-90°F", ">90°F"]

        # Add temperature bins and day of week to dataframe
        df_filt = df_filt.copy()
        df_filt["temp_bin"] = pd.cut(df_filt["tmax_f"], bins=bins, labels=labels)
        df_filt["day_of_week"] = df_filt["date"].dt.day_name()

        # Use sidebar city selection for heatmap
        df_heat = df_filt.copy()

        # Check if we have enough data
        if len(df_heat) == 0:
            st.warning("No data available for the selected filters.")
        else:
            # Create pivot table for heatmap: average energy by temp bin and day of week
            heatmap_data = (df_heat.groupby(["temp_bin", "day_of_week"], observed=False)
                            ['energy_mwh'].mean().unstack())
        
        # Reindex to ensure proper order
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        heatmap_data = heatmap_data.reindex(index=labels, columns=days, fill_value=np.nan)
        
        # Check if we have any valid data
        if heatmap_data.isna().all().all():
            st.warning("No valid data points found for the heatmap with current filters.")
        else:
            # Create heatmap figure
            fig_heat = go.Figure(data=go.Heatmap(
                z=heatmap_data.values,
                x=heatmap_data.columns,
                y=heatmap_data.index,
                colorscale="RdBu_r",  # Blue (low) to Red (high)
                colorbar=dict(
                    title="Avg Energy (MWh)"
                ),
                # Remove the duplicate text parameter - we'll handle annotations separately
                hovertemplate="<b>%{y}</b><br>" +
                              "Day: %{x}<br>" +
                              "Avg Energy: %{z:.1f} MWh<br>" +
                              "<extra></extra>",
                # Handle missing values better
                zmid=None,  # Let plotly auto-scale
                zmin=np.nanmin(heatmap_data.values) if not np.isnan(heatmap_data.values).all() else 0,
                zmax=np.nanmax(heatmap_data.values) if not np.isnan(heatmap_data.values).all() else 100
            ))
            
            # Add text annotations on cells (improved version)
            for i, temp_range in enumerate(heatmap_data.index):
                for j, day in enumerate(heatmap_data.columns):
                    val = heatmap_data.iloc[i, j]
                    if not pd.isna(val) and val > 0:
                        # Choose text color based on value for better contrast
                        # Use the actual min/max for better contrast calculation
                        val_range = np.nanmax(heatmap_data.values) - np.nanmin(heatmap_data.values)
                        val_normalized = (val - np.nanmin(heatmap_data.values)) / val_range if val_range > 0 else 0
                        text_color = "white" if val_normalized > 0.5 else "black"
                        
                        fig_heat.add_annotation(
                            x=j,  # Use index instead of label for better positioning
                            y=i,   # Use index instead of label for better positioning
                            text=f"{val:.1f}", 
                            showarrow=False, 
                            font=dict(color=text_color, size=10, family="Arial"),
                            xref="x", 
                            yref="y"
                        )
            
            # Update layout with better styling
            fig_heat.update_layout(
                title=dict(
                    text=f"Average Energy Usage by Temperature Range and Day of Week" + 
                         (f" - {cities[0]}" if len(cities) == 1 else ""),
                    font=dict(size=16, color="black"),
                    x=0.5,  # Center the title
                    xanchor='center'
                ),
                xaxis_title="Day of Week",
                yaxis_title="Temperature Range",
                height=500,
                width=None,  # Let it be responsive
                font=dict(size=12, family="Arial"),
                plot_bgcolor='white',
                paper_bgcolor='white',
                # Improve axis styling
                xaxis=dict(
                    tickangle=45,
                    tickfont=dict(size=11),
                    title_font=dict(size=13, color="black")
                ),
                yaxis=dict(
                    tickfont=dict(size=11),
                    title_font=dict(size=13, color="black")
                ),
                margin=dict(l=80, r=80, t=80, b=80)
            )
            
            # Display the chart
            st.plotly_chart(fig_heat, use_container_width=True)
            
            # Add summary statistics with improved calculations
            with st.expander("Heatmap Summary Statistics"):
                col1, col2, col3 = st.columns(3)
                
                # Remove NaN values for statistics
                valid_data = heatmap_data.values[~np.isnan(heatmap_data.values)]
                
                if len(valid_data) > 0:
                    with col1:
                        st.write("**Highest Usage:**")
                        max_val = np.nanmax(heatmap_data.values)
                        max_pos = np.where(heatmap_data.values == max_val)
                        if len(max_pos[0]) > 0:
                            max_temp = heatmap_data.index[max_pos[0][0]]
                            max_day = heatmap_data.columns[max_pos[1][0]]
                            st.write(f"- {max_val:.1f} MWh")
                            st.write(f"- {max_temp} on {max_day}")
                    
                    with col2:
                        st.write("**Lowest Usage:**")
                        valid_nonzero = valid_data[valid_data > 0]
                        if len(valid_nonzero) > 0:
                            min_val = np.min(valid_nonzero)
                            min_pos = np.where(heatmap_data.values == min_val)
                            if len(min_pos[0]) > 0:
                                min_temp = heatmap_data.index[min_pos[0][0]]
                                min_day = heatmap_data.columns[min_pos[1][0]]
                                st.write(f"- {min_val:.1f} MWh")
                                st.write(f"- {min_temp} on {min_day}")
                    
                    with col3:
                        st.write("**Overall Statistics:**")
                        st.write(f"- Average: {np.mean(valid_data):.1f} MWh")
                        st.write(f"- Data points: {len(valid_data)}")
                        st.write(f"- Temperature ranges: {(~np.isnan(heatmap_data.values)).sum(axis=1).max()}")
                else:
                    st.write("No valid data available for statistics.")
            
            # Add insights section
            with st.expander("Pattern Insights"):
                st.write("**Key Patterns to Look For:**")
                st.write("- **Weekend vs Weekday**: Compare Saturday/Sunday to weekdays")
                st.write("- **Temperature Impact**: Higher usage in extreme temperatures (very hot/cold)")
                st.write("- **Peak Days**: Which day of the week has consistently higher usage")
                st.write("- **Seasonal Variations**: Different temperature ranges dominate in different seasons")
                
                # Calculate some basic insights if data is available
                if len(valid_data) > 0:
                    # Weekend vs weekday analysis
                    weekend_cols = ['Saturday', 'Sunday']
                    weekday_cols = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
                    
                    weekend_avg = np.nanmean(heatmap_data[weekend_cols].values)
                    weekday_avg = np.nanmean(heatmap_data[weekday_cols].values)
                    
                    if not np.isnan(weekend_avg) and not np.isnan(weekday_avg):
                        st.write(f"**Calculated Insights:**")
                        st.write(f"- Weekend average: {weekend_avg:.1f} MWh")
                        st.write(f"- Weekday average: {weekday_avg:.1f} MWh")
                        
                        if weekend_avg > weekday_avg:
                            st.write("- Higher usage on weekends (residential pattern)")
                        else:
                            st.write("- Higher usage on weekdays (commercial pattern)")

# Data quality dashboard page: shows data quality metrics and trends
# ------------------------------------------------------------------
def show_data_quality_dashboard():
    """Main function to render the enhanced data quality dashboard."""
    
    # Initialize dashboard
    # dashboard = DataQualityDashboard() # This line is removed as the class is removed
    
    # Render header
    render_header()
    
    # Load reports
    df_reports = load_quality_reports()
    
    if df_reports.empty:
        st.info("Once you run the data pipeline, quality reports will appear here.")
        return
    
    # Render sidebar controls
    controls = render_sidebar_controls(df_reports)
    filtered_df = controls['filtered_df']
    
    if filtered_df.empty:
        st.warning("No reports found for the selected date range.")
        return
    
    # Get latest report
    latest_report = filtered_df.iloc[-1]
    
    # Render main dashboard sections
    render_kpi_metrics(latest_report, filtered_df)
    render_alerts_section(latest_report)
    render_quality_trends(filtered_df)
    render_detailed_analysis(latest_report)
    render_documentation()
    
    # Footer with report info
    st.markdown("---")
    st.markdown(f"""
    **Report Information:**
    - Latest Report: {latest_report['date'].strftime('%Y-%m-%d %H:%M:%S')}
    - Total Reports: {len(filtered_df)}
    - Records Analyzed: {latest_report.get('total_records', 'N/A'):,}
    """)



# --- Sidebar Navigation ---
# Choose which dashboard page to show
st.set_page_config(page_title="Energy & Weather Dashboard", layout="wide")
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Main Dashboard", "Data Quality"])

if page == "Main Dashboard":
    show_main_dashboard()
else:
    show_data_quality_dashboard() 