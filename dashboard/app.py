import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import glob
import os
import json
from sklearn.linear_model import LinearRegression
from datetime import date, timedelta, datetime
from pytz import timezone

# City coordinates for mapping
CITY_COORDS = {
    "New York": {"lat": 40.7128, "lon": -74.0060, "tz": "America/New_York"},
    "Chicago": {"lat": 41.8781, "lon": -87.6298, "tz": "America/Chicago"},
    "Houston": {"lat": 29.7604, "lon": -95.3698, "tz": "America/Chicago"},
    "Phoenix": {"lat": 33.4484, "lon": -112.0740, "tz": "America/Phoenix"},
    "Seattle": {"lat": 47.6062, "lon": -122.3321, "tz": "America/Los_Angeles"},
}

@st.cache_data(ttl=3600)
def load_data(filepath):
    return pd.read_csv(filepath)

def show_main_dashboard():
    st.title("US Energy & Weather Data Dashboard")
    # Find latest merged data file
    def get_latest_data_file():
        files = glob.glob("data/merged_*.csv")
        if not files:
            return None
        return max(files, key=os.path.getctime)

    latest_file = get_latest_data_file()
    if latest_file:
        df = load_data(latest_file)
        df["date"] = pd.to_datetime(df["date"])
    else:
        st.error("No data file found. Please run the pipeline first.")
        st.stop()

    # Use New York timezone for 'last updated' if multiple cities, or selected city's timezone if only one
    if len(df["city"].unique()) == 1:
        city = df["city"].unique()[0]
        tz_str = CITY_COORDS.get(city, {}).get("tz", "America/New_York")
    else:
        tz_str = "America/New_York"
    local_tz = timezone(tz_str)
    now_local = datetime.now(local_tz)
    st.caption(f"Last checked: {now_local.strftime('%Y-%m-%d %H:%M %Z')}")

    # Sidebar filters
    default_start = df["date"].min().date()
    default_end = df["date"].max().date()
    date_range = st.sidebar.date_input("Date range", [default_start, default_end], min_value=default_start, max_value=default_end)
    cities = st.sidebar.multiselect("City", list(CITY_COORDS.keys()), default=list(CITY_COORDS.keys()))

    # Filter data
    mask = (
        (df["date"] >= pd.to_datetime(date_range[0])) &
        (df["date"] <= pd.to_datetime(date_range[1])) &
        (df["city"].isin(cities))
    )
    df_filt = df[mask].copy()

    st.caption(f"Last updated: {df['date'].max().date()}")

    # --- Visualization 1: Geographic Overview ---
    st.subheader("1. Geographic Overview")
    latest_day = df_filt["date"].max()
    prev_day = latest_day - pd.Timedelta(days=1)
    df_today = df_filt[df_filt["date"].dt.date == latest_day.date()]
    df_yest = df_filt[df_filt["date"].dt.date == prev_day.date()]
    map_data = []
    for city in cities:
        city_data = df_filt[df_filt["city"] == city].sort_values("date")
        # Find the most recent day with energy data
        city_data_nonan = city_data.dropna(subset=["energy_mwh"])
        if city_data_nonan.empty:
            temp = energy = energy_yest = pct_change = None
            day_used = None
        else:
            day_used = city_data_nonan["date"].max()
            row_today = city_data_nonan[city_data_nonan["date"] == day_used]
            temp = row_today["tmax_f"].dropna().values[0] if not row_today["tmax_f"].dropna().empty else None
            energy = row_today["energy_mwh"].dropna().values[0] if not row_today["energy_mwh"].dropna().empty else None
            # Find previous day with energy data
            prev_days = city_data_nonan[city_data_nonan["date"] < day_used]
            if not prev_days.empty:
                prev_day = prev_days["date"].max()
                row_yest = prev_days[prev_days["date"] == prev_day]
                energy_yest = row_yest["energy_mwh"].dropna().values[0] if not row_yest["energy_mwh"].dropna().empty else None
            else:
                energy_yest = None
            pct_change = None
            if energy is not None and energy_yest is not None and energy_yest != 0:
                pct_change = 100 * (energy - energy_yest) / energy_yest
        # Color logic
        if energy is None or energy_yest is None:
            color = "gray"
        elif energy > energy_yest:
            color = "red"
        else:
            color = "green"
        map_data.append({
            "city": city,
            "lat": CITY_COORDS[city]["lat"],
            "lon": CITY_COORDS[city]["lon"],
            "Current Temp (°F)": f"{temp:.1f}" if temp is not None else "N/A",
            "Today's Energy (MWh)": f"{energy:.1f}" if energy is not None else "N/A",
            "% Change from Yesterday": f"{pct_change:+.1f}%" if pct_change is not None else "N/A",
            "color": color,
            "Day Used": str(day_used.date()) if day_used is not None else "N/A"
        })
    map_df = pd.DataFrame(map_data)
    fig = px.scatter_mapbox(
        map_df,
        lat="lat",
        lon="lon",
        hover_name="city",
        hover_data={
            "Current Temp (°F)": True,
            "Today's Energy (MWh)": True,
            "% Change from Yesterday": True,
            "Day Used": True,
            "lat": False,
            "lon": False,
            "color": False
        },
        color="color",
        color_discrete_map={"red": "red", "green": "green", "gray": "gray"},
        size_max=20,
        zoom=3,
        height=500
    )
    fig.update_layout(
        mapbox_style="open-street-map",
        margin={"r":0,"t":40,"l":0,"b":0},
        title=f"Current Temperature, Energy Usage, and % Change from Yesterday<br><sup>Last updated: {latest_day.date()}</sup>"
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- Visualization 2: Time Series Analysis ---
    st.subheader("2. Time Series Analysis")
    city_options = ["All Cities"] + cities
    selected_city = st.selectbox("Select city for time series", city_options, index=0)
    df_ts = df_filt.copy()
    if selected_city != "All Cities":
        df_ts = df_ts[df_ts["city"] == selected_city]
    def is_weekend(dt):
        return dt.weekday() >= 5
    fig_ts = go.Figure()
    fig_ts.add_trace(go.Scatter(
        x=df_ts["date"], y=df_ts["tmax_f"], name="Max Temp (°F)", yaxis="y1", mode="lines", line=dict(color="royalblue")
    ))
    fig_ts.add_trace(go.Scatter(
        x=df_ts["date"], y=df_ts["energy_mwh"], name="Energy (MWh)", yaxis="y2", mode="lines", line=dict(color="firebrick", dash="dot")
    ))
    for d in df_ts["date"].dt.date.unique():
        if is_weekend(pd.Timestamp(d)):
            fig_ts.add_vrect(x0=d, x1=d + pd.Timedelta(days=1), fillcolor="lightgray", opacity=0.2, line_width=0)
    fig_ts.update_layout(
        yaxis=dict(title="Max Temp (°F)", side="left"),
        yaxis2=dict(title="Energy (MWh)", overlaying="y", side="right"),
        xaxis_title="Date",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400
    )
    st.plotly_chart(fig_ts, use_container_width=True)

    # --- Visualization 3: Correlation Analysis ---
    st.subheader("3. Correlation Analysis")
    corr_df = df_filt.dropna(subset=["tmax_f", "energy_mwh"])
    if not corr_df.empty:
        X = corr_df[["tmax_f"]].values
        y = corr_df["energy_mwh"].values
        reg = LinearRegression().fit(X, y)
        y_pred = reg.predict(X)
        r2 = reg.score(X, y)
        corr_coef = np.corrcoef(corr_df["tmax_f"], corr_df["energy_mwh"])[0,1]
        eqn = f"y = {reg.coef_[0]:.2f}x + {reg.intercept_:.2f}"
        fig_corr = px.scatter(
            corr_df, x="tmax_f", y="energy_mwh", color="city",
            hover_data=["date"],
            labels={"tmax_f": "Max Temp (°F)", "energy_mwh": "Energy (MWh)"},
            title=f"Temp vs Energy (R²={r2:.2f}, r={corr_coef:.2f})"
        )
        fig_corr.add_traces(go.Scatter(
            x=corr_df["tmax_f"], y=y_pred, mode="lines", name="Regression Line",
            line=dict(color="black", dash="dash")
        ))
        fig_corr.add_annotation(
            x=0.05, y=0.95, xref="paper", yref="paper",
            text=f"<b>{eqn}</b>", showarrow=False, bgcolor="white"
        )
        st.plotly_chart(fig_corr, use_container_width=True)
    else:
        st.info("Not enough data for correlation analysis.")

    # --- Visualization 4: Usage Patterns Heatmap ---
    st.subheader("4. Usage Patterns Heatmap")
    bins = [-100, 50, 60, 70, 80, 90, 1000]
    labels = ["<50°F", "50-60°F", "60-70°F", "70-80°F", "80-90°F", ">90°F"]
    df_filt["temp_bin"] = pd.cut(df_filt["tmax_f"], bins=bins, labels=labels)
    df_filt["day_of_week"] = df_filt["date"].dt.day_name()
    city_heat = st.selectbox("Filter city for heatmap", ["All Cities"] + cities, key="heatmap_city")
    if city_heat != "All Cities":
        df_heat = df_filt[df_filt["city"] == city_heat]
    else:
        df_heat = df_filt.copy()
    heatmap_data = df_heat.groupby(["temp_bin", "day_of_week"], observed=False)['energy_mwh'].mean().unstack().reindex(index=labels)
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    heatmap_data = heatmap_data.reindex(columns=days, fill_value=0)
    fig_heat = go.Figure(data=go.Heatmap(
        z=heatmap_data.values,
        x=heatmap_data.columns,
        y=heatmap_data.index,
        colorscale="RdBu_r",
        colorbar=dict(title="Avg Energy (MWh)"),
        text=np.round(heatmap_data.values, 1),
        texttemplate="%{text}",
        hovertemplate="Day: %{x}<br>Temp: %{y}<br>Avg Energy: %{z}<extra></extra>"
    ))
    for i, temp in enumerate(heatmap_data.index):
        for j, day in enumerate(heatmap_data.columns):
            val = heatmap_data.loc[temp, day]
            if not pd.isna(val):
                fig_heat.add_annotation(
                    x=day, y=temp, text=f"{val:.1f}", showarrow=False, font=dict(color="black"),
                    xref="x", yref="y"
                )
    fig_heat.update_layout(
        xaxis_title="Day of Week",
        yaxis_title="Temperature Range",
        title="Average Energy Usage by Temp Range and Day of Week",
        height=500
    )
    st.plotly_chart(fig_heat, use_container_width=True)

def show_data_quality_dashboard():
    st.title("Energy Data Quality Monitoring")
    report_files = glob.glob("reports/quality_*.json")
    reports = []
    for file in report_files:
        with open(file) as f:
            report = json.load(f)
            reports.append({
                'date': report['run_date'],
                'missing_tmax': report['missing_values']['summary'].get('tmax_f', 0),
                'missing_tmin': report['missing_values']['summary'].get('tmin_f', 0),
                'missing_energy': report['missing_values']['summary'].get('energy_mwh', 0),
                'temp_outliers': report['outliers']['temperature']['count'],
                'energy_outliers': report['outliers']['energy']['count'],
                'stale_data': report['freshness']['is_stale'],
                'days_since_update': report['freshness']['days_since_update']
            })
    if not reports:
        st.warning("No quality reports found. Run the pipeline first.")
        st.stop()
    df_reports = pd.DataFrame(reports)
    df_reports['date'] = pd.to_datetime(df_reports['date'])
    latest = df_reports.iloc[-1]
    st.subheader("Latest Report Summary")
    col1, col2, col3 = st.columns(3)
    col1.metric("Missing Values", f"{latest['missing_tmax'] + latest['missing_tmin'] + latest['missing_energy']}")
    col2.metric("Data Outliers", f"{latest['temp_outliers'] + latest['energy_outliers']}")
    col3.metric("Data Freshness", 
                "Stale" if latest['stale_data'] else "Fresh",
                f"{latest['days_since_update']} days since update")
    st.subheader("Data Quality Over Time")
    tab1, tab2, tab3 = st.tabs(["Missing Values", "Outliers", "Freshness"])
    with tab1:
        fig_missing = px.line(df_reports, x='date', y=['missing_tmax', 'missing_tmin', 'missing_energy'],
                             title="Missing Values Trend")
        st.plotly_chart(fig_missing, use_container_width=True)
    with tab2:
        fig_outliers = px.line(df_reports, x='date', y=['temp_outliers', 'energy_outliers'],
                              title="Data Outliers Trend")
        st.plotly_chart(fig_outliers, use_container_width=True)
    with tab3:
        fig_freshness = px.bar(df_reports, x='date', y='days_since_update',
                              title="Days Since Latest Data Update")
        fig_freshness.update_traces(marker_color=df_reports['days_since_update'].apply(
            lambda x: 'red' if x > 2 else 'orange' if x > 1 else 'green'))
        st.plotly_chart(fig_freshness, use_container_width=True)
    st.subheader("Quality Check Documentation")
    expander = st.expander("Understanding Data Quality Metrics")
    with expander:
        st.markdown("""
        **Missing Values Check**  
        *Why it matters*: Missing data points reduce forecasting accuracy and indicate pipeline failures.  
        *Check*: Counts nulls in temperature and energy fields.
        
        **Temperature Outliers**  
        *Why it matters*: Extreme values indicate sensor errors or data corruption.  
        *Check*: Flags temperatures >130°F, < -50°F, or when max < min.
        
        **Energy Outliers**  
        *Why it matters*: Negative consumption is physically impossible.  
        *Check*: Identifies negative energy values.
        
        **Data Freshness**  
        *Why it matters*: Stale data reduces forecast relevance.  
        *Check*: Verifies data is <48 hours old.
        
        **Data Consistency**  
        *Why it matters*: Ensures complete geographic coverage.  
        *Check*: Confirms all 5 cities are present with no duplicates.
        """)
    st.subheader("Latest Detailed Findings")
    if report_files:
        with open(report_files[-1]) as f:
            latest_report = json.load(f)
        cols = st.columns(2)
        with cols[0]:
            st.write("**Temperature Outliers**")
            if latest_report['outliers']['temperature']['records']:
                temp_df = pd.DataFrame(latest_report['outliers']['temperature']['records'])
                st.dataframe(temp_df)
            else:
                st.success("No temperature outliers found")
        with cols[1]:
            st.write("**Missing Energy Data**")
            if latest_report['details'].get('missing_energy_mwh'):
                missing_df = pd.DataFrame(latest_report['details']['missing_energy_mwh'])
                st.dataframe(missing_df[['date', 'city']])
            else:
                st.success("No missing energy data")

# --- Sidebar Navigation ---
st.set_page_config(page_title="Energy & Weather Dashboard", layout="wide")
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Main Dashboard", "Data Quality"])

if page == "Main Dashboard":
    show_main_dashboard()
else:
    show_data_quality_dashboard() 