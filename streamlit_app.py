"""
AEMO NEM Emissions Intensity Dashboard
Reads from local CSV and visualises grid emissions intensity over time.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os

# ── Page config ───────────────────────────────────────────────────
st.set_page_config(
    page_title="NEM Emissions Intensity",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Styling ───────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');
    html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
    .main { background-color: #0a0e1a; }
    .block-container { padding-top: 2rem; }
    h1, h2, h3 { font-family: 'IBM Plex Mono', monospace !important; color: #e8f4f8 !important; }
    .metric-card {
        background: linear-gradient(135deg, #111827 0%, #1a2332 100%);
        border: 1px solid #1e3a5f;
        border-radius: 8px;
        padding: 1.2rem 1.5rem;
        margin-bottom: 1rem;
    }
    .metric-label {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.7rem;
        color: #4a9eba;
        letter-spacing: 0.15em;
        text-transform: uppercase;
        margin-bottom: 0.3rem;
    }
    .metric-value {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 2rem;
        font-weight: 600;
        color: #7dd3f0;
    }
    .metric-unit { font-size: 0.85rem; color: #4a9eba; margin-left: 0.3rem; }
    .stSelectbox label, .stSlider label {
        color: #4a9eba !important;
        font-family: 'IBM Plex Mono', monospace !important;
        font-size: 0.75rem !important;
        letter-spacing: 0.1em !important;
        text-transform: uppercase !important;
    }
    footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ── Emissions factors (tCO2-e/MWh) ───────────────────────────────
# Source: Australian National Greenhouse Accounts factors
EMISSIONS_FACTORS = {
    "Black Coal":  0.91,
    "Brown Coal":  1.23,
    "Natural Gas": 0.51,
    "Diesel":      0.70,
    "Hydro":       0.0,
    "Wind":        0.0,
    "Solar":       0.0,
    "Battery":     0.0,
    "Unknown":     0.0,
}

# ── DUID to fuel type mapping ─────────────────────────────────────
# Sourced from AEMO DUDETAILSUMMARY — extend as needed
DUID_FUEL_MAP = {
    # Black Coal — NSW
    "BAYSW":   "Black Coal", "ERARING": "Black Coal",
    "MUSET1":  "Black Coal", "MUSET2":  "Black Coal",
    "MUSET3":  "Black Coal", "MUSET4":  "Black Coal",
    "VALES1":  "Black Coal", "VALES2":  "Black Coal",
    # Black Coal — QLD
    "CALLIDE": "Black Coal", "TARONG":  "Black Coal",
    "TARONGN": "Black Coal", "MILMERNG": "Black Coal",
    # Brown Coal — VIC
    "LOYYANG": "Brown Coal", "LOYYB1":  "Brown Coal",
    "LOYYB2":  "Brown Coal", "HAZELWD": "Brown Coal",
    # Gas — various
    "AGLHAL":  "Natural Gas", "AGLSOM":  "Natural Gas",
    "OAKEY1":  "Natural Gas", "OAKEY2":  "Natural Gas",
    "MORTLK1": "Natural Gas", "MORTLK2": "Natural Gas",
    # Hydro
    "TUMUT1":  "Hydro", "TUMUT2":  "Hydro", "TUMUT3":  "Hydro",
    "MURRAY1": "Hydro", "MURRAY2": "Hydro",
    "SNOWY1":  "Hydro", "SNOWY2":  "Hydro",
    # Wind
    "ARWF1":   "Wind", "CATHROCK": "Wind",
    "WPWF":    "Wind", "NBHWF1":  "Wind",
    # Solar
    "BARCALDN": "Solar", "BROKENH1": "Solar",
}


# ── Data loading ──────────────────────────────────────────────────
CSV_PATH = os.path.join(os.path.dirname(__file__), "data", "dispatch_scada.csv")

@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    if not os.path.exists(CSV_PATH):
        return pd.DataFrame()
    df = pd.read_csv(CSV_PATH, parse_dates=["SETTLEMENTDATE"])
    df["fuel_type"]       = df["DUID"].map(DUID_FUEL_MAP).fillna("Unknown")
    df["emissions_factor"] = df["fuel_type"].map(EMISSIONS_FACTORS).fillna(0)
    df["emissions_tco2e"] = df["SCADAVALUE"] * df["emissions_factor"] * (5 / 60)
    df["date"]            = df["SETTLEMENTDATE"].dt.date
    return df


def aggregate_daily(df: pd.DataFrame) -> pd.DataFrame:
    daily = df.groupby("date").agg(
        total_mwh       = ("SCADAVALUE", lambda x: (x * 5 / 60).sum()),
        total_emissions = ("emissions_tco2e", "sum")
    ).reset_index()
    daily["intensity"] = (
        daily["total_emissions"] / daily["total_mwh"].replace(0, float("nan"))
    )
    daily["date"] = pd.to_datetime(daily["date"])
    return daily.sort_values("date")


# ── Sidebar ───────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ NEM Emissions")
    st.markdown("---")

    days_back = st.slider(
        "Date range (days)",
        min_value=7, max_value=365, value=90, step=7
    )

    st.markdown("---")
    st.markdown("""
    <div style='font-family: IBM Plex Mono; font-size: 0.7rem; color: #4a9eba; line-height: 1.8;'>
    <b>DATA SOURCE</b><br>
    AEMO NEMWEB<br>
    Dispatch SCADA<br><br>
    <b>COVERAGE</b><br>
    NEM — QLD, NSW,<br>
    VIC, SA, TAS<br><br>
    <b>EXCLUDES</b><br>
    WEM (Western Australia)<br>
    I-NTEM (Northern Territory)<br>
    Rooftop solar (not in SCADA)
    </div>
    """, unsafe_allow_html=True)


# ── Main content ──────────────────────────────────────────────────
st.markdown("# NEM Grid Emissions Intensity")
st.markdown(
    "<p style='color:#4a9eba; font-family: IBM Plex Mono; font-size:0.85rem;'>"
    "National Electricity Market · tCO₂-e per MWh · Daily average"
    "</p>",
    unsafe_allow_html=True
)
st.markdown("---")

with st.spinner("Loading data..."):
    df = load_data()

if df.empty:
    st.warning("No data found. Run importdata.py to populate data/dispatch_scada.csv", icon="⚠️")
    st.stop()

# Filter to selected date range
cutoff = pd.Timestamp.now() - pd.Timedelta(days=days_back)
df     = df[df["SETTLEMENTDATE"] >= cutoff]
daily  = aggregate_daily(df)

# ── KPI metrics ───────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

latest  = daily.iloc[-1]["intensity"] if not daily.empty else 0
avg_30  = daily.tail(30)["intensity"].mean()
min_val = daily["intensity"].min()
max_val = daily["intensity"].max()

for col, label, val in [
    (col1, "Latest intensity",  latest),
    (col2, "30-day average",    avg_30),
    (col3, "Period minimum",    min_val),
    (col4, "Period maximum",    max_val),
]:
    with col:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{val:.3f}<span class="metric-unit">tCO₂-e/MWh</span></div>
        </div>""", unsafe_allow_html=True)

# ── Main chart ────────────────────────────────────────────────────
st.markdown("### Emissions Intensity Over Time")

fig = go.Figure()

fig.add_trace(go.Scatter(
    x=daily["date"],
    y=daily["intensity"],
    mode="lines",
    name="Daily intensity",
    line=dict(color="#7dd3f0", width=2),
    fill="tozeroy",
    fillcolor="rgba(125, 211, 240, 0.08)"
))

if len(daily) >= 7:
    daily["rolling_7d"] = daily["intensity"].rolling(7).mean()
    fig.add_trace(go.Scatter(
        x=daily["date"],
        y=daily["rolling_7d"],
        mode="lines",
        name="7-day average",
        line=dict(color="#f59e0b", width=2, dash="dot")
    ))

fig.update_layout(
    paper_bgcolor="#0a0e1a",
    plot_bgcolor="#0d1117",
    font=dict(family="IBM Plex Mono", color="#4a9eba", size=11),
    xaxis=dict(gridcolor="#1e3a5f", linecolor="#1e3a5f", title=None),
    yaxis=dict(gridcolor="#1e3a5f", linecolor="#1e3a5f",
               title="tCO₂-e / MWh", titlefont=dict(color="#4a9eba")),
    legend=dict(bgcolor="#111827", bordercolor="#1e3a5f", borderwidth=1,
                font=dict(color="#e8f4f8")),
    hovermode="x unified",
    margin=dict(l=0, r=0, t=10, b=0),
    height=420
)

st.plotly_chart(fig, use_container_width=True)

# ── Generation by fuel type ───────────────────────────────────────
st.markdown("### Generation Mix by Fuel Type")

fuel_daily = df.groupby(["date", "fuel_type"]).agg(
    mwh=("SCADAVALUE", lambda x: (x * 5 / 60).sum())
).reset_index()

fuel_pivot = fuel_daily.pivot(index="date", columns="fuel_type", values="mwh").fillna(0)

colours = {
    "Black Coal":  "#4a4a4a",
    "Brown Coal":  "#7a5c2e",
    "Natural Gas": "#e57c3a",
    "Diesel":      "#c0392b",
    "Hydro":       "#3498db",
    "Wind":        "#2ecc71",
    "Solar":       "#f1c40f",
    "Battery":     "#9b59b6",
    "Unknown":     "#555555",
}

fig2 = go.Figure()
for fuel in fuel_pivot.columns:
    fig2.add_trace(go.Bar(
        x=fuel_pivot.index,
        y=fuel_pivot[fuel],
        name=fuel,
        marker_color=colours.get(fuel, "#888"),
    ))

fig2.update_layout(
    barmode="stack",
    paper_bgcolor="#0a0e1a",
    plot_bgcolor="#0d1117",
    font=dict(family="IBM Plex Mono", color="#4a9eba", size=11),
    xaxis=dict(gridcolor="#1e3a5f", linecolor="#1e3a5f", title=None),
    yaxis=dict(gridcolor="#1e3a5f", linecolor="#1e3a5f",
               title="MWh", titlefont=dict(color="#4a9eba")),
    legend=dict(bgcolor="#111827", bordercolor="#1e3a5f", borderwidth=1,
                font=dict(color="#e8f4f8")),
    hovermode="x unified",
    margin=dict(l=0, r=0, t=10, b=0),
    height=380
)

st.plotly_chart(fig2, use_container_width=True)

# ── Raw data ──────────────────────────────────────────────────────
with st.expander("View raw daily data"):
    display = daily[["date", "intensity", "total_mwh", "total_emissions"]].copy()
    display.columns = ["Date", "Intensity (tCO₂-e/MWh)", "Total Generation (MWh)", "Total Emissions (tCO₂-e)"]
    st.dataframe(display.sort_values("Date", ascending=False), use_container_width=True)

# ── Footer ────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<p style='font-family: IBM Plex Mono; font-size: 0.7rem; color: #1e3a5f;'>"
    "Data: AEMO NEMWEB · Emissions factors: Australian National Greenhouse Accounts · "
    "Coverage: NEM five-region interconnected system · Excludes rooftop solar, WEM and I-NTEM"
    "</p>",
    unsafe_allow_html=True
)