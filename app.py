"""
app.py, ELT Project by Tanjim Islam
======================================================
Data flow:
  data/dispatch_scada.csv    → 5-min SCADA generation (MW) per DUID, ingested nightly
  data/duid_lookup.csv       → DUID → Technology Type, Region (AEMO Gen Info Jan 2026)
  data/emissions_factors.csv → Technology Type → t CO₂-e/MWh (NGA Factors 2025)
"""

import base64
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

# ─────────────────────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="AEMO Electricity Generation & Emissions Data Pipeline",

    layout="wide",
)

# ─────────────────────────────────────────────────────────────
# Styling
# ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap');
  :root {
    --font-family-sans: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
    --text-sm: 0.8125rem;
    --text-base: 0.9375rem;
    --text-lg: 1.5rem;
    --font-weight-normal: 400;
    --font-weight-semibold: 600;
    --leading-base: 1.5;
    --background: #fafaf8;
    --foreground: #171717;
    --card: #ffffff;
    --muted: #ececf0;
    --muted-foreground: #717182;
    --accent: #e9ebef;
    --border: rgba(0, 0, 0, 0.1);
    --input-background: #f3f3f5;
    --primary: #111827;
    --radius: 0.625rem;
  }

  /* ── Base ── */
  html { font-size: 16px; }
  .stApp { background-color: var(--background) !important; }
  html, body, [class*="css"] {
    font-family: var(--font-family-sans);
    background-color: var(--background);
    color: var(--foreground);
    font-size: var(--text-base);
    font-weight: var(--font-weight-normal);
    line-height: var(--leading-base);
  }
  h1 {
    font-family: var(--font-family-sans) !important;
    color: var(--foreground) !important;
    font-size: var(--text-lg) !important;
    font-weight: var(--font-weight-semibold) !important;
    letter-spacing: 0 !important;
    line-height: var(--leading-base) !important;
  }
  h2, h3, h4, h5, h6 {
    font-family: var(--font-family-sans) !important;
    color: var(--foreground) !important;
    font-size: var(--text-lg) !important;
    font-weight: var(--font-weight-semibold) !important;
    line-height: var(--leading-base) !important;
  }

  /* ── Container width ── */
  .block-container {
    max-width: 1200px !important;
    padding-top: 2rem !important;
    padding-left: 2.5rem !important;
    padding-right: 2.5rem !important;
  }

  /* ── Sidebar ── */
  section[data-testid="stSidebar"] {
    display: none !important;
  }
  div[data-testid="collapsedControl"] { display: none !important; }
  section[data-testid="stSidebar"] * { color: #374151 !important; }
  section[data-testid="stSidebar"] a { color: #1a3a5c !important; }
  section[data-testid="stSidebar"] label { color: #374151 !important; }
  /* Sidebar widget inputs, force light background */
  section[data-testid="stSidebar"] .stSelectbox > div > div,
  section[data-testid="stSidebar"] .stDateInput > div > div,
  section[data-testid="stSidebar"] .stMultiSelect > div > div,
  section[data-testid="stSidebar"] input[type="text"] {
    background-color: #FFFFFF !important;
    color: #1a1a2e !important;
    border: 1px solid #D1D5DB !important;
  }
  /* Multiselect tag pills */
  section[data-testid="stSidebar"] .stMultiSelect span[data-baseweb="tag"] {
    background-color: #E0E7EF !important;
    color: #1a3a5c !important;
  }
  .sidebar-note,
  .controls-note {
    font-size: var(--text-sm);
    color: var(--muted-foreground);
    line-height: var(--leading-base);
    margin-top: 0.65rem;
  }

  /* ── Metric cards ── */
  .metric-card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 16px 20px;
    text-align: center;
  }
  .metric-card.positive {
    background: #e8f3ff;
    border-color: #bfdbfe;
  }
  .metric-label {
    font-size: var(--text-sm);
    color: var(--muted-foreground);
    font-weight: var(--font-weight-normal);
  }
  .metric-value {
    font-size: var(--text-lg);
    font-weight: var(--font-weight-semibold);
    font-family: var(--font-family-sans);
    color: var(--foreground);
  }
  .metric-sub {
    font-size: var(--text-sm);
    color: var(--muted-foreground);
    font-family: var(--font-family-sans);
  }
  .comparison-card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1rem 1.1rem;
    height: 100%;
  }
  .comparison-label {
    font-size: var(--text-sm);
    color: var(--muted-foreground);
    margin-bottom: 0.4rem;
  }
  .comparison-value {
    font-size: var(--text-lg);
    font-weight: var(--font-weight-semibold);
    color: var(--foreground);
    margin-bottom: 0.35rem;
  }
  .comparison-sub {
    font-size: var(--text-base);
    color: var(--muted-foreground);
    line-height: var(--leading-base);
  }
  .comparison-panel {
    background: #f2f3f5;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1rem 1.15rem;
    height: 100%;
  }
  .comparison-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.9rem 1.2rem;
    margin-top: 0.9rem;
  }
  .comparison-item-label {
    font-size: var(--text-sm);
    color: var(--muted-foreground);
    margin-bottom: 0.25rem;
  }
  .comparison-item-value {
    font-size: 1.125rem;
    font-weight: var(--font-weight-semibold);
    color: var(--foreground);
    margin-bottom: 0.2rem;
  }
  .comparison-item-copy {
    font-size: var(--text-base);
    color: var(--muted-foreground);
    line-height: var(--leading-base);
  }

  /* ── Section text (bottom sections) ── */
  .section-text {
    line-height: var(--leading-base);
    font-size: var(--text-base);
    color: var(--foreground);
    margin: 0 auto 24px auto;
    max-width: 860px;
    font-family: var(--font-family-sans);
  }

  /* ── Intro hero ── */
  .intro-hero {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.5rem 1.6rem;
    margin-bottom: 1.5rem;
    max-width: 1000px;
    margin-left: auto;
    margin-right: auto;
  }
  .intro-hero h2 {
    font-family: var(--font-family-sans) !important;
    font-size: var(--text-lg) !important;
    color: var(--foreground) !important;
    margin-bottom: 0.9rem !important;
    line-height: var(--leading-base) !important;
    font-weight: var(--font-weight-semibold) !important;
  }
  .intro-hero p {
    font-family: var(--font-family-sans);
    font-size: var(--text-base);
    color: var(--foreground);
    line-height: var(--leading-base);
    margin: 0;
  }
  .intro-hero strong,
  .intro-hero em { color: var(--foreground); font-style: normal; font-weight: var(--font-weight-semibold); }

  /* ── ASRS cards ── */
  .asrs-card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.3rem 1.5rem;
    height: 100%;
  }
  .asrs-card .asrs-tag {
    font-family: var(--font-family-sans);
    font-size: var(--text-sm);
    color: var(--muted-foreground);
    margin-bottom: 0.4rem;
  }
  .asrs-card .asrs-group {
    font-family: var(--font-family-sans);
    font-size: var(--text-lg);
    font-weight: var(--font-weight-semibold);
    color: var(--foreground);
    margin-bottom: 0.4rem;
  }
  .asrs-card .asrs-date {
    font-family: var(--font-family-sans);
    font-size: var(--text-sm);
    color: var(--muted-foreground);
    font-weight: var(--font-weight-normal);
    margin-bottom: 0.7rem;
  }
  .asrs-card .asrs-threshold {
    font-family: var(--font-family-sans);
    font-size: var(--text-base);
    color: var(--foreground);
    line-height: var(--leading-base);
  }

  /* ── Use case grid ── */
  .usecase-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 0.75rem;
    margin-top: 0.5rem;
    max-width: 1000px;
    margin-left: auto;
    margin-right: auto;
  }
  .usecase-item {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1rem 1.1rem;
    font-family: var(--font-family-sans);
    font-size: var(--text-base);
    color: var(--foreground);
    line-height: var(--leading-base);
  }
  .usecase-item .usecase-sector {
    font-family: var(--font-family-sans);
    font-size: var(--text-sm);
    color: var(--muted-foreground);
    margin-bottom: 0.35rem;
  }

  /* ── Methodology note ── */
  .methodology-note {
    margin-top: 2rem;
    max-width: 1000px;
    margin-left: auto;
    margin-right: auto;
    font-family: var(--font-family-sans);
    font-size: var(--text-base);
    color: var(--foreground);
    line-height: var(--leading-base);
  }
  .methodology-note h2 {
    font-family: var(--font-family-sans) !important;
    font-size: var(--text-lg) !important;
    color: var(--foreground) !important;
    margin-bottom: 0.9rem !important;
    line-height: var(--leading-base) !important;
    font-weight: var(--font-weight-semibold) !important;
  }
  .methodology-note strong { color: var(--foreground); }
  .methodology-note a { color: var(--foreground); text-decoration: none; border-bottom: 1px solid var(--border); }

  /* ── Chart insight callout ── */
  .chart-insight {
    background: var(--accent);
    border-left: 3px solid var(--foreground);
    padding: 1rem 1.3rem;
    margin-top: 0.5rem;
    margin-bottom: 1.5rem;
    font-family: var(--font-family-sans);
    font-size: var(--text-base);
    color: var(--foreground);
    line-height: var(--leading-base);
    width: 100%;
    max-width: none;
    box-sizing: border-box;
  }
  .chart-insight strong { color: var(--foreground); }

  /* ── Page title deck ── */
  .page-deck {
    font-family: var(--font-family-sans);
    font-size: var(--text-base);
    color: var(--muted-foreground);
    line-height: var(--leading-base);
    max-width: none;
    margin: 0 0 0.5rem 0;
  }
  .meta-line {
    font-family: var(--font-family-sans);
    font-size: var(--text-sm);
    color: var(--muted-foreground);
    margin-bottom: 1rem;
  }
  .section-heading {
    font-family: var(--font-family-sans);
    font-size: var(--text-lg);
    color: var(--foreground);
    font-weight: var(--font-weight-semibold);
    line-height: var(--leading-base);
    margin: 0 0 0.9rem 0;
  }
  .eyebrow {
    font-family: var(--font-family-sans);
    font-size: var(--text-sm);
    color: var(--muted-foreground);
    margin: 0 0 0.75rem 0;
  }
  .page-header {
    font-family: var(--font-family-sans);
    font-size: var(--text-lg);
    font-weight: var(--font-weight-semibold);
    color: var(--foreground);
    margin: 0;
    text-align: center;
  }
  .header-band {
    background: #f6efe5;
    border-top: 1px solid #e7ddcf;
    border-bottom: 1px solid #e7ddcf;
    padding: 1.55rem 0 1.45rem 0;
    margin: 0 0 1.2rem 0;
    width: 100vw;
    margin-left: calc(-50vw + 50%);
    margin-right: calc(-50vw + 50%);
  }
  .header-band .page-deck {
    text-align: center;
    margin: 0.35rem auto 0 auto;
  }
  .hero-image-band {
    width: 100vw;
    margin-left: calc(-50vw + 50%);
    margin-right: calc(-50vw + 50%);
    margin-bottom: 1.5rem;
  }
  .hero-image-band img {
    display: block;
    width: 100%;
    height: 224px;
    object-fit: cover;
    object-position: center top;
    user-select: none;
    -webkit-user-drag: none;
    pointer-events: none;
  }
  .floating-nav {
    position: fixed;
    left: 22px;
    top: 38%;
    transform: translateY(-50%);
    z-index: 1000;
    background: rgba(255, 255, 255, 0.96);
    border: 1px solid var(--border);
    border-radius: 10px;
    box-shadow: 0 8px 24px rgba(0, 0, 0, 0.08);
    padding: 0.5rem 0.65rem;
    min-width: 145px;
  }
  .floating-nav .nav-title {
    font-family: var(--font-family-sans);
    font-size: var(--text-sm);
    color: var(--muted-foreground);
    margin: 0.25rem 0.4rem 0.45rem 0.4rem;
  }
  .floating-nav a {
    display: block;
    color: var(--foreground);
    text-decoration: none;
    font-family: var(--font-family-sans);
    font-size: var(--text-sm);
    padding: 0.35rem 0.4rem;
    border-radius: 6px;
  }
  .floating-nav a:hover { background: var(--accent); color: var(--foreground); }
  .page-footer {
    margin: 2.2rem 0 0 0;
    width: 100vw;
    margin-left: calc(-50vw + 50%);
    margin-right: calc(-50vw + 50%);
    border-top: 1px solid #d5cec2;
    background: #ece8df;
    padding: 0.9rem 2rem;
    color: var(--muted-foreground);
    font-size: var(--text-sm);
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    flex-wrap: wrap;
  }
  .linkedin-link {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    color: var(--foreground);
    text-decoration: none;
    font-weight: var(--font-weight-semibold);
  }
  .footer-inner {
    max-width: 1100px;
    margin: 0 auto;
    width: 100%;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    flex-wrap: wrap;
  }
  .linkedin-link:hover { color: #111827; }
  .linkedin-icon {
    width: 18px;
    height: 18px;
    fill: #374151;
  }
  @media (max-width: 1100px) {
    .floating-nav { display: none; }
  }

  footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

st.markdown('<div id="top"></div>', unsafe_allow_html=True)
st.markdown("""
<div class="floating-nav">
  <div class="nav-title">Sections</div>
  <a href="#top">Introduction</a>
  <a href="#dashboard">Data</a>
  <a href="#limitations">Limitations</a>
</div>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# Load & join all three tables
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_data():
    base = Path(__file__).parent

    required = {
        "SCADA data":         base / "data" / "dispatch_scada.csv",
        "DUID lookup":        base / "data" / "duid_lookup.csv",
        "Emissions factors":  base / "data" / "emissions_factors.csv",
    }
    missing = []
    empty   = []
    for label, path in required.items():
        if not path.exists():
            missing.append(f"`{path.relative_to(base)}` ({label})")
        elif path.stat().st_size == 0:
            empty.append(f"`{path.relative_to(base)}` ({label})")

    if missing or empty:
        msg = []
        if missing:
            msg.append("**Missing files:**\n" + "\n".join(f"- {f}" for f in missing))
        if empty:
            msg.append("**Empty files (GitHub Actions may not have run yet):**\n" + "\n".join(f"- {f}" for f in empty))
        msg.append(
            "\n**To fix:** ensure all three CSVs are committed to the `data/` folder in your repo "
            "and that `.gitignore` does not exclude `*.csv` or `data/`."
        )
        raise FileNotFoundError("\n\n".join(msg))

    scada = pd.read_csv(
        base / "data" / "dispatch_scada.csv",
        parse_dates=["SETTLEMENTDATE"]
    )
    scada = scada[scada["SCADAVALUE"] > 0].copy()

    lookup = (
        pd.read_csv(base / "data" / "duid_lookup.csv")
        [["DUID", "Unit Name", "Technology Type", "Region"]]
        .drop_duplicates("DUID")
    )

    ef_raw = pd.read_csv(base / "data" / "emissions_factors.csv")
    ef_s1 = (
        ef_raw[ef_raw["scope"] == "scope_1"]
        [["technology_type", "emission_factor_tCO2e_MWh"]]
        .rename(columns={"technology_type": "Technology Type",
                         "emission_factor_tCO2e_MWh": "ef_scope1"})
    )
    ef_s3 = (
        ef_raw[ef_raw["scope"] == "scope_3"]
        [["technology_type", "emission_factor_tCO2e_MWh"]]
        .rename(columns={"technology_type": "Technology Type",
                         "emission_factor_tCO2e_MWh": "ef_scope3"})
    )

    df = (
        scada
        .merge(lookup, on="DUID", how="left")
        .merge(ef_s1,  on="Technology Type", how="left")
        .merge(ef_s3,  on="Technology Type", how="left")
    )

    df["Technology Type"] = df["Technology Type"].fillna("Unknown")
    df["ef_scope1"] = df["ef_scope1"].fillna(0.1855)
    df["ef_scope3"] = df["ef_scope3"].fillna(0.0)

    df["mwh"]          = df["SCADAVALUE"] * (5 / 60)
    df["tco2e_scope1"] = df["mwh"] * df["ef_scope1"]
    df["tco2e_scope3"] = df["mwh"] * df["ef_scope3"]
    df["tco2e_total"]  = df["tco2e_scope1"] + df["tco2e_scope3"]

    return df


try:
    df = load_data()
except FileNotFoundError as e:
    st.error("### ⚠️ Data files not found")
    st.markdown(str(e))
    st.stop()

TECH_COLORS = {
    "Coal":            "#c0392b",
    "Brown coal":      "#e67e22",
    "Gas Turbine":     "#f39c12",
    "Other":           "#95a5a6",
    "Wind":            "#27ae60",
    "Solar PV":        "#f1c40f",
    "Hydro":           "#2980b9",
    "Battery Storage": "#8e44ad",
    "Unknown":         "#444444",
}
ZERO_EMISSION = {"Wind", "Solar PV", "Hydro", "Battery Storage"}

RESOLUTIONS = {
    "5 minutes":  "5min",
    "1 hour": "1h",
}

INTERVAL_MINUTES = {
    "5 minutes": 5,
    "1 hour": 60,
}


date_min = df["SETTLEMENTDATE"].dt.date.min()
date_max = df["SETTLEMENTDATE"].dt.date.max()
regions = sorted(df["Region"].dropna().unique().tolist())
default_selected_date = pd.Timestamp("2026-02-23").date()
if default_selected_date < date_min:
    default_selected_date = date_min
elif default_selected_date > date_max:
    default_selected_date = date_max

if "selected_date" not in st.session_state:
    st.session_state.selected_date = default_selected_date
if "resolution_label" not in st.session_state:
    st.session_state.resolution_label = "1 hour"
if "scope_choice" not in st.session_state:
    st.session_state.scope_choice = "Scope 1 only"
if "sel_regions" not in st.session_state:
    st.session_state.sel_regions = regions.copy()

selected_date = st.session_state.selected_date
resolution_label = st.session_state.resolution_label
resolution = RESOLUTIONS[resolution_label]
interval_minutes = INTERVAL_MINUTES[resolution_label]
scope_choice = st.session_state.scope_choice
sel_regions = st.session_state.sel_regions


# ─────────────────────────────────────────────────────────────
# Filter
# ─────────────────────────────────────────────────────────────
mask_day = (
    (df["SETTLEMENTDATE"].dt.date == selected_date) &
    (df["Region"].isin(sel_regions))
)
dff = df[mask_day].copy()

mask_all = df["Region"].isin(sel_regions)
dff_all = df[mask_all].copy()

emission_col = "tco2e_scope1" if scope_choice == "Scope 1 only" else "tco2e_total"

business_hours_mask = (
    (dff["SETTLEMENTDATE"].dt.hour >= 9) &
    (dff["SETTLEMENTDATE"].dt.hour < 17)
)
after_hours_mask = ~business_hours_mask

business_mwh = dff.loc[business_hours_mask, "mwh"].sum()
business_tco2e = dff.loc[business_hours_mask, emission_col].sum()
business_avg_intensity = business_tco2e / business_mwh if business_mwh > 0 else 0

after_hours_mwh = dff.loc[after_hours_mask, "mwh"].sum()
after_hours_tco2e = dff.loc[after_hours_mask, emission_col].sum()
after_hours_avg_intensity = after_hours_tco2e / after_hours_mwh if after_hours_mwh > 0 else 0

five_min_benchmark = (
    dff.groupby(dff["SETTLEMENTDATE"].dt.floor("5min"))
    .agg(mwh=("mwh", "sum"), tco2e=(emission_col, "sum"))
    .sort_index()
)
five_min_benchmark["intensity"] = (
    five_min_benchmark["tco2e"] / five_min_benchmark["mwh"]
).where(five_min_benchmark["mwh"] > 0)

clean_window_start = None
clean_window_end = None
clean_window_intensity = None
if len(five_min_benchmark) >= 48:
    rolling_window = five_min_benchmark[["mwh", "tco2e"]].rolling(window=48, min_periods=48).sum()
    rolling_window["intensity"] = rolling_window["tco2e"] / rolling_window["mwh"]
    if rolling_window["intensity"].notna().any():
        clean_window_end = rolling_window["intensity"].idxmin()
        clean_window_start = clean_window_end - pd.Timedelta(minutes=5 * 47)
        clean_window_intensity = rolling_window.loc[clean_window_end, "intensity"]


def masked_avg_intensity(mask):
    mask_mwh = dff.loc[mask, "mwh"].sum()
    mask_tco2e = dff.loc[mask, emission_col].sum()
    return mask_tco2e / mask_mwh if mask_mwh > 0 else 0


def to_decimal_hour(ts):
    return ts.hour + ts.minute / 60


def decimal_hour_to_label(hour_value):
    total_minutes = int(round(hour_value * 60))
    hours = total_minutes // 60
    minutes = total_minutes % 60
    return f"{hours:02d}:{minutes:02d}"


def time_window_mask(start_hour, end_hour):
    start_minutes = int(round(start_hour * 60))
    end_minutes = int(round(end_hour * 60))
    minutes_of_day = dff["SETTLEMENTDATE"].dt.hour * 60 + dff["SETTLEMENTDATE"].dt.minute
    return (minutes_of_day >= start_minutes) & (minutes_of_day < end_minutes)


clean_window_start_hour = to_decimal_hour(clean_window_start) if clean_window_start is not None else 12
clean_window_end_hour = (
    to_decimal_hour(clean_window_end) + (5 / 60) if clean_window_end is not None else 16
)

business_windows = [
    {
        "window": "Night shift",
        "display_label": "Night shift",
        "start": 0,
        "end": 6,
        "color": "#1e40af",
        "opacity": 0.76,
        "text_color": "#FFFFFF",
        "label_y_ratio": 0.54,
        "mask": time_window_mask(0, 6),
    },
    {
        "window": "Standard hours",
        "display_label": "Standard hours",
        "start": 9,
        "end": 17,
        "color": "#1d4ed8",
        "opacity": 0.58,
        "text_color": "#FFFFFF",
        "label_y_ratio": 0.52,
        "mask": time_window_mask(9, 17),
    },
    {
        "window": "Food operations",
        "display_label": "Food operations",
        "start": 6,
        "end": 15,
        "color": "#2563eb",
        "opacity": 0.72,
        "text_color": "#FFFFFF",
        "label_y_ratio": 0.88,
        "mask": time_window_mask(6, 15),
    },
    {
        "window": "Small site operations",
        "display_label": "Small site operations",
        "start": 8,
        "end": 16,
        "color": "#60a5fa",
        "opacity": 0.84,
        "text_color": "#FFFFFF",
        "label_y_ratio": 0.70,
        "mask": time_window_mask(8, 16),
    },
    {
        "window": "Cheapest 4 hours",
        "display_label": "Cheapest 4 hours",
        "start": clean_window_start_hour,
        "end": clean_window_end_hour,
        "color": "#94a3b8",
        "opacity": 1.0,
        "text_color": "#0f172a",
        "label_y_ratio": 0.45,
        "mask": (
            (dff["SETTLEMENTDATE"] >= clean_window_start) &
            (dff["SETTLEMENTDATE"] <= clean_window_end)
        ) if clean_window_start is not None and clean_window_end is not None else pd.Series(False, index=dff.index),
    },
    {
        "window": "Late hours",
        "display_label": "Late hours",
        "start": 17,
        "end": 22,
        "color": "#3b82f6",
        "opacity": 0.76,
        "text_color": "#FFFFFF",
        "label_y_ratio": 0.54,
        "mask": time_window_mask(17, 22),
    },
]

business_window_df = pd.DataFrame(business_windows)
business_window_df["duration"] = business_window_df["end"] - business_window_df["start"]
business_window_df["start_label"] = business_window_df["start"].map(decimal_hour_to_label)
business_window_df["end_label"] = business_window_df["end"].map(decimal_hour_to_label)
business_window_df["mwh"] = [
    dff.loc[mask, "mwh"].sum() for mask in business_window_df["mask"]
]
business_window_df["emissions"] = [
    dff.loc[mask, emission_col].sum() for mask in business_window_df["mask"]
]
business_window_df["avg_intensity"] = (
    business_window_df["emissions"] / business_window_df["mwh"]
).where(business_window_df["mwh"] > 0, 0)

intensity_scale = ["#dbeafe", "#93c5fd", "#60a5fa", "#3b82f6", "#2563eb", "#1e3a8a"]
window_ranks = business_window_df["avg_intensity"].rank(method="first", ascending=True).astype(int) - 1
business_window_df["intensity_color"] = window_ranks.map(lambda idx: intensity_scale[idx])
window_metrics = business_window_df.set_index("window").to_dict("index")

ILLUSTRATIVE_CARBON_RATE = 75
operation_profiles = {
    "Food manufacturing": {
        "current_window": "Food operations",
        "alternative_window": "Cheapest 4 hours",
        "flexible_load_mwh": 20,
        "description": "Illustrative batch cooking, pasteurisation or CIP cleaning load shifted away from its usual production window.",
    },
    "Small site operations": {
        "current_window": "Small site operations",
        "alternative_window": "Cheapest 4 hours",
        "flexible_load_mwh": 8,
        "description": "Illustrative EV charging, HVAC pre-cooling or pump load for a smaller commercial or light-industrial site.",
    },
    "Commercial HVAC": {
        "current_window": "Standard hours",
        "alternative_window": "Cheapest 4 hours",
        "flexible_load_mwh": 5,
        "description": "Illustrative pre-cooling or thermal storage strategy where part of the load can move earlier in the day.",
    },
    "Cold storage / warehouse": {
        "current_window": "Late hours",
        "alternative_window": "Cheapest 4 hours",
        "flexible_load_mwh": 12,
        "description": "Illustrative refrigeration defrost, charging or dispatch-prep load moved away from the evening shoulder.",
    },
}


# ─────────────────────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────────────────────
st.markdown('<div id="introduction"></div>', unsafe_allow_html=True)
st.markdown("""
<div class="header-band">
  <div class="page-header">Australia East Emission Interactive Dashboard</div>
  <div class='page-deck'>This is a personal project by Tanjim Islam, for demonstrational purposes only, and does not constitute professional advice</div>
</div>
""", unsafe_allow_html=True)

header_image_path = Path(__file__).parent / "Photography" / "gettyimages-1340827964-2048x2048.jpg"
if header_image_path.exists():
    header_image_b64 = base64.b64encode(header_image_path.read_bytes()).decode("utf-8")
    st.markdown(
        f"""
        <div class="hero-image-band">
          <img src="data:image/jpeg;base64,{header_image_b64}" alt="Header image" draggable="false" oncontextmenu="return false;">
        </div>
        """,
        unsafe_allow_html=True,
    )

_, reading_col, _ = st.columns([1, 5, 1])
with reading_col:
    st.title("NEM Scope 2 Timing Tool")
    st.markdown(
        "<div class='page-deck'>The hour you draw power is as important as how much you use.</div>",
        unsafe_allow_html=True
    )
    st.markdown(
        f"<div class='meta-line'>AEMO NEM  ·  {selected_date.strftime('%d %B %Y')}  ·  "
        f"{', '.join(sel_regions) if sel_regions else 'No region selected'}  ·  "
        f"{scope_choice}  ·  {resolution_label} intervals</div>",
        unsafe_allow_html=True
    )

    # ── Hero statement ────────────────────────────────────────────
    st.markdown("""
    <div class="intro-hero">
        <h2>Australian businesses might be paying a hidden Scope 2 premium, because they don't know <em>when</em> to use electricity.</h2>
        <p>
            Australia's grid varies by up to <strong>4&times; in emissions intensity</strong> across a single day.
            If your business draws power flexibly, the hour you choose matters as much as how much you use.<br><br>
            If your organisation is preparing for <strong>ASRS Scope 2 disclosure</strong>, the accuracy of your
            calculation depends on when you drew power from the grid, not just how much.
        </p>
    </div>
    """, unsafe_allow_html=True)

    # ── ASRS tiers ───────────────────────────────────────────────
    st.markdown(
        "<p class='eyebrow'>ASRS Reporting Thresholds, Who Must Disclose</p>",
        unsafe_allow_html=True
    )

    col_g1, col_g2, col_g3 = st.columns(3)

    with col_g1:
        st.markdown("""
        <div class="asrs-card">
            <div class="asrs-tag">In effect</div>
            <div class="asrs-group">Group 1</div>
            <div class="asrs-date">From January 2025</div>
            <div class="asrs-threshold">
                Revenue &gt; $1B<br>OR assets &gt; $500M<br>OR &gt; 500 employees
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col_g2:
        st.markdown("""
        <div class="asrs-card">
            <div class="asrs-tag">Coming soon</div>
            <div class="asrs-group">Group 2</div>
            <div class="asrs-date">From January 2026</div>
            <div class="asrs-threshold">
                Revenue &gt; $200M<br>OR assets &gt; $500M<br>OR &gt; 250 employees
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col_g3:
        st.markdown("""
        <div class="asrs-card">
            <div class="asrs-tag">On the horizon</div>
            <div class="asrs-group">Group 3</div>
            <div class="asrs-date">From January 2027</div>
            <div class="asrs-threshold">
                Smaller entities<br>Thresholds TBC<br>&nbsp;
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("""
        <div class="methodology-note">
        A regional food manufacturer with 300 staff is already in scope under Group 2.
        The Safeguard Mechanism threshold of 100,000 tCO&#8322;-e is separate, and much higher.
        Most mid-market operators are not Safeguard-covered, but all are ASRS-covered.</p>
        The expectation isn't ‘turn things off’, it's ‘time what you can, when the grid is cleanest.’</p>
    
 """, unsafe_allow_html=True)

    st.markdown("""
    <div class="usecase-grid">
        <div class="usecase-item">
            <div class="usecase-sector">Cold Chain</div>
            Schedule defrost cycles and pre-cooling loads to clean renewable windows
        </div>
        <div class="usecase-item">
            <div class="usecase-sector">Food Manufacturing</div>
            Time batch cooking, pasteurisation, and CIP cleaning runs to midday solar peaks
        </div>
        <div class="usecase-item">
            <div class="usecase-sector">Construction</div>
            Schedule EV fleet charging, concrete batching, and crane ops to low-intensity periods
        </div>
        <div class="usecase-item">
            <div class="usecase-sector">Retail</div>
            Pre-cool HVAC systems before peak dirty hours rather than reacting to heat
        </div>
        <div class="usecase-item">
            <div class="usecase-sector">Data Centres</div>
            Shift batch compute jobs and server cooling to renewable-heavy windows
        </div>
        <div class="usecase-item">
            <div class="usecase-sector">Any Flexible Load</div>
            Fixed loads are a sunk cost. Flexible loads are the opportunity. Every operation has some of both.
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Methodology note ──────────────────────────────────────────
    st.markdown("""
    <div class="methodology-note">
        <h2>Why this calculation matters for your disclosure</h2>
        Under ASRS, organisations must disclose Scope 2 using location-based or market-based methodology.
        Location-based uses the annual average grid factor, blunt and typically unfavourable.
        The more accurate approach rewards businesses that time consumption to cleaner windows.
        This tool derives grid intensity from AEMO's live 5-minute dispatch data, mapped to
        <strong>National Greenhouse Accounts (NGA) emission factors</strong> published by DCCEEW, the same factors used in official Australian carbon accounting.
    </div>
    """, unsafe_allow_html=True)

    intensity_order = [
        "Night shift",
        "Food operations",
        "Small site operations",
        "Standard hours",
        "Late hours",
        "Cheapest 4 hours",
    ]
    intensity_df = (
        business_window_df.set_index("window")
        .loc[intensity_order]
        .reset_index()
    )
    intensity_df["xpos"] = [0, 1, 2, 3, 4, 6]
    st.markdown("<div style='height:1.1rem'></div>", unsafe_allow_html=True)
    intensity_fig = go.Figure(
        go.Bar(
            x=intensity_df["xpos"],
            y=intensity_df["avg_intensity"],
            width=[0.78] * len(intensity_df),
            marker=dict(
                color=intensity_df["intensity_color"],
                line=dict(color="#FFFFFF", width=0.8),
            ),
            customdata=intensity_df[["display_label", "start_label", "end_label", "avg_intensity"]],
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Shift hours: %{customdata[1]} to %{customdata[2]}<br>"
                "Average emissions: %{customdata[3]:.3f} t CO₂-e / MWh<extra></extra>"
            ),
        )
    )
    intensity_fig.update_layout(
        title=dict(
            text="Average Emissions Intensity by Operating Window",
            font=dict(family="Inter, sans-serif", color="#171717", size=15),
            x=0,
            xanchor="left",
        ),
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        font=dict(color="#374151", family="Inter, sans-serif"),
        margin=dict(l=10, r=10, t=46, b=10),
        height=320,
        showlegend=False,
        xaxis=dict(
            showgrid=False,
            color="#6B7280",
            tickvals=intensity_df["xpos"],
            ticktext=intensity_df["display_label"],
            range=[-0.5, 6.6],
        ),
        yaxis=dict(
            color="#6B7280",
            title_text="t CO₂-e / MWh",
            showgrid=True,
            gridcolor="#F3F4F6",
            rangemode="tozero",
        ),
    )
    st.plotly_chart(intensity_fig, use_container_width=True)

    st.markdown("<h3 class='section-heading'>What this means for my operation</h3>", unsafe_allow_html=True)
    selected_operation = st.selectbox(
        "Select an operating profile",
        list(operation_profiles.keys()),
        key="operation_profile",
    )
    scenario = operation_profiles[selected_operation]
    current_metrics = window_metrics[scenario["current_window"]]
    alternative_metrics = window_metrics[scenario["alternative_window"]]
    flexible_load_mwh = scenario["flexible_load_mwh"]
    current_emissions = flexible_load_mwh * current_metrics["avg_intensity"]
    alternative_emissions = flexible_load_mwh * alternative_metrics["avg_intensity"]
    emissions_delta = current_emissions - alternative_emissions
    reduction_pct = (emissions_delta / current_emissions * 100) if current_emissions > 0 else 0
    illustrative_cost_delta = emissions_delta * ILLUSTRATIVE_CARBON_RATE

    scenario_fig = go.Figure()
    scenario_fig.add_trace(
        go.Bar(
            x=["Current timing", "Potential alternative"],
            y=[current_emissions, alternative_emissions],
            marker=dict(color=["#1e3a8a", "#93c5fd"]),
            customdata=[
                [
                    scenario["current_window"],
                    current_metrics["start_label"],
                    current_metrics["end_label"],
                    current_metrics["avg_intensity"],
                ],
                [
                    scenario["alternative_window"],
                    alternative_metrics["start_label"],
                    alternative_metrics["end_label"],
                    alternative_metrics["avg_intensity"],
                ],
            ],
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Shift hours: %{customdata[1]} to %{customdata[2]}<br>"
                "Average emissions: %{customdata[3]:.3f} t CO₂-e / MWh<br>"
                "Scenario emissions: %{y:,.1f} t CO₂-e<extra></extra>"
            ),
        )
    )
    scenario_fig.update_layout(
        title=dict(
            text="Current versus alternative timing",
            font=dict(family="Inter, sans-serif", color="#171717", size=15),
            x=0,
            xanchor="left",
        ),
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        font=dict(color="#374151", family="Inter, sans-serif"),
        margin=dict(l=10, r=10, t=46, b=10),
        height=300,
        showlegend=False,
        xaxis=dict(showgrid=False, color="#6B7280"),
        yaxis=dict(
            color="#6B7280",
            title_text="t CO₂-e for illustrative load",
            showgrid=True,
            gridcolor="#F3F4F6",
            rangemode="tozero",
        ),
    )

    scenario_left, scenario_right = st.columns([1.5, 1], gap="large")
    with scenario_left:
        st.plotly_chart(scenario_fig, use_container_width=True)
    with scenario_right:
        st.markdown(
            f"""
            <div class="comparison-panel">
                <div class="comparison-label">Illustrative operating case</div>
                <div class="comparison-value">{selected_operation}</div>
                <div class="comparison-sub">{scenario["description"]}</div>
                <div class="comparison-grid">
                    <div>
                        <div class="comparison-item-label">Current window</div>
                        <div class="comparison-item-value">{scenario["current_window"]}</div>
                        <div class="comparison-item-copy">{current_metrics["start_label"]} to {current_metrics["end_label"]}<br>{current_metrics["avg_intensity"]:.3f} t CO&#8322;-e / MWh</div>
                    </div>
                    <div>
                        <div class="comparison-item-label">Potential reduction</div>
                        <div class="comparison-item-value">{reduction_pct:.0f}%</div>
                        <div class="comparison-item-copy">{emissions_delta:,.1f} t CO&#8322;-e avoided for a {flexible_load_mwh:.0f} MWh flexible load</div>
                    </div>
                    <div style="grid-column:1 / -1;">
                        <div class="comparison-item-label">Illustrative carbon-cost equivalent</div>
                        <div class="comparison-item-value">A${illustrative_cost_delta:,.0f}</div>
                        <div class="comparison-item-copy">Uses A${ILLUSTRATIVE_CARBON_RATE}/t CO&#8322;-e as a simple reference point. This is not an electricity tariff estimate.</div>
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    st.markdown("<div style='height:0.8rem'></div>", unsafe_allow_html=True)
    st.markdown(
        """
        <div class="comparison-panel">
            <div class="comparison-label">Operational caveat</div>
            <div class="comparison-sub">
                The cleanest window is not automatically the best business choice. Staffing, delivery cut-offs, product quality, thermal inertia and customer demand can outweigh emissions benefits in some operations. This app models emissions timing, not your contracted tariff, so a decision that reduces reported Scope 2 can still be operationally or financially wrong for a specific site.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# KPI cards
# ─────────────────────────────────────────────────────────────
total_mwh     = dff["mwh"].sum()
total_tco2e   = dff[emission_col].sum()
avg_intensity = total_tco2e / total_mwh if total_mwh > 0 else 0
renewable_mwh = dff[dff["Technology Type"].isin(ZERO_EMISSION)]["mwh"].sum()
re_share      = 100 * renewable_mwh / total_mwh if total_mwh > 0 else 0

interval_agg = (
    dff.groupby(dff["SETTLEMENTDATE"].dt.floor(resolution))
    .agg(mwh=("mwh", "sum"), tco2e=(emission_col, "sum"))
    .assign(intensity=lambda x: x["tco2e"] / x["mwh"])
)
period_low  = interval_agg["intensity"].min() if not interval_agg.empty else 0
period_high = interval_agg["intensity"].max() if not interval_agg.empty else 0

def kpi(col, label, value, sub="", positive=False):
    card_class = "metric-card positive" if positive else "metric-card"
    col.markdown(
        f'<div class="{card_class}">'
        f'<div class="metric-label">{label}</div>'
        f'<div class="metric-value">{value}</div>'
        f'<div class="metric-sub">{sub}</div>'
        f'</div>',
        unsafe_allow_html=True
    )

# ─────────────────────────────────────────────────────────────
# Aggregate to selected interval
# ─────────────────────────────────────────────────────────────
dff["period"] = dff["SETTLEMENTDATE"].dt.floor(resolution)

agg = (
    dff.groupby("period")
    .agg(mwh=("mwh", "sum"), tco2e=(emission_col, "sum"))
    .reset_index()
)
agg["intensity"] = (agg["tco2e"] / agg["mwh"]).where(agg["mwh"] > 0)

mix = (
    dff.groupby(["period", "Technology Type"])
    .agg(mwh=("mwh", "sum"))
    .reset_index()
)

tech_order = [t for t in TECH_COLORS if t in mix["Technology Type"].unique()]

# ── Narrative chart title (dynamic, insight-led) ──────────────
if not interval_agg.empty and period_low > 0:
    ratio = period_high / period_low
    best_t  = interval_agg["intensity"].idxmin()
    worst_t = interval_agg["intensity"].idxmax()
    best_hr  = best_t.strftime("%H:%M")
    worst_hr = worst_t.strftime("%H:%M")
    chart_title = (
        f"Grid ran {ratio:.1f}× cleaner at {best_hr} than at {worst_hr} today"
        f", coal still sets the overnight floor"
    )
else:
    chart_title = "Generation Mix & Absolute Emissions"


# ─────────────────────────────────────────────────────────────
# Combo chart, stacked bars (MWh) + absolute emissions line
# ─────────────────────────────────────────────────────────────
fig = make_subplots(specs=[[{"secondary_y": True}]])

for tech in tech_order:
    subset = mix[mix["Technology Type"] == tech]
    fig.add_trace(
        go.Bar(
            x=subset["period"],
            y=subset["mwh"],
            name=tech,
            marker=dict(
                color=TECH_COLORS.get(tech, "#555"),
                line=dict(color="#FAFAF8", width=0.35),
            ),
            hovertemplate=f"{tech}<br>%{{x|%H:%M}}<br><b>%{{y:,.0f}}</b> MWh<extra></extra>",
        ),
        secondary_y=False,
    )

fig.add_trace(
    go.Scatter(
        x=agg["period"],
        y=agg["tco2e"],
        name="Emissions (t CO\u2082-e)",
        mode="lines",
        line=dict(color="#000000", width=3),
        hovertemplate="%{x|%H:%M}<br><b>%{y:,.0f}</b> t CO\u2082-e<extra></extra>",
    ),
    secondary_y=True,
)

fig.update_layout(
    barmode="stack",
    bargap=0,
    bargroupgap=0,
    title=dict(
        text=chart_title,
        font=dict(family="Inter, sans-serif", color="#171717", size=15),
        x=0,
        xanchor="left",
    ),
    xaxis=dict(
        showgrid=False,
        color="#6B7280",
        tickformat="%H:%M",
        dtick=3600000 * 2,
    ),
    plot_bgcolor="#FFFFFF",
    paper_bgcolor="#FFFFFF",
    font=dict(color="#374151", family="Inter, sans-serif"),
    legend=dict(
        bgcolor="#F9FAFB",
        bordercolor="#E5E7EB",
        orientation="h",
        yanchor="top",
        y=-0.14,
        xanchor="center",
        x=0.5,
    ),
    margin=dict(l=10, r=10, t=92, b=86),
    hovermode="x unified",
    height=520,
)

fig.update_yaxes(
    title_text="MWh per interval",
    showgrid=True, gridcolor="#F3F4F6",
    zeroline=False, color="#6B7280",
    secondary_y=False,
)
fig.update_yaxes(
    title_text="t CO\u2082-e per interval",
    showgrid=False,
    zeroline=False,
    range=[0, int(((1600 * interval_minutes / 15) + 99) // 100) * 100],
    color="#9CA3AF",
    secondary_y=True,
)

st.markdown('<div id="dashboard"></div>', unsafe_allow_html=True)
_, chart_col, _ = st.columns([0.2, 9.6, 0.2])
with chart_col:
    c1, c2, c3, c4 = st.columns([1.15, 1.1, 1.35, 2.4], gap="medium")
    with c1:
        st.date_input(
            "Date",
            min_value=date_min,
            max_value=date_max,
            key="selected_date",
            format="DD/MM/YYYY",
        )
    with c2:
        st.selectbox("Interval", list(RESOLUTIONS.keys()), key="resolution_label")
    with c3:
        st.radio(
            "Emissions scope",
            ["Scope 1 only", "Scope 1 + 3 (combined)"],
            key="scope_choice",
            help="Scope 1 = direct combustion. Scope 3 = upstream fuel extraction (coal only in NGA 2025).",
            horizontal=True,
        )
    with c4:
        st.multiselect("Regions", regions, key="sel_regions")

    business_notice = (
        f"Business hours average intensity is **{business_avg_intensity:.3f} t CO₂-e / MWh** "
        f"versus **{after_hours_avg_intensity:.3f}** after hours."
    )
    st.info(business_notice)

    if scope_choice == "Scope 1 + 3 (combined)":
        s1 = dff["tco2e_scope1"].sum()
        s3 = dff["tco2e_scope3"].sum()
        if s1 > 0:
            st.info(
                f"Scope 3 adds **{100 * s3 / s1:.1f}%** on top of Scope 1 for {selected_date.strftime('%d %B %Y')} "
                f"({s3:,.0f} t upstream vs {s1:,.0f} t direct). "
                "NGA 2025 specifies Scope 3 factors for coal fuels only."
            )

    st.plotly_chart(fig, use_container_width=True)

    k1, k2, k3, k4, k5 = st.columns(5)
    kpi(k1, "Avg Intensity",       f"{avg_intensity:.3f}",   "t CO&#8322;-e / MWh")
    kpi(k2, "Daily Low",           f"{period_low:.3f}",      "t CO&#8322;-e / MWh")
    kpi(k3, "Daily High",          f"{period_high:.3f}",     "t CO&#8322;-e / MWh")
    kpi(k4, "Total Generation",    f"{total_mwh/1e3:.1f}k",  "MWh")
    kpi(k5, "Zero-Emission Share", f"{re_share:.1f}%",       "of total generation", positive=True)
    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("""
       <div class="controls-note">
       <b>Data sources</b>:
       Generation: <a href="https://nemweb.com.au" style="color:#1a3a5c">AEMO NEMWEB</a>, Dispatch SCADA.
       Unit metadata: AEMO Generation Information (Jan 2026).
       Emission factors: <a href="https://www.dcceew.gov.au/climate-change/publications/national-greenhouse-accounts-factors" style="color:#1a3a5c">NGA Factors 2025</a>, Tables 4 &amp; 5.<br>
       <b>Coverage</b>: NEM regions only (QLD, NSW, VIC, SA, TAS), excludes WEM, NT grids, rooftop solar.
       </div>
       """, unsafe_allow_html=True)

    st.markdown(
        f"<div class='chart-insight'>"
        f"Businesses with flexible load operating during today's cleanest 4-hour window could avoid an estimated "
        f"<strong>~30% of the Scope 2 emissions</strong> they would have incurred running the same load overnight "
        f", rising above 50% on high-renewable days. "
        f"Average derived from NEM dispatch data; low estimate ~20% (winter, low solar), high estimate ~50–55% (peak summer renewable days)."
        f"</div>",
        unsafe_allow_html=True
    )


# ─────────────────────────────────────────────────────────────
# All-data summary table + donut
# ─────────────────────────────────────────────────────────────
st.markdown("<h3 class='section-heading'>All-Data Summary by Technology</h3>", unsafe_allow_html=True)

tech_summary = (
    dff_all.groupby("Technology Type")
    .agg(total_mwh=("mwh", "sum"), total_tco2e=(emission_col, "sum"))
    .reset_index()
    .sort_values("total_mwh", ascending=False)
)
tech_summary["share_pct"]  = 100 * tech_summary["total_mwh"] / tech_summary["total_mwh"].sum()
tech_summary["avg_factor"] = tech_summary["total_tco2e"] / tech_summary["total_mwh"]

left, right = st.columns([2, 1])

with left:
    disp = tech_summary.copy()
    disp["Total MWh"]     = disp["total_mwh"].map("{:,.0f}".format)
    disp["Total t CO\u2082-e"] = disp["total_tco2e"].map("{:,.1f}".format)
    disp["Gen Share"]     = disp["share_pct"].map("{:.1f}%".format)
    disp["Avg Intensity"] = disp["avg_factor"].apply(
        lambda x: f"{x:.4f} t/MWh" if pd.notna(x) and x > 0 else "0 (zero-emission)"
    )
    st.dataframe(
        disp[["Technology Type", "Total MWh", "Total t CO\u2082-e", "Gen Share", "Avg Intensity"]],
        use_container_width=True, hide_index=True
    )

with right:
    fig_donut = go.Figure(go.Pie(
        labels=tech_summary["Technology Type"],
        values=tech_summary["total_mwh"],
        hole=0.55,
        marker=dict(colors=[TECH_COLORS.get(t, "#555") for t in tech_summary["Technology Type"]]),
        textinfo="percent",
        textfont=dict(size=11),
        hovertemplate="%{label}<br><b>%{value:,.0f}</b> MWh (%{percent})<extra></extra>",
    ))
    fig_donut.update_layout(
        showlegend=False,
        plot_bgcolor="#FFFFFF", paper_bgcolor="#FFFFFF",
        margin=dict(l=0, r=0, t=0, b=0),
        font=dict(color="#374151"),
        annotations=[dict(
            text="Gen Mix", x=0.5, y=0.5,
            font_size=13, font_color="#9CA3AF", showarrow=False
        )]
    )
    st.plotly_chart(fig_donut, use_container_width=True)


# ─────────────────────────────────────────────────────────────
# Raw data expanders
# ─────────────────────────────────────────────────────────────
with st.expander("📄 Raw interval data"):
    st.dataframe(
        agg.rename(columns={
            "period":    f"Period ({resolution_label})",
            "mwh":       "MWh",
            "tco2e":     "t CO\u2082-e",
            "intensity": "Intensity (t CO\u2082-e/MWh)",
        }).sort_values(f"Period ({resolution_label})", ascending=False),
        use_container_width=True, hide_index=True
    )

with st.expander("🔍 Emissions factors reference (NGA 2025)"):
    ef_display = pd.read_csv(Path(__file__).parent / "data" / "emissions_factors.csv")
    st.dataframe(ef_display, use_container_width=True, hide_index=True)
    st.caption(
        "Source: National Greenhouse Accounts Factors 2025, DCCEEW. "
        "Table 4 (solid fuels Scope 1 & 3), Table 5 (gaseous fuels Scope 1). "
        "Converted: kg CO\u2082-e/GJ \u00d7 3.6 GJ/MWh \u00f7 1000 = t CO\u2082-e/MWh."
    )

st.markdown("---")

# ─────────────────────────────────────────────────────────────
# Limitations and Scope
# ─────────────────────────────────────────────────────────────
st.markdown('<div id="limitations"></div>', unsafe_allow_html=True)
_, bottom_text_col, _ = st.columns([1.4, 4.2, 1.4])
with bottom_text_col:
    st.markdown("""
    <div class="section-text">
    <h3 class="section-heading">Limitations and Scope</h3>
    The project covers 5 NEM regions in the AEMO dispatch framework (QLD, NSW, SA, VIC and TAS).<br><br>
    WA has a separate grid, Wholesale Electricity Market (WEM) which supplies separate data.
    Gas and coal are the primary fuel types.<br><br>
    NT has three grids for Darwin-Katherine, Tennant Creek and Alice Springs, with some data supplied by
    Interim Northern Territory Electricity Market (I-NTEM). Gas is primary fuel type.<br><br>
    A 'national emissions data pipeline' incorporating WEM and NT data sources is outside the current scope
    of this project and represents a natural extension for future development.
    </div>
    """, unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# Data
# ─────────────────────────────────────────────────────────────
st.markdown('<div id="data"></div>', unsafe_allow_html=True)
with bottom_text_col:
    st.markdown("""
    <div class="section-text">
    <h3 class="section-heading">Data</h3>
    <b>Ingestion &amp; orchestration</b>: GitHub Actions orchestrates a daily job that fetches AEMO zip files via <code>urllib</code>, appends to a historical dataset, and writes curated CSV outputs.<br><br>
    <b>Transform &amp; model</b>: Python code join dispatch_scada.csv, duid_lookup.csv, and emissions_factors.csv, joins SCADA and DUID, and Emissions on Technology Type. It converts dispatch MW into interval MWh with: mwh = SCADAVALUE * (5 / 60)<br><br>
    <b>Data Validation and Quality</b>: Power BI was used for manual validation.
    </div>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# References
# ─────────────────────────────────────────────────────────────
with bottom_text_col:
    st.markdown("""
    <div class="section-text">
    <h3 class="section-heading">References</h3>
    <b>Data Sources</b><br>
    &bull; AEMO Dispatch SCADA, 5-minute generator output:
      <a href="https://nemweb.com.au/Reports/Current/Dispatch_SCADA/" style="color:#1a3a5c">nemweb.com.au</a><br>
    &bull; AEMO Generation Information (Jan 2026), DUID fuel type metadata:
      <a href="https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-forecasting-and-planning/forecasting-and-planning-data/generation-information" style="color:#1a3a5c">aemo.com.au</a><br>
    &bull; National Greenhouse Accounts Factors 2025, emission factors by fuel type:
      <a href="https://www.dcceew.gov.au/climate-change/publications/national-greenhouse-accounts-factors" style="color:#1a3a5c">dcceew.gov.au</a><br><br>
    <b>Regulatory Context</b><br>
    &bull; Australian Sustainability Reporting Standards (ASRS), mandatory climate disclosure framework:
      <a href="https://www.aasb.gov.au/australian-sustainability-reporting-standards/" style="color:#1a3a5c">aasb.gov.au</a><br>
    &bull; AEMO Carbon Dioxide Equivalent Intensity Index (CDEII), AEMO's own daily regional emissions intensity procedure:
      <a href="https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/market-operations/settlements-and-payments/settlements/carbon-dioxide-equivalent-intensity-index" style="color:#1a3a5c">aemo.com.au</a><br>
    &bull; National Greenhouse and Energy Reporting (NGER) Act 2007, legislative basis for Australian emissions reporting<br><br>
    While reviewing this project, the UNSW NEMED tool may have come to mind.<br>
    NEMED is a Python library that gives researchers a package to pull emissions data programmatically;
    it's designed for a different audience and purpose. It was not used for this project as data requirements
    and processing are different.<br><br>
    This project is framed around ESG reporting obligations and demonstrates an end-to-end data engineering pipeline.
    </div>
    """, unsafe_allow_html=True)

st.markdown("""
<div class="page-footer">
  <div class="footer-inner">
    <div>
      This is a personal project by Tanjim Islam, for demonstrational purposes only, and does not constitute professional advice.
    </div>
    <a class="linkedin-link" href="https://www.linkedin.com/in/tanjimislam/" target="_blank" rel="noopener noreferrer">
      <svg class="linkedin-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path d="M19 3A2 2 0 0 1 21 5V19A2 2 0 0 1 19 21H5A2 2 0 0 1 3 19V5A2 2 0 0 1 5 3H19ZM8.34 18V9.66H5.66V18H8.34ZM7 8.54C7.86 8.54 8.54 7.85 8.54 7S7.86 5.46 7 5.46 5.46 6.14 5.46 7 6.14 8.54 7 8.54ZM18.54 18V13.43C18.54 10.98 17.23 9.43 14.9 9.43 13.78 9.43 12.96 10.05 12.66 10.63V9.66H10V18H12.68V13.88C12.68 12.79 12.88 11.73 14.22 11.73 15.54 11.73 15.56 12.98 15.56 13.95V18H18.54Z"/>
      </svg>
      Contact on LinkedIn
    </a>
  </div>
</div>
""", unsafe_allow_html=True)
