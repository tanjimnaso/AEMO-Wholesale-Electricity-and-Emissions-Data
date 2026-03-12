"""
app.py, Australia East Emissions Intensity Dashboard
======================================================
Data flow:
  data/dispatch_scada.csv    → 5-min SCADA generation (MW) per DUID, ingested nightly
  data/duid_lookup.csv       → DUID → Technology Type, Region (AEMO Gen Info Jan 2026)
  data/emissions_factors.csv → Technology Type → t CO₂-e/MWh (NGA Factors 2025)
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

# ─────────────────────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Australia East Emissions Intensity Dashboard",

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
    "15 minutes": "15min",
    "30 minutes": "30min",
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
    st.session_state.resolution_label = "15 minutes"
if "scope_choice" not in st.session_state:
    st.session_state.scope_choice = "Scope 1 only"
if "sel_regions" not in st.session_state:
    st.session_state.sel_regions = regions.copy()

selected_date = st.session_state.selected_date
resolution_label = st.session_state.resolution_label
resolution = RESOLUTIONS[resolution_label]
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


# ─────────────────────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────────────────────
st.markdown('<div id="introduction"></div>', unsafe_allow_html=True)
st.markdown("""
<div class="header-band">
  <div class="page-header">Australia East Emission Interactive Dashboard</div>
  <div class='page-deck'>This is not professional advice</div>
</div>
""", unsafe_allow_html=True)

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
        A regional food manufacturer with 300 staff is already in scope under Group 2.<br>
        The Safeguard Mechanism threshold of 100,000 tCO&#8322;-e is separate, and much higher.<br>
        Most mid-market operators are not Safeguard-covered, but all are ASRS-covered.</p>
 """, unsafe_allow_html=True)

    # ── Flexible load use cases ───────────────────────────────────
    st.markdown("""
    <div class="methodology-note">
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

def kpi(col, label, value, sub=""):
    col.markdown(
        f'<div class="metric-card">'
        f'<div class="metric-label">{label}</div>'
        f'<div class="metric-value">{value}</div>'
        f'<div class="metric-sub">{sub}</div>'
        f'</div>',
        unsafe_allow_html=True
    )

k1, k2, k3, k4, k5 = st.columns(5)
kpi(k1, "Avg Intensity",       f"{avg_intensity:.3f}",   "t CO&#8322;-e / MWh")
kpi(k2, "Daily Low",           f"{period_low:.3f}",      "t CO&#8322;-e / MWh")
kpi(k3, "Daily High",          f"{period_high:.3f}",     "t CO&#8322;-e / MWh")
kpi(k4, "Total Generation",    f"{total_mwh/1e3:.1f}k",  "MWh")
kpi(k5, "Zero-Emission Share", f"{re_share:.1f}%",        "of total generation")
st.markdown("<br>", unsafe_allow_html=True)


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
            marker_color=TECH_COLORS.get(tech, "#555"),
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
        line=dict(color="#9ca3af", width=3),
        hovertemplate="%{x|%H:%M}<br><b>%{y:,.0f}</b> t CO\u2082-e<extra></extra>",
    ),
    secondary_y=True,
)

fig.update_layout(
    barmode="stack",
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
    zeroline=False, color="#9CA3AF",
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

    st.plotly_chart(fig, use_container_width=True)

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


if scope_choice == "Scope 1 + 3 (combined)":
    s1 = dff["tco2e_scope1"].sum()
    s3 = dff["tco2e_scope3"].sum()
    if s1 > 0:
        st.info(
            f"Scope 3 adds **{100 * s3 / s1:.1f}%** on top of Scope 1 for {selected_date.strftime('%d %B %Y')} "
            f"({s3:,.0f} t upstream vs {s1:,.0f} t direct). "
            "NGA 2025 specifies Scope 3 factors for coal fuels only."
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
