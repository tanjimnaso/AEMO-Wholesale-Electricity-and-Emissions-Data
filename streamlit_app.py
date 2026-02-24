"""
app.py — Australia East Emissions Intensity Dashboard
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
    page_icon="⚡",
    layout="wide",
)

# ─────────────────────────────────────────────────────────────
# Styling
# ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');
  html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
    background-color: #0e1117;
    color: #c9d1d9;
  }
  h1, h2, h3 { font-family: 'IBM Plex Mono', monospace; color: #58a6ff; }
  .metric-card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 16px 20px;
    text-align: center;
  }
  .metric-label { font-size: 0.75rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.08em; }
  .metric-value { font-size: 2rem; font-weight: 600; font-family: 'IBM Plex Mono', monospace; color: #58a6ff; }
  .metric-sub   { font-size: 0.75rem; color: #8b949e; }
  .section-text {
    line-height: 1.8;
    font-size: 0.92rem;
    color: #c9d1d9;
    margin-bottom: 24px;
  }
  .sidebar-note { font-size: 0.75rem; color: #8b949e; line-height: 1.6; }

  /* ── Intro section styles ── */
  .intro-hero {
    background: #161b22;
    border: 1px solid #30363d;
    border-left: 4px solid #58a6ff;
    border-radius: 8px;
    padding: 1.8rem 2rem;
    margin-bottom: 1.5rem;
  }
  .intro-hero h2 {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 1.55rem !important;
    color: #e6edf3 !important;
    margin-bottom: 0.9rem !important;
    letter-spacing: 0.01em;
    line-height: 1.35 !important;
  }
  .intro-hero p {
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 1.05rem;
    color: #c9d1d9;
    line-height: 1.75;
    margin: 0;
  }
  .intro-hero strong { color: #58a6ff; }
  .intro-hero em { color: #f59e0b; font-style: normal; font-weight: 600; }

  .asrs-card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 1.2rem 1.4rem;
    height: 100%;
  }
  .asrs-card .asrs-tag {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.72rem;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    color: #8b949e;
    margin-bottom: 0.4rem;
  }
  .asrs-card .asrs-group {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.2rem;
    font-weight: 600;
    color: #58a6ff;
    margin-bottom: 0.5rem;
  }
  .asrs-card .asrs-date {
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 0.88rem;
    color: #f59e0b;
    margin-bottom: 0.6rem;
  }
  .asrs-card .asrs-threshold {
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 0.95rem;
    color: #c9d1d9;
    line-height: 1.7;
  }

  .usecase-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 0.75rem;
    margin-top: 0.5rem;
  }
  .usecase-item {
    background: #0e1117;
    border: 1px solid #30363d;
    border-radius: 6px;
    padding: 0.9rem 1rem;
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 0.95rem;
    color: #c9d1d9;
    line-height: 1.6;
  }
  .usecase-item .usecase-sector {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.75rem;
    color: #8b949e;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 0.35rem;
  }

  .methodology-note {
    margin-top: 2rem;
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 1.05rem;
    color: #c9d1d9;
    line-height: 1.75;
  }
  .methodology-note h2 {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 1.55rem !important;
    color: #e6edf3 !important;
    margin-bottom: 0.9rem !important;
    letter-spacing: 0.01em;
    line-height: 1.35 !important;
  }
  .methodology-note strong { color: #58a6ff; }
  .methodology-note a { color: #58a6ff; text-decoration: none; border-bottom: 1px solid #30363d; }
</style>
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


# ─────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────
st.sidebar.title("Controls")

date_min = df["SETTLEMENTDATE"].dt.date.min()
date_max = df["SETTLEMENTDATE"].dt.date.max()

selected_date = st.sidebar.date_input(
    "Date", value=date_max, min_value=date_min, max_value=date_max
)

resolution_label = st.sidebar.selectbox(
    "Interval",
    list(RESOLUTIONS.keys()),
    index=1,
)
resolution = RESOLUTIONS[resolution_label]

scope_choice = st.sidebar.radio(
    "Emissions scope",
    ["Scope 1 only", "Scope 1 + 3 (combined)"],
    help="Scope 1 = direct combustion. Scope 3 = upstream fuel extraction (coal only in NGA 2025)."
)

regions = sorted(df["Region"].dropna().unique().tolist())
sel_regions = st.sidebar.multiselect("Regions", regions, default=regions)

st.sidebar.markdown("---")
st.sidebar.markdown("""
<div class="sidebar-note">
<b>Data sources</b><br>
Generation: <a href="https://nemweb.com.au" style="color:#58a6ff">AEMO NEMWEB</a> — Dispatch SCADA<br>
Unit metadata: AEMO Generation Information (Jan 2026)<br>
Emission factors: <a href="https://www.dcceew.gov.au/climate-change/publications/national-greenhouse-accounts-factors" style="color:#58a6ff">NGA Factors 2025</a>, Tables 4 &amp; 5<br><br>
<b>Coverage</b><br>
NEM regions only: QLD, NSW, VIC, SA, TAS.<br>
Excludes WEM, NT grids, rooftop solar.
</div>
""", unsafe_allow_html=True)


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
st.title("Australia East Emissions Intensity Dashboard")
st.caption(
    f"AEMO NEM  ·  {selected_date.strftime('%d %B %Y')}  ·  "
    f"{', '.join(sel_regions) if sel_regions else 'No region selected'}  ·  "
    f"{scope_choice}  ·  {resolution_label} intervals"
)

# ── Hero statement ────────────────────────────────────────────
st.markdown("""
<div class="intro-hero">
    <h2>Australian businesses might be paying a hidden Scope 2 premium — because they don't know <em>when</em> to use electricity.</h2>
    <p>
        Australia's grid varies by up to <strong>4&times; in emissions intensity</strong> across a single day.
        If your business draws power flexibly, the hour you choose matters as much as how much you use.<br><br>
        If your organisation is preparing for <strong>ASRS Scope 2 disclosure</strong>, the accuracy of your
        calculation depends on when you drew power from the grid — not just how much.
    </p>
</div>
""", unsafe_allow_html=True)

# ── ASRS tiers ───────────────────────────────────────────────
st.markdown(
    "<p style='font-family: IBM Plex Mono; font-size: 0.72rem; color: #8b949e; "
    "letter-spacing: 0.15em; text-transform: uppercase; margin-bottom: 0.75rem;'>"
    "ASRS Reporting Thresholds — Who Must Disclose</p>",
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

st.markdown(
    "<p style='font-family: IBM Plex Sans; font-size: 0.88rem; color: #8b949e; "
    "margin-top: 0.6rem; margin-bottom: 1.2rem;'>"
    "A regional food manufacturer with 300 staff is already in scope under Group 2. "
    "The Safeguard Mechanism threshold of 100,000 tCO&#8322;-e is separate — and much higher. "
    "Most mid-market operators are not Safeguard-covered, but all are ASRS-covered.</p>",
    unsafe_allow_html=True
)

# ── Flexible load use cases ───────────────────────────────────
st.markdown(
    "<p style='font-family: IBM Plex Mono; font-size: 0.72rem; color: #8b949e; "
    "letter-spacing: 0.15em; text-transform: uppercase; margin-bottom: 0.5rem;'>"
    "The expectation isn't 'turn things off' &#8212; it's 'time what you can, when the grid is cleanest'</p>",
    unsafe_allow_html=True
)

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
    Location-based uses the annual average grid factor — blunt and typically unfavourable.
    The more accurate approach rewards businesses that time consumption to cleaner windows.
    This tool derives grid intensity from AEMO's live 5-minute dispatch data, mapped to
    <strong>National Greenhouse Accounts (NGA) emission factors</strong> published by DCCEEW —
    the same factors used in official Australian carbon accounting.
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


# ─────────────────────────────────────────────────────────────
# Combo chart — stacked bars (MWh) + absolute emissions line
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
        text=(
            f"Generation Mix & Absolute Emissions — "
            f"{selected_date.strftime('%d %B %Y')}"
            f" ({resolution_label} intervals)"
        ),
        font=dict(family="IBM Plex Mono", color="#58a6ff", size=13),
    ),
    xaxis=dict(
        showgrid=False,
        color="#8b949e",
        tickformat="%H:%M",
        dtick=3600000 * 2,
    ),
    plot_bgcolor="#0e1117",
    paper_bgcolor="#0e1117",
    font=dict(color="#c9d1d9", family="IBM Plex Sans"),
    legend=dict(
        bgcolor="#161b22",
        bordercolor="#30363d",
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="left",
        x=0,
    ),
    margin=dict(l=10, r=10, t=80, b=10),
    hovermode="x unified",
    height=520,
)

fig.update_yaxes(
    title_text="MWh per interval",
    showgrid=True, gridcolor="#21262d",
    zeroline=False, color="#8b949e",
    secondary_y=False,
)
fig.update_yaxes(
    title_text="t CO\u2082-e per interval",
    showgrid=False,
    zeroline=False, color="#9ca3af",
    secondary_y=True,
)

st.plotly_chart(fig, use_container_width=True)

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
st.markdown("### All-Data Summary by Technology")

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
        plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
        margin=dict(l=0, r=0, t=0, b=0),
        font=dict(color="#c9d1d9"),
        annotations=[dict(
            text="Gen Mix", x=0.5, y=0.5,
            font_size=13, font_color="#8b949e", showarrow=False
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
st.markdown("""
<div class="section-text">
<b style="color:#58a6ff; font-family:'IBM Plex Mono',monospace;">Limitations and Scope</b><br><br>
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
# References
# ─────────────────────────────────────────────────────────────
st.markdown("""
<div class="section-text">
<b style="color:#58a6ff; font-family:'IBM Plex Mono',monospace;">References</b><br><br>
<b>Data Sources</b><br>
&bull; AEMO Dispatch SCADA — 5-minute generator output:
  <a href="https://nemweb.com.au/Reports/Current/Dispatch_SCADA/" style="color:#58a6ff">nemweb.com.au</a><br>
&bull; AEMO Generation Information (Jan 2026) — DUID fuel type metadata:
  <a href="https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-forecasting-and-planning/forecasting-and-planning-data/generation-information" style="color:#58a6ff">aemo.com.au</a><br>
&bull; National Greenhouse Accounts Factors 2025 — emission factors by fuel type:
  <a href="https://www.dcceew.gov.au/climate-change/publications/national-greenhouse-accounts-factors" style="color:#58a6ff">dcceew.gov.au</a><br><br>
<b>Regulatory Context</b><br>
&bull; Australian Sustainability Reporting Standards (ASRS) — mandatory climate disclosure framework:
  <a href="https://www.aasb.gov.au/australian-sustainability-reporting-standards/" style="color:#58a6ff">aasb.gov.au</a><br>
&bull; AEMO Carbon Dioxide Equivalent Intensity Index (CDEII) — AEMO's own daily regional emissions intensity procedure:
  <a href="https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/market-operations/settlements-and-payments/settlements/carbon-dioxide-equivalent-intensity-index" style="color:#58a6ff">aemo.com.au</a><br>
&bull; National Greenhouse and Energy Reporting (NGER) Act 2007 — legislative basis for Australian emissions reporting<br><br>
While reviewing this project, the UNSW NEMED tool may have come to mind.<br>
NEMED is a Python library that gives researchers a package to pull emissions data programmatically;
it's designed for a different audience and purpose. It was not used for this project as data requirements
and processing are different.<br><br>
This project is framed around ESG reporting obligations and demonstrates an end-to-end data engineering pipeline:<br>
&bull; Ingestion<br>
&bull; Warehouse schema<br>
&bull; Python stored procedures to GitHub Actions as orchestration, writing out daily data file for reporting
</div>
""", unsafe_allow_html=True)
