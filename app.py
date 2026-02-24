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
  .intro-box {
    background: #161b22;
    border: 1px solid #30363d;
    border-left: 4px solid #58a6ff;
    border-radius: 8px;
    padding: 20px 24px;
    margin-bottom: 24px;
    line-height: 1.7;
    font-size: 1.1rem;
    color: #c9d1d9;
    max-width: 900px;
  }
  .section-text {
    line-height: 1.8;
    font-size: 0.92rem;
    color: #c9d1d9;
    margin-bottom: 24px;
  }
  .sidebar-note { font-size: 0.75rem; color: #8b949e; line-height: 1.6; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# Load & join all three tables
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_data():
    base = Path(__file__).parent

    # Check all required files exist and are non-empty before loading
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

    # MWh per 5-min interval: MW × (5/60)
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
Emission factors: <a href="https://www.dcceew.gov.au/climate-change/publications/national-greenhouse-accounts-factors" style="color:#58a6ff">NGA Factors 2025</a>, Tables 4 & 5<br><br>
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
# Header + intro
# ─────────────────────────────────────────────────────────────
st.title("Australia East Emissions Intensity Dashboard")
st.caption(
    f"AEMO NEM  ·  {selected_date.strftime('%d %B %Y')}  ·  "
    f"{', '.join(sel_regions) if sel_regions else 'No region selected'}  ·  "
    f"{scope_choice}  ·  {resolution_label} intervals"
)

st.markdown("""
<div class="intro-box">
<b>Australia's grid isn't equally clean at all hours.</b><br><br>

Under the Australian Sustainability Reporting Standards (ASRS), large 
organisations must disclose their <b>Scope 2 emissions</b>, the indirect 
emissions from purchased electricity. The most accurate approach requires 
knowing the emissions intensity of the grid at the time of consumption.<br><br>

That intensity swings by up to 4x across a single day depending on how much 
solar, wind, gas, and coal is online in your region.<br><br>

This tool calculates that intensity in real time from AEMO's 5-minute 
dispatch data, using official National Greenhouse Accounts (NGA) emission 
factors from DCCEEW — so you can see not just how much electricity your 
business uses, but when it matters.
</div>
""", unsafe_allow_html=True)


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
kpi(k1, "Avg Intensity",       f"{avg_intensity:.3f}",   "t CO₂-e / MWh")
kpi(k2, "Daily Low",           f"{period_low:.3f}",      "t CO₂-e / MWh")
kpi(k3, "Daily High",          f"{period_high:.3f}",     "t CO₂-e / MWh")
kpi(k4, "Total Generation",    f"{total_mwh/1e3:.1f}k",  "MWh")
kpi(k5, "Zero-Emission Share", f"{re_share:.1f}%",        "of total generation")
st.markdown("<br>", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# Aggregate to selected interval
# ─────────────────────────────────────────────────────────────
dff["period"] = dff["SETTLEMENTDATE"].dt.floor(resolution)

# Total MWh and absolute t CO₂-e per interval (summed across all DUIDs)
agg = (
    dff.groupby("period")
    .agg(mwh=("mwh", "sum"), tco2e=(emission_col, "sum"))
    .reset_index()
)
agg["intensity"] = (agg["tco2e"] / agg["mwh"]).where(agg["mwh"] > 0)

# Generation mix by technology per interval
mix = (
    dff.groupby(["period", "Technology Type"])
    .agg(mwh=("mwh", "sum"))
    .reset_index()
)

tech_order = [t for t in TECH_COLORS if t in mix["Technology Type"].unique()]


# ─────────────────────────────────────────────────────────────
# Combo chart — stacked bars (MWh) + absolute emissions line (t CO₂-e)
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

# Absolute emissions (t CO₂-e) — thick grey line, right axis
fig.add_trace(
    go.Scatter(
        x=agg["period"],
        y=agg["tco2e"],
        name="Emissions (t CO₂-e)",
        mode="lines",
        line=dict(color="#9ca3af", width=3),
        hovertemplate="%{x|%H:%M}<br><b>%{y:,.0f}</b> t CO₂-e<extra></extra>",
    ),
    secondary_y=True,
)

fig.update_layout(
    barmode="stack",
    title=dict(
        text=(
            f"Generation Mix & Absolute Emissions — "
            f"{'23rd February 2026' if selected_date.strftime('%Y-%m-%d') == '2026-02-23' else selected_date.strftime('%-d %B %Y')}"
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
    title_text="t CO₂-e per interval",
    showgrid=False,
    zeroline=False, color="#9ca3af",
    secondary_y=True,
)

st.plotly_chart(fig, use_container_width=True)

# Scope 3 info callout
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
    disp["Total t CO₂-e"] = disp["total_tco2e"].map("{:,.1f}".format)
    disp["Gen Share"]     = disp["share_pct"].map("{:.1f}%".format)
    disp["Avg Intensity"] = disp["avg_factor"].apply(
        lambda x: f"{x:.4f} t/MWh" if pd.notna(x) and x > 0 else "0 (zero-emission)"
    )
    st.dataframe(
        disp[["Technology Type", "Total MWh", "Total t CO₂-e", "Gen Share", "Avg Intensity"]],
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
# Raw data expander
# ─────────────────────────────────────────────────────────────
with st.expander("📄 Raw interval data"):
    st.dataframe(
        agg.rename(columns={
            "period":    f"Period ({resolution_label})",
            "mwh":       "MWh",
            "tco2e":     "t CO₂-e",
            "intensity": "Intensity (t CO₂-e/MWh)",
        }).sort_values(f"Period ({resolution_label})", ascending=False),
        use_container_width=True, hide_index=True
    )

with st.expander("🔍 Emissions factors reference (NGA 2025)"):
    ef_display = pd.read_csv(Path(__file__).parent / "data" / "emissions_factors.csv")
    st.dataframe(ef_display, use_container_width=True, hide_index=True)
    st.caption(
        "Source: National Greenhouse Accounts Factors 2025, DCCEEW. "
        "Table 4 (solid fuels Scope 1 & 3), Table 5 (gaseous fuels Scope 1). "
        "Converted: kg CO₂-e/GJ × 3.6 GJ/MWh ÷ 1000 = t CO₂-e/MWh."
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
• AEMO Dispatch SCADA — 5-minute generator output:
  <a href="https://nemweb.com.au/Reports/Current/Dispatch_SCADA/" style="color:#58a6ff">nemweb.com.au</a><br>
• AEMO Generation Information (Jan 2026) — DUID fuel type metadata:
  <a href="https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-forecasting-and-planning/forecasting-and-planning-data/generation-information" style="color:#58a6ff">aemo.com.au</a><br>
• National Greenhouse Accounts Factors 2025 — emission factors by fuel type:
  <a href="https://www.dcceew.gov.au/climate-change/publications/national-greenhouse-accounts-factors" style="color:#58a6ff">dcceew.gov.au</a><br><br>
<b>Regulatory Context</b><br>
• Australian Sustainability Reporting Standards (ASRS) — mandatory climate disclosure framework:
  <a href="https://www.aasb.gov.au/australian-sustainability-reporting-standards/" style="color:#58a6ff">aasb.gov.au</a><br>
• AEMO Carbon Dioxide Equivalent Intensity Index (CDEII) — AEMO's own daily regional emissions intensity procedure:
  <a href="https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/market-operations/settlements-and-payments/settlements/carbon-dioxide-equivalent-intensity-index" style="color:#58a6ff">aemo.com.au</a><br>
• National Greenhouse and Energy Reporting (NGER) Act 2007 — legislative basis for Australian emissions reporting<br><br>
While reviewing this project, the UNSW NEMED tool may have come to mind.<br>
NEMED is a Python library that gives researchers a package to pull emissions data programmatically;
it's designed for a different audience and purpose. It was not used for this project as data requirements
and processing are different.<br><br>
This project is framed around ESG reporting obligations and demonstrates an end-to-end data engineering pipeline:<br>
• Ingestion<br>
• Warehouse schema<br>
• Python stored procedures to GitHub Actions as orchestration, writing out daily data file for reporting
</div>
""", unsafe_allow_html=True)
