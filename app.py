"""
app.py — NEM Emissions Intensity Dashboard
==========================================
Data flow:
  data/dispatch_scada.csv    → raw 5-min SCADA generation (MW) per DUID
  data/duid_lookup.csv       → DUID → Technology Type, Region, Unit Name
  data/emissions_factors.csv → Technology Type → tCO2e/MWh (Scope 1 & 3, NGA 2025)

All three files are kept separate and joined here at load time.
Streamlit caches the result so the join only runs once per session.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="NEM Emissions Intensity",
    page_icon="⚡",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Styling — dark industrial theme
# ---------------------------------------------------------------------------
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
  .sidebar-note { font-size: 0.75rem; color: #8b949e; line-height: 1.6; }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Load & join all three tables (cached — runs once per session)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_data():
    base = Path(__file__).parent

    # 1. SCADA — 5-min generation (MW) per DUID
    scada = pd.read_csv(
        base / "data" / "dispatch_scada.csv",
        parse_dates=["SETTLEMENTDATE"]
    )
    scada = scada[scada["SCADAVALUE"] > 0].copy()

    # 2. DUID lookup — Technology Type, Region, Unit Name
    lookup = pd.read_csv(base / "data" / "duid_lookup.csv")
    lookup = lookup[["DUID", "Unit Name", "Technology Type", "Region"]].drop_duplicates("DUID")

    # 3. Emissions factors — Scope 1 and Scope 3, keyed on Technology Type
    ef_raw = pd.read_csv(base / "data" / "emissions_factors.csv")

    ef_s1 = (
        ef_raw[ef_raw["scope"] == "scope_1"]
        [["technology_type", "emission_factor_tCO2e_MWh"]]
        .rename(columns={"technology_type": "Technology Type", "emission_factor_tCO2e_MWh": "ef_scope1"})
    )
    ef_s3 = (
        ef_raw[ef_raw["scope"] == "scope_3"]
        [["technology_type", "emission_factor_tCO2e_MWh"]]
        .rename(columns={"technology_type": "Technology Type", "emission_factor_tCO2e_MWh": "ef_scope3"})
    )

    # Join: SCADA → DUID lookup → Emissions factors
    df = (
        scada
        .merge(lookup, on="DUID", how="left")
        .merge(ef_s1,  on="Technology Type", how="left")
        .merge(ef_s3,  on="Technology Type", how="left")
    )

    # Unknown DUIDs: flag and assign gas proxy (conservative estimate)
    df["Technology Type"] = df["Technology Type"].fillna("Unknown")
    df["ef_scope1"] = df["ef_scope1"].fillna(0.1855)  # natural gas pipeline proxy
    df["ef_scope3"] = df["ef_scope3"].fillna(0.0)

    # Calculations
    df["mwh"]          = df["SCADAVALUE"] * (5 / 60)      # MW × 5-min interval → MWh
    df["tco2e_scope1"] = df["mwh"] * df["ef_scope1"]
    df["tco2e_scope3"] = df["mwh"] * df["ef_scope3"]
    df["tco2e_total"]  = df["tco2e_scope1"] + df["tco2e_scope3"]
    df["date"]         = df["SETTLEMENTDATE"].dt.date

    return df


df = load_data()

# Colour palette — one per Technology Type
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


# ---------------------------------------------------------------------------
# Sidebar controls
# ---------------------------------------------------------------------------
st.sidebar.title("⚡ Controls")

date_min = df["date"].min()
date_max = df["date"].max()

c1, c2 = st.sidebar.columns(2)
with c1:
    date_from = st.date_input("From", value=date_min, min_value=date_min, max_value=date_max)
with c2:
    date_to = st.date_input("To",   value=date_max, min_value=date_min, max_value=date_max)

scope_choice = st.sidebar.radio(
    "Emissions scope",
    ["Scope 1 only", "Scope 1 + 3 (combined)"],
    help=(
        "Scope 1 = direct combustion emissions from the generator.\n"
        "Scope 3 = upstream extraction and transport of fuel (coal only in NGA 2025)."
    )
)

regions = sorted(df["Region"].dropna().unique().tolist())
sel_regions = st.sidebar.multiselect("Regions", regions, default=regions)

st.sidebar.markdown("---")
st.sidebar.markdown("""
<div class="sidebar-note">
<b>Data sources</b><br>
Generation: <a href="https://nemweb.com.au" style="color:#58a6ff">AEMO NEMWEB</a> — Dispatch SCADA<br>
Unit metadata: AEMO Generation Information (Jan 2026)<br>
Emission factors: <a href="https://www.dcceew.gov.au/climate-change/publications/national-greenhouse-accounts-factors"
style="color:#58a6ff">NGA Factors 2025</a>, Tables 4 & 5<br><br>
<b>Coverage</b><br>
NEM regions only: QLD, NSW, VIC, SA, TAS.<br>
Excludes WEM, NT grids, rooftop solar.<br><br>
<b>Methodology</b><br>
MWh = MW output × (5 min ÷ 60)<br>
t CO₂-e = MWh × NGA factor<br>
Intensity = Σ t CO₂-e ÷ Σ MWh<br>
NGA factors converted: kg CO₂-e/GJ × 3.6 ÷ 1000
</div>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Filter
# ---------------------------------------------------------------------------
mask = (
    (df["date"] >= date_from) &
    (df["date"] <= date_to) &
    (df["Region"].isin(sel_regions))
)
dff = df[mask].copy()
emission_col = "tco2e_scope1" if scope_choice == "Scope 1 only" else "tco2e_total"


# ---------------------------------------------------------------------------
# Daily aggregates
# ---------------------------------------------------------------------------
daily = (
    dff.groupby("date")
    .agg(total_mwh=("mwh", "sum"), total_tco2e=(emission_col, "sum"))
    .reset_index()
)
daily["intensity"]       = (daily["total_tco2e"] / daily["total_mwh"]).where(daily["total_mwh"] > 0)
daily["intensity_7d_avg"] = daily["intensity"].rolling(7, min_periods=1).mean()
daily["date"]            = pd.to_datetime(daily["date"])

daily_mix = (
    dff.groupby(["date", "Technology Type"])
    .agg(mwh=("mwh", "sum"))
    .reset_index()
)
daily_mix["date"] = pd.to_datetime(daily_mix["date"])

tech_summary = (
    dff.groupby("Technology Type")
    .agg(total_mwh=("mwh", "sum"), total_tco2e=(emission_col, "sum"))
    .reset_index()
    .sort_values("total_mwh", ascending=False)
)
tech_summary["share_pct"]  = 100 * tech_summary["total_mwh"] / tech_summary["total_mwh"].sum()
tech_summary["avg_factor"] = tech_summary["total_tco2e"] / tech_summary["total_mwh"]


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("NEM Emissions Intensity Dashboard")
st.caption(
    f"National Electricity Market · {date_from} → {date_to} · "
    f"{', '.join(sel_regions) if sel_regions else 'No region selected'} · {scope_choice}"
)
st.markdown("---")


# ---------------------------------------------------------------------------
# KPI cards
# ---------------------------------------------------------------------------
def kpi(col, label, value, sub=""):
    col.markdown(
        f'<div class="metric-card">'
        f'<div class="metric-label">{label}</div>'
        f'<div class="metric-value">{value}</div>'
        f'<div class="metric-sub">{sub}</div>'
        f'</div>',
        unsafe_allow_html=True
    )

total_mwh       = daily["total_mwh"].sum()
renewable_mwh   = dff[dff["Technology Type"].isin(ZERO_EMISSION)]["mwh"].sum()
renewable_share = 100 * renewable_mwh / total_mwh if total_mwh > 0 else 0
latest          = daily["intensity"].iloc[-1]  if not daily.empty else 0
avg_i           = daily["intensity"].mean()    if not daily.empty else 0
min_i           = daily["intensity"].min()     if not daily.empty else 0
max_i           = daily["intensity"].max()     if not daily.empty else 0

k1, k2, k3, k4, k5 = st.columns(5)
kpi(k1, "Latest Daily Intensity", f"{latest:.3f}", "t CO₂-e / MWh")
kpi(k2, "Period Average",         f"{avg_i:.3f}",  "t CO₂-e / MWh")
kpi(k3, "Period Low",             f"{min_i:.3f}",  "t CO₂-e / MWh")
kpi(k4, "Period High",            f"{max_i:.3f}",  "t CO₂-e / MWh")
kpi(k5, "Zero-Emission Share",    f"{renewable_share:.1f}%", "of total generation")
st.markdown("<br>", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Chart 1 — Emissions intensity over time
# ---------------------------------------------------------------------------
fig1 = go.Figure()
fig1.add_trace(go.Scatter(
    x=daily["date"], y=daily["intensity"],
    name="Daily", mode="lines",
    line=dict(color="#30363d", width=1),
    fill="tozeroy", fillcolor="rgba(88,166,255,0.08)",
))
fig1.add_trace(go.Scatter(
    x=daily["date"], y=daily["intensity_7d_avg"],
    name="7-day avg", mode="lines",
    line=dict(color="#58a6ff", width=2),
))
fig1.update_layout(
    title=dict(text="Grid Emissions Intensity (t CO₂-e / MWh)", font=dict(family="IBM Plex Mono", color="#58a6ff")),
    xaxis=dict(showgrid=False, color="#8b949e"),
    yaxis=dict(title="t CO₂-e / MWh", showgrid=True, gridcolor="#21262d", zeroline=False, color="#8b949e"),
    plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
    font=dict(color="#c9d1d9", family="IBM Plex Sans"),
    legend=dict(bgcolor="#161b22", bordercolor="#30363d"),
    margin=dict(l=10, r=10, t=50, b=10),
    hovermode="x unified",
)
st.plotly_chart(fig1, use_container_width=True)


# ---------------------------------------------------------------------------
# Chart 2 — Generation mix stacked bar
# ---------------------------------------------------------------------------
tech_order = [t for t in TECH_COLORS if t in daily_mix["Technology Type"].unique()]
fig2 = go.Figure()
for tech in tech_order:
    subset = daily_mix[daily_mix["Technology Type"] == tech]
    fig2.add_trace(go.Bar(
        x=subset["date"], y=subset["mwh"],
        name=tech, marker_color=TECH_COLORS.get(tech, "#555"),
    ))
fig2.update_layout(
    barmode="stack",
    title=dict(text="Daily Generation Mix (MWh)", font=dict(family="IBM Plex Mono", color="#58a6ff")),
    xaxis=dict(showgrid=False, color="#8b949e"),
    yaxis=dict(title="MWh", showgrid=True, gridcolor="#21262d", zeroline=False, color="#8b949e"),
    plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
    font=dict(color="#c9d1d9", family="IBM Plex Sans"),
    legend=dict(bgcolor="#161b22", bordercolor="#30363d"),
    margin=dict(l=10, r=10, t=50, b=10),
)
st.plotly_chart(fig2, use_container_width=True)


# ---------------------------------------------------------------------------
# Period summary table + donut
# ---------------------------------------------------------------------------
st.markdown("### Period Summary by Technology")
left, right = st.columns([2, 1])

with left:
    disp = tech_summary.copy()
    disp["Total MWh"]      = disp["total_mwh"].map("{:,.0f}".format)
    disp["Total t CO₂-e"]  = disp["total_tco2e"].map("{:,.1f}".format)
    disp["Gen Share"]      = disp["share_pct"].map("{:.1f}%".format)
    disp["Avg Intensity"]  = disp["avg_factor"].apply(
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
    ))
    fig_donut.update_layout(
        showlegend=False,
        plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
        margin=dict(l=0, r=0, t=0, b=0),
        font=dict(color="#c9d1d9"),
        annotations=[dict(text="Gen Mix", x=0.5, y=0.5, font_size=13, font_color="#8b949e", showarrow=False)]
    )
    st.plotly_chart(fig_donut, use_container_width=True)


# ---------------------------------------------------------------------------
# Scope 3 note
# ---------------------------------------------------------------------------
if scope_choice == "Scope 1 + 3 (combined)":
    s1 = dff["tco2e_scope1"].sum()
    s3 = dff["tco2e_scope3"].sum()
    if s1 > 0:
        st.info(
            f"Scope 3 adds **{100 * s3 / s1:.1f}%** on top of Scope 1 for this period "
            f"({s3:,.0f} t upstream vs {s1:,.0f} t direct). "
            "NGA 2025 only specifies Scope 3 factors for coal fuels."
        )


# ---------------------------------------------------------------------------
# Raw data + factor reference
# ---------------------------------------------------------------------------
with st.expander("📄 Raw daily data"):
    st.dataframe(
        daily.rename(columns={
            "date": "Date", "total_mwh": "Total MWh",
            "total_tco2e": "Total t CO₂-e",
            "intensity": "Intensity (t CO₂-e / MWh)",
            "intensity_7d_avg": "7-day Avg"
        }).sort_values("Date", ascending=False),
        use_container_width=True, hide_index=True
    )

with st.expander("🔍 Emissions factors used (NGA 2025)"):
    ef_display = pd.read_csv(Path(__file__).parent / "data" / "emissions_factors.csv")
    st.dataframe(ef_display, use_container_width=True, hide_index=True)
    st.caption(
        "Source: National Greenhouse Accounts Factors 2025, DCCEEW. "
        "Table 4 (solid fuels, Scope 1 & 3), Table 5 (gaseous fuels, Scope 1). "
        "Converted from kg CO₂-e/GJ using 3.6 GJ/MWh."
    )
