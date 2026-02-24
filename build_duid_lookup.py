"""
build_duid_lookup.py
--------------------
Builds data/duid_lookup.csv from the AEMO Generator Information file.

Run this manually whenever a new Generator Information Excel is released (quarterly).
Output is committed to the repo and read by both importdata.py and app.py.

Columns produced:
  DUID | Unit Name | Technology Type | Region | Dispatch Type | source
"""

import pandas as pd
from pathlib import Path

# ---------------------------------------------------------------------------
# 1. Load AEMO Generator Information Excel
# ---------------------------------------------------------------------------
# Download from:
# https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/
#   nem-forecasting-and-planning/forecasting-and-planning-data/generation-information

EXCEL_PATH = Path("reference/NEM_Generation_Info_Jan_2026.xlsx")
SHEET_NAME = "Generator Information"
HEADER_ROW = 3  # 0-indexed; row 4 in Excel contains the column names

gen = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME, header=HEADER_ROW)

# Keep only the columns we need
gen = (
    gen[["DUID", "Unit Name", "Technology Type", "Region", "Dispatch Type"]]
    .dropna(subset=["DUID"])
    .assign(source="AEMO Gen Info")
)

# ---------------------------------------------------------------------------
# 2. Manual fallback map for DUIDs not yet in the Gen Info file
#
#    Covers:
#      DG_*     - distributed generation aggregates (treated as Solar PV / Other)
#      RT_*     - rooftop solar market units (Solar PV, non-dispatchable)
#      PUMP*    - pumped hydro in load mode (Hydro)
#      SNOWYP   - Snowy pump load
#      Newer units not yet published in this quarterly release
# ---------------------------------------------------------------------------
FALLBACK = [
    # DUIDs,                              Unit Name,           Technology Type, Region, Dispatch Type
    ("DG_NSW1",  "Distrib. Gen NSW",        "Other",          "NSW1",  "Non-scheduled"),
    ("DG_QLD1",  "Distrib. Gen QLD",        "Other",          "QLD1",  "Non-scheduled"),
    ("DG_SA1",   "Distrib. Gen SA",         "Other",          "SA1",   "Non-scheduled"),
    ("DG_TAS1",  "Distrib. Gen TAS",        "Other",          "TAS1",  "Non-scheduled"),
    ("DG_VIC1",  "Distrib. Gen VIC",        "Other",          "VIC1",  "Non-scheduled"),
    ("RT_NSW1",  "RT Solar NSW 1",          "Solar PV",       "NSW1",  "Non-scheduled"),
    ("RT_NSW2",  "RT Solar NSW 2",          "Solar PV",       "NSW1",  "Non-scheduled"),
    ("RT_NSW3",  "RT Solar NSW 3",          "Solar PV",       "NSW1",  "Non-scheduled"),
    ("RT_NSW4",  "RT Solar NSW 4",          "Solar PV",       "NSW1",  "Non-scheduled"),
    ("RT_NSW5",  "RT Solar NSW 5",          "Solar PV",       "NSW1",  "Non-scheduled"),
    ("RT_NSW6",  "RT Solar NSW 6",          "Solar PV",       "NSW1",  "Non-scheduled"),
    ("RT_QLD1",  "RT Solar QLD",            "Solar PV",       "QLD1",  "Non-scheduled"),
    ("RT_SA1",   "RT Solar SA 1",           "Solar PV",       "SA1",   "Non-scheduled"),
    ("RT_SA2",   "RT Solar SA 2",           "Solar PV",       "SA1",   "Non-scheduled"),
    ("RT_SA3",   "RT Solar SA 3",           "Solar PV",       "SA1",   "Non-scheduled"),
    ("RT_SA4",   "RT Solar SA 4",           "Solar PV",       "SA1",   "Non-scheduled"),
    ("RT_SA5",   "RT Solar SA 5",           "Solar PV",       "SA1",   "Non-scheduled"),
    ("RT_SA6",   "RT Solar SA 6",           "Solar PV",       "SA1",   "Non-scheduled"),
    ("RT_TAS1",  "RT Solar TAS",            "Solar PV",       "TAS1",  "Non-scheduled"),
    ("RT_VIC1",  "RT Solar VIC 1",          "Solar PV",       "VIC1",  "Non-scheduled"),
    ("RT_VIC2",  "RT Solar VIC 2",          "Solar PV",       "VIC1",  "Non-scheduled"),
    ("RT_VIC3",  "RT Solar VIC 3",          "Solar PV",       "VIC1",  "Non-scheduled"),
    ("RT_VIC4",  "RT Solar VIC 4",          "Solar PV",       "VIC1",  "Non-scheduled"),
    ("RT_VIC5",  "RT Solar VIC 5",          "Solar PV",       "VIC1",  "Non-scheduled"),
    ("RT_VIC6",  "RT Solar VIC 6",          "Solar PV",       "VIC1",  "Non-scheduled"),
    ("RT_VIC7",  "RT Solar VIC 7",          "Solar PV",       "VIC1",  "Non-scheduled"),
    ("RT_VIC8",  "RT Solar VIC 8",          "Solar PV",       "VIC1",  "Non-scheduled"),
    ("RT_VIC9",  "RT Solar VIC 9",          "Solar PV",       "VIC1",  "Non-scheduled"),
    ("RT_VIC10", "RT Solar VIC 10",         "Solar PV",       "VIC1",  "Non-scheduled"),
    ("RT_VIC11", "RT Solar VIC 11",         "Solar PV",       "VIC1",  "Non-scheduled"),
    ("RT_VIC12", "RT Solar VIC 12",         "Solar PV",       "VIC1",  "Non-scheduled"),
    ("PUMP1",    "Pumped Hydro Load 1",     "Hydro",          "NSW1",  "Scheduled"),
    ("PUMP2",    "Pumped Hydro Load 2",     "Hydro",          "NSW1",  "Scheduled"),
    ("SNOWYP",   "Snowy Pump Load",         "Hydro",          "NSW1",  "Scheduled"),
    ("SHPUMP",   "Shoalhaven Pump",         "Hydro",          "NSW1",  "Scheduled"),
    ("SHOAL1",   "Shoalhaven",              "Hydro",          "NSW1",  "Scheduled"),
    ("MLSP1",    "Mt Lyell Smelter",        "Other",          "TAS1",  "Non-scheduled"),
    ("PIONEER",  "Pioneer Sugar Mill",      "Other",          "QLD1",  "Non-scheduled"),
    ("BUSF1",    "Bulli Creek Solar Farm",  "Solar PV",       "QLD1",  "Semi-scheduled"),
    ("CRWARP1",  "Crown Point Solar",       "Solar PV",       "QLD1",  "Semi-scheduled"),
    ("GPWFWST1", "Golden Plains Wind W1",   "Wind",           "VIC1",  "Semi-scheduled"),
    ("GPWFWST2", "Golden Plains Wind W2",   "Wind",           "VIC1",  "Semi-scheduled"),
    ("KIATAWF1", "Kiata Wind Farm",         "Wind",           "VIC1",  "Semi-scheduled"),
    ("KIDSPHL1", "Kidston Pump Hydro 1",    "Hydro",          "QLD1",  "Scheduled"),
    ("KIDSPHL2", "Kidston Pump Hydro 2",    "Hydro",          "QLD1",  "Scheduled"),
    ("LANCSF1",  "Lancaster Solar Farm",    "Solar PV",       "VIC1",  "Semi-scheduled"),
    ("NESBESS1", "Neoen BESS 1",            "Battery Storage","SA1",   "Scheduled"),
    ("NESBESS2", "Neoen BESS 2",            "Battery Storage","SA1",   "Scheduled"),
    ("TRGBESS1", "Torrens Island BESS",     "Battery Storage","SA1",   "Scheduled"),
    ("WNSF1",    "Walla Walla North Solar", "Solar PV",       "NSW1",  "Semi-scheduled"),
]

fallback_df = pd.DataFrame(
    FALLBACK,
    columns=["DUID", "Unit Name", "Technology Type", "Region", "Dispatch Type"]
).assign(source="manual_fallback")

# ---------------------------------------------------------------------------
# 3. Combine — AEMO Gen Info takes precedence over fallback
# ---------------------------------------------------------------------------
lookup = pd.concat([gen, fallback_df], ignore_index=True)
lookup = lookup.drop_duplicates(subset=["DUID"], keep="first")
lookup = lookup.sort_values("DUID").reset_index(drop=True)

# ---------------------------------------------------------------------------
# 4. Save
# ---------------------------------------------------------------------------
output_path = Path("data/duid_lookup.csv")
output_path.parent.mkdir(exist_ok=True)
lookup.to_csv(output_path, index=False)

print(f"Saved {len(lookup)} DUIDs to {output_path}")
print(f"  From AEMO Gen Info:  {(lookup['source'] == 'AEMO Gen Info').sum()}")
print(f"  From manual fallback: {(lookup['source'] == 'manual_fallback').sum()}")
print()
print("Technology Type breakdown:")
print(lookup["Technology Type"].value_counts().to_string())
