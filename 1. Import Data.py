"""
AEMO Dispatch SCADA Data Importer
Scrapes NEMWEB for 5-minute dispatch SCADA ZIP files,
extracts CSVs, and appends new data to a local CSV.
"""

import os
import io
import zipfile
import requests
import pandas as pd
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────
BASE_URL   = "https://nemweb.com.au"
SCADA_URL  = f"{BASE_URL}/Reports/Current/Dispatch_SCADA/"
OUTPUT_CSV = os.path.join(os.path.dirname(__file__), "data", "dispatch_scada.csv")

KEEP_COLUMNS = ["SETTLEMENTDATE", "DUID", "SCADAVALUE"]


def get_zip_links():
    """Scrape the NEMWEB directory for all .zip file links."""
    response = requests.get(SCADA_URL)
    response.raise_for_status()
    soup  = BeautifulSoup(response.content, "html.parser")
    links = [a["href"] for a in soup.find_all("a") if a["href"].endswith(".zip")]
    print(f"Found {len(links)} ZIP files on NEMWEB")
    return links


def download_and_extract(links):
    """Download each ZIP, extract CSV, return combined DataFrame."""
    all_frames = []

    for i, link in enumerate(links):
        url = f"{BASE_URL}{link}"
        try:
            r = requests.get(url)
            r.raise_for_status()
            z = zipfile.ZipFile(io.BytesIO(r.content))
            for csv_name in z.namelist():
                df = pd.read_csv(z.open(csv_name), skiprows=1)
                df = df[df.iloc[:, 0] != "C"]
                df = df[KEEP_COLUMNS]
                all_frames.append(df)
        except Exception as e:
            print(f"  Error processing {link}: {e}")
            continue

        if (i + 1) % 50 == 0:
            print(f"  Downloaded {i + 1}/{len(links)}")

    if not all_frames:
        print("No new data extracted.")
        return pd.DataFrame(columns=KEEP_COLUMNS)

    new_data = pd.concat(all_frames, ignore_index=True)
    print(f"Extracted {len(new_data)} rows from {len(all_frames)} files")
    return new_data


def load_existing():
    """Load the existing CSV if it exists, otherwise return empty DataFrame."""
    if os.path.exists(OUTPUT_CSV):
        existing = pd.read_csv(OUTPUT_CSV)
        print(f"Loaded {len(existing)} existing rows")
        return existing
    print("No existing CSV found — starting fresh")
    return pd.DataFrame(columns=KEEP_COLUMNS)


def save(df):
    """Write DataFrame to CSV, creating the data/ directory if needed."""
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved {len(df)} rows to {OUTPUT_CSV}")


def main():
    print("=" * 60)
    print("AEMO Dispatch SCADA Importer")
    print("=" * 60)

    links    = get_zip_links()
    new_data = download_and_extract(links)

    if new_data.empty:
        print("Nothing new to save.")
        return

    existing = load_existing()
    combined = pd.concat([existing, new_data], ignore_index=True)

    before = len(combined)
    combined.drop_duplicates(subset=["SETTLEMENTDATE", "DUID"], inplace=True)
    print(f"Dropped {before - len(combined)} duplicate rows")

    combined.sort_values(["SETTLEMENTDATE", "DUID"], inplace=True)
    save(combined)

    print("=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()