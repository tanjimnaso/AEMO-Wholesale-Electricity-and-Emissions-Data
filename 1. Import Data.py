"""
AEMO Dispatch SCADA Data Importer
Scrapes NEMWEB for 5-minute dispatch SCADA ZIP files,
extracts CSVs, and appends new data to Azure SQL Database.
"""

import os
import io
import zipfile
import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text

# ── Config ────────────────────────────────────────────────────────
BASE_URL = "https://nemweb.com.au"
SCADA_URL = f"{BASE_URL}/Reports/Current/Dispatch_SCADA/"

# Columns we actually want
KEEP_COLUMNS = ["SETTLEMENTDATE", "DUID", "SCADAVALUE"]

TABLE_NAME = "dispatch_scada"


def get_engine():
    """Create SQLAlchemy engine from environment variables."""
    server   = os.environ["AZURE_SQL_SERVER"]
    database = os.environ["AZURE_SQL_DATABASE"]
    username = os.environ["AZURE_SQL_USERNAME"]
    password = os.environ["AZURE_SQL_PASSWORD"]

    connection_string = (
        f"mssql+pyodbc://{username}:{password}"
        f"@{server}/{database}"
        f"?driver=ODBC+Driver+18+for+SQL+Server"
    )
    return create_engine(connection_string)


def get_max_date(engine):
    """Return the latest SETTLEMENTDATE already in the database, or None."""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT MAX(SETTLEMENTDATE) FROM {TABLE_NAME}")
            )
            max_date = result.scalar()
            if max_date:
                print(f"Latest date in database: {max_date}")
            else:
                print("Table is empty — will load all available data")
            return max_date
    except Exception:
        print("Table does not exist yet — will create on first insert")
        return None


def get_zip_links():
    """Scrape the NEMWEB directory for all .zip file links."""
    response = requests.get(SCADA_URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
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

                # Drop the AEMO footer row ("C, END OF REPORT, ...")
                df = df[df.iloc[:, 0] != "C"]

                # Keep only the useful columns
                df = df[KEEP_COLUMNS]

                all_frames.append(df)

        except Exception as e:
            print(f"  Error processing {link}: {e}")
            continue

        if (i + 1) % 50 == 0:
            print(f"  Downloaded {i + 1}/{len(links)}")

    if not all_frames:
        print("No data extracted.")
        return pd.DataFrame(columns=KEEP_COLUMNS)

    new_data = pd.concat(all_frames, ignore_index=True)
    print(f"Extracted {len(new_data)} rows from {len(all_frames)} files")
    return new_data


def main():
    print("=" * 60)
    print("AEMO Dispatch SCADA Importer")
    print("=" * 60)

    # 1. Connect to Azure SQL
    engine = get_engine()

    # 2. Get the latest date already loaded — skip anything older
    max_date = get_max_date(engine)

    # 3. Scrape NEMWEB for ZIP links
    links = get_zip_links()

    # 4. Download and extract
    new_data = download_and_extract(links)

    if new_data.empty:
        print("Nothing to save.")
        return

    # 5. Filter to only rows newer than what's already in the database
    if max_date:
        new_data["SETTLEMENTDATE"] = pd.to_datetime(new_data["SETTLEMENTDATE"])
        new_data = new_data[new_data["SETTLEMENTDATE"] > pd.to_datetime(max_date)]
        print(f"After deduplication filter: {len(new_data)} new rows to insert")

    if new_data.empty:
        print("No new rows to insert — database is already up to date.")
        return

    # 6. Append to Azure SQL
    new_data.to_sql(
        TABLE_NAME,
        engine,
        if_exists="append",
        index=False,
        chunksize=1000
    )
    print(f"Inserted {len(new_data)} rows into {TABLE_NAME}")

    print("=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()