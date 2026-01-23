"""
Scrape historical WBB polls data from Sports Reference (2025-2010).

This script downloads historical polls data for multiple seasons and maintains:
- Timestamped snapshot files for each run
- A cumulative master file with all historical data
- Proper rate limiting to avoid being blocked
- Robust error handling with detailed reporting

Output files:
- data/polls_historical/polls_historical_YYYYMMDD.csv (snapshot)
- data/polls_historical/polls_historical_master.csv (cumulative)
"""
import io
import time
from datetime import date
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Paths / constants
DATA_DIR = Path("data")
POLLS_HISTORICAL_DIR = DATA_DIR / "polls_historical"

# Ensure directory exists
POLLS_HISTORICAL_DIR.mkdir(parents=True, exist_ok=True)

# Master file path
MASTER_FILE = POLLS_HISTORICAL_DIR / "polls_historical_master.csv"

# Years to scrape (from most recent to oldest)
START_YEAR = 2025
END_YEAR = 2010
YEARS_TO_SCRAPE = list(range(START_YEAR, END_YEAR - 1, -1))

# Rate limiting: seconds to wait between requests
REQUEST_DELAY = 3  # 3 seconds between requests to be polite


def strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace from all string values in a DataFrame."""
    return df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))


def normalize_col(c) -> str:
    """Normalize column names by extracting the last non-empty part."""
    if isinstance(c, (tuple, list)):
        parts = [str(x).strip() for x in c if str(x).strip() and str(x).strip().lower() != "nan"]
        return parts[-1] if parts else ""
    return str(c).strip()


def clean_tables(tables: List[pd.DataFrame]) -> List[pd.DataFrame]:
    """Clean a list of DataFrames by stripping strings and removing empty rows."""
    cleaned = []
    for df in tables:
        df = strip_strings(df)
        df.dropna(how="all", inplace=True)
        cleaned.append(df)
    return cleaned


def fetch_polls_for_year(year: int) -> Tuple[List[pd.DataFrame], bool]:
    """
    Fetch polls tables for a specific year from Sports Reference.

    Args:
        year: The year to fetch (e.g., 2025)

    Returns:
        Tuple of (list of DataFrames, success boolean)
    """
    url = f"https://www.sports-reference.com/cbb/seasons/women/{year}-polls.html"

    try:
        print(f"  Fetching: {url}")
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()

        html_buf = io.StringIO(resp.text)
        tables = pd.read_html(html_buf)

        if not tables:
            print(f"    ⚠ No tables found for {year}")
            return [], False

        cleaned = clean_tables(tables)
        print(f"    ✓ Found {len(cleaned)} table(s) with {sum(len(df) for df in cleaned)} total rows")
        return cleaned, True

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"    ✗ Page not found for {year} (404)")
        else:
            print(f"    ✗ HTTP error for {year}: {e}")
        return [], False
    except Exception as e:
        print(f"    ✗ Error fetching {year}: {e}")
        return [], False


def process_polls_tables(tables: List[pd.DataFrame], year: int) -> pd.DataFrame:
    """
    Process polls tables and combine them into a single DataFrame.

    Handles multi-level columns, cleans data, and adds a year column.

    Args:
        tables: List of DataFrames from pd.read_html
        year: The year these polls are from

    Returns:
        pd.DataFrame: Combined and processed DataFrame
    """
    if not tables:
        return pd.DataFrame()

    all_data = []

    for table_idx, df in enumerate(tables, start=1):
        df = df.copy()

        # Handle multi-level columns
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [normalize_col(c) for c in df.columns]
        else:
            df.columns = [str(c).strip() for c in df.columns]

        # Remove rows where all values are NaN
        df.dropna(how='all', inplace=True)

        # Remove header rows that might have been included as data
        # (e.g., rows where 'Rk' column contains 'Rk' or 'School' contains 'School')
        if 'Rk' in df.columns:
            df = df[df['Rk'] != 'Rk']
        if 'School' in df.columns:
            df = df[df['School'].notna()]
            df = df[df['School'].str.lower() != 'school']

        # Add metadata columns
        df['year'] = year
        df['table_number'] = table_idx

        all_data.append(df)

    if not all_data:
        return pd.DataFrame()

    # Combine all tables
    combined = pd.concat(all_data, ignore_index=True)

    return combined


def append_to_master(new_data: pd.DataFrame, master_path: Path) -> None:
    """
    Append new data to the master file, avoiding duplicates.

    Uses year + table_number + row content to detect duplicates.

    Args:
        new_data: DataFrame with new data to append
        master_path: Path to the master CSV file
    """
    if new_data.empty:
        print(f"  No data to append to master file")
        return

    if not master_path.exists():
        # First time: create the file
        new_data.to_csv(master_path, index=False)
        print(f"  ✓ Created master file with {len(new_data)} rows")
    else:
        # Load existing data
        existing = pd.read_csv(master_path)

        # Check for year overlap
        new_years = set(new_data['year'].unique())
        existing_years = set(existing['year'].unique())
        overlap = new_years & existing_years

        if overlap:
            print(f"  ⚠ Data for years {sorted(overlap)} already exists in master file")
            # Filter out overlapping years from new data
            new_data = new_data[~new_data['year'].isin(overlap)]

            if new_data.empty:
                print(f"  No new data to append after removing duplicates")
                return

        # Append new data
        new_data.to_csv(master_path, mode='a', header=False, index=False)
        print(f"  ✓ Appended {len(new_data)} rows to master file")


def main():
    """Main function to scrape historical polls data."""
    print("=" * 70)
    print("WBB Historical Polls Scraper")
    print("=" * 70)
    print(f"Run Date: {date.today().strftime('%Y-%m-%d')}")
    print(f"Target Years: {START_YEAR} → {END_YEAR}")
    print(f"Request Delay: {REQUEST_DELAY} seconds")
    print("=" * 70)
    print()

    all_data = []
    successful_years = []
    failed_years = []

    # Scrape each year
    for i, year in enumerate(YEARS_TO_SCRAPE):
        print(f"[{i+1}/{len(YEARS_TO_SCRAPE)}] Processing {year}...")

        # Fetch data for this year
        tables, success = fetch_polls_for_year(year)

        if success and tables:
            # Process and store the data
            processed = process_polls_tables(tables, year)

            if not processed.empty:
                all_data.append(processed)
                successful_years.append(year)
                print(f"    ✓ Processed {len(processed)} rows for {year}")
            else:
                print(f"    ⚠ No data extracted for {year}")
                failed_years.append(year)
        else:
            failed_years.append(year)

        # Rate limiting: wait before next request (except for last iteration)
        if i < len(YEARS_TO_SCRAPE) - 1:
            print(f"    ⏳ Waiting {REQUEST_DELAY} seconds before next request...")
            time.sleep(REQUEST_DELAY)

        print()

    # Combine all data
    print("=" * 70)
    print("Processing Results")
    print("=" * 70)

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        print(f"✓ Successfully scraped {len(successful_years)} years")
        print(f"  Total rows collected: {len(combined)}")
        print(f"  Years: {', '.join(map(str, sorted(successful_years, reverse=True)))}")

        # Save timestamped snapshot
        today_str = date.today().strftime("%Y%m%d")
        snapshot_path = POLLS_HISTORICAL_DIR / f"polls_historical_{today_str}.csv"
        combined.to_csv(snapshot_path, index=False)
        print(f"\n✓ Saved snapshot: {snapshot_path}")

        # Append to master file
        print(f"\nUpdating master file...")
        append_to_master(combined, MASTER_FILE)
        print(f"✓ Master file: {MASTER_FILE}")

    else:
        print("✗ No data collected from any year")

    if failed_years:
        print(f"\n⚠ Failed to scrape {len(failed_years)} year(s):")
        print(f"  Years: {', '.join(map(str, sorted(failed_years, reverse=True)))}")

    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"✓ Successful: {len(successful_years)}/{len(YEARS_TO_SCRAPE)} years")
    if successful_years:
        print(f"  {', '.join(map(str, sorted(successful_years, reverse=True)))}")
    if failed_years:
        print(f"✗ Failed: {len(failed_years)}/{len(YEARS_TO_SCRAPE)} years")
        print(f"  {', '.join(map(str, sorted(failed_years, reverse=True)))}")
    print("=" * 70)
    print()


if __name__ == "__main__":
    main()
