import logging
import io
import os
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Import centralized team name standardization
from team_name_utils import standardize_team_names as apply_team_name_standardization

# Paths / constants
DATA_DIR = Path("data")
SOS_DIR = DATA_DIR / "sos"

# Ensure directory exists
SOS_DIR.mkdir(parents=True, exist_ok=True)

URL_RATINGS = "https://www.sports-reference.com/cbb/seasons/women/2026-ratings.html"
MASTER_RATINGS = SOS_DIR / "ratings_master.csv"

def strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace from all string values in a DataFrame."""
    return df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))

def normalize_col(c) -> str:
    """Normalize column names by extracting the last non-empty part."""
    if isinstance(c, (tuple, list)):
        parts = [str(x).strip() for x in c if str(x).strip() and str(x).strip().lower() != "nan"]
        return parts[-1] if parts else ""
    return str(c).strip()

def clean_tables(tables: list[pd.DataFrame]) -> list[pd.DataFrame]:
    """Clean a list of DataFrames by stripping strings and removing empty rows."""
    cleaned = []
    for df in tables:
        df = strip_strings(df)
        df.dropna(how="all", inplace=True)
        cleaned.append(df)
    return cleaned

def standardize_team_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize team names to match polls dataset conventions.

    Uses centralized team name mappings from team_name_utils module.
    This ensures consistency across all datasets for analysis and visualization.
    """
    # Use centralized standardization for School column
    return apply_team_name_standardization(df, ['School'])

def fetch_ratings_table(url: str) -> pd.DataFrame:
    """
    Fetch the ratings table from the specified URL.
    Uses BeautifulSoup to extract the table with id='ratings'.
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")

    # Find the table with id="ratings"
    ratings_table = soup.find("table", {"id": "ratings"})

    if ratings_table is None:
        raise ValueError("Could not find table with id='ratings' on the page")

    # Convert the table to a DataFrame
    table_html = str(ratings_table)
    tables = pd.read_html(io.StringIO(table_html))

    if not tables:
        raise ValueError("No tables found in the ratings table HTML")

    df = tables[0]

    # Clean the DataFrame
    df = strip_strings(df)

    # Handle multi-level columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [normalize_col(c) for c in df.columns]
    else:
        df.columns = [str(c).strip() for c in df.columns]

    # Remove rows where all values are NaN
    df.dropna(how="all", inplace=True)

    # Remove header rows that might have been included as data
    # (e.g., rows where 'Rk' column contains 'Rk')
    if 'Rk' in df.columns:
        df = df[df['Rk'] != 'Rk']
    if 'School' in df.columns:
        df = df[df['School'].notna()]
        df = df[df['School'].str.lower() != 'school']

    return df

def append_csv_with_dedup(df: pd.DataFrame, path: Path, date_col: str = "run_date") -> None:
    """
    Append DataFrame to CSV with deduplication based on run_date.
    If data for the current run_date already exists, skip appending.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    if df is None or df.empty:
        print(f"[append_csv_with_dedup] No rows to append for {path}. Skipping.")
        return

    if not path.exists():
        # First time: write with header
        df.to_csv(path, mode="w", header=True, index=False)
        print(f"Created new master file: {path} ({len(df)} rows)")
    else:
        # Check if data for this run_date already exists
        # Use on_bad_lines='skip' to handle any malformed rows from previous scrapes
            existing = pd.read_csv(path, on_bad_lines='skip')
            logging.info(f"[append_csv_with_dedup] Loaded existing file: {path} ({len(existing)} rows)")
        except TypeError:
            # Fallback for older pandas versions (< 1.3)
            existing = pd.read_csv(path, error_bad_lines=False, warn_bad_lines=True)
            logging.warning(f"[append_csv_with_dedup] Loaded existing file: {path} ({len(existing)} rows, some malformed rows skipped)")
        except pd.errors.EmptyDataError:
            logging.warning(f"[append_csv_with_dedup] Existing file {path} is empty. Starting fresh.")
            existing = pd.DataFrame()
        if date_col in df.columns and date_col in existing.columns:
            current_dates = df[date_col].unique()
            existing_dates = existing[date_col].unique()

            overlapping_dates = set(current_dates) & set(existing_dates)

            if overlapping_dates:
                print(f"[append_csv_with_dedup] Data for {overlapping_dates} already exists in {path}. Skipping append to prevent duplicates.")
                return

        # No overlap: safe to append
        df.to_csv(path, mode="a", header=False, index=False)
        print(f"Appended to master file: {path} ({len(df)} new rows)")

def main() -> None:
    """Main function to scrape ratings data and save to files."""
    today = date.today()
    today_str = today.strftime("%Y%m%d")
    today_iso = today.strftime("%Y-%m-%d")

    print(f"Fetching ratings data from {URL_RATINGS}...")

    try:
        ratings_df = fetch_ratings_table(URL_RATINGS)

        # Standardize team names for consistency with polls data
        ratings_df = standardize_team_names(ratings_df)

        # Add run_date column for tracking
        ratings_df["run_date"] = today_iso

        print(f"Successfully fetched {len(ratings_df)} rows from ratings table")

        # Save timestamped snapshot
        snapshot_path = SOS_DIR / f"ratings_{today_str}.csv"
        ratings_df.to_csv(snapshot_path, index=False)
        print(f"Saved snapshot: {snapshot_path}")

        # Append to master file with deduplication
        append_csv_with_dedup(ratings_df, MASTER_RATINGS, date_col="run_date")

        print("✓ Ratings scrape completed successfully")

    except Exception as e:
        print(f"✗ Error scraping ratings: {e}")
        raise

if __name__ == "__main__":
    main()
