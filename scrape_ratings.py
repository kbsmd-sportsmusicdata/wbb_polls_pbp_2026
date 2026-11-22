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
from season_config import get_current_season, get_season_sos_dir

# Paths / constants
# Season-aware: data/sos/ for the legacy 2025-26 season, data/seasons/<label>/sos/
# for every later one (created if missing). See season_config.get_season_sos_dir().
SOS_DIR = get_season_sos_dir()

# Pythagorean exponent for college basketball. Her Hoop Stats suggests 10; other
# basketball models use ~13.91 (Morey) or 16.5. We use 13.91.
PYTHAGOREAN_EXPONENT = 13.91

SEASON = get_current_season()
URL_RATINGS = f"https://www.sports-reference.com/cbb/seasons/women/{SEASON}-ratings.html"
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

def drop_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop the blank spacer columns sports-reference emits ('Unnamed: 3_level_1',
    ...). They carry no data and their position shifts between scrapes, which
    used to misalign rows appended to the master file."""
    return df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]

def add_pythagorean_metrics(df: pd.DataFrame, k: float = PYTHAGOREAN_EXPONENT) -> pd.DataFrame:
    """Add Pythagorean expectation columns to a ratings table.

    Pts / Opp are per-game averages; the ratio of averages equals the ratio of
    season totals, so the formula can be applied to them directly.

      pyth_win_pct         = Pts^k / (Pts^k + Opp^k)
      pyth_wins            = pyth_win_pct * (W + L)
      wins_minus_pyth_wins = W - pyth_wins   (positive = won more than the
                                              scoring margin predicts, i.e. 'lucky')

    Rows with no games played or missing Pts/Opp get NaN, not 0.
    """
    df = df.copy()
    pts = pd.to_numeric(df["Pts"], errors="coerce")
    opp = pd.to_numeric(df["Opp"], errors="coerce")
    wins = pd.to_numeric(df["W"], errors="coerce")
    losses = pd.to_numeric(df["L"], errors="coerce")
    games = wins + losses

    # Algebraically identical to Pts^k / (Pts^k + Opp^k), but avoids raising
    # ~100 to the 13.91th power.
    valid = (pts > 0) & (opp > 0) & (games > 0)
    pct = (1.0 / (1.0 + (opp.where(valid) / pts.where(valid)) ** k))
    pyth_wins = pct * games

    df["pyth_win_pct"] = pct.round(4)
    df["pyth_wins"] = pyth_wins.round(2)
    df["wins_minus_pyth_wins"] = (wins - pyth_wins).round(2) + 0.0  # + 0.0 turns -0.0 into 0.0
    return df

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

    # Remove blank spacer columns so the schema is stable scrape to scrape
    df = drop_unnamed_columns(df)

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
    Append a snapshot to the master CSV, skipping it if its run_date is already
    there. Rows are matched to the master by column NAME (not position), and the
    master is rewritten with the union of columns, so a schema change between
    scrapes (a new metric, a dropped spacer column) can never misalign rows.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    if df is None or df.empty:
        print(f"[append_csv_with_dedup] No rows to append for {path}. Skipping.")
        return

    if not path.exists():
        df.to_csv(path, index=False)
        print(f"Created new master file: {path} ({len(df)} rows)")
        return

    try:
        existing = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        print(f"[append_csv_with_dedup] Existing file {path} is empty. Starting fresh.")
        df.to_csv(path, index=False)
        return

    if date_col in df.columns and date_col in existing.columns:
        overlapping = set(df[date_col].unique()) & set(existing[date_col].unique())
        if overlapping:
            print(f"[append_csv_with_dedup] Data for {overlapping} already exists in {path}. Skipping append to prevent duplicates.")
            return

    combined = pd.concat([existing, df], ignore_index=True)
    # Keep run_date as the last column
    cols = [c for c in combined.columns if c != date_col] + ([date_col] if date_col in combined.columns else [])
    combined[cols].to_csv(path, index=False)
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

        # Calculated fields: Pythagorean win %, Pythagorean wins, wins over expected
        ratings_df = add_pythagorean_metrics(ratings_df)

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
