"""
Scrape WBB Schedules Data from sportsdataverse wehoop-wbb-data repository.

This script:
1. Downloads parquet data from GitHub
2. Maintains a raw CSV with idempotency (no duplicate game_id)
3. Creates a filtered CSV for ranked teams with "once ranked" tracking
4. Only includes games where at least one team is or was ever ranked (1-25)

Output files:
- data/raw/wbb_schedule_raw.csv (all games, no duplicates)
- data/wbb_schedule/wbb_schedule.csv (ranked teams only)
"""

import os
from datetime import date
from pathlib import Path
from typing import Set
import pandas as pd
import requests
import tempfile

# Configuration
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
# Updated paths per your request
RAW_CSV = RAW_DIR / "wbb_schedule_raw.csv"
FILTERED_CSV = DATA_DIR / "wbb_schedule.csv"

# Updated Raw Parquet Link (pointing to the direct download version)
PARQUET_URL = "https://github.com/sportsdataverse/wehoop-wbb-data/raw/b40b39b7477f0d66b458e38fa409d135e58c8354/wbb/schedules/parquet/wbb_schedule_2026.parquet"

# Ensure directories exist
RAW_DIR.mkdir(parents=True, exist_ok=True)

def download_parquet_data(url: str) -> pd.DataFrame:
    """Download parquet file from URL and return as DataFrame."""
    print(f"Downloading WBB Schedule data from:\n  {url}")
    
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
    except Exception as e:
        print(f"  ❌ Failed to download data: {e}")
        return pd.DataFrame()

    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
            temp_path = Path(temp_file.name)
            temp_file.write(response.content)

        df = pd.read_parquet(temp_path)
        print(f"  ✓ Downloaded {len(df)} games")
        return df
    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink()

def get_once_ranked_teams(filtered_csv_path: Path) -> Set[str]:
    """Identify teams that were historically ranked based on schedule records."""
    if not filtered_csv_path.exists():
        return set()

    try:
        existing = pd.read_csv(filtered_csv_path)
        once_ranked = set()

        # In schedule data, columns are typically home_name/away_name
        for prefix in ['home', 'away']:
            name_col = f'{prefix}_name'
            rank_col = f'{prefix}_rank'
            if name_col in existing.columns and rank_col in existing.columns:
                ranked = existing[
                    (existing[rank_col].notna()) & 
                    (existing[rank_col] >= 1) & 
                    (existing[rank_col] <= 25)
                ][name_col].unique()
                once_ranked.update(ranked)

        return once_ranked
    except Exception:
        return set()

def filter_ranked_games(df: pd.DataFrame, once_ranked_teams: Set[str]) -> pd.DataFrame:
    """Filter for games involving currently ranked or historically ranked teams."""
    print("Filtering for ranked match-ups...")
    df = df.copy()

    # Define ranking thresholds
    UNRANKED_VALUE = 99
    # Schedule columns: home_rank and away_rank
    home_rank = pd.to_numeric(df.get('home_rank', pd.Series([UNRANKED_VALUE] * len(df))), errors='coerce').fillna(UNRANKED_VALUE)
    away_rank = pd.to_numeric(df.get('away_rank', pd.Series([UNRANKED_VALUE] * len(df))), errors='coerce').fillna(UNRANKED_VALUE)

    currently_ranked = ((home_rank >= 1) & (home_rank <= 25)) | ((away_rank >= 1) & (away_rank <= 25))

    once_ranked_condition = pd.Series([False] * len(df))
    if once_ranked_teams:
        # Schedule columns: home_name and away_name
        home_once = df.get('home_name', pd.Series([None])).isin(once_ranked_teams)
        away_once = df.get('away_name', pd.Series([None])).isin(once_ranked_teams)
        once_ranked_condition = home_once | away_once

    return df[currently_ranked | once_ranked_condition].copy()

def append_new_rows(csv_path: Path, new_df: pd.DataFrame, id_column: str = 'game_id') -> None:
    """Appends unique rows to CSV to prevent duplicates."""
    if new_df.empty:
        return

    if csv_path.exists():
        existing_df = pd.read_csv(csv_path)
        existing_ids = set(existing_df[id_column].astype(str))
        rows_to_add = new_df[~new_df[id_column].astype(str).isin(existing_ids)]
        
        if not rows_to_add.empty:
            rows_to_add.to_csv(csv_path, mode='a', header=False, index=False)
            print(f"  ✓ Added {len(rows_to_add)} new records to {csv_path.name}")
    else:
        new_df.to_csv(csv_path, index=False)
        print(f"  ✓ Initialized {csv_path.name}")

def main():
    print("=" * 30)
    print("WBB Schedule Scraper 2026")
    print("=" * 30)

    df = download_parquet_data(PARQUET_URL)
    if df.empty:
        return

    # Determine ID column (Schedules use 'game_id')
    id_col = 'game_id' if 'game_id' in df.columns else 'id'
    
    # Update raw archive
    append_new_rows(RAW_CSV, df, id_column=id_col)

    # Process filtered schedule
    once_ranked = get_once_ranked_teams(FILTERED_CSV)
    filtered_df = filter_ranked_games(df, once_ranked)
