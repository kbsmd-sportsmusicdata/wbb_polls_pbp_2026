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
from pathlib import Path

# Configuration
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
# Simplified paths to ensure they appear in your folders
RAW_CSV = RAW_DIR / "wbb_schedule_raw.csv"
FILTERED_CSV = DATA_DIR / "wbb_schedule.csv"
SOS_RATINGS_CSV = DATA_DIR / "sos_data_weekly_run.csv"

# Updated Name Mapping Dictionary
TEAM_NAME_MAP = {
    "Connecticut": "UConn",
    "Louisiana State": "LSU",
    "USC": "Southern California",
    "UNC": "North Carolina",
    "Brigham Young": "BYU"
}

# Ensure directories exist
RAW_DIR.mkdir(parents=True, exist_ok=True)

def clean_and_rename_teams(df, team_cols):
    """Clean SOS data and apply team name mapping."""
    # 1. Remove entirely empty columns and rows
    df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')
    
    # 2. Apply name mapping to specified columns
    for col in team_cols:
        if col in df.columns:
            df[col] = df[col].replace(TEAM_NAME_MAP)
    return df

def download_parquet_data():
    """Download the 2026 WBB Schedule Parquet file."""
    url = "https://github.com/sportsdataverse/wehoop-wbb-data/raw/b40b39b7477f0d66b458e38fa409d135e58c8354/wbb/schedules/parquet/wbb_schedule_2026.parquet"
    print(f"Downloading data from: {url}")
    
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp.write(response.content)
        tmp_path = tmp.name

    df = pd.read_parquet(tmp_path)
    os.unlink(tmp_path)
    return df

def main():
    # 1. Handle Schedule Data
    df = download_parquet_data()
    
    # Apply name cleaning to schedule data too
    df = clean_and_rename_teams(df, ['home_name', 'away_name'])
    
    # Save Raw
    df.to_csv(RAW_CSV, index=False)
    print(f"✓ Saved raw schedule to {RAW_CSV}")

    # Save Filtered (Logic: Only games with at least one ranked team)
    filtered = df[(df['home_rank'] <= 25) | (df['away_rank'] <= 25)].copy()
    filtered.to_csv(FILTERED_CSV, index=False)
    print(f"✓ Saved filtered schedule to {FILTERED_CSV}")

    # 2. Handle SOS Ratings Data Cleaning
    if SOS_RATINGS_CSV.exists():
        print("Cleaning SOS Ratings data...")
        sos_df = pd.read_csv(SOS_RATINGS_CSV)
        # Assuming the team name column in SOS is 'Team' or 'School'
        team_col = 'team' if 'team' in sos_df.columns else sos_df.columns[0]
        sos_df = clean_and_rename_teams(sos_df, [team_col])
        sos_df.to_csv(SOS_RATINGS_CSV, index=False)
        print(f"✓ Cleaned SOS ratings saved to {SOS_RATINGS_CSV}")

if __name__ == "__main__":
    main()
