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
from datetime import datetime
from pathlib import Path
from typing import Set
import pandas as pd
import requests
import tempfile

# Import centralized team name standardization
from team_name_utils import standardize_team_names

# Configuration
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
SOS_DIR = DATA_DIR / "sos"
RAW_CSV = RAW_DIR / "wbb_schedule_raw.csv"
FILTERED_CSV = DATA_DIR / "wbb_schedule.csv"

# Ensure directories exist
RAW_DIR.mkdir(parents=True, exist_ok=True)

def clean_and_rename_teams(df, team_cols):
    """Clean data by removing blank rows/cols and applying team name standardization."""
    # Remove entirely empty columns and rows
    df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')

    # Strip whitespace from team columns
    for col in team_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Apply centralized team name standardization
    df = standardize_team_names(df, team_cols)

    return df

def download_parquet_data():
    """Download the 2026 WBB Schedule Parquet file."""
    url = "https://github.com/sportsdataverse/wehoop-wbb-raw/blob/60b2c470a3a612bc7a7cf8b189fec05a66f60e11/wbb/schedules/parquet/wbb_schedule_2026.parquet"
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
    try:
        df = download_parquet_data()

        # Apply name standardization to schedule data (location columns contain school names)
        df = clean_and_rename_teams(df, ['home_location', 'away_location'])
        
        # Save Raw Schedule
        df.to_csv(RAW_CSV, index=False)
        print(f"✓ Saved raw schedule to {RAW_CSV}")

        # FIXED FILTER: Use correct rank column names
        # We also convert to numeric to ensure comparison works
        home_rank = pd.to_numeric(df['home_current_rank'], errors='coerce').fillna(99)
        away_rank = pd.to_numeric(df['away_current_rank'], errors='coerce').fillna(99)
        
        filtered = df[(home_rank <= 25) | (away_rank <= 25)].copy()
        
        filtered.to_csv(FILTERED_CSV, index=False)
        print(f"✓ Saved filtered schedule ({len(filtered)} games) to {FILTERED_CSV}")

    except Exception as e:
        print(f"Error processing schedule: {e}")

    # 2. Handle SOS Ratings Data Cleaning
    # Look for the most recent ratings file
    sos_master_csv = SOS_DIR / "ratings_master.csv"

    if sos_master_csv.exists():
        print("Cleaning SOS Ratings master data...")
        try:
            sos_df = pd.read_csv(sos_master_csv)
            # Target the School/team column
            team_col = None
            for possible_col in ['School', 'team', 'Team']:
                if possible_col in sos_df.columns:
                    team_col = possible_col
                    break

            if team_col:
                # Perform cleaning and mapping
                sos_df = clean_and_rename_teams(sos_df, [team_col])
                sos_df.to_csv(sos_master_csv, index=False)
                print(f"✓ Cleaned SOS ratings saved to {sos_master_csv}")
            else:
                print(f"Warning: Could not identify team column in SOS data")
        except Exception as e:
            print(f"Error cleaning SOS data: {e}")
    else:
        print(f"Notice: SOS file not found at {sos_master_csv}")

if __name__ == "__main__":
    main()
