"""
Scrape WBB Team Box Score Data from sportsdataverse wehoop-wbb-data repository.

This script:
1. Downloads parquet data from GitHub
2. Maintains a raw CSV with idempotency (no duplicate game_id)
3. Creates a filtered CSV for ranked teams with "once ranked" tracking
4. Only includes games where at least one team is or was ever ranked (1-25)

Output files:
- data/raw/wbb_teambox_raw.csv (all games, no duplicates)
- data/wbb_team_box/wbb_teambox_filtered.csv (ranked teams only)
"""
import os
from datetime import date
from pathlib import Path
from typing import Set

import pandas as pd
import requests

# Configuration
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
TEAMBOX_DIR = DATA_DIR / "wbb_team_box"

RAW_CSV = RAW_DIR / "wbb_teambox_raw.csv"
FILTERED_CSV = TEAMBOX_DIR / "wbb_teambox_filtered.csv"

PARQUET_URL = "https://github.com/sportsdataverse/wehoop-wbb-data/raw/main/wbb/team_box/parquet/team_box_2026.parquet"

# Ensure directories exist
RAW_DIR.mkdir(parents=True, exist_ok=True)
TEAMBOX_DIR.mkdir(parents=True, exist_ok=True)


def download_parquet_data(url: str) -> pd.DataFrame:
    """
    Download parquet file from URL and return as DataFrame.

    Args:
        url: URL to the parquet file

    Returns:
        pd.DataFrame: Data from the parquet file
    """
    print(f"Downloading parquet data from:\n  {url}")

    # Download the file to memory
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers, timeout=60)
    response.raise_for_status()

    import tempfile
    # Save to temporary file and read
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
        temp_path = Path(temp_file.name)
        temp_file.write(response.content)

        # Read parquet file
        df = pd.read_parquet(temp_path)
        print(f"  ✓ Downloaded {len(df)} rows")
        return df

    finally:
        # Clean up temp file
        if temp_path.exists():
            temp_path.unlink()


def get_existing_game_ids(csv_path: Path) -> Set[str]:
    """
    Get set of existing game IDs from a CSV file.

    Args:
        csv_path: Path to CSV file

    Returns:
        Set[str]: Set of game IDs that already exist
    """
    if not csv_path.exists():
        return set()

    try:
        existing = pd.read_csv(csv_path)

        # Try to find the ID column (could be 'game_id' or 'id')
        id_col = None
        for col in ['game_id', 'id']:
            if col in existing.columns:
                id_col = col
                break

        if id_col is None:
            print(f"  ⚠ Warning: No game_id or id column found in {csv_path.name}")
            return set()

        game_ids = set(existing[id_col].astype(str).unique())
        print(f"  Found {len(game_ids)} existing game IDs in {csv_path.name}")
        return game_ids

    except Exception as e:
        print(f"  ⚠ Error reading {csv_path.name}: {e}")
        return set()


def get_once_ranked_teams(filtered_csv_path: Path) -> Set[str]:
    """
    Build a set of team names that have ever been ranked (1-25) in our history.

    Scans the existing filtered CSV to find all unique team names that appeared
    with a current_rank between 1-25.

    Args:
        filtered_csv_path: Path to the filtered CSV file

    Returns:
        Set[str]: Set of team names that have ever been ranked
    """
    if not filtered_csv_path.exists():
        print("  No existing filtered CSV - starting fresh 'once ranked' list")
        return set()

    try:
        existing = pd.read_csv(filtered_csv_path)
        once_ranked = set()

        # Check home teams
        if 'home_team' in existing.columns and 'home_current_rank' in existing.columns:
            home_ranked = existing[
                (existing['home_current_rank'].notna()) &
                (existing['home_current_rank'] >= 1) &
                (existing['home_current_rank'] <= 25)
            ]['home_team'].unique()
            once_ranked.update(home_ranked)

        # Check away teams
        if 'away_team' in existing.columns and 'away_current_rank' in existing.columns:
            away_ranked = existing[
                (existing['away_current_rank'].notna()) &
                (existing['away_current_rank'] >= 1) &
                (existing['away_current_rank'] <= 25)
            ]['away_team'].unique()
            once_ranked.update(away_ranked)

        print(f"  Found {len(once_ranked)} teams that have been ranked in history")
        if once_ranked:
            # Show sample for verification
            sample = sorted(list(once_ranked))[:10]
            print(f"  Sample: {', '.join(sample)}")

        return once_ranked

    except Exception as e:
        print(f"  ⚠ Error reading filtered CSV for once-ranked teams: {e}")
        return set()


def filter_ranked_games(df: pd.DataFrame, once_ranked_teams: Set[str]) -> pd.DataFrame:
    """
    Filter DataFrame to only include games with ranked teams.

    Includes games where:
    - home_current_rank OR away_current_rank is between 1-25, OR
    - home_team OR away_team is in the once_ranked_teams set

    Args:
        df: DataFrame with team box score data
        once_ranked_teams: Set of team names that have ever been ranked

    Returns:
        pd.DataFrame: Filtered DataFrame
    """
    print("\nFiltering for ranked games...")
    print(f"  Total rows before filtering: {len(df)}")

    # Make a copy to avoid modifying the original
    df = df.copy()

    # Convert rank columns to numeric (handle NaN and 'UNRANKED_VALUE' for unranked)
    UNRANKED_VALUE = 99
    home_rank = pd.to_numeric(df.get('home_current_rank', pd.Series([UNRANKED_VALUE] * len(df))), errors='coerce')
    away_rank = pd.to_numeric(df.get('away_current_rank', pd.Series([UNRANKED_VALUE] * len(df))), errors='coerce')

    # Condition 1: Currently ranked (1-25)
    currently_ranked = (
        ((home_rank >= 1) & (home_rank <= 25)) |
        ((away_rank >= 1) & (away_rank <= 25))
    )

    # Condition 2: Once ranked (in historical list)
    once_ranked_condition = False
    if once_ranked_teams:
        home_once_ranked = df.get('home_team', pd.Series([None] * len(df))).isin(once_ranked_teams)
        away_once_ranked = df.get('away_team', pd.Series([None] * len(df))).isin(once_ranked_teams)
        once_ranked_condition = home_once_ranked | away_once_ranked

    # Combined filter
    mask = currently_ranked | once_ranked_condition

    filtered = df[mask].copy()

    print(f"  Rows passing 'currently ranked' filter: {currently_ranked.sum()}")
    if once_ranked_teams:
        print(f"  Additional rows from 'once ranked' filter: {(once_ranked_condition & ~currently_ranked).sum()}")
    print(f"  Total rows after filtering: {len(filtered)}")

    return filtered


def append_new_rows(new_df: pd.DataFrame, csv_path: Path, id_column: str = 'game_id') -> None:
    """
    Append only new rows to a CSV file based on unique ID.

    Args:
        new_df: DataFrame with new data
        csv_path: Path to CSV file
        id_column: Column name to use for deduplication
    """
    if new_df.empty:
        print(f"  No new rows to append to {csv_path.name}")
        return

    # Ensure the ID column exists
   def append_new_rows(csv_path, new_df, id_column='game_id'):
    """Appends only new rows to a CSV based on a unique ID column."""
    if new_df.empty:
        return

    # Ensure the ID column exists
    if id_column not in new_df.columns:
        raise ValueError(f"Critical Error: Unique ID column '{id_column}' not found in data. "
                         "Cannot guarantee idempotency. Please check the data source schema.")

    if csv_path.exists():
        existing_df = pd.read_csv(csv_path)
        # Filter for rows not already in the CSV
        existing_ids = set(existing_df[id_column].astype(str))
        new_df = new_df[~new_df[id_column].astype(str).isin(existing_ids)]
        
        if not new_df.empty:
            new_df.to_csv(csv_path, mode='a', header=False, index=False)
            print(f"  ✓ Appended {len(new_df)} new rows to {csv_path.name}")
        else:
            print(f"  → No new rows to add to {csv_path.name}")
    else:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        new_df.to_csv(csv_path, mode='w', header=True, index=False)
        print(f"  ✓ Created {csv_path.name} with {len(new_df)} rows")

    # Get existing IDs
    existing_ids = get_existing_game_ids(csv_path)

    # Filter to only new rows
    new_df_ids = new_df[id_column].astype(str)
    mask = ~new_df_ids.isin(existing_ids)
    rows_to_append = new_df[mask]

    if rows_to_append.empty:
        print(f"  ✓ No new rows to append to {csv_path.name} (all already exist)")
        return

    # Append to CSV
    if csv_path.exists():
        rows_to_append.to_csv(csv_path, mode='a', header=False, index=False)
        print(f"  ✓ Appended {len(rows_to_append)} new rows to {csv_path.name}")
    else:
        rows_to_append.to_csv(csv_path, mode='w', header=True, index=False)
        print(f"  ✓ Created {csv_path.name} with {len(rows_to_append)} rows")


def main():
    """Main function to scrape and process team box score data."""
    print("=" * 60)
    print("WBB Team Box Score Scraper")
    print("=" * 60)
    print(f"Run Date: {date.today().strftime('%Y-%m-%d')}\n")

    try:
        # Step 1: Download parquet data
        def main():
    # 1. Download the data
    df = download_parquet_data(PARQUET_URL)
    
    if df.empty:
        return

    # 2. Determine the ID column ONCE here
    # This addresses the duplication mentioned in the review
    id_col = 'game_id' if 'game_id' in df.columns else 'id'
    print(f"  → Detected unique identifier: {id_col}")

    # ... [Perform your filtering/processing] ...

    # 3. Pass the 'id_col' variable as an argument to the append function
    # This uses the refactored version of append_new_rows you just committed
    append_new_rows(RAW_CSV_PATH, df, id_column=id_col)
    
    # Do the same for your filtered dataset
    append_new_rows(FILTERED_CSV_PATH, filtered_df, id_column=id_col)

        # Step 2: Save raw data with idempotency
        print("Step 2: Updating raw CSV...")
        append_new_rows(df, RAW_CSV, id_column=id_col)

        # Step 3: Build "once ranked" list from existing filtered data
        print("\nStep 3: Building 'once ranked' team list...")
        once_ranked = get_once_ranked_teams(FILTERED_CSV)

        # Step 4: Filter new data for ranked games
        print("\nStep 4: Filtering for ranked games...")
        filtered_df = filter_ranked_games(df, once_ranked)

        # Step 5: Append filtered data
        print("\nStep 5: Updating filtered CSV...")
        append_new_rows(filtered_df, FILTERED_CSV, id_column=id_col)

        # Summary
        print("\n" + "=" * 60)
        print("✅ Scrape completed successfully!")
        print("=" * 60)
        print(f"Raw data: {RAW_CSV}")
        print(f"Filtered data: {FILTERED_CSV}")
        print("=" * 60 + "\n")

    except Exception as e:
        print("\n" + "=" * 60)
        print(f"❌ Error during scrape: {e}")
        print("=" * 60 + "\n")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
