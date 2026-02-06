"""
Process historical polls data into analysis-ready long format.

FIXED VERSION - Properly handles multi-year data with different week columns per year.

This script transforms the raw historical polls tables into a clean,
normalized long format suitable for data analysis and visualization.

Input: data/polls_historical/polls_historical_master.csv (wide format)
Output: data/polls_historical/polls_historical_long.csv (long format)

The long format provides one row per team per week per year, making it easy to:
- Filter by year, team, or week
- Analyze ranking trends over time
- Compare teams across seasons
- Visualize rankings evolution
"""
import re
from datetime import date
from pathlib import Path
from typing import Set

import pandas as pd
import numpy as np

# Paths
DATA_DIR = Path("data")
POLLS_HISTORICAL_DIR = DATA_DIR / "polls_historical"

MASTER_FILE = POLLS_HISTORICAL_DIR / "polls_historical_master.csv"
LONG_FILE = POLLS_HISTORICAL_DIR / "polls_historical_long.csv"


def strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace from all string values in a DataFrame."""
    return df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))


def normalize_col(c) -> str:
    """Normalize column names by extracting the last non-empty part."""
    if isinstance(c, (tuple, list)):
        parts = [str(x).strip() for x in c if str(x).strip() and str(x).strip().lower() != "nan"]
        return parts[-1] if parts else ""
    return str(c).strip()


def find_col(cols, candidates: Set[str]):
    """Find a column name that matches one of the candidates."""
    for c in cols:
        if normalize_col(c).lower() in candidates:
            return c
    return None


def is_week_col(col_label) -> bool:
    """
    Determine if a column represents a poll week.

    Week columns include:
    - 'Pre' or 'Preseason'
    - Dates like '11/13', '12/1'
    - 'Post' or 'Final'

    Non-week columns: Rk, Rank, School, Team, Conf, Conference, year, table_number, etc.
    """
    s = normalize_col(col_label)
    s_low = s.lower()

    # Exclude known non-week columns
    if s_low in {"rk", "rank", "prev", "previous", "chng", "change",
                 "conf", "conference", "school", "team", "year", "table_number"}:
        return False

    # Include preseason and postseason
    if s_low in {"pre", "preseason", "post", "postseason", "final"}:
        return True

    # Include date patterns (11/13, 12/1, 1/6, etc.)
    if re.match(r"^\d{1,2}[\/\-.]\d{1,2}$", s):
        return True

    return False


def identify_valid_weeks_for_year(df_year: pd.DataFrame, week_cols: list) -> list:
    """
    Identify which week columns actually have data for a given year.

    A week column is valid if it has at least one non-null rank value.

    Args:
        df_year: DataFrame for a single year
        week_cols: List of potential week column names

    Returns:
        List of week column names that have data
    """
    return [col for col in week_cols if df_year[col].notna().any()]


def process_historical_to_long(master_csv_path: Path) -> pd.DataFrame:
    """
    Convert wide-format historical polls data to long format.

    FIXED: Properly handles years with different week columns by:
    1. Identifying valid weeks per year (weeks with actual data)
    2. Only melting those valid weeks
    3. Filtering out rows where rank is genuinely null (week doesn't apply)

    Args:
        master_csv_path: Path to the master CSV file

    Returns:
        pd.DataFrame: Long-format DataFrame with one row per team/week/year
    """
    print(f"Reading master file: {master_csv_path}")

    if not master_csv_path.exists():
        print(f"  ✗ Master file not found: {master_csv_path}")
        return pd.DataFrame()

    # Read the master file
    df = pd.read_csv(master_csv_path)
    print(f"  ✓ Loaded {len(df)} rows, {len(df.columns)} columns")

    if df.empty:
        print("  ✗ Master file is empty")
        return pd.DataFrame()

    # Check for year column
    if 'year' not in df.columns:
        print("  ✗ 'year' column not found in master file")
        return pd.DataFrame()

    # Get unique years and table numbers
    years = sorted(df['year'].unique())
    print(f"  ✓ Found {len(years)} years: {min(years)} to {max(years)}")

    # Identify all potential week columns (before we know which are valid per year)
    all_cols = list(df.columns)
    school_col = find_col(all_cols, {"school", "team"})
    conf_col = find_col(all_cols, {"conf", "conference"})

    if school_col is None:
        print(f"  ✗ No School/Team column found")
        return pd.DataFrame()

    # Find all potential week columns
    potential_week_cols = [c for c in all_cols if is_week_col(c)]
    print(f"  ✓ Found {len(potential_week_cols)} potential week columns across all years")

    # Process each year separately
    long_parts = []

    for year in years:
        year_data = df[df['year'] == year].copy()

        # Get unique table numbers for this year
        if 'table_number' in year_data.columns:
            tables = year_data['table_number'].unique()
        else:
            tables = [1]

        for table_num in tables:
            if 'table_number' in year_data.columns:
                table_data = year_data[year_data['table_number'] == table_num].copy()
            else:
                table_data = year_data.copy()

            print(f"  Processing {year} - Table {table_num} ({len(table_data)} teams)")

            # Remove rows with no school name or header rows
            table_data = table_data[table_data[school_col].notna()]
            table_data = table_data[table_data[school_col].str.lower() != 'school']

            if len(table_data) == 0:
                print(f"    ⚠ No valid teams found for {year} table {table_num}, skipping")
                continue

            # CRITICAL FIX: Identify which weeks actually have data for THIS year
            valid_week_cols = identify_valid_weeks_for_year(table_data, potential_week_cols)

            if not valid_week_cols:
                print(f"    ⚠ No valid week columns found for {year} table {table_num}, skipping")
                continue

            print(f"    Found {len(valid_week_cols)} valid weeks: {', '.join(valid_week_cols[:5])}{'...' if len(valid_week_cols) > 5 else ''}")

            # Prepare data for melting
            id_vars = [school_col]
            if conf_col is not None and conf_col in table_data.columns:
                id_vars.append(conf_col)
            if 'year' in table_data.columns:
                id_vars.append('year')
            if 'table_number' in table_data.columns:
                id_vars.append('table_number')

            # Melt ONLY the valid week columns for this year
            melted = table_data.melt(
                id_vars=id_vars,
                value_vars=valid_week_cols,
                var_name='poll_week',
                value_name='rank'
            )

            # Normalize column names
            melted['poll_week'] = melted['poll_week'].map(normalize_col)

            # Rename columns to standard names
            rename_map = {school_col: 'team'}
            if conf_col is not None:
                rename_map[conf_col] = 'conference'

            melted.rename(columns=rename_map, inplace=True)

            # Add conference column if it doesn't exist
            if 'conference' not in melted.columns:
                melted['conference'] = None

            # CRITICAL FIX: Filter out rows where rank is null
            # These are weeks that don't apply to this team/year combination
            # (e.g., team didn't exist yet, or week column was from a different year)
            initial_rows = len(melted)
            melted = melted[melted['rank'].notna()].copy()
            filtered_rows = initial_rows - len(melted)
            if filtered_rows > 0:
                print(f"    Filtered {filtered_rows} rows with null ranks (non-applicable weeks)")

            # Convert rank to float
            melted['rank'] = pd.to_numeric(melted['rank'], errors='coerce')

            # Create rank_numeric
            # - For ranked teams (1-25): use their rank
            # - For unranked teams with empty rank: use 26
            # Note: After filtering nulls above, remaining empty ranks are teams that
            #       appeared in the dataset that week but weren't ranked
            melted['rank_numeric'] = melted['rank'].fillna(26).astype(int)

            # Add table_id for consistency with current polls
            melted['table_id'] = table_num

            print(f"    Result: {len(melted)} rows")

            long_parts.append(melted)

    if not long_parts:
        print("  ✗ No data processed")
        return pd.DataFrame()

    # Combine all processed data
    print("\nCombining all data...")
    combined = pd.concat(long_parts, ignore_index=True)

    # Reorder columns for clarity
    columns_order = ['year', 'team', 'conference', 'poll_week', 'rank',
                     'rank_numeric', 'table_id']

    # Only include columns that exist
    columns_order = [c for c in columns_order if c in combined.columns]

    combined = combined[columns_order]

    print(f"  ✓ Total rows in long format: {len(combined):,}")
    print(f"  ✓ Unique years: {len(combined['year'].unique())}")
    print(f"  ✓ Unique teams: {len(combined['team'].unique())}")
    print(f"  ✓ Unique weeks: {len(combined['poll_week'].unique())}")

    return combined


def main():
    """Main function to process historical polls data."""
    print("=" * 70)
    print("Historical Polls Data Processor (FIXED VERSION)")
    print("=" * 70)
    print(f"Process Date: {date.today().strftime('%Y-%m-%d')}")
    print("=" * 70)
    print()

    # Check if master file exists
    if not MASTER_FILE.exists():
        print(f"✗ Master file not found: {MASTER_FILE}")
        print(f"\nPlease run 'python polls_historical.py' first to scrape the data.")
        print("=" * 70)
        return

    # Process to long format
    print("Processing historical polls to long format...\n")
    long_df = process_historical_to_long(MASTER_FILE)

    if long_df.empty:
        print("\n✗ No data to save")
        print("=" * 70)
        return

    # Save long format
    print(f"\nSaving long format to: {LONG_FILE}")
    long_df.to_csv(LONG_FILE, index=False)
    print(f"  ✓ Saved {len(long_df):,} rows")

    # Display sample data
    print("\n" + "=" * 70)
    print("Sample Data (First 20 rows)")
    print("=" * 70)
    print(long_df.head(20).to_string(index=False))

    # Display summary statistics
    print("\n" + "=" * 70)
    print("Summary Statistics")
    print("=" * 70)

    print(f"\nYears covered:")
    year_counts = long_df.groupby('year').size().sort_index(ascending=False)
    for year, count in year_counts.items():
        print(f"  {year}: {count:,} rows")

    print(f"\nTop 10 teams by appearances:")
    team_counts = long_df['team'].value_counts().head(10)
    for team, count in team_counts.items():
        print(f"  {team}: {count:,} weeks")

    print(f"\nWeeks per season:")
    weeks = long_df['poll_week'].nunique()
    print(f"  {weeks} unique poll weeks across all years")

    print(f"\nRanking distribution:")
    print(f"  Ranked (1-25): {(long_df['rank_numeric'] <= 25).sum():,} rows ({(long_df['rank_numeric'] <= 25).sum() / len(long_df) * 100:.1f}%)")
    print(f"  Unranked (26): {(long_df['rank_numeric'] == 26).sum():,} rows ({(long_df['rank_numeric'] == 26).sum() / len(long_df) * 100:.1f}%)")

    # Show sample data for one year
    sample_year = long_df['year'].max()
    sample_data = long_df[long_df['year'] == sample_year].head(10)
    print(f"\nSample data for year {sample_year}:")
    print(sample_data.to_string(index=False))

    print("\n" + "=" * 70)
    print("✅ Processing Complete!")
    print("=" * 70)
    print(f"Output file: {LONG_FILE}")
    print("=" * 70)
    print()


if __name__ == "__main__":
    main()
