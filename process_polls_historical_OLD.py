"""
Process historical polls data into analysis-ready long format.

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
    - Numeric week indicators

    Non-week columns: Rk, Rank, School, Team, Conf, Conference, etc.
    """
    s = normalize_col(col_label)
    s_low = s.lower()

    # Exclude known non-week columns
    if s_low in {"rk", "rank", "prev", "previous", "chng", "change",
                 "conf", "conference", "school", "team", "year", "table_number"}:
        return False

    # Include preseason
    if s_low in {"pre", "preseason"}:
        return True

    # Include date patterns (11/13, 12/1, etc.)
    if re.match(r"^\d{1,2}[\/\-.]\d{1,2}$", s):
        return True

    # Include numeric week indicators
    if any(ch.isdigit() for ch in s) and len(s) <= 10:
        if len(re.findall(r"\d", s)) >= 2:
            return True

    return False


def process_historical_to_long(master_csv_path: Path) -> pd.DataFrame:
    """
    Convert wide-format historical polls data to long format.

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
    print(f"  ✓ Loaded {len(df)} rows")

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

    # Process each year/table combination separately
    long_parts = []

    for year in years:
        year_data = df[df['year'] == year]

        # Get unique table numbers for this year
        if 'table_number' in year_data.columns:
            tables = year_data['table_number'].unique()
        else:
            tables = [1]  # Assume single table if no table_number column

        for table_num in tables:
            if 'table_number' in year_data.columns:
                table_data = year_data[year_data['table_number'] == table_num].copy()
            else:
                table_data = year_data.copy()

            print(f"  Processing {year} - Table {table_num} ({len(table_data)} rows)")

            # Handle multi-level columns
            if isinstance(table_data.columns, pd.MultiIndex):
                table_data.columns = table_data.columns.get_level_values(-1)

            table_data.columns = [str(c).strip() for c in table_data.columns]

            # Find key columns
            cols = list(table_data.columns)
            school_col = find_col(cols, {"school", "team"})
            conf_col = find_col(cols, {"conf", "conference"})

            if school_col is None:
                print(f"    ⚠ No School/Team column found for {year} table {table_num}, skipping")
                continue

            # Remove rows with no school name
            table_data = table_data[table_data[school_col].notna()]
            table_data = table_data[table_data[school_col].str.lower() != 'school']

            # Find week columns
            week_cols = [c for c in cols if is_week_col(c)]

            if not week_cols:
                print(f"    ⚠ No week columns found for {year} table {table_num}, skipping")
                continue

            print(f"    Found {len(week_cols)} week columns")

            # Prepare data for melting
            id_vars = [school_col]
            if conf_col is not None:
                id_vars.append(conf_col)

            # Add year and table_number to id_vars if they exist
            if 'year' in table_data.columns:
                id_vars.append('year')
            if 'table_number' in table_data.columns:
                id_vars.append('table_number')

            # Melt to long format
            melted = table_data.melt(
                id_vars=id_vars,
                value_vars=week_cols,
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

            # Convert rank to numeric
            melted['rank'] = pd.to_numeric(melted['rank'], errors='coerce')

            # Add rank_numeric (26 for unranked)
            melted['rank_numeric'] = melted['rank'].fillna(26).astype(int)

            # Add table_id for consistency with current polls
            melted['table_id'] = table_num

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
    print("Historical Polls Data Processor")
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
    print(f"  Ranked (1-25): {(long_df['rank_numeric'] <= 25).sum():,} rows")
    print(f"  Unranked (26): {(long_df['rank_numeric'] == 26).sum():,} rows")

    print("\n" + "=" * 70)
    print("✅ Processing Complete!")
    print("=" * 70)
    print(f"Output file: {LONG_FILE}")
    print("=" * 70)
    print()


if __name__ == "__main__":
    main()
