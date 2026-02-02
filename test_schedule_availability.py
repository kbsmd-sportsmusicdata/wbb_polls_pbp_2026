"""
Test script to diagnose WBB schedule data availability for 2025-2026 season.
Checks if December and January games are present in the sportsdataverse API.
"""

import pandas as pd
import sportsdataverse as sdv
from datetime import datetime
from tqdm import tqdm

def test_schedule_2026():
    """
    Test schedule availability for 2025-2026 season.

    Returns diagnostic information about what data is available.
    """
    season = 2026
    print("=" * 70)
    print(f"Testing WBB Schedule Data for {season-1}-{season} Season")
    print("=" * 70)
    print()

    # Step 1: Get the calendar
    print("Step 1: Fetching calendar...")
    try:
        df_calendar = sdv.wbb.espn_wbb_calendar(season, ondays=True)
        calendar_dates = df_calendar['dateURL'].tolist()
        print(f"  ✓ Found {len(calendar_dates)} dates in calendar")
        print(f"  Date range: {calendar_dates[0]} to {calendar_dates[-1]}")
        print()
    except Exception as e:
        print(f"  ✗ Error fetching calendar: {e}")
        return

    # Step 2: Fetch schedules for all dates
    print("Step 2: Fetching schedules for all calendar dates...")
    all_games = pd.DataFrame()

    for date_url in tqdm(calendar_dates, desc="Downloading schedules"):
        try:
            date_schedule = sdv.wbb.espn_wbb_schedule(dates=date_url, groups=50)
            all_games = pd.concat([all_games, date_schedule], axis=0, ignore_index=True)
        except Exception as e:
            print(f"  ! Error fetching {date_url}: {e}")
            continue

    print(f"\n  ✓ Downloaded {len(all_games)} total games")
    print()

    # Step 3: Filter to regular season and postseason (season_type 2 and 3)
    if 'season_type' in all_games.columns:
        filtered_games = all_games[all_games['season_type'].isin([2, 3])]
        print(f"Step 3: Filtered to regular/postseason games")
        print(f"  ✓ {len(filtered_games)} games (season_type 2 or 3)")
        print()
    else:
        print("  ⚠ No 'season_type' column found")
        filtered_games = all_games

    # Step 4: Analyze date distribution
    print("Step 4: Analyzing game date distribution...")
    if 'game_date' in filtered_games.columns:
        filtered_games['game_date_parsed'] = pd.to_datetime(filtered_games['game_date'], errors='coerce')
        filtered_games['month'] = filtered_games['game_date_parsed'].dt.month
        filtered_games['year'] = filtered_games['game_date_parsed'].dt.year

        month_counts = filtered_games['month'].value_counts().sort_index()
        month_names = {11: 'November', 12: 'December', 1: 'January',
                      2: 'February', 3: 'March', 4: 'April'}

        print("\n  Games by month:")
        for month, count in month_counts.items():
            month_name = month_names.get(month, f"Month {month}")
            print(f"    {month_name:12} {count:4} games")
        print()

    # Step 5: Check game completion status
    print("Step 5: Checking game completion status...")
    if 'status_type_completed' in filtered_games.columns:
        completed = filtered_games['status_type_completed'].sum()
        total = len(filtered_games)
        print(f"  Completed games: {completed} / {total} ({completed/total*100:.1f}%)")

        # Break down by month
        if 'month' in filtered_games.columns:
            print("\n  Completed games by month:")
            completion_by_month = filtered_games.groupby('month').agg(
                total_games=('game_id', 'count'),
                completed_games=('status_type_completed', 'sum')
            )

            for month, row in completion_by_month.iterrows():
                month_name = month_names.get(month, f"Month {month}")
                pct = row['completed_games'] / row['total_games'] * 100 if row['total_games'] > 0 else 0
                print(f"    {month_name:12} {int(row['completed_games']):4} / {int(row['total_games']):4} ({pct:.1f}%)")
    print()

    # Step 6: Sample data
    print("Step 6: Sample of December/January games (if any)...")
    if 'month' in filtered_games.columns:
        dec_jan_games = filtered_games[filtered_games['month'].isin([12, 1])]

        if len(dec_jan_games) > 0:
            print(f"\n  Found {len(dec_jan_games)} games in December/January")

            # Show sample
            sample = dec_jan_games[['game_date', 'home_team_name', 'away_team_name',
                                    'status_type_completed', 'status_type_name']].head(10)
            print("\n  Sample games:")
            print(sample.to_string(index=False))
        else:
            print("  ⚠ No December or January games found!")
            print("  This may indicate:")
            print("    - Games haven't been scheduled yet by ESPN")
            print("    - API hasn't been updated with future games")
            print("    - Data is still being populated")

    print()
    print("=" * 70)
    print("Diagnosis Summary")
    print("=" * 70)

    # Save diagnostic data
    output_file = "data/raw/schedule_2026_diagnostic.csv"
    filtered_games.to_csv(output_file, index=False)
    print(f"✓ Saved full diagnostic data to: {output_file}")
    print(f"  Total rows: {len(filtered_games)}")

    if 'month' in filtered_games.columns:
        has_dec_jan = len(filtered_games[filtered_games['month'].isin([12, 1])]) > 0
        if has_dec_jan:
            print("✓ December/January games ARE present in the data")
        else:
            print("✗ December/January games are NOT present in the data")
            print("  → This is likely a source data limitation from ESPN/sportsdataverse")

    print("=" * 70)
    print()

    return filtered_games


if __name__ == "__main__":
    print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    try:
        result = test_schedule_2026()
        print("\n✅ Test completed successfully!")

    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
