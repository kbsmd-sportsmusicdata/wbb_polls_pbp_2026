"""
Test script for WBB play-by-play data processing.
Tests the initial setup and filtering logic with start_year=2025.
"""

import os
import pandas as pd
import argparse
from pathlib import Path

path_to_raw = "wbb/json/raw"
path_to_final = "wbb/json/final"
path_to_errors = "wbb/errors"
run_processing = True
rescrape_all = False

def main():
    print("=" * 70)
    print("WBB Play-by-Play Script Test (Start Year: 2025)")
    print("=" * 70)
    print()

    # Process arguments
    if args.start_year < 2002:
        start_year = 2002
    else:
        start_year = args.start_year

    if args.end_year is None:
        end_year = start_year
    else:
        end_year = args.end_year

    years_arr = range(start_year, end_year + 1)

    print(f"Processing seasons: {list(years_arr)}")
    print()

    # Load schedule
    print("Step 1: Loading schedule from wbb_schedule_master.parquet...")
    try:
        schedule = pd.read_parquet('wbb_schedule_master.parquet', engine='auto', columns=None)
        print(f"  ✓ Loaded {len(schedule)} total games")
    except FileNotFoundError:
        print("  ✗ Error: wbb_schedule_master.parquet not found")
        return

    # Sort by season and season_type
    print("\nStep 2: Sorting by season and season_type...")
    schedule = schedule.sort_values(by=['season', 'season_type'], ascending=True)
    print(f"  ✓ Sorted")

    # Convert game_id to int
    print("\nStep 3: Converting game_id to integer...")
    schedule["game_id"] = schedule["game_id"].astype(int)
    print(f"  ✓ Converted")

    # Filter to completed games only
    print("\nStep 4: Filtering to completed games only...")
    initial_count = len(schedule)
    schedule = schedule[schedule['status_type_completed'] == True]
    print(f"  ✓ Filtered from {initial_count} to {len(schedule)} completed games")

    # Check if we should skip already-scraped games
    print("\nStep 5: Checking for already-scraped games...")
    if args.rescrape == False:
        repo_file = 'wbb/wbb_games_in_data_repo.parquet'
        if Path(repo_file).exists():
            schedule_in_repo = pd.read_parquet(repo_file, engine='auto', columns=None)
            schedule_in_repo["game_id"] = schedule_in_repo["game_id"].astype(int)
            done_already = schedule_in_repo['game_id']
            before_filter = len(schedule)
            schedule = schedule[~schedule['game_id'].isin(done_already)]
            print(f"  ✓ Skipped {before_filter - len(schedule)} already-scraped games")
            print(f"  ✓ {len(schedule)} games remaining to scrape")
        else:
            print(f"  ! Repo file not found: {repo_file}")
            print(f"    Will process all {len(schedule)} completed games")
    else:
        print(f"  ! Rescrape mode: Will process all {len(schedule)} completed games")

    # Filter to games from 2002 onwards (when PBP became available)
    print("\nStep 6: Filtering to seasons >= 2002 (PBP availability)...")
    before_2002_filter = len(schedule)
    schedule_with_pbp = schedule[schedule['season'] >= 2002]
    print(f"  ✓ Filtered from {before_2002_filter} to {len(schedule_with_pbp)} games")

    # Filter to requested year range
    print(f"\nStep 7: Filtering to requested seasons ({start_year}-{end_year})...")
    before_year_filter = len(schedule_with_pbp)
    schedule_with_pbp = schedule_with_pbp[schedule_with_pbp['season'].isin(years_arr)]
    print(f"  ✓ Filtered from {before_year_filter} to {len(schedule_with_pbp)} games")

    # Show summary by season
    if len(schedule_with_pbp) > 0:
        print("\n" + "=" * 70)
        print("Summary by Season:")
        print("=" * 70)
        season_summary = schedule_with_pbp.groupby('season').agg(
            total_games=('game_id', 'count'),
            completed=('status_type_completed', 'sum')
        )
        print(season_summary)

        print("\n" + "=" * 70)
        print(f"Total games ready for PBP scraping: {len(schedule_with_pbp)}")
        print("=" * 70)
    else:
        print("\n⚠️  No games found matching criteria!")

    return schedule_with_pbp


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test WBB PBP scraping setup')
    parser.add_argument('--start_year', '-s', type=int, required=True,
                        help='Start year (YYYY), e.g., 2025 for 2024-25 season')
    parser.add_argument('--end_year', '-e', type=int, default=None,
                        help='End year (YYYY), optional')
    parser.add_argument('--rescrape', action='store_true', default=False,
                        help='Rescrape all games (ignore already-scraped list)')

    args = parser.parse_args()

    result = main()

    if result is not None and len(result) > 0:
        print("\n✅ Test completed successfully!")
        print(f"Ready to scrape {len(result)} games for season(s): {sorted(result['season'].unique())}")
    else:
        print("\n⚠️  Test found no games to process")
