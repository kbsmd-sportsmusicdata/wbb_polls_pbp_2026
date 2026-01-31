"""
Generate enhanced analytics table for AP Poll data with rank changes and movement.

This script transforms the polls_long.csv data into a Tableau-ready analytics table with:
- Deduplication (keeps latest run_date per team/week)
- Proper chronological sorting via week_number field
- Current rank
- Previous week rank
- Rank change (week-over-week)
- Movement category (Rising/Falling/Stable/New/Dropped)
- Weeks in Top 25 (cumulative count)
- All original metadata (team, conference, week, etc.)

Output: data/polls_analytics.csv
"""
import pandas as pd
from pathlib import Path
from datetime import date, datetime

# Paths
DATA_DIR = Path("data")
POLLS_LONG = DATA_DIR / "polls_long.csv"
POLLS_ANALYTICS = DATA_DIR / "polls_analytics.csv"
SCHEDULE_FILTERED = DATA_DIR / "wbb_schedule" / "schedule_filtered.csv"

# Define chronological order of poll weeks for 2025-26 season
# This is crucial for proper rank_change calculations
WEEK_ORDER = {
    'Pre': 0,      # Preseason
    '11/10': 1,
    '11/17': 2,
    '11/24': 3,
    '12/1': 4,
    '12/8': 5,
    '12/15': 6,
    '12/22': 7,
    '1/5': 8,
    '1/12': 9,
    '1/19': 10,
    '1/26': 11,
    # Add more weeks as season progresses
}


def assign_week_number(poll_week: str) -> int:
    """
    Assign a numeric week number for chronological sorting.

    Args:
        poll_week: Poll week string (e.g., 'Pre', '11/10', '1/19')

    Returns:
        int: Week number (0 for Pre, 1-N for subsequent weeks)

    Raises:
        ValueError: If poll_week is not found in WEEK_ORDER dictionary
    """
    if poll_week in WEEK_ORDER:
        return WEEK_ORDER[poll_week]

    # Poll week not found - this is an error that needs to be fixed
    # The fallback logic was buggy and could assign incorrect week numbers.
    # It's safer to require all poll weeks to be explicitly defined.
    raise ValueError(
        f"Poll week '{poll_week}' not found in WEEK_ORDER. "
        f"Please update the WEEK_ORDER dictionary in {__file__} to ensure correct chronological sorting. "
        f"Current defined weeks: {list(WEEK_ORDER.keys())}"
    )


def calculate_movement_category(current_rank: float, prev_rank: float, rank_change: float) -> str:
    """
    Determine movement category based on rank changes.

    Categories:
    - 'New': First time appearing in Top 25 (prev_rank = 26, current_rank <= 25)
    - 'Rising': Rank improved (rank_change > 0)
    - 'Falling': Rank worsened (rank_change < 0)
    - 'Stable': Rank unchanged (rank_change = 0, both ranked)
    - 'Dropped': Fell out of Top 25 (prev_rank <= 25, current_rank = 26)
    - 'Unranked': Never ranked in both weeks (both = 26)

    Args:
        current_rank: Current week's rank (26 = unranked)
        prev_rank: Previous week's rank (26 = unranked)
        rank_change: Rank change value (positive = improvement)

    Returns:
        Movement category string
    """
    # Handle NaN values
    if pd.isna(current_rank) or pd.isna(prev_rank):
        return 'Unknown'

    current_ranked = current_rank <= 25
    prev_ranked = prev_rank <= 25

    # New to Top 25
    if current_ranked and not prev_ranked:
        return 'New'

    # Dropped from Top 25
    if not current_ranked and prev_ranked:
        return 'Dropped'

    # Both unranked
    if not current_ranked and not prev_ranked:
        return 'Unranked'

    # Both ranked - check movement
    if pd.isna(rank_change) or rank_change == 0:
        return 'Stable'
    elif rank_change > 0:
        return 'Rising'
    else:  # rank_change < 0
        return 'Falling'


def calculate_team_identity(group_df: pd.DataFrame, latest_week: int) -> str:
    """
    Calculate Team Identity based on recent 5-week momentum and performance.

    Categories:
    - Newcomer: Appeared in fewer than 3 of last 5 weeks
    - Elite Lock: Low volatility (stdev < 1.5) + Top 5 average rank in last 5 weeks
    - Surging: Avg rank improved by 3+ spots (last 5 vs previous 5 weeks)
    - Falling: Avg rank dropped by 3+ spots (last 5 vs previous 5 weeks)
    - Volatile: High volatility (stdev > 4.5) in last 5 weeks
    - Steady: Stable, consistent performers

    Args:
        group_df: DataFrame for a single team (sorted by week_number)
        latest_week: Latest week number in the dataset

    Returns:
        Team identity category string
    """
    # Get last 5 weeks of data
    recent_5 = group_df[group_df['week_number'] >= latest_week - 4]

    # Newcomer: Appeared in fewer than 3 of last 5 weeks
    if len(recent_5) < 3:
        return "Newcomer"

    # Get only ranked weeks for calculations
    recent_5_ranked = recent_5[recent_5['rank_numeric'] <= 25]

    # If not ranked in recent weeks, return Unranked
    if len(recent_5_ranked) == 0:
        return "Unranked"

    recent_ranks = recent_5_ranked['rank_numeric'].values
    recent_avg = recent_ranks.mean()
    recent_stdev = recent_ranks.std()

    # Elite Lock: Low volatility + Top 5 average rank
    if recent_stdev < 1.5 and recent_avg <= 5:
        return "Elite Lock"

    # Calculate trend (compare last 5 weeks vs previous 5 weeks)
    previous_5 = group_df[
        (group_df['week_number'] >= latest_week - 9) &
        (group_df['week_number'] < latest_week - 4)
    ]
    previous_5_ranked = previous_5[previous_5['rank_numeric'] <= 25]

    if len(previous_5_ranked) >= 2:  # Need at least 2 weeks for comparison
        previous_avg = previous_5_ranked['rank_numeric'].mean()
        # Lower rank number = better, so negative change = improvement
        trend = recent_avg - previous_avg

        # Surging: Improved by 3+ spots (trend is negative)
        if trend <= -3:
            return "Surging"

        # Falling: Dropped by 3+ spots (trend is positive)
        if trend >= 3:
            return "Falling"

    # Volatile: High standard deviation in recent weeks
    if recent_stdev > 4.5:
        return "Volatile"

    # Default: Steady
    return "Steady"


def load_top25_opponent_metrics(schedule_path: Path) -> pd.DataFrame:
    """
    Calculate Top 25 opponent metrics from schedule data.

    Metrics calculated per team:
    - games_vs_top25: Total games played against Top 25 opponents
    - wins_vs_top25: Wins against Top 25 opponents
    - losses_vs_top25: Losses against Top 25 opponents
    - win_pct_vs_top25: Win percentage vs Top 25 (wins/games)

    Args:
        schedule_path: Path to schedule_filtered.csv

    Returns:
        DataFrame with team-level Top 25 opponent metrics
    """
    if not schedule_path.exists():
        print(f"  ⚠ Schedule file not found: {schedule_path}")
        print(f"    Skipping Top 25 opponent metrics")
        return pd.DataFrame(columns=['team', 'games_vs_top25', 'wins_vs_top25',
                                     'losses_vs_top25', 'win_pct_vs_top25'])

    try:
        schedule_df = pd.read_csv(schedule_path)

        # Filter to games vs Top 25 opponents (Opponent Rank <= 25)
        # Note: rank of 99 typically means unranked
        schedule_df['Opponent Rank'] = pd.to_numeric(schedule_df['Opponent Rank'], errors='coerce')
        top25_games = schedule_df[schedule_df['Opponent Rank'] <= 25].copy()

        if len(top25_games) == 0:
            print(f"  ⚠ No Top 25 opponent games found in schedule")
            return pd.DataFrame(columns=['team', 'games_vs_top25', 'wins_vs_top25',
                                         'losses_vs_top25', 'win_pct_vs_top25'])

        # Calculate metrics per team
        top25_games['is_win'] = top25_games['Game Result'] == 'W'

        metrics = top25_games.groupby('Team').agg(
            games_vs_top25=('Opponent', 'count'),
            wins_vs_top25=('is_win', 'sum')
        ).reset_index()

        metrics.rename(columns={'Team': 'team'}, inplace=True)
        metrics['losses_vs_top25'] = metrics['games_vs_top25'] - metrics['wins_vs_top25']
        metrics['win_pct_vs_top25'] = (
            metrics['wins_vs_top25'] / metrics['games_vs_top25']
        ).round(3)

        print(f"  ✓ Calculated Top 25 metrics for {len(metrics)} teams")
        print(f"    Total games vs Top 25: {metrics['games_vs_top25'].sum()}")

        return metrics

    except (pd.errors.ParserError, KeyError, ValueError) as e:
        print(f"  ⚠ Error loading Top 25 metrics: {e}")
        return pd.DataFrame(columns=['team', 'games_vs_top25', 'wins_vs_top25',
                                     'losses_vs_top25', 'win_pct_vs_top25'])


def generate_analytics_table(polls_long_path: Path) -> pd.DataFrame:
    """
    Generate enhanced analytics table from polls_long data.

    Args:
        polls_long_path: Path to polls_long.csv

    Returns:
        pd.DataFrame: Analytics table with movement calculations
    """
    print(f"Reading polls data from: {polls_long_path}")

    if not polls_long_path.exists():
        raise FileNotFoundError(f"Polls data not found: {polls_long_path}")

    # Read data
    df = pd.read_csv(polls_long_path)
    print(f"  ✓ Loaded {len(df)} rows (may contain duplicates)")

    # Step 1: Deduplicate - keep only latest run_date per team/poll_week
    print("\nStep 1: Deduplicating data...")
    print(f"  Original rows: {len(df)}")

    # Sort by run_date to ensure we keep the latest
    df = df.sort_values('run_date')

    # Keep only the last occurrence of each team/poll_week combination
    df_dedup = df.groupby(['team', 'poll_week'], as_index=False).last()

    print(f"  After deduplication: {len(df_dedup)} rows")
    print(f"  Removed {len(df) - len(df_dedup)} duplicate rows")

    # Step 2: Add week_number for proper chronological sorting
    print("\nStep 2: Creating week_number field for chronological sorting...")
    df_dedup['week_number'] = df_dedup['poll_week'].apply(assign_week_number)

    # Verify week numbers assigned
    week_mapping = df_dedup[['poll_week', 'week_number']].drop_duplicates().sort_values('week_number')
    print("  Week number mapping:")
    for _, row in week_mapping.iterrows():
        print(f"    {row['poll_week']:10} → Week {row['week_number']}")

    # Step 3: Sort by team and week_number (CRITICAL for correct calculations)
    print("\nStep 3: Sorting by team and week_number (chronological order)...")
    df_sorted = df_dedup.sort_values(['team', 'week_number']).reset_index(drop=True)
    print(f"  ✓ Sorted {len(df_sorted)} rows")

    print("\nStep 4: Calculating analytics metrics...")

    # Calculate previous week's rank using shift within each team
    df_sorted['prev_rank_numeric'] = df_sorted.groupby('team')['rank_numeric'].shift(1)

    # Calculate rank change (positive = improvement, so prev - current)
    # Example: Rank 10 -> 5 = change of +5 (improved)
    # Example: Rank 5 -> 10 = change of -5 (worsened)
    df_sorted['rank_change'] = df_sorted['prev_rank_numeric'] - df_sorted['rank_numeric']

    # Calculate movement category
    df_sorted['movement_category'] = df_sorted.apply(
        lambda row: calculate_movement_category(
            row['rank_numeric'],
            row['prev_rank_numeric'],
            row['rank_change']
        ),
        axis=1
    )

    # Calculate cumulative weeks in Top 25 for each team
    df_sorted['is_ranked'] = (df_sorted['rank_numeric'] <= 25).astype(int)
    df_sorted['weeks_in_top25'] = df_sorted.groupby('team')['is_ranked'].cumsum()

    # Calculate consecutive weeks ranked (resets when unranked)
    df_sorted['ranked_streak'] = df_sorted.groupby('team')['is_ranked'].apply(
        lambda x: x.groupby((x != x.shift()).cumsum()).cumsum()
    ).reset_index(level=0, drop=True)

    # For unranked teams, set streak to 0
    df_sorted.loc[df_sorted['rank_numeric'] == 26, 'ranked_streak'] = 0

    # Add season field
    df_sorted['season'] = 2026

    # Step 5: Calculate Team Identity (Recent Momentum)
    print("\nStep 5: Calculating Team Identity (Recent Momentum)...")
    latest_week = df_sorted['week_number'].max()

    # Calculate team identity for each team using groupby().apply()
    # The initial sort of df_sorted ensures that each group is already in chronological order.
    identities = df_sorted.groupby('team', sort=False).apply(calculate_team_identity, latest_week=latest_week)
    identities.name = 'team_identity'
    df_sorted = df_sorted.merge(identities, on='team', how='left')

    identity_df = pd.DataFrame(team_identities)
    df_sorted = df_sorted.merge(identity_df, on='team', how='left')

    # Show identity distribution
    identity_counts = df_sorted[df_sorted['week_number'] == latest_week]['team_identity'].value_counts()
    print(f"  ✓ Team Identity Distribution (Latest Week):")
    for identity, count in identity_counts.items():
        print(f"    {identity:15} {count:3} teams")

    # Step 6: Load Top 25 Opponent Metrics
    print("\nStep 6: Loading Top 25 Opponent Metrics...")
    top25_metrics = load_top25_opponent_metrics(SCHEDULE_FILTERED)

    # Merge with analytics table (left join - not all teams may have Top 25 games)
    df_sorted = df_sorted.merge(top25_metrics, on='team', how='left')

    # Fill NaN values with 0 for teams without Top 25 games
    fill_values = {col: 0 for col in top25_metrics.columns if col != 'team'}
    df_sorted.fillna(value=fill_values, inplace=True)

    # Reorder columns for clarity
    columns_order = [
        'season',
        'team',
        'conference',
        'poll_week',
        'week_number',
        'rank',
        'rank_numeric',
        'prev_rank_numeric',
        'rank_change',
        'movement_category',
        'weeks_in_top25',
        'ranked_streak',
        'team_identity',
        'games_vs_top25',
        'wins_vs_top25',
        'losses_vs_top25',
        'win_pct_vs_top25',
        'run_date',
        'table_id'
    ]

    # Only include columns that exist
    columns_order = [col for col in columns_order if col in df_sorted.columns]
    df_final = df_sorted[columns_order]

    print(f"  ✓ Calculated metrics for {df_final['team'].nunique()} teams")
    print(f"  ✓ Identified {len(df_final[df_final['movement_category'] == 'New'])} 'New' entries")
    print(f"  ✓ Identified {len(df_final[df_final['movement_category'] == 'Rising'])} 'Rising' movements")
    print(f"  ✓ Identified {len(df_final[df_final['movement_category'] == 'Falling'])} 'Falling' movements")

    return df_final


def main():
    """Main function to generate analytics table."""
    print("=" * 70)
    print("AP Poll Analytics Table Generator (v2.0 - Deduplicated)")
    print("=" * 70)
    print(f"Process Date: {date.today().strftime('%Y-%m-%d')}")
    print("=" * 70)
    print()

    try:
        # Generate analytics table
        analytics_df = generate_analytics_table(POLLS_LONG)

        # Save to CSV
        print(f"\nSaving analytics table to: {POLLS_ANALYTICS}")
        analytics_df.to_csv(POLLS_ANALYTICS, index=False)
        print(f"  ✓ Saved {len(analytics_df)} rows")

        # Display summary statistics
        print("\n" + "=" * 70)
        print("Summary Statistics")
        print("=" * 70)

        print(f"\nTotal rows: {len(analytics_df):,}")
        print(f"Unique teams: {analytics_df['team'].nunique()}")
        print(f"Poll weeks: {analytics_df['poll_week'].nunique()}")

        print("\nMovement Category Distribution:")
        movement_counts = analytics_df['movement_category'].value_counts()
        for category, count in movement_counts.items():
            pct = (count / len(analytics_df)) * 100
            print(f"  {category:12} {count:4} ({pct:5.1f}%)")

        print("\nTop 5 Teams by Weeks in Top 25:")
        top_teams = analytics_df.groupby('team')['weeks_in_top25'].max().sort_values(ascending=False).head()
        for team, weeks in top_teams.items():
            print(f"  {team:20} {weeks} weeks")

        print("\nLongest Current Ranked Streaks:")
        latest_week_num = analytics_df['week_number'].max()
        current_streaks = analytics_df[
            analytics_df['week_number'] == latest_week_num
        ].nlargest(5, 'ranked_streak')[['team', 'ranked_streak']]
        for _, row in current_streaks.iterrows():
            print(f"  {row['team']:20} {row['ranked_streak']} consecutive weeks")

        print("\nTeam Identity Distribution (Latest Week):")
        if 'team_identity' in analytics_df.columns:
            latest_identities = analytics_df[
                analytics_df['week_number'] == latest_week_num
            ]['team_identity'].value_counts()
            for identity, count in latest_identities.items():
                print(f"  {identity:15} {count:3} teams")

        print("\nTop 5 Teams vs Top 25 Opponents:")
        if 'games_vs_top25' in analytics_df.columns:
            top_vs_t25 = analytics_df[
                analytics_df['week_number'] == latest_week_num
            ].nlargest(5, 'games_vs_top25')[['team', 'games_vs_top25', 'wins_vs_top25', 'win_pct_vs_top25']]
            for _, row in top_vs_t25.iterrows():
                games = int(row['games_vs_top25'])
                wins = int(row['wins_vs_top25'])
                pct = row['win_pct_vs_top25']
                print(f"  {row['team']:20} {games} games ({wins}-{games-wins}, {pct:.1%})")

        # Display sample data - show chronological progression for one team
        print("\n" + "=" * 70)
        print("Sample Data: UConn's Season Progression (Chronological)")
        print("=" * 70)
        uconn_data = analytics_df[analytics_df['team'] == 'UConn'].sort_values('week_number')
        print(uconn_data[['poll_week', 'week_number', 'rank', 'prev_rank_numeric', 'rank_change', 'movement_category', 'weeks_in_top25']].to_string(index=False))

        print("\n" + "=" * 70)
        print("✅ Analytics Table Generation Complete!")
        print("=" * 70)
        print(f"Output: {POLLS_ANALYTICS}")
        print("\nKey Improvements in v2.0:")
        print("  ✓ Deduplicated data (removed duplicate run_dates)")
        print("  ✓ Proper chronological sorting via week_number")
        print("  ✓ Accurate rank_change calculations")
        print("  ✓ Correct cumulative metrics")
        print("  ✓ Team Identity (Recent Momentum) - 6 categories based on last 5-10 weeks")
        print("  ✓ Top 25 Opponent Metrics - games, wins, losses, win % vs Top 25")
        print("\nThis table is ready for:")
        print("  • Tableau Public dashboards")
        print("  • Excel pivot tables")
        print("  • Python/R analysis")
        print("  • SQL database import")
        print("=" * 70)
        print()

    except Exception as e:
        print(f"\n✗ Error generating analytics table: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
