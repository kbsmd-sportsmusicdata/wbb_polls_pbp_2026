"""
Generate historical analytics table for AP Poll data (2021-2025) with 2026 cross-reference.

This script:
- Uses polls_historical_long.csv (2021-2025) as the primary input
- Merges 2026 current-season data from polls_long.csv for cross-year comparison
- Dynamically orders poll weeks chronologically for each season (no hardcoded week map)
- Produces two primary outputs:
    1. polls_historical_analytics.csv  — row-level analytics (one row per team/week/season)
    2. polls_historical_season_summary.csv — season-level summary per team
- Optionally merges recruiting class rankings if data/recruiting/recruiting_classes.csv exists
    Expected columns: season (int), team (str), recruiting_class_rank (int, 26=unranked)
    Produces: polls_recruiting_correlation.csv

Seasons analyzed: 2021-2026 (configurable via SEASONS_TO_ANALYZE)

NOTE on schedule data:
    Top 25 opponent metrics require per-season schedule files. Only the 2026 schedule is
    currently available at data/wbb_schedule/schedule_filtered.csv. To add historical
    schedules (2021-2025), download from sportsdataverse GitHub releases or wehoop data
    releases and place them at:
        data/wbb_schedule/schedule_filtered_{season}.csv
    e.g. data/wbb_schedule/schedule_filtered_2025.csv
    If files are missing, Top 25 metrics are skipped gracefully for those seasons.
"""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date, datetime

# ── Configuration ──────────────────────────────────────────────────────────────
DATA_DIR = Path("data")

POLLS_HISTORICAL_LONG = DATA_DIR / "polls_historical" / "polls_historical_long.csv"
POLLS_LONG_2026       = DATA_DIR / "polls_long.csv"
SCHEDULE_DIR          = DATA_DIR / "wbb_schedule"
RECRUITING_CSV        = DATA_DIR / "recruiting" / "recruiting_classes.csv"

OUT_ANALYTICS         = DATA_DIR / "polls_historical_analytics.csv"
OUT_SEASON_SUMMARY    = DATA_DIR / "polls_historical_season_summary.csv"
OUT_RECRUITING_CORR   = DATA_DIR / "polls_recruiting_correlation.csv"

# Seasons to include in historical analysis (season = ending calendar year of season)
SEASONS_TO_ANALYZE = [2021, 2022, 2023, 2024, 2025, 2026]

# Name used for 2026 season's schedule file (current season)
SCHEDULE_2026_FILE = SCHEDULE_DIR / "schedule_filtered.csv"

# ── Week Ordering ──────────────────────────────────────────────────────────────

def build_week_order(poll_weeks: list[str], season_year: int) -> dict[str, int]:
    """
    Build a chronological week-number mapping for a given season's poll weeks.

    The season spans two calendar years:
    - Months 8-12 belong to (season_year - 1)  [fall]
    - Months 1-7  belong to (season_year)       [spring]

    Special labels:
    - 'Pre'  → placed before all dated weeks (earliest)
    - 'Final' / 'Post' → placed after all dated weeks (latest, in that order)

    Args:
        poll_weeks: List of unique poll week labels for this season
        season_year: Season ending year (e.g., 2025 for 2024-25 season)

    Returns:
        Dict mapping poll_week label → integer week number (0-based)
    """
    week_dates: dict[str, date] = {}

    for pw in poll_weeks:
        if pw == 'Pre':
            # Anchor before any possible dated week (Aug 1 of fall year)
            week_dates[pw] = date(season_year - 1, 9, 1)
        elif pw == 'Final':
            week_dates[pw] = date(season_year, 6, 1)
        elif pw == 'Post':
            week_dates[pw] = date(season_year, 6, 2)
        else:
            try:
                parts = pw.split('/')
                month, day = int(parts[0]), int(parts[1])
                cal_year = (season_year - 1) if month >= 8 else season_year
                week_dates[pw] = date(cal_year, month, day)
            except (ValueError, IndexError):
                # Unknown format — append at end
                week_dates[pw] = date(season_year, 7, 1)

    sorted_weeks = sorted(week_dates.items(), key=lambda x: x[1])
    return {pw: i for i, (pw, _) in enumerate(sorted_weeks)}


# ── Movement / Identity Helpers ────────────────────────────────────────────────

def calculate_movement_category(
    current_rank: float, prev_rank: float, rank_change: float
) -> str:
    """
    Categorize week-over-week rank movement.

    Categories:
    - 'New'      : First appearance in Top 25 (prev unranked, current ranked)
    - 'Rising'   : Rank improved (rank_change > 0, both ranked)
    - 'Falling'  : Rank worsened (rank_change < 0, both ranked)
    - 'Stable'   : No change (rank_change == 0, both ranked)
    - 'Dropped'  : Fell out of Top 25 (prev ranked, current unranked)
    - 'Unranked' : Unranked in both weeks
    - 'Unknown'  : Data issue (NaN)
    """
    if pd.isna(current_rank) or pd.isna(prev_rank):
        return 'Unknown'

    current_ranked = current_rank <= 25
    prev_ranked    = prev_rank <= 25

    if current_ranked and not prev_ranked:
        return 'New'
    if not current_ranked and prev_ranked:
        return 'Dropped'
    if not current_ranked and not prev_ranked:
        return 'Unranked'

    if pd.isna(rank_change) or rank_change == 0:
        return 'Stable'
    return 'Rising' if rank_change > 0 else 'Falling'


def calculate_team_identity(group_df: pd.DataFrame, latest_week: int) -> str:
    """
    Classify a team's recent momentum over the last 5 weeks.

    Categories (in priority order):
    - 'Newcomer'    : < 3 appearances in last 5 weeks
    - 'Elite Lock'  : low volatility (stdev < 1.5) + avg rank <= 5 in last 5
    - 'Surging'     : avg rank improved 3+ spots vs prior 5 weeks
    - 'Falling'     : avg rank dropped 3+ spots vs prior 5 weeks
    - 'Volatile'    : high volatility (stdev > 4.5) in last 5 weeks
    - 'Steady'      : default — consistent, unremarkable

    Args:
        group_df: Sorted (ascending week_number) DataFrame for a single team
        latest_week: Maximum week_number in the full dataset
    """
    recent_5 = group_df[group_df['week_number'] >= latest_week - 4]
    if len(recent_5) < 3:
        return 'Newcomer'

    recent_5_ranked = recent_5[recent_5['rank_numeric'] <= 25]
    if len(recent_5_ranked) == 0:
        return 'Unranked'

    recent_ranks  = recent_5_ranked['rank_numeric'].values
    recent_avg    = recent_ranks.mean()
    recent_stdev  = recent_ranks.std() if len(recent_ranks) > 1 else 0.0

    if recent_stdev < 1.5 and recent_avg <= 5:
        return 'Elite Lock'

    prev_5 = group_df[
        (group_df['week_number'] >= latest_week - 9) &
        (group_df['week_number'] <  latest_week - 4)
    ]
    prev_5_ranked = prev_5[prev_5['rank_numeric'] <= 25]

    if len(prev_5_ranked) >= 2:
        prev_avg = prev_5_ranked['rank_numeric'].mean()
        trend    = recent_avg - prev_avg   # negative = improved
        if trend <= -3:
            return 'Surging'
        if trend >= 3:
            return 'Falling'

    if recent_stdev > 4.5:
        return 'Volatile'

    return 'Steady'


# ── Schedule Metrics ───────────────────────────────────────────────────────────

def load_schedule_metrics(schedule_path: Path) -> pd.DataFrame:
    """
    Compute per-team Top-25 opponent metrics from a schedule file.

    Returns DataFrame with columns:
        team, games_vs_top25, wins_vs_top25, losses_vs_top25, win_pct_vs_top25
    Returns empty DataFrame (with those columns) if file is missing or invalid.
    """
    empty = pd.DataFrame(columns=[
        'team', 'games_vs_top25', 'wins_vs_top25', 'losses_vs_top25', 'win_pct_vs_top25'
    ])

    if not schedule_path.exists():
        print(f"    ⚠ Schedule file not found: {schedule_path} — skipping Top 25 metrics")
        return empty

    try:
        sched = pd.read_csv(schedule_path)
        sched['Opponent Rank'] = pd.to_numeric(sched['Opponent Rank'], errors='coerce')
        top25_games = sched[sched['Opponent Rank'] <= 25].copy()

        if top25_games.empty:
            print(f"    ⚠ No Top 25 opponent games found in {schedule_path.name}")
            return empty

        top25_games['is_win'] = top25_games['Game Result'] == 'W'
        metrics = top25_games.groupby('Team').agg(
            games_vs_top25 =('Opponent', 'count'),
            wins_vs_top25  =('is_win', 'sum')
        ).reset_index().rename(columns={'Team': 'team'})

        metrics['losses_vs_top25']  = metrics['games_vs_top25'] - metrics['wins_vs_top25']
        metrics['win_pct_vs_top25'] = (
            metrics['wins_vs_top25'] / metrics['games_vs_top25']
        ).round(3)

        print(f"    ✓ Top 25 metrics: {len(metrics)} teams, "
              f"{int(metrics['games_vs_top25'].sum())} games")
        return metrics

    except (pd.errors.ParserError, KeyError, ValueError) as e:
        print(f"    ⚠ Error reading {schedule_path.name}: {e}")
        return empty


# ── Data Loading ───────────────────────────────────────────────────────────────

def load_historical(path: Path, seasons: list[int]) -> pd.DataFrame:
    """
    Load polls_historical_long.csv and filter to requested seasons.

    Standardizes output to columns:
        season, team, conference, poll_week, rank, rank_numeric, table_id
    """
    df = pd.read_csv(path)
    df = df.rename(columns={'year': 'season'})
    df = df[df['season'].isin(seasons)].copy()
    print(f"  ✓ Historical: {len(df)} rows across seasons {sorted(df['season'].unique())}")
    return df[['season', 'team', 'conference', 'poll_week', 'rank', 'rank_numeric', 'table_id']]


def load_current_2026(path: Path) -> pd.DataFrame:
    """
    Load polls_long.csv (2026 season), deduplicate by keeping the latest run_date
    per team/poll_week, and standardize column names to match historical format.
    """
    df = pd.read_csv(path)
    # Deduplicate — keep latest run_date per team/poll_week
    df = df.sort_values('run_date').groupby(['team', 'poll_week'], as_index=False).last()
    df['season'] = 2026
    print(f"  ✓ 2026 current: {len(df)} rows (after dedup)")
    return df[['season', 'team', 'conference', 'poll_week', 'rank', 'rank_numeric', 'table_id']]


# ── Core Analytics ─────────────────────────────────────────────────────────────

def build_analytics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate row-level analytics for a combined multi-season DataFrame.

    For each season independently:
      - Assigns chronological week_number using date parsing
      - Calculates prev_rank_numeric, rank_change, movement_category
      - Calculates cumulative weeks_in_top25 and ranked_streak per team
      - Assigns team_identity based on recent 5-week momentum

    Returns enriched DataFrame.
    """
    results = []

    for season in sorted(df['season'].unique()):
        print(f"\n  Processing season {season}...")
        season_df = df[df['season'] == season].copy()

        # Build week order for this season
        unique_weeks = season_df['poll_week'].unique().tolist()
        week_order   = build_week_order(unique_weeks, int(season))

        unmapped = [w for w in unique_weeks if w not in week_order]
        if unmapped:
            print(f"    ⚠ Unmapped weeks (will be placed at end): {unmapped}")

        season_df['week_number'] = season_df['poll_week'].map(week_order)

        # Print week mapping summary
        wk_map = (
            season_df[['poll_week', 'week_number']]
            .drop_duplicates()
            .sort_values('week_number')
        )
        print(f"    Weeks: {dict(zip(wk_map['poll_week'], wk_map['week_number']))}")

        # Sort chronologically within each team
        season_df = season_df.sort_values(['team', 'week_number']).reset_index(drop=True)

        # Rank change metrics
        season_df['prev_rank_numeric'] = (
            season_df.groupby('team')['rank_numeric'].shift(1)
        )
        # Positive = improvement (went from rank 10 to rank 5 → change of +5)
        season_df['rank_change'] = (
            season_df['prev_rank_numeric'] - season_df['rank_numeric']
        )

        # Movement category
        season_df['movement_category'] = season_df.apply(
            lambda row: calculate_movement_category(
                row['rank_numeric'], row['prev_rank_numeric'], row['rank_change']
            ),
            axis=1
        )

        # Cumulative weeks in Top 25
        season_df['is_ranked']      = (season_df['rank_numeric'] <= 25).astype(int)
        season_df['weeks_in_top25'] = season_df.groupby('team')['is_ranked'].cumsum()

        # Consecutive ranked-weeks streak
        season_df['ranked_streak'] = (
            season_df.groupby('team')['is_ranked']
            .apply(lambda x: x.groupby((x != x.shift()).cumsum()).cumsum())
            .reset_index(level=0, drop=True)
        )
        season_df.loc[season_df['rank_numeric'] == 26, 'ranked_streak'] = 0

        # Team identity (momentum)
        latest_week = season_df['week_number'].max()
        identities  = (
            season_df.groupby('team', sort=False)
            .apply(calculate_team_identity, latest_week=latest_week)
        )
        identities.name = 'team_identity'
        season_df = season_df.merge(identities, on='team', how='left')

        results.append(season_df)

    return pd.concat(results, ignore_index=True)


def build_season_summary(analytics_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate row-level analytics into one summary row per team per season.

    Columns:
        season, team, conference, preseason_rank, best_rank, worst_rank,
        avg_rank_when_ranked, final_rank, weeks_in_top25_total,
        peak_streak, first_ranked_week, last_ranked_week
    """
    rows = []

    for (season, team), grp in analytics_df.groupby(['season', 'team']):
        grp = grp.sort_values('week_number')

        # Conference (should be constant per team/season)
        conference = grp['conference'].mode()[0] if not grp['conference'].isna().all() else None

        # Preseason rank
        pre_rows = grp[grp['poll_week'] == 'Pre']
        preseason_rank = int(pre_rows['rank_numeric'].iloc[0]) if len(pre_rows) > 0 else None

        # Final / Post rank (end-of-season poll)
        final_rows = grp[grp['poll_week'].isin(['Final', 'Post'])]
        final_rank = int(final_rows['rank_numeric'].iloc[-1]) if len(final_rows) > 0 else None

        # Only regular (non-Pre, non-Final/Post) ranked weeks for averages
        midseason = grp[~grp['poll_week'].isin(['Pre', 'Final', 'Post'])]
        ranked    = midseason[midseason['rank_numeric'] <= 25]

        best_rank  = int(ranked['rank_numeric'].min())  if len(ranked) > 0 else None
        worst_rank = int(ranked['rank_numeric'].max())  if len(ranked) > 0 else None
        avg_rank   = round(ranked['rank_numeric'].mean(), 2) if len(ranked) > 0 else None

        # Total weeks in Top 25 (all weeks including Pre/Final)
        all_ranked  = grp[grp['rank_numeric'] <= 25]
        weeks_top25 = len(all_ranked)

        # Peak streak
        peak_streak = int(grp['ranked_streak'].max())

        # First and last week labels when ranked
        if weeks_top25 > 0:
            first_ranked_wk = grp.loc[all_ranked.index.min(), 'poll_week']
            last_ranked_wk  = grp.loc[all_ranked.index.max(), 'poll_week']
        else:
            first_ranked_wk = None
            last_ranked_wk  = None

        rows.append({
            'season':              season,
            'team':                team,
            'conference':          conference,
            'preseason_rank':      preseason_rank,
            'best_rank':           best_rank,
            'worst_rank':          worst_rank,
            'avg_rank_when_ranked': avg_rank,
            'final_rank':          final_rank,
            'weeks_in_top25':      weeks_top25,
            'peak_streak':         peak_streak,
            'first_ranked_week':   first_ranked_wk,
            'last_ranked_week':    last_ranked_wk,
        })

    return pd.DataFrame(rows)


def build_recruiting_correlation(
    season_summary: pd.DataFrame,
    recruiting_path: Path
) -> pd.DataFrame | None:
    """
    Merge season poll summary with recruiting class rankings and compute correlations.

    Expected recruiting CSV columns:
        season (int), team (str), recruiting_class_rank (int; 26 = outside Top 25)

    Returns merged DataFrame with additional columns:
        recruiting_class_rank, recruiting_in_top25 (bool),
        poll_best_rank, poll_final_rank, poll_weeks_in_top25, avg_rank_when_ranked

    Returns None if recruiting file does not exist.
    """
    if not recruiting_path.exists():
        return None

    try:
        rec = pd.read_csv(recruiting_path)
        required = {'season', 'team', 'recruiting_class_rank'}
        if not required.issubset(rec.columns):
            print(f"  ⚠ Recruiting CSV missing columns: {required - set(rec.columns)}")
            return None

        rec['recruiting_in_top25'] = rec['recruiting_class_rank'] <= 25

        merged = rec.merge(
            season_summary[[
                'season', 'team', 'conference',
                'preseason_rank', 'best_rank', 'worst_rank',
                'avg_rank_when_ranked', 'final_rank', 'weeks_in_top25'
            ]],
            on=['season', 'team'],
            how='left'
        )

        # Numeric correlation columns (all should be int/float for corr)
        corr_cols = [
            'recruiting_class_rank', 'preseason_rank', 'best_rank',
            'avg_rank_when_ranked', 'final_rank', 'weeks_in_top25'
        ]
        available = [c for c in corr_cols if c in merged.columns]

        print(f"\n  Recruiting × Poll Correlations (Pearson r):")
        print(f"  {'Metric':<30} {'vs recruiting_class_rank':>25}")
        for col in available[1:]:  # skip recruiting_class_rank itself
            valid = merged[['recruiting_class_rank', col]].dropna()
            if len(valid) >= 5:
                r = valid['recruiting_class_rank'].corr(valid[col])
                note = ""
                if col in ('best_rank', 'avg_rank_when_ranked', 'final_rank'):
                    note = "  [lower rank# = better]"
                print(f"    {col:<30} r = {r:+.3f}{note}")

        return merged

    except (pd.errors.ParserError, KeyError, ValueError) as e:
        print(f"  ⚠ Error loading recruiting data: {e}")
        return None


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("AP Poll Historical Analytics Generator (Multi-Season)")
    print("=" * 70)
    print(f"Process Date : {date.today().strftime('%Y-%m-%d')}")
    print(f"Seasons      : {SEASONS_TO_ANALYZE}")
    print("=" * 70)

    # ── Load data ──────────────────────────────────────────────────────────────
    print("\nStep 1: Loading poll data...")

    historical_seasons = [s for s in SEASONS_TO_ANALYZE if s <= 2025]
    current_seasons    = [s for s in SEASONS_TO_ANALYZE if s == 2026]

    frames = []

    if historical_seasons and POLLS_HISTORICAL_LONG.exists():
        frames.append(load_historical(POLLS_HISTORICAL_LONG, historical_seasons))
    elif historical_seasons:
        print(f"  ⚠ {POLLS_HISTORICAL_LONG} not found — skipping historical seasons")

    if current_seasons and POLLS_LONG_2026.exists():
        frames.append(load_current_2026(POLLS_LONG_2026))
    elif current_seasons:
        print(f"  ⚠ {POLLS_LONG_2026} not found — skipping 2026 season")

    if not frames:
        raise RuntimeError("No data loaded. Check input file paths.")

    df = pd.concat(frames, ignore_index=True)
    print(f"\n  Total rows combined: {len(df):,}")
    print(f"  Seasons present   : {sorted(df['season'].unique())}")
    print(f"  Teams (all-time)  : {df['team'].nunique()}")

    # ── Build analytics ────────────────────────────────────────────────────────
    print("\nStep 2: Building row-level analytics...")
    analytics_df = build_analytics(df)

    # ── Load schedule metrics (per season) ────────────────────────────────────
    print("\nStep 3: Loading schedule-based Top 25 opponent metrics...")
    schedule_metrics_frames = []

    for season in sorted(analytics_df['season'].unique()):
        if season == 2026:
            sched_path = SCHEDULE_2026_FILE
        else:
            sched_path = SCHEDULE_DIR / f"schedule_filtered_{season}.csv"

        print(f"  Season {season}: {sched_path.name}")
        m = load_schedule_metrics(sched_path)
        if not m.empty:
            m['season'] = season
            schedule_metrics_frames.append(m)

    if schedule_metrics_frames:
        all_sched = pd.concat(schedule_metrics_frames, ignore_index=True)
        analytics_df = analytics_df.merge(all_sched, on=['season', 'team'], how='left')
        for col in ['games_vs_top25', 'wins_vs_top25', 'losses_vs_top25', 'win_pct_vs_top25']:
            analytics_df[col] = analytics_df[col].fillna(0)
        print(f"\n  ✓ Schedule metrics merged for {len(schedule_metrics_frames)} season(s)")
    else:
        print("  ⚠ No schedule files found — Top 25 opponent metrics not included")
        print("    To add historical schedules, see the module docstring for file naming.")

    # ── Finalize analytics column order ───────────────────────────────────────
    base_cols = [
        'season', 'team', 'conference', 'poll_week', 'week_number',
        'rank', 'rank_numeric', 'prev_rank_numeric', 'rank_change',
        'movement_category', 'weeks_in_top25', 'ranked_streak', 'team_identity',
    ]
    optional_sched_cols = [
        'games_vs_top25', 'wins_vs_top25', 'losses_vs_top25', 'win_pct_vs_top25',
    ]
    tail_cols = ['table_id']

    final_cols = (
        base_cols
        + [c for c in optional_sched_cols if c in analytics_df.columns]
        + [c for c in tail_cols if c in analytics_df.columns]
    )
    analytics_df = analytics_df[final_cols]

    # ── Build season summary ───────────────────────────────────────────────────
    print("\nStep 4: Building season summary...")
    season_summary = build_season_summary(analytics_df)
    print(f"  ✓ {len(season_summary)} team-season records")

    # ── Recruiting cross-reference ─────────────────────────────────────────────
    print("\nStep 5: Recruiting class cross-reference...")
    if RECRUITING_CSV.exists():
        recruiting_corr = build_recruiting_correlation(season_summary, RECRUITING_CSV)
        if recruiting_corr is not None:
            recruiting_corr.to_csv(OUT_RECRUITING_CORR, index=False)
            print(f"  ✓ Saved recruiting correlation: {OUT_RECRUITING_CORR}")
    else:
        print(f"  ⚠ No recruiting data found at {RECRUITING_CSV}")
        print(f"    To enable: create that file with columns: season, team, recruiting_class_rank")
        print(f"    (recruiting_class_rank: 1-25 for Top 25 class, 26 = outside Top 25)")

    # ── Save outputs ───────────────────────────────────────────────────────────
    print("\nStep 6: Saving outputs...")

    analytics_df.to_csv(OUT_ANALYTICS, index=False)
    print(f"  ✓ Row-level analytics : {OUT_ANALYTICS}  ({len(analytics_df):,} rows)")

    season_summary.to_csv(OUT_SEASON_SUMMARY, index=False)
    print(f"  ✓ Season summary      : {OUT_SEASON_SUMMARY}  ({len(season_summary):,} rows)")

    # ── Summary statistics ─────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("Summary Statistics")
    print("=" * 70)

    print(f"\nTotal rows            : {len(analytics_df):,}")
    print(f"Seasons               : {sorted(analytics_df['season'].unique())}")
    print(f"Unique teams (all)    : {analytics_df['team'].nunique()}")

    print("\nRows per season:")
    for season, cnt in analytics_df.groupby('season').size().items():
        n_teams = analytics_df[analytics_df['season'] == season]['team'].nunique()
        print(f"  {season}: {cnt:4} rows  ({n_teams} teams)")

    print("\nMovement Category Distribution (all seasons combined):")
    mv = analytics_df['movement_category'].value_counts()
    for cat, cnt in mv.items():
        pct = 100 * cnt / len(analytics_df)
        print(f"  {cat:12} {cnt:5}  ({pct:5.1f}%)")

    print("\nTop 10 Teams by Total Weeks in Top 25 (all seasons):")
    top_teams = (
        season_summary.groupby('team')['weeks_in_top25']
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )
    for team, total in top_teams.items():
        seasons_present = season_summary[season_summary['team'] == team]['season'].nunique()
        print(f"  {team:<25} {total:3} weeks  ({seasons_present} seasons)")

    print("\nFinal Rank Leaders per Season:")
    for season in sorted(season_summary['season'].unique()):
        s = season_summary[
            (season_summary['season'] == season) &
            (season_summary['final_rank'].notna()) &
            (season_summary['final_rank'] <= 25)
        ].sort_values('final_rank')
        if not s.empty:
            top3 = s.head(3)
            leaders = ', '.join(
                f"{row['team']} (#{int(row['final_rank'])})"
                for _, row in top3.iterrows()
            )
            print(f"  {season}: {leaders}")

    print("\nPreseason → Final Rank Journeys (2021-2026, ranked both Pre and Final):")
    journey = season_summary[
        season_summary['preseason_rank'].notna() &
        season_summary['final_rank'].notna() &
        (season_summary['preseason_rank'] <= 25)
    ].copy()
    journey['rank_delta'] = journey['preseason_rank'] - journey['final_rank']
    # Top risers (most improved: high preseason → low final number)
    risers = journey.nlargest(5, 'rank_delta')
    print("  Top 5 Risers (Preseason → Final):")
    for _, row in risers.iterrows():
        delta_str = f"+{int(row['rank_delta'])}" if row['rank_delta'] > 0 else str(int(row['rank_delta']))
        print(f"    {row['season']} {row['team']:<20}  Pre #{int(row['preseason_rank'])} → Final #{int(row['final_rank'])}  ({delta_str})")

    print("\n" + "=" * 70)
    print("✅ Historical Analytics Generation Complete!")
    print("=" * 70)
    print(f"\nOutputs:")
    print(f"  {OUT_ANALYTICS}")
    print(f"  {OUT_SEASON_SUMMARY}")
    if RECRUITING_CSV.exists():
        print(f"  {OUT_RECRUITING_CORR}")
    print()
    print("NOTE: Historical schedule data needed for Top 25 opponent metrics.")
    print("  Download per-season schedule files from sportsdataverse/wehoop GitHub")
    print("  releases and place at:")
    print("  data/wbb_schedule/schedule_filtered_{season}.csv  (e.g. _2025.csv)")
    print()
    print("NOTE: To enable recruiting correlation analysis, create:")
    print(f"  {RECRUITING_CSV}")
    print("  Columns: season (int), team (str), recruiting_class_rank (int)")
    print("=" * 70)


if __name__ == "__main__":
    main()
