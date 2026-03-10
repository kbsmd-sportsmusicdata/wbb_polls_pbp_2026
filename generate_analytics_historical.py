"""
Generate historical analytics table for AP Poll data (2021-2025) with 2026 cross-reference.

This script:
- Uses polls_historical_long.csv (2021-2025) as the primary input
- Merges 2026 current-season data from polls_long.csv for cross-year comparison
- Dynamically orders poll weeks chronologically for each season (no hardcoded week map)
- Downloads and caches per-season schedule parquets from the sportsdataverse
  wehoop-wbb-raw GitHub repository to compute Top 25 opponent metrics
- Produces two primary outputs:
    1. polls_historical_analytics.csv  — row-level analytics (one row per team/week/season)
    2. polls_historical_season_summary.csv — season-level summary per team
- Optionally merges recruiting class rankings if data/recruiting/recruiting_classes.csv exists
    Expected columns: season (int), team (str), recruiting_class_rank (int, 26=unranked)
    Produces: polls_recruiting_correlation.csv

Seasons analyzed: 2021-2026 (configurable via SEASONS_TO_ANALYZE)

Schedule parquet source:
    https://raw.githubusercontent.com/sportsdataverse/wehoop-wbb-raw/main/
        wbb/schedules/parquet/wbb_schedule_{season}.parquet
    Files are cached at data/wbb_schedule/wbb_schedule_{season}.parquet after first download.
    Raw parquet columns used: home_location, away_location, home_current_rank,
        away_current_rank, home_winner, away_winner, status_type_name
"""
import os
import tempfile
import pandas as pd
import numpy as np
import requests
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

# Schedule parquet base URL — {season} is replaced at runtime
SCHEDULE_PARQUET_URL = (
    "https://raw.githubusercontent.com/sportsdataverse/wehoop-wbb-raw/main"
    "/wbb/schedules/parquet/wbb_schedule_{season}.parquet"
)

# Poll team name → schedule parquet location name where they differ
# Keys are names as they appear in polls_historical_long.csv / polls_long.csv
# Values are names as they appear in the wehoop parquet home_location/away_location columns
POLL_TO_SCHEDULE_NAME: dict[str, str] = {
    "UNC":              "North Carolina",
    "St. John's (NY)":  "St. John's",
    "Miami (FL)":       "Miami",
}

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


def _poll_week_to_date(poll_week: str, season_year: int) -> date:
    """
    Convert a poll week label to its actual calendar date.

    Mirrors the logic inside build_week_order so other functions can reuse it.
    """
    if poll_week == 'Pre':
        return date(season_year - 1, 9, 1)
    if poll_week in ('Final', 'Post'):
        return date(season_year, 6, 1)
    parts = poll_week.split('/')
    month, day = int(parts[0]), int(parts[1])
    cal_year = (season_year - 1) if month >= 8 else season_year
    return date(cal_year, month, day)


def _build_rank_calendar(polls_df: pd.DataFrame, season: int) -> dict[str, list[tuple]]:
    """
    Build a per-team sorted timeline of (poll_date, rank_numeric) for a season.

    Used to derive opponent ranks for schedule parquets that lack rank columns.

    Returns dict: team → sorted list of (date, rank_numeric) ascending by date.
    """
    s = polls_df[polls_df['season'] == season].copy()
    timelines: dict[str, list] = {}
    for _, row in s.iterrows():
        try:
            d = _poll_week_to_date(str(row['poll_week']), int(season))
        except (ValueError, IndexError):
            continue
        team = row['team']
        if team not in timelines:
            timelines[team] = []
        timelines[team].append((d, int(row['rank_numeric'])))
    for team in timelines:
        timelines[team].sort(key=lambda x: x[0])
    return timelines


def _rank_at_date(
    timelines: dict[str, list[tuple]], team: str, game_date: date
) -> int:
    """
    Return a team's AP poll rank as of a game date using the nearest prior poll.

    Walks backward through the team's sorted timeline to find the latest poll
    released on or before the game date. Returns 99 (unranked) if no prior poll exists.
    """
    entries = timelines.get(team, [])
    rank = 99
    for poll_date, poll_rank in entries:
        if poll_date <= game_date:
            rank = poll_rank
        else:
            break
    return rank


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

_SCHEDULE_METRICS_EMPTY = pd.DataFrame(columns=[
    'team', 'games_vs_top25', 'wins_vs_top25', 'losses_vs_top25', 'win_pct_vs_top25'
])


def _load_or_download_parquet(season: int) -> pd.DataFrame | None:
    """
    Return the raw schedule parquet for a season, loading from local cache or downloading.

    Cache path: data/wbb_schedule/wbb_schedule_{season}.parquet
    Download URL: SCHEDULE_PARQUET_URL (from sportsdataverse wehoop-wbb-raw)

    Returns None on any failure so callers can skip gracefully.
    """
    SCHEDULE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = SCHEDULE_DIR / f"wbb_schedule_{season}.parquet"

    if cache_path.exists():
        print(f"    ✓ Loaded cached parquet: {cache_path.name}")
        return pd.read_parquet(cache_path)

    url = SCHEDULE_PARQUET_URL.format(season=season)
    print(f"    Downloading: {url}")
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp.write(response.content)
            tmp_path = tmp.name
        df = pd.read_parquet(tmp_path)
        os.unlink(tmp_path)
        df.to_parquet(cache_path, index=False)
        print(f"    ✓ Cached to {cache_path.name}  ({len(df):,} games)")
        return df
    except requests.HTTPError as e:
        print(f"    ⚠ HTTP error downloading season {season}: {e}")
    except requests.RequestException as e:
        print(f"    ⚠ Network error downloading season {season}: {e}")
    except Exception as e:
        print(f"    ⚠ Unexpected error for season {season}: {e}")
    return None


def compute_schedule_metrics(
    season: int, polls_df: pd.DataFrame | None = None
) -> pd.DataFrame:
    """
    Compute per-team Top-25 opponent metrics for a season from the raw parquet.

    Two rank-resolution strategies (auto-detected by schema):

    Schema A — rank columns present (2024+):
        Uses home_current_rank / away_current_rank embedded in the parquet.

    Schema B — no rank columns (2021-2023):
        Derives opponent ranks from the AP poll data via date-based lookup.
        Requires polls_df (combined historical + 2026 DataFrame) to be passed.
        For each game, finds the most recent poll released on or before game date
        and uses that poll's rank for the opponent.

    Process:
    1. Load (or download) wbb_schedule_{season}.parquet
    2. Filter to STATUS_FINAL games
    3. Resolve home/away ranks (schema A or B)
    4. Expand to team-centric rows and filter to games vs Top 25 opponents
    5. Aggregate wins/losses/games per team

    Returns DataFrame: team, games_vs_top25, wins_vs_top25, losses_vs_top25, win_pct_vs_top25
    Returns empty DataFrame if parquet unavailable or no qualifying games found.
    """
    raw = _load_or_download_parquet(season)
    if raw is None:
        return _SCHEDULE_METRICS_EMPTY.copy()

    try:
        completed = raw[raw['status_type_name'] == 'STATUS_FINAL'].copy()

        # ── Schema A: rank columns available ──────────────────────────────────
        if 'home_current_rank' in completed.columns:
            completed['home_rank'] = pd.to_numeric(
                completed['home_current_rank'], errors='coerce'
            ).fillna(99).astype(int)
            completed['away_rank'] = pd.to_numeric(
                completed['away_current_rank'], errors='coerce'
            ).fillna(99).astype(int)
            print(f"    Using embedded rank columns (Schema A)")

        # ── Schema B: derive ranks from poll data by game date ─────────────────
        else:
            if polls_df is None:
                print(f"    ⚠ No rank columns and no polls_df provided — skipping season {season}")
                return _SCHEDULE_METRICS_EMPTY.copy()

            print(f"    No rank columns — deriving from poll data by game date (Schema B)")
            rank_calendar = _build_rank_calendar(polls_df, season)

            # Parse game dates (ISO format: '2020-11-25T17:00Z')
            game_dates = pd.to_datetime(
                completed['start_date'], utc=True, errors='coerce'
            ).dt.date

            # Translate schedule names → poll names for rank lookup
            sched_to_poll = {v: k for k, v in POLL_TO_SCHEDULE_NAME.items()}
            completed['home_rank'] = [
                _rank_at_date(rank_calendar, sched_to_poll.get(str(home), str(home)), gd)
                for home, gd in zip(completed['home_location'], game_dates)
            ]
            completed['away_rank'] = [
                _rank_at_date(rank_calendar, sched_to_poll.get(str(away), str(away)), gd)
                for away, gd in zip(completed['away_location'], game_dates)
            ]

        # ── Expand to team-centric rows ────────────────────────────────────────
        home_view = completed[
            ['home_location', 'away_location', 'home_winner', 'away_rank']
        ].copy()
        home_view.columns = ['team', 'opponent', 'won', 'opp_rank']

        away_view = completed[
            ['away_location', 'home_location', 'away_winner', 'home_rank']
        ].copy()
        away_view.columns = ['team', 'opponent', 'won', 'opp_rank']

        all_games = pd.concat([home_view, away_view], ignore_index=True)
        top25_games = all_games[all_games['opp_rank'] <= 25].copy()

        if top25_games.empty:
            print(f"    ⚠ No completed games vs Top 25 found for season {season}")
            return _SCHEDULE_METRICS_EMPTY.copy()

        top25_games['won'] = top25_games['won'].astype(bool)

        metrics = top25_games.groupby('team').agg(
            games_vs_top25=('opponent', 'count'),
            wins_vs_top25 =('won',      'sum')
        ).reset_index()

        metrics['losses_vs_top25']  = metrics['games_vs_top25'] - metrics['wins_vs_top25']
        metrics['win_pct_vs_top25'] = (
            metrics['wins_vs_top25'] / metrics['games_vs_top25']
        ).round(3)

        # Translate schedule team names → poll names so the join to analytics works
        sched_to_poll = {v: k for k, v in POLL_TO_SCHEDULE_NAME.items()}
        metrics['team'] = metrics['team'].replace(sched_to_poll)

        print(f"    ✓ Top 25 metrics: {len(metrics)} teams, "
              f"{int(metrics['games_vs_top25'].sum())} games (season {season})")
        return metrics

    except (KeyError, ValueError) as e:
        print(f"    ⚠ Error computing metrics for season {season}: {e}")
        return _SCHEDULE_METRICS_EMPTY.copy()


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
        print(f"  Season {season}:")
        m = compute_schedule_metrics(int(season), polls_df=df)
        if not m.empty:
            m['season'] = season
            schedule_metrics_frames.append(m)

    if schedule_metrics_frames:
        all_sched = pd.concat(schedule_metrics_frames, ignore_index=True)
        analytics_df = analytics_df.merge(all_sched, on=['season', 'team'], how='left')
        for col in ['games_vs_top25', 'wins_vs_top25', 'losses_vs_top25', 'win_pct_vs_top25']:
            analytics_df[col] = analytics_df[col].fillna(0)
        seasons_loaded = all_sched['season'].nunique()
        print(f"\n  ✓ Schedule metrics merged for {seasons_loaded} season(s)")
    else:
        print("  ⚠ No schedule data loaded — Top 25 opponent metrics not included")

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
    print("NOTE: Schedule parquets are auto-downloaded from sportsdataverse/wehoop-wbb-raw")
    print("  and cached at data/wbb_schedule/wbb_schedule_{season}.parquet.")
    print()
    print("NOTE: To enable recruiting correlation analysis, create:")
    print(f"  {RECRUITING_CSV}")
    print("  Columns: season (int), team (str), recruiting_class_rank (int)")
    print("=" * 70)


if __name__ == "__main__":
    main()
