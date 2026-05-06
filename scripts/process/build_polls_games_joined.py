"""
WBB AP Poll + Game Schedule Join Pipeline
==========================================
Creates the polls_games_joined CSV by combining AP Poll analytics data
with game schedule data. Pre-computes all derived fields for Tableau.

Usage:
    # Full rebuild (overwrites existing output)
    python build_polls_games_joined.py --polls polls.csv --games schedule.csv --output joined.csv

    # Append new poll weeks only (preserves existing data)
    python build_polls_games_joined.py --polls polls.csv --games schedule.csv --output joined.csv --append

    # Dry run (shows what would change without writing)
    python build_polls_games_joined.py --polls polls.csv --games schedule.csv --output joined.csv --append --dry-run

Files:
    --polls  : AP Poll analytics CSV (github_polls_analytics_*.csv)
    --games  : Game schedule CSV (schedule_filtered_*.csv)
    --output : Output joined CSV path

Author: Krystal Beasley / WBB AP Poll Weekly Landscape Project
Last updated: 2026-03-04
"""

import pandas as pd
import numpy as np
import argparse
import os
import sys
import json
from datetime import datetime


# =============================================================================
# CONFIGURATION
# =============================================================================

# Conference ID to name mapping (ESPN IDs)
# Complete mapping based on 2025-26 season conference structure
CONF_ID_MAP = {
    1: 'Am. East',
    2: 'ACC',
    4: 'Big East',
    5: 'Big Sky',
    7: 'Big Ten',
    8: 'Big 12',
    9: 'Big West',
    10: 'CAA',
    11: 'CUSA',
    12: 'Ivy',
    13: 'MAAC',
    14: 'MAC',
    16: 'MEAC',
    18: 'MVC',
    19: 'NEC',
    23: 'SEC',
    25: 'Southland',
    26: 'SWAC',
    29: 'WCC',
    30: 'WAC',
    44: 'Mountain West',
    45: 'Horizon',
    47: 'Summit',
    62: 'American'
}

# Poll week date windows (fallback values if config file not found)
# Games played within these windows are attributed to that poll week.
# RECOMMENDED: Update via config/poll_week_windows.json instead of editing here.
_DEFAULT_POLL_WEEK_WINDOWS = {
    'Pre':   ('2025-11-03', '2025-11-09'),
    '11/10': ('2025-11-03', '2025-11-16'),
    '11/17': ('2025-11-17', '2025-11-23'),
    '11/24': ('2025-11-24', '2025-11-30'),
    '12/1':  ('2025-12-01', '2025-12-07'),
    '12/8':  ('2025-12-08', '2025-12-14'),
    '12/15': ('2025-12-15', '2025-12-21'),
    '12/22': ('2025-12-22', '2026-01-04'),
    '1/5':   ('2026-01-05', '2026-01-11'),
    '1/12':  ('2026-01-12', '2026-01-18'),
    '1/19':  ('2026-01-19', '2026-01-25'),
    '1/26':  ('2026-01-26', '2026-02-01'),
    '2/2':   ('2026-02-02', '2026-02-08'),
    '2/9':   ('2026-02-09', '2026-02-15'),
    '2/16':  ('2026-02-16', '2026-02-22'),
    '2/23':  ('2026-02-23', '2026-03-01'),
    '3/2':   ('2026-03-02', '2026-03-08'),
    '3/9':   ('2026-03-09', '2026-03-15'),
    '3/16':  ('2026-03-16', '2026-03-22'),
    'Post':  ('2026-03-23', '2026-04-06'),
}


def load_poll_week_windows():
    """Load poll week windows from config file with fallback to defaults.

    Attempts to load from config/poll_week_windows.json. If the file doesn't
    exist or can't be parsed, falls back to _DEFAULT_POLL_WEEK_WINDOWS.

    Returns:
        dict: Poll week windows as {poll_week_label: (start_date, end_date)}
    """
    # Try to find config file relative to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, '../../config/poll_week_windows.json')

    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                windows = config.get('windows', {})
                # Convert list format to tuple format
                return {k: tuple(v) for k, v in windows.items()}
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            print(f"  WARNING: Could not parse {config_path}: {e}")
            print(f"  → Falling back to hardcoded default windows")

    return _DEFAULT_POLL_WEEK_WINDOWS


# Load poll week windows from config file (or use defaults)
POLL_WEEK_WINDOWS = load_poll_week_windows()

# Closeness bucket definitions: (label, display_label, range_label, sort_order, min_margin, max_margin)
CLOSENESS_BUCKETS = [
    ('Nail-Biter (≤5)',    'Nail-Biter',  '≤5',    1, 0,  5),
    ('Close (6-10)',       'Close',        '6-10',  2, 6,  10),
    ('Comfortable (11-20)','Comfortable',  '11-20', 3, 11, 20),
    ('Blowout (20+)',      'Blowout',      '20+',   4, 21, 999),
]

# Game columns to carry from schedule CSV
GAME_COLS_TO_KEEP = [
    'id', 'date', 'team', 'poll_week',  # join keys + identifiers
    'Opponent', 'Location', 'Game Result',
    'Team Score', 'Opponent Score',
    'Team Rank', 'Opponent Rank',
    'Team Conf ID', 'Opponent Conf ID',
    'Overall Record', 'Conf Record',
    'is_conf_game', 'notes_headline',
    'Team Logo', 'Opponent Logo', 'groups_short_name',
    'Team Abbreviation', 'Opponent Abbreviation',
    'Team Color', 'Opponent Color',
]


# =============================================================================
# STEP 1: ASSIGN POLL WEEKS TO GAMES
# =============================================================================

def assign_poll_weeks(games_df):
    """Map each game date to its corresponding AP Poll week window.

    Optimized with vectorized operations instead of row-by-row apply().
    """
    games_df['game_date_parsed'] = pd.to_datetime(games_df['date'].str[:10])
    games_df['poll_week'] = None

    # Vectorized assignment: iterate through windows and assign matching dates
    # Use "first match wins" behavior by only assigning where poll_week is still None
    for pw, (start, end) in POLL_WEEK_WINDOWS.items():
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        mask = (
            (games_df['game_date_parsed'] >= start_ts) &
            (games_df['game_date_parsed'] <= end_ts) &
            (games_df['poll_week'].isna())
        )
        games_df.loc[mask, 'poll_week'] = pw

    unassigned = games_df['poll_week'].isna().sum()
    if unassigned > 0:
        dates = games_df[games_df['poll_week'].isna()]['game_date_parsed'].unique()
        print(f"  WARNING: {unassigned} games have no poll_week assignment.")
        print(f"  Unassigned dates: {sorted(dates)}")
        print(f"  → Add new windows to POLL_WEEK_WINDOWS in the script config.")

    return games_df


# =============================================================================
# STEP 2: PREPARE GAME COLUMNS
# =============================================================================

def prepare_game_columns(games_df):
    """Select, rename, and compute derived game-level fields."""
    # Keep only needed columns
    available = [c for c in GAME_COLS_TO_KEEP if c in games_df.columns]
    missing = [c for c in GAME_COLS_TO_KEEP if c not in games_df.columns]
    if missing:
        print(f"  NOTE: Missing columns in schedule CSV (will be null): {missing}")

    gdf = games_df[available + ['game_date_parsed']].copy()

    # Prefix game columns (except join keys)
    rename_map = {}
    for col in gdf.columns:
        if col not in ('team', 'poll_week'):
            rename_map[col] = f'game_{col}'
    gdf = gdf.rename(columns=rename_map)

    # --- Scoring margin ---
    gdf['game_scoring_margin'] = gdf['game_Team Score'] - gdf['game_Opponent Score']
    gdf['game_is_win'] = (gdf['game_Game Result'] == 'W').astype(int)

    # --- Upset classification (multi-tier) ---
    # An upset occurs when the worse-ranked (or unranked) team wins.
    # Categories: "Major Upset" (rank diff >= 10 or ranked vs unranked),
    #             "Upset" (rank diff 1-9), "No Upset" (expected outcome).
    team_ranked = gdf['game_Team Rank'].notna() & (gdf['game_Team Rank'] <= 25)
    opp_ranked = gdf['game_Opponent Rank'].notna() & (gdf['game_Opponent Rank'] <= 25)
    is_loss = gdf['game_Game Result'] == 'L'
    is_win = gdf['game_Game Result'] == 'W'

    # Both ranked: who is better ranked (lower rank number = better)
    both_ranked = team_ranked & opp_ranked
    team_better = both_ranked & (gdf['game_Team Rank'] < gdf['game_Opponent Rank'])
    opp_better = both_ranked & (gdf['game_Opponent Rank'] < gdf['game_Team Rank'])
    rank_diff = (gdf['game_Opponent Rank'] - gdf['game_Team Rank']).abs()

    # Major Upset: ranked loses to unranked, unranked beats ranked,
    #              OR both ranked with diff >= 10 and better-ranked team lost
    major_upset = (
        (team_ranked & ~opp_ranked & is_loss) |
        (~team_ranked & opp_ranked & is_win) |
        (team_better & is_loss & (rank_diff >= 10)) |
        (opp_better & is_win & (rank_diff >= 10))
    )

    # Upset: both ranked, better-ranked team lost (or worse-ranked won), diff 1-9
    upset = (
        (team_better & is_loss & (rank_diff >= 1) & (rank_diff <= 9)) |
        (opp_better & is_win & (rank_diff >= 1) & (rank_diff <= 9))
    )

    gdf['game_is_upset'] = np.where(
        major_upset, 'Major Upset',
        np.where(upset, 'Upset', 'No Upset')
    )

    # Helper columns for weekly aggregate upset counts
    gdf['game_upset_loss'] = ((gdf['game_is_upset'] != 'No Upset') & is_loss).astype(int)
    gdf['game_upset_win'] = ((gdf['game_is_upset'] != 'No Upset') & is_win).astype(int)

    # --- Game quality tier ---
    def _quality(row):
        t_ranked = pd.notna(row['game_Team Rank']) and row['game_Team Rank'] <= 25
        o_ranked = pd.notna(row['game_Opponent Rank']) and row['game_Opponent Rank'] <= 25
        if t_ranked and o_ranked:
            return 'Top 25 Matchup'
        elif t_ranked or o_ranked:
            return 'Ranked vs Unranked'
        return 'Unranked Matchup'

    gdf['game_quality_tier'] = gdf.apply(_quality, axis=1)

    # --- Game closeness ---
    abs_margin = gdf['game_scoring_margin'].abs()
    for full, label, rng, sort_ord, lo, hi in CLOSENESS_BUCKETS:
        mask = (abs_margin >= lo) & (abs_margin <= hi)
        gdf.loc[mask, 'game_closeness'] = full
        gdf.loc[mask, 'game_closeness_label'] = label
        gdf.loc[mask, 'game_closeness_range'] = rng
        gdf.loc[mask, 'closeness_sort_order'] = sort_ord

    # --- Opponent quality label ---
    def _opp_quality(rank):
        if pd.isna(rank) or rank > 25:
            return 'Unranked'
        elif rank <= 5:
            return 'Top 5'
        elif rank <= 10:
            return 'Top 10'
        return 'Top 25'

    gdf['game_opponent_quality'] = gdf['game_Opponent Rank'].apply(_opp_quality)

    # --- Opponent conference name ---
    gdf['game_opponent_conference'] = gdf['game_Opponent Conf ID'].map(CONF_ID_MAP).fillna('Other')

    return gdf


# =============================================================================
# STEP 3: WEEKLY AGGREGATES
# =============================================================================

def compute_weekly_aggregates(games_slim):
    """Compute per-team, per-poll-week summary statistics."""
    agg = games_slim.groupby(['team', 'poll_week']).agg(
        games_played=('game_id', 'count'),
        wins=('game_is_win', 'sum'),
        losses=('game_is_win', lambda x: len(x) - x.sum()),
        avg_scoring_margin=('game_scoring_margin', 'mean'),
        total_scoring_margin=('game_scoring_margin', 'sum'),
        best_win_margin=('game_scoring_margin', 'max'),
        worst_loss_margin=('game_scoring_margin', 'min'),
        upsets_suffered=('game_upset_loss', 'sum'),
        upsets_pulled=('game_upset_win', 'sum'),
        top25_games=('game_quality_tier', lambda x: (x == 'Top 25 Matchup').sum()),
        nail_biters=('game_closeness', lambda x: (x == 'Nail-Biter (≤5)').sum()),
        blowouts=('game_closeness', lambda x: (x == 'Blowout (20+)').sum()),
        conf_games=('game_is_conf_game', 'sum'),
    ).reset_index()

    agg.columns = ['team', 'poll_week'] + ['gw_' + c for c in agg.columns[2:]]
    agg['gw_avg_scoring_margin'] = agg['gw_avg_scoring_margin'].round(1)

    return agg


# =============================================================================
# STEP 4: JOIN POLLS + GAMES
# =============================================================================

def join_polls_and_games(polls_df, games_slim, weekly_agg):
    """Left join polls data with individual game rows and weekly aggregates."""
    joined = polls_df.merge(games_slim, on=['team', 'poll_week'], how='left')
    joined = joined.merge(weekly_agg, on=['team', 'poll_week'], how='left')
    joined['gw_games_played'] = joined['gw_games_played'].fillna(0).astype(int)

    # Format game_date_parsed for Tableau
    if 'game_game_date_parsed' in joined.columns:
        joined['game_game_date_parsed'] = joined['game_game_date_parsed'].astype(str).replace('NaT', '')

    return joined


# =============================================================================
# STEP 5: RANK RANGE (FIXED LOD)
# =============================================================================

def add_rank_range(df):
    """Pre-compute best/worst rank across all poll weeks per team."""
    ranked = df[df['rank_numeric'] <= 25]
    best = ranked.groupby('team')['rank_numeric'].min().rename('best_rank')
    worst = ranked.groupby('team')['rank_numeric'].max().rename('worst_rank')

    # Drop existing columns if present (for rebuild mode)
    for col in ['best_rank', 'worst_rank', 'rank_range', 'rank_range_label']:
        if col in df.columns:
            df = df.drop(columns=[col])

    df = df.merge(best, on='team', how='left')
    df = df.merge(worst, on='team', how='left')
    df['rank_range'] = df['worst_rank'] - df['best_rank']
    df['rank_range_label'] = df.apply(
        lambda r: f"#{int(r['best_rank'])} \u2013 #{int(r['worst_rank'])}"
        if pd.notna(r['best_rank']) else "Never Ranked", axis=1
    )
    return df


# =============================================================================
# STEP 6: ENTRY/EXIT EVENT + TRIGGER GAME
# =============================================================================

def add_entry_exit_and_trigger(df):
    """Flag entry/exit events and identify the trigger game from the prior week.

    Vectorized implementation using groupby operations instead of row-by-row iteration.
    """
    df['entry_exit_event'] = np.where(
        (df['rank_numeric'] <= 25) & (df['prev_rank_numeric'] > 25), 'Entered',
        np.where(
            (df['rank_numeric'] > 25) & (df['prev_rank_numeric'] <= 25), 'Exited', 'None'
        )
    )

    # Get unique events
    events = df[df['entry_exit_event'] != 'None'][['team', 'week_number', 'entry_exit_event']].drop_duplicates()

    if events.empty:
        return df

    # Build game lookup
    games_only = df[df['game_id'].notna()].copy()
    if 'game_game_date_parsed' in games_only.columns:
        games_only['_gdate'] = pd.to_datetime(games_only['game_game_date_parsed'], errors='coerce')
    else:
        games_only['_gdate'] = pd.NaT

    # Add prev_week to events
    events = events.copy()
    events['prev_week'] = events['week_number'] - 1

    # Merge games with events to get prev_week games for each event
    prev_week_games = games_only.merge(
        events[['team', 'prev_week', 'week_number', 'entry_exit_event']],
        left_on=['team', 'week_number'],
        right_on=['team', 'prev_week'],
        how='inner',
        suffixes=('', '_event')
    )

    if prev_week_games.empty:
        return df

    # Vectorized trigger selection: compute priority scores
    # For "Entered": prefer wins with highest margin, then latest game
    # For "Exited": prefer losses with lowest margin, then latest game
    is_win = (prev_week_games['game_Game Result'] == 'W').astype(int)
    is_loss = (prev_week_games['game_Game Result'] == 'L').astype(int)
    is_entered = (prev_week_games['entry_exit_event'] == 'Entered').astype(int)

    # Convert date to numeric for sorting (nanoseconds since epoch)
    date_rank = prev_week_games['_gdate'].fillna(pd.Timestamp('1900-01-01')).astype(np.int64)

    # Priority score calculation:
    # - For "Entered": wins (1e15) + margin (1e10) + date
    # - For "Exited": losses (1e15) - margin (1e10) + date
    prev_week_games['_priority'] = (
        # Entered events: prioritize wins, then margin (higher is better), then date
        is_entered * (is_win * 1e15 + prev_week_games['game_scoring_margin'].fillna(-999) * 1e10 + date_rank) +
        # Exited events: prioritize losses, then margin (lower/more negative is worse), then date
        (1 - is_entered) * (is_loss * 1e15 - prev_week_games['game_scoring_margin'].fillna(999) * 1e10 + date_rank)
    )

    # Get the highest priority game for each event
    trigger_idx = prev_week_games.groupby(['team', 'week_number_event'])['_priority'].idxmax()
    triggers = prev_week_games.loc[trigger_idx].copy()

    # Build trigger DataFrame
    trigger_df = pd.DataFrame({
        'team': triggers['team'].values,
        'week_number': triggers['week_number_event'].values,
        'trigger_game_opponent': triggers['game_Opponent'].values,
        'trigger_game_result': triggers['game_Game Result'].values,
        'trigger_game_team_score': triggers['game_Team Score'].values,
        'trigger_game_opp_score': triggers['game_Opponent Score'].values,
        'trigger_game_date': triggers['_gdate'].apply(lambda x: str(x.date()) if pd.notna(x) else None).values,
        'trigger_game_margin': triggers['game_scoring_margin'].values,
        'trigger_game_location': triggers['game_Location'].values,
    })

    # Drop existing trigger cols for clean merge
    for col in trigger_df.columns:
        if col in df.columns and col not in ('team', 'week_number'):
            df = df.drop(columns=[col])

    df = df.merge(trigger_df, on=['team', 'week_number'], how='left')

    return df


# =============================================================================
# STEP 7: TEAM GAME LOG (Running records, game numbers)
# =============================================================================

def add_running_records(df):
    """Compute cumulative records: overall, conference, road, home, vs Top 25."""
    game_mask = df['game_id'].notna()
    if game_mask.sum() == 0:
        return df

    gdf = df[game_mask].copy()
    gdf['_gdate'] = pd.to_datetime(gdf['game_game_date_parsed'], errors='coerce')
    gdf = gdf.sort_values(['team', '_gdate', 'game_id'])

    # Season game number
    gdf['season_game_number'] = gdf.groupby('team').cumcount() + 1

    # Helper flags
    gdf['_w'] = (gdf['game_Game Result'] == 'W').astype(int)
    gdf['_l'] = (gdf['game_Game Result'] == 'L').astype(int)
    gdf['_conf'] = (gdf['game_is_conf_game'].astype(str) == 'True').astype(int)
    gdf['_road'] = (gdf['game_Location'] == 'Away').astype(int)
    gdf['_home'] = (gdf['game_Location'] == 'Home').astype(int)
    gdf['_vs25'] = ((gdf['game_Opponent Rank'].notna()) & (gdf['game_Opponent Rank'] <= 25)).astype(int)

    # Build all running record combos
    splits = {
        '': {'filter': None},                 # overall
        'conf': {'filter': '_conf'},           # conference
        'road': {'filter': '_road'},           # road
        'home': {'filter': '_home'},           # home
        'vs_top25': {'filter': '_vs25'},       # vs Top 25
    }

    for suffix, cfg in splits.items():
        prefix = f"running_{suffix}_" if suffix else "running_"
        f = cfg['filter']

        if f:
            gdf[f'_{suffix}_win'] = (gdf[f] & gdf['_w']).astype(int)
            gdf[f'_{suffix}_loss'] = (gdf[f] & gdf['_l']).astype(int)
        else:
            gdf[f'_{suffix}_win'] = gdf['_w']
            gdf[f'_{suffix}_loss'] = gdf['_l']

        gdf[f'{prefix}wins'] = gdf.groupby('team')[f'_{suffix}_win'].cumsum()
        gdf[f'{prefix}losses'] = gdf.groupby('team')[f'_{suffix}_loss'].cumsum()
        gdf[f'{prefix}record'] = (
            gdf[f'{prefix}wins'].astype(int).astype(str) + '-' +
            gdf[f'{prefix}losses'].astype(int).astype(str)
        )
        total = gdf[f'{prefix}wins'] + gdf[f'{prefix}losses']
        gdf[f'{prefix}win_pct'] = np.where(total > 0, (gdf[f'{prefix}wins'] / total).round(3), None)

    # Running average margin
    gdf['running_avg_margin'] = (
        gdf.groupby('team')['game_scoring_margin']
        .expanding().mean().reset_index(level=0, drop=True).round(1)
    )

    # Columns to write back
    out_cols = ['season_game_number', 'running_avg_margin']
    for suffix in ['', 'conf', 'road', 'home', 'vs_top25']:
        prefix = f"running_{suffix}_" if suffix else "running_"
        out_cols += [f'{prefix}wins', f'{prefix}losses', f'{prefix}record', f'{prefix}win_pct']

    # Drop existing columns for clean write
    for col in out_cols:
        if col in df.columns:
            df = df.drop(columns=[col])

    for col in out_cols:
        df.loc[gdf.index, col] = gdf[col].values

    return df


# =============================================================================
# STEP 8: UPSET RADAR
# =============================================================================

def add_upset_columns(df):
    """Compute upset magnitude and display labels.

    Simplified logic for better readability and maintainability.
    """
    def _magnitude(row):
        if pd.isna(row.get('game_Team Rank')) or pd.isna(row.get('game_Opponent Rank')):
            return None
        tr = row['game_Team Rank'] if row['game_Team Rank'] <= 25 else 26
        opr = row['game_Opponent Rank'] if row['game_Opponent Rank'] <= 25 else 26
        return abs(tr - opr)

    def _label(row):
        upset_type = row.get('game_is_upset')
        if upset_type == 'No Upset':
            return None

        # Format ranks as strings with '#' prefix if ranked, otherwise 'Unranked'
        tr_rank = row.get('game_Team Rank')
        opr_rank = row.get('game_Opponent Rank')

        tr_str = f"#{int(tr_rank)}" if pd.notna(tr_rank) and tr_rank <= 25 else "Unranked"
        opr_str = f"#{int(opr_rank)}" if pd.notna(opr_rank) and opr_rank <= 25 else "Unranked"

        team = row['team']
        opponent = row.get('game_Opponent', '')
        result = row.get('game_Game Result')

        if result == 'L':
            return f"{tr_str} {team} lost to {opr_str} {opponent}"
        else:  # W
            return f"{tr_str} {team} beat {opr_str} {opponent}"

    df['upset_magnitude'] = df.apply(_magnitude, axis=1)
    df['upset_rank_label'] = df.apply(_label, axis=1)

    return df


# =============================================================================
# STEP 9: MATCHUP MATRIX
# =============================================================================

def add_matchup_columns(df):
    """Compute canonical matchup key and rank differential."""
    def _key(row):
        if pd.isna(row.get('game_Opponent')):
            return None
        pair = sorted([row['team'], row['game_Opponent']])
        return f"{pair[0]} vs {pair[1]}"

    def _diff(row):
        if pd.isna(row.get('game_Team Rank')) or pd.isna(row.get('game_Opponent Rank')):
            return None
        tr = row['game_Team Rank'] if row['game_Team Rank'] <= 25 else 26
        opr = row['game_Opponent Rank'] if row['game_Opponent Rank'] <= 25 else 26
        return int(tr - opr)

    df['matchup_key'] = df.apply(_key, axis=1)
    df['rank_differential'] = df.apply(_diff, axis=1)

    return df


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def run_pipeline(polls_path, games_path, output_path, append=False, dry_run=False):
    """Execute the full join pipeline."""
    print("=" * 60)
    print("WBB AP Poll + Games Join Pipeline")
    print("=" * 60)
    print(f"  Polls:  {polls_path}")
    print(f"  Games:  {games_path}")
    print(f"  Output: {output_path}")
    print(f"  Mode:   {'APPEND' if append else 'FULL REBUILD'}")

    # Show poll week windows source
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, '../../config/poll_week_windows.json')
    if os.path.exists(config_path):
        print(f"  Config: Loaded {len(POLL_WEEK_WINDOWS)} poll weeks from poll_week_windows.json")
    else:
        print(f"  Config: Using {len(POLL_WEEK_WINDOWS)} default poll weeks (no config file found)")
    print()

    # --- Load data ---
    print("[1/9] Loading source data...")
    polls = pd.read_csv(polls_path)
    games = pd.read_csv(games_path)

    # Normalize column name: "Team" -> "team" to match expected schema
    if 'Team' in games.columns and 'team' not in games.columns:
        games = games.rename(columns={'Team': 'team'})

    # Load team-to-state mapping
    state_mapping_path = os.path.join(os.path.dirname(polls_path), 'team_state_mapping.csv')
    try:
        state_mapping = pd.read_csv(state_mapping_path)
        polls = polls.merge(state_mapping, on='team', how='left')
        print(f"  Loaded state mapping for {state_mapping.shape[0]} teams")
    except FileNotFoundError:
        print(f"  WARNING: State mapping not found at {state_mapping_path}")
        polls['state'] = None
    except Exception as e:
        print(f"  WARNING: Failed to process state mapping file {state_mapping_path}: {e}")
        polls['state'] = None

    print(f"  Polls: {polls.shape[0]} rows, {polls['team'].nunique()} teams, "
          f"{polls['poll_week'].nunique()} poll weeks")
    print(f"  Games: {games.shape[0]} rows, {games['team'].nunique()} teams")

    # --- Append mode: detect new data ---
    existing_weeks = set()
    if append and os.path.exists(output_path):
        existing = pd.read_csv(output_path)
        existing_weeks = set(existing['poll_week'].unique())
        all_weeks = set(polls['poll_week'].unique())
        new_weeks = all_weeks - existing_weeks

        if not new_weeks:
            print(f"\n  No new poll weeks found. Existing data covers: {sorted(existing_weeks)}")
            print("  Nothing to append. Exiting.")
            return

        print(f"\n  Existing poll weeks: {sorted(existing_weeks)}")
        print(f"  New poll weeks to add: {sorted(new_weeks)}")

        # Filter polls to only new weeks
        polls = polls[polls['poll_week'].isin(new_weeks)]
        print(f"  Filtered polls to {polls.shape[0]} new rows")

        if dry_run:
            print(f"\n  [DRY RUN] Would append {polls.shape[0]} poll rows for weeks: {sorted(new_weeks)}")
            return

    # --- Assign poll weeks to games ---
    print("\n[2/9] Assigning poll weeks to games...")
    games = assign_poll_weeks(games)

    # If appending, filter games to only new poll weeks
    if append and existing_weeks:
        games = games[games['poll_week'].isin(polls['poll_week'].unique())]
        print(f"  Filtered games to {games.shape[0]} rows for new poll weeks")

    # --- Prepare game columns ---
    print("\n[3/9] Preparing game columns...")
    games_slim = prepare_game_columns(games)

    # --- Weekly aggregates ---
    print("\n[4/9] Computing weekly aggregates...")
    weekly_agg = compute_weekly_aggregates(games_slim)

    # --- Join ---
    print("\n[5/9] Joining polls + games...")
    joined = join_polls_and_games(polls, games_slim, weekly_agg)
    print(f"  Joined: {joined.shape[0]} rows")

    # If appending, combine with existing data BEFORE computing cross-row fields
    if append and existing_weeks:
        print(f"\n  Combining with existing {existing.shape[0]} rows...")
        joined = pd.concat([existing, joined], ignore_index=True)
        print(f"  Combined: {joined.shape[0]} rows")

    # --- Rank range (needs full dataset) ---
    print("\n[6/9] Computing rank range...")
    joined = add_rank_range(joined)

    # --- Entry/exit + trigger games ---
    print("\n[7/9] Computing entry/exit events + trigger games...")
    joined = add_entry_exit_and_trigger(joined)

    # --- Running records (needs full dataset, sorted chronologically) ---
    print("\n[8/9] Computing running records (overall, conf, road, home, vs Top 25)...")
    joined = add_running_records(joined)

    # --- Upset + matchup columns ---
    print("\n[9/9] Computing upset radar + matchup matrix columns...")
    joined = add_upset_columns(joined)
    joined = add_matchup_columns(joined)

    # --- Sort and save ---
    joined = joined.sort_values(['team', 'week_number', 'game_game_date_parsed']).reset_index(drop=True)

    # Reorder columns to match Tableau dataset field order
    tableau_column_order = [
        'season', 'team', 'conference', 'poll_week', 'week_number', 'rank', 'rank_numeric',
        'prev_rank_numeric', 'rank_change', 'movement_category', 'weeks_in_top25', 'ranked_streak',
        'team_identity', 'games_vs_top25', 'wins_vs_top25', 'losses_vs_top25', 'win_pct_vs_top25',
        'run_date', 'table_id', 'state', 'game_id', 'game_date', 'game_Opponent', 'game_Location',
        'game_Game Result', 'game_Team Score', 'game_Opponent Score', 'game_Team Rank',
        'game_Opponent Rank', 'game_Team Conf ID', 'game_Opponent Conf ID', 'game_Overall Record',
        'game_Conf Record', 'game_notes_headline', 'game_is_conf_game', 'game_game_date_parsed',
        'game_scoring_margin', 'game_is_win', 'game_is_upset', 'game_quality_tier', 'game_closeness',
        'game_closeness_label', 'game_closeness_range', 'closeness_sort_order', 'game_opponent_quality',
        'game_opponent_conference', 'gw_games_played', 'gw_wins', 'gw_losses', 'gw_avg_scoring_margin',
        'gw_total_scoring_margin', 'gw_best_win_margin', 'gw_worst_loss_margin', 'gw_upsets_suffered',
        'gw_upsets_pulled', 'gw_top25_games', 'gw_nail_biters', 'gw_blowouts', 'gw_conf_games',
        'best_rank', 'worst_rank', 'rank_range', 'rank_range_label', 'entry_exit_event',
        'trigger_game_opponent', 'trigger_game_result', 'trigger_game_team_score', 'trigger_game_opp_score',
        'trigger_game_date', 'trigger_game_margin', 'trigger_game_location', 'season_game_number',
        'running_avg_margin', 'running_wins', 'running_losses', 'running_record', 'running_win_pct',
        'running_conf_wins', 'running_conf_losses', 'running_conf_record', 'running_conf_win_pct',
        'running_road_wins', 'running_road_losses', 'running_road_record', 'running_road_win_pct',
        'running_home_wins', 'running_home_losses', 'running_home_record', 'running_home_win_pct',
        'running_vs_top25_wins', 'running_vs_top25_losses', 'running_vs_top25_record',
        'running_vs_top25_win_pct', 'upset_magnitude', 'upset_rank_label', 'matchup_key',
        'rank_differential', 'game_Team Logo', 'game_Opponent Logo', 'game_groups_short_name',
        'game_Team Abbreviation', 'game_Opponent Abbreviation', 'game_Team Color', 'game_Opponent Color'
    ]

    # Only reorder columns that exist in the dataframe
    available_cols = [col for col in tableau_column_order if col in joined.columns]
    missing_cols = [col for col in joined.columns if col not in tableau_column_order]

    if missing_cols:
        print(f"  NOTE: New columns not in Tableau order will be appended: {missing_cols}")

    # Reorder: specified columns first, then any extras
    joined = joined[available_cols + missing_cols]

    joined.to_csv(output_path, index=False)
    file_size = os.path.getsize(output_path) / 1024

    print(f"\n{'=' * 60}")
    print(f"DONE! Saved to: {output_path}")
    print(f"  Shape: {joined.shape[0]} rows x {joined.shape[1]} columns")
    print(f"  Size:  {file_size:.0f} KB")
    print(f"  Teams: {joined['team'].nunique()}")
    print(f"  Poll weeks: {sorted(joined['poll_week'].unique())}")
    print(f"{'=' * 60}")

    return joined


# =============================================================================
# CLI
# =============================================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='WBB AP Poll + Game Schedule Join Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full rebuild
  python build_polls_games_joined.py \\
    --polls github_polls_analytics_02022026.csv \\
    --games schedule_filtered_02032026.csv \\
    --output polls_games_joined.csv

  # Append new poll weeks
  python build_polls_games_joined.py \\
    --polls github_polls_analytics_02092026.csv \\
    --games schedule_filtered_02092026.csv \\
    --output polls_games_joined.csv \\
    --append

  # Dry run (see what would change)
  python build_polls_games_joined.py \\
    --polls github_polls_analytics_02092026.csv \\
    --games schedule_filtered_02092026.csv \\
    --output polls_games_joined.csv \\
    --append --dry-run
        """
    )

    parser.add_argument('--polls', required=True, help='Path to AP Poll analytics CSV')
    parser.add_argument('--games', required=True, help='Path to game schedule CSV')
    parser.add_argument('--output', required=True, help='Path for output joined CSV')
    parser.add_argument('--append', action='store_true',
                        help='Append new poll weeks only (skips existing weeks)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would change without writing')

    args = parser.parse_args()

    run_pipeline(
        polls_path=args.polls,
        games_path=args.games,
        output_path=args.output,
        append=args.append,
        dry_run=args.dry_run,
    )
