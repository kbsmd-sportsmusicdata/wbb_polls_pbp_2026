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
Last updated: 2026-02-08
"""

import pandas as pd
import numpy as np
import argparse
import os
import sys
from datetime import datetime


# =============================================================================
# CONFIGURATION
# =============================================================================

# Conference ID to name mapping (ESPN IDs)
CONF_ID_MAP = {
    2: 'ACC', 4: 'Big 12', 7: 'Big Ten', 8: 'Big East',
    23: 'SEC', 46: 'A-10', 20: 'Ivy League'
}

# Poll week date windows: poll_week_label -> (start_date, end_date)
# Games played within these windows are attributed to that poll week.
# Update these each season to match the AP Poll release schedule.
POLL_WEEK_WINDOWS = {
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
    # --- ADD NEW POLL WEEKS BELOW THIS LINE ---
    # '2/9':   ('2026-02-09', '2026-02-15'),
    # '2/16':  ('2026-02-16', '2026-02-22'),
    # '2/23':  ('2026-02-23', '2026-03-01'),
    # '3/2':   ('2026-03-02', '2026-03-08'),
    # '3/9':   ('2026-03-09', '2026-03-15'),
    # 'Final': ('2026-03-16', '2026-03-22'),
}

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
    """Map each game date to its corresponding AP Poll week window."""
    games_df['game_date_parsed'] = pd.to_datetime(games_df['date'].str[:10])

    def _assign(game_date):
        for pw, (start, end) in POLL_WEEK_WINDOWS.items():
            if pd.Timestamp(start) <= game_date <= pd.Timestamp(end):
                return pw
        return None

    games_df['poll_week'] = games_df['game_date_parsed'].apply(_assign)

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

    # --- Upset classification ---
    gdf['game_is_upset'] = np.where(
        (gdf['game_Team Rank'].notna()) & (gdf['game_Team Rank'] <= 25) &
        ((gdf['game_Opponent Rank'].isna()) | (gdf['game_Opponent Rank'] > 25)) &
        (gdf['game_Game Result'] == 'L'),
        'Upset Loss',
        np.where(
            ((gdf['game_Team Rank'].isna()) | (gdf['game_Team Rank'] > 25)) &
            (gdf['game_Opponent Rank'].notna()) & (gdf['game_Opponent Rank'] <= 25) &
            (gdf['game_Game Result'] == 'W'),
            'Upset Win', 'No Upset'
        )
    )

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
        upsets_suffered=('game_is_upset', lambda x: (x == 'Upset Loss').sum()),
        upsets_pulled=('game_is_upset', lambda x: (x == 'Upset Win').sum()),
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
    """Flag entry/exit events and identify the trigger game from the prior week."""
    df['entry_exit_event'] = np.where(
        (df['rank_numeric'] <= 25) & (df['prev_rank_numeric'] > 25), 'Entered',
        np.where(
            (df['rank_numeric'] > 25) & (df['prev_rank_numeric'] <= 25), 'Exited', 'None'
        )
    )

    # Build game lookup for trigger identification
    games_only = df[df['game_id'].notna()].copy()
    if 'game_game_date_parsed' in games_only.columns:
        games_only['_gdate'] = pd.to_datetime(games_only['game_game_date_parsed'], errors='coerce')
    else:
        games_only['_gdate'] = pd.NaT

    def _pick_trigger(team, week_num, event_type):
        prev_week = week_num - 1
        cands = games_only[(games_only['team'] == team) & (games_only['week_number'] == prev_week)]
        if cands.empty:
            return None
        if event_type == 'Entered':
            wins = cands[cands['game_Game Result'] == 'W']
            if not wins.empty:
                return wins.loc[wins['game_scoring_margin'].idxmax()]
            return cands.loc[cands['_gdate'].idxmax()]
        else:
            losses = cands[cands['game_Game Result'] == 'L']
            if not losses.empty:
                return losses.loc[losses['game_scoring_margin'].idxmin()]
            return cands.loc[cands['_gdate'].idxmax()]

    events = df[df['entry_exit_event'] != 'None'][['team', 'week_number', 'entry_exit_event']].drop_duplicates()
    records = []
    for _, row in events.iterrows():
        game = _pick_trigger(row['team'], row['week_number'], row['entry_exit_event'])
        if game is not None:
            records.append({
                'team': row['team'], 'week_number': row['week_number'],
                'trigger_game_opponent': game.get('game_Opponent'),
                'trigger_game_result': game.get('game_Game Result'),
                'trigger_game_team_score': game.get('game_Team Score'),
                'trigger_game_opp_score': game.get('game_Opponent Score'),
                'trigger_game_date': str(game['_gdate'].date()) if pd.notna(game.get('_gdate')) else None,
                'trigger_game_margin': game.get('game_scoring_margin'),
                'trigger_game_location': game.get('game_Location'),
            })

    if records:
        trigger_df = pd.DataFrame(records)
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
    """Compute upset magnitude and display labels."""
    def _magnitude(row):
        if pd.isna(row.get('game_Team Rank')) or pd.isna(row.get('game_Opponent Rank')):
            return None
        tr = row['game_Team Rank'] if row['game_Team Rank'] <= 25 else 26
        opr = row['game_Opponent Rank'] if row['game_Opponent Rank'] <= 25 else 26
        return abs(tr - opr)

    def _label(row):
        if row.get('game_is_upset') == 'Upset Loss':
            tr = int(row['game_Team Rank']) if row['game_Team Rank'] <= 25 else 'Unranked'
            opr = int(row['game_Opponent Rank']) if row['game_Opponent Rank'] <= 25 else 'Unranked'
            if isinstance(tr, int):
                return f"#{tr} {row['team']} lost to {opr} {row['game_Opponent']}"
        elif row.get('game_is_upset') == 'Upset Win':
            tr = int(row['game_Team Rank']) if row['game_Team Rank'] <= 25 else 'Unranked'
            opr = int(row['game_Opponent Rank']) if row['game_Opponent Rank'] <= 25 else 'Unranked'
            if isinstance(opr, int):
                return f"{tr} {row['team']} beat #{opr} {row['game_Opponent']}"
        return None

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
    print()

    # --- Load data ---
    print("[1/9] Loading source data...")
    polls = pd.read_csv(polls_path)
    games = pd.read_csv(games_path)
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
