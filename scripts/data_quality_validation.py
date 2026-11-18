#!/usr/bin/env python3
"""
Comprehensive Data Quality Validation for polls_games_joined.csv

Performs extensive validation checks across all critical fields to ensure
data integrity for Tableau consumption.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import sys
from pathlib import Path

# season_config.py lives at the repo root; running this script directly
# (python scripts/data_quality_validation.py) does not put the repo root on
# sys.path, only this file's own directory -- add it explicitly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from season_config import get_season_data_dir

POLLS_GAMES_JOINED = get_season_data_dir() / "polls_games_joined.csv"
POLLS_GAMES_DASHBOARD = get_season_data_dir() / "polls_games_dashboard.csv"
POLL_WEEK_CONFIG = Path(__file__).resolve().parent.parent / "config" / "poll_week_windows.json"

# Conference mapping for validation
CONF_ID_MAP = {
    1: 'Am. East', 2: 'ACC', 4: 'Big East', 5: 'Big Sky', 7: 'Big Ten',
    8: 'Big 12', 9: 'Big West', 10: 'CAA', 11: 'CUSA', 12: 'Ivy',
    13: 'MAAC', 14: 'MAC', 16: 'MEAC', 18: 'MVC', 19: 'NEC',
    23: 'SEC', 25: 'Southland', 26: 'SWAC', 29: 'WCC', 30: 'WAC',
    44: 'Mountain West', 45: 'Horizon', 47: 'Summit', 62: 'American'
}


def validate_dataset():
    """Main validation function"""

    print("=" * 80)
    print("COMPREHENSIVE DATA QUALITY VALIDATION")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # Load dataset
    df = pd.read_csv(POLLS_GAMES_JOINED)

    print(f"\nDataset: {POLLS_GAMES_JOINED}")
    print(f"  Rows: {len(df):,}")
    print(f"  Columns: {len(df.columns)}")
    print(f"  Size: {df.memory_usage(deep=True).sum() / 1024:.1f} KB")

    issues_found = []

    # =========================================================================
    # 1. CONFERENCE MAPPING VALIDATION
    # =========================================================================
    print("\n" + "=" * 80)
    print("1. CONFERENCE MAPPING VALIDATION")
    print("=" * 80)

    df['expected_conference'] = df['game_Opponent Conf ID'].map(CONF_ID_MAP).fillna('Other')
    df['conf_mismatch'] = df['game_opponent_conference'] != df['expected_conference']

    # Exclude NaN opponents from conference validation
    real_opponents = df[df['game_Opponent'].notna()]
    conf_errors = real_opponents[real_opponents['conf_mismatch']]

    print(f"\n✓ Conference assignments validated")
    print(f"  Total games: {len(real_opponents):,}")
    print(f"  Correct assignments: {len(real_opponents) - len(conf_errors):,}")
    print(f"  Incorrect assignments: {len(conf_errors):,}")
    print(f"  Accuracy: {((len(real_opponents) - len(conf_errors)) / len(real_opponents) * 100):.2f}%")

    if len(conf_errors) > 0:
        issues_found.append(f"Conference mapping errors: {len(conf_errors)}")
        print("\n  ❌ ERRORS FOUND:")
        for idx, row in conf_errors.head(10).iterrows():
            print(f"    {row['game_Opponent']:25s} | ID: {row['game_Opponent Conf ID']:.0f} | "
                  f"Current: {row['game_opponent_conference']:15s} | Expected: {row['expected_conference']:15s}")

    # =========================================================================
    # 2. DATA COMPLETENESS CHECKS
    # =========================================================================
    print("\n" + "=" * 80)
    print("2. DATA COMPLETENESS CHECKS")
    print("=" * 80)

    critical_fields = [
        'season', 'team', 'conference', 'poll_week', 'rank_numeric',
        'game_id', 'game_date', 'game_Location', 'game_Game Result',
        'game_Team Score', 'game_Opponent Score', 'game_scoring_margin',
        'game_is_win', 'game_closeness', 'game_opponent_conference'
    ]

    print("\nChecking critical fields for missing values:")
    for field in critical_fields:
        if field in df.columns:
            missing = df[field].isna().sum()
            pct = (missing / len(df)) * 100
            status = "✓" if missing == 0 else "⚠️"
            print(f"  {status} {field:30s}: {missing:4d} missing ({pct:5.2f}%)")

            if missing > 0 and field not in ['game_Opponent', 'rank_numeric']:
                issues_found.append(f"{field}: {missing} missing values")

    # =========================================================================
    # 3. DATA INTEGRITY CHECKS
    # =========================================================================
    print("\n" + "=" * 80)
    print("3. DATA INTEGRITY CHECKS")
    print("=" * 80)

    # Check scoring margins match scores
    print("\n3a. Scoring margin consistency:")
    df['calc_margin'] = df['game_Team Score'] - df['game_Opponent Score']
    margin_mismatch = df[df['game_scoring_margin'] != df['calc_margin']].dropna(subset=['game_scoring_margin', 'calc_margin'])

    if len(margin_mismatch) == 0:
        print(f"  ✓ All scoring margins are consistent with team/opponent scores")
    else:
        print(f"  ❌ {len(margin_mismatch)} records have incorrect scoring margins")
        issues_found.append(f"Scoring margin errors: {len(margin_mismatch)}")

    # Check win/loss consistency
    print("\n3b. Win/loss flag consistency:")
    df['calc_is_win'] = (df['game_Team Score'] > df['game_Opponent Score']).astype(float)
    win_mismatch = df[df['game_is_win'] != df['calc_is_win']].dropna(subset=['game_is_win', 'calc_is_win'])

    if len(win_mismatch) == 0:
        print(f"  ✓ All game_is_win flags are consistent with scores")
    else:
        print(f"  ❌ {len(win_mismatch)} records have incorrect game_is_win flags")
        issues_found.append(f"Win/loss flag errors: {len(win_mismatch)}")

    # Check running records consistency
    print("\n3c. Running records consistency:")
    for team in df['team'].unique():
        team_df = df[df['team'] == team].sort_values('season_game_number')

        # Check running win_pct
        invalid_pct = team_df[
            (team_df['running_win_pct'].notna()) &
            ((team_df['running_win_pct'] < 0) | (team_df['running_win_pct'] > 1))
        ]

        if len(invalid_pct) > 0:
            print(f"  ⚠️  {team}: {len(invalid_pct)} records with invalid running_win_pct")
            issues_found.append(f"{team}: Invalid win_pct values")

    # =========================================================================
    # 4. POLL WEEK ASSIGNMENTS
    # =========================================================================
    print("\n" + "=" * 80)
    print("4. POLL WEEK ASSIGNMENTS")
    print("=" * 80)

    poll_week_dist = df['poll_week'].value_counts().sort_index()
    print(f"\nGames distribution across {len(poll_week_dist)} poll weeks:")
    for week, count in poll_week_dist.items():
        print(f"  {week:8s}: {count:4d} games")

    # Check for games without poll week
    no_poll_week = df[df['poll_week'].isna()]
    if len(no_poll_week) > 0:
        print(f"\n  ⚠️  {len(no_poll_week)} games without poll_week assignment")
        issues_found.append(f"Missing poll_week: {len(no_poll_week)} games")

    # =========================================================================
    # 5. CLOSENESS CATEGORIZATION
    # =========================================================================
    print("\n" + "=" * 80)
    print("5. CLOSENESS CATEGORIZATION")
    print("=" * 80)

    closeness_dist = df['game_closeness'].value_counts()
    print(f"\nGame closeness distribution:")
    for category, count in closeness_dist.items():
        print(f"  {category:20s}: {count:4d} games")

    # =========================================================================
    # 6. QUALITY TIER DISTRIBUTION
    # =========================================================================
    print("\n" + "=" * 80)
    print("6. QUALITY TIER DISTRIBUTION")
    print("=" * 80)

    quality_dist = df['game_quality_tier'].value_counts()
    print(f"\nGame quality tier distribution:")
    for tier, count in quality_dist.items():
        print(f"  {tier:25s}: {count:4d} games")

    # =========================================================================
    # 7. UPSET DETECTION
    # =========================================================================
    print("\n" + "=" * 80)
    print("7. UPSET DETECTION")
    print("=" * 80)

    upsets = df[df['game_is_upset'] == 'Upset']
    print(f"\nTotal upsets detected: {len(upsets)}")
    print(f"  Upsets suffered: {df['gw_upsets_suffered'].sum():.0f}")
    print(f"  Upsets pulled: {df['gw_upsets_pulled'].sum():.0f}")

    # =========================================================================
    # 8. SUMMARY
    # =========================================================================
    print("\n" + "=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)

    if len(issues_found) == 0:
        print("\n✅ ALL VALIDATION CHECKS PASSED!")
        print("\nDataset is ready for Tableau consumption.")
        return True
    else:
        print(f"\n⚠️  {len(issues_found)} ISSUES FOUND:")
        for i, issue in enumerate(issues_found, 1):
            print(f"  {i}. {issue}")
        print("\nReview and address issues before using in Tableau.")
        return False


def check_dashboard_frame(df, joined_columns=None, windows=None):
    """Validate a polls_games_dashboard frame; return a list of issue strings.

    Kept free of file I/O so tests can feed it small frames.
    - one unique game per game_id (is_unique_game)
    - no 26 / 99 sentinels left in the *_clean / tip-off rank columns or labels
    - carried running stats only on rows with no game (and never on Pre)
    - every original joined column is still present
    - every game's Eastern date sits inside its poll-week window
    """
    issues = []
    has_game = df['game_id'].notna()

    # 1. one unique game per game_id
    flagged = df[df['is_unique_game'].astype(bool)]
    if (flagged['game_id'].isna()).any():
        issues.append("is_unique_game is True on rows with no game")
    dup = flagged['game_id'].duplicated().sum()
    if dup:
        issues.append(f"is_unique_game flags {dup} game_id(s) more than once")
    missing_flag = set(df.loc[has_game, 'game_id']) - set(flagged['game_id'])
    if missing_flag:
        issues.append(f"{len(missing_flag)} game_id(s) have no is_unique_game row")

    # 2. sentinels must be gone from the clean columns
    for col in ['rank_numeric_clean', 'team_rank_tipoff', 'opp_rank_tipoff', 'next_poll_rank_numeric']:
        bad = df[col].notna() & ((df[col] > 25) | (df[col] < 1))
        if bad.any():
            issues.append(f"{col}: {int(bad.sum())} value(s) outside 1-25 (26/99 sentinel left in)")
    for col in ['poll_rank_label', 'team_rank_tipoff_label', 'opp_rank_tipoff_label', 'next_poll_rank_label']:
        bad = df[col].isin(['#26', '#99'])
        if bad.any():
            issues.append(f"{col}: {int(bad.sum())} label(s) show an unranked sentinel")
    if (df['is_ranked'] != df['rank_numeric_clean'].notna()).any():
        issues.append("is_ranked disagrees with rank_numeric_clean")

    # 3. carry-forward only where there is no game, never on the preseason poll
    carried = df['running_stats_carried'].astype(bool)
    if (carried & has_game).any():
        issues.append(f"{int((carried & has_game).sum())} row(s) have a game AND running_stats_carried")
    if (carried & (df['week_number'] == 0)).any():
        issues.append("running_stats_carried set on preseason (week 0) rows")
    run_cols = [c for c in df.columns if c.startswith('running_') and c != 'running_stats_carried']
    ordered = df.sort_values(['team', 'week_number']).copy()
    last = ordered[ordered['game_id'].notna()].groupby('team')['running_wins'].last()
    for team, row in ordered[carried.reindex(ordered.index)].groupby('team')['running_wins'].last().items():
        if team in last.index and row != last[team] and ordered.loc[ordered['team'] == team, 'game_id'].notna().any():
            issues.append(f"{team}: carried running_wins ({row}) != last game row ({last[team]})")

    # 4. no original column lost
    if joined_columns is not None:
        lost = [c for c in joined_columns if c not in df.columns]
        if lost:
            issues.append(f"columns missing vs polls_games_joined.csv: {lost}")

    # 5. Eastern-date windows
    if windows is not None and has_game.any():
        ts = pd.to_datetime(df.loc[has_game, 'game_date'], utc=True, errors='coerce')
        et = ts.dt.tz_convert('America/New_York').dt.tz_localize(None).dt.normalize()
        starts = df.loc[has_game, 'poll_week'].map(lambda w: pd.Timestamp(windows[w][0]) if w in windows else pd.NaT)
        ends = df.loc[has_game, 'poll_week'].map(lambda w: pd.Timestamp(windows[w][1]) if w in windows else pd.NaT)
        outside = (et < starts) | (et > ends) | starts.isna()
        if outside.any():
            issues.append(f"{int(outside.sum())} game row(s) fall outside their poll-week window by Eastern date")
    return issues


def validate_dashboard_dataset():
    """Validate polls_games_dashboard.csv (written by prep_dashboard_data.py)."""
    import json

    print("\n" + "=" * 80)
    print("DASHBOARD DATASET VALIDATION (polls_games_dashboard.csv)")
    print("=" * 80)

    if not POLLS_GAMES_DASHBOARD.exists():
        print(f"  ❌ {POLLS_GAMES_DASHBOARD} not found -- run scripts/process/prep_dashboard_data.py")
        return False

    df = pd.read_csv(POLLS_GAMES_DASHBOARD)
    joined_cols = list(pd.read_csv(POLLS_GAMES_JOINED, nrows=0).columns) if POLLS_GAMES_JOINED.exists() else None
    windows = None
    if POLL_WEEK_CONFIG.exists():
        windows = json.loads(POLL_WEEK_CONFIG.read_text()).get('windows')
    issues = check_dashboard_frame(df, joined_cols, windows)

    if POLLS_GAMES_JOINED.exists():
        n_joined = len(pd.read_csv(POLLS_GAMES_JOINED, usecols=['team']))
        if n_joined != len(df):
            issues.append(f"row count {len(df)} != polls_games_joined.csv ({n_joined})")

    print(f"  Rows: {len(df):,}   unique games: {int(df['is_unique_game'].sum())}   "
          f"carried rows: {int(df['running_stats_carried'].sum())}")
    if issues:
        for i, issue in enumerate(issues, 1):
            print(f"  ❌ {i}. {issue}")
        return False
    print("  ✅ one unique game per game_id; no 26/99 in clean columns; carry-forward only on no-game rows; "
          "all joined columns kept; Eastern-date windows hold")
    return True


if __name__ == '__main__':
    success = validate_dataset()
    dashboard_ok = validate_dashboard_dataset()
    sys.exit(0 if (success and dashboard_ok) else 1)
