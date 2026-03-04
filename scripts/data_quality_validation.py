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
    df = pd.read_csv('data/polls_games_joined.csv')

    print(f"\nDataset: data/polls_games_joined.csv")
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


if __name__ == '__main__':
    success = validate_dataset()
    sys.exit(0 if success else 1)
