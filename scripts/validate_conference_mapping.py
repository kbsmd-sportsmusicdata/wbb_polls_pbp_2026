#!/usr/bin/env python3
"""
Validate Conference Mapping in polls_games_joined.csv

This script validates that game_opponent_conference assignments are correct
by comparing against the authoritative conference ID mapping table.
"""

import pandas as pd
import sys

# Authoritative conference ID mapping from user-provided table
CORRECT_CONF_ID_MAP = {
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

# Current (incorrect) mapping from build_polls_games_joined.py
CURRENT_INCORRECT_MAP = {
    2: 'ACC',
    4: 'Big 12',      # WRONG - should be 'Big East'
    7: 'Big Ten',
    8: 'Big East',    # WRONG - should be 'Big 12'
    23: 'SEC',
    46: 'A-10',       # WRONG - ID 46 doesn't exist
    20: 'Ivy League'  # WRONG - should be 12: 'Ivy'
}


def validate_conference_mapping():
    """Main validation function"""

    print("="*80)
    print("CONFERENCE MAPPING VALIDATION REPORT")
    print("="*80)

    # 1. Compare the mappings
    print("\n1. MAPPING COMPARISON")
    print("-" * 80)
    print("\nCurrent INCORRECT mapping vs CORRECT mapping:\n")

    all_ids = set(CORRECT_CONF_ID_MAP.keys()) | set(CURRENT_INCORRECT_MAP.keys())

    errors_found = []
    for conf_id in sorted(all_ids):
        current = CURRENT_INCORRECT_MAP.get(conf_id, '(missing)')
        correct = CORRECT_CONF_ID_MAP.get(conf_id, '(not in correct map)')

        if conf_id in CURRENT_INCORRECT_MAP and conf_id in CORRECT_CONF_ID_MAP:
            if current != correct:
                status = "❌ WRONG"
                errors_found.append((conf_id, current, correct))
            else:
                status = "✓ correct"
        elif conf_id in CURRENT_INCORRECT_MAP:
            status = "❌ ID doesn't exist in correct mapping"
            errors_found.append((conf_id, current, correct))
        else:
            status = "⚠️  missing from current map"

        print(f"  ID {conf_id:3d}: Current='{current:20s}' | Correct='{correct:20s}' | {status}")

    print(f"\nErrors found: {len(errors_found)}")
    print(f"Missing conferences: {len(CORRECT_CONF_ID_MAP) - len([k for k in CURRENT_INCORRECT_MAP.keys() if k in CORRECT_CONF_ID_MAP])}")

    # 2. Validate polls_games_joined.csv
    print("\n" + "="*80)
    print("2. VALIDATING polls_games_joined.csv")
    print("-" * 80)

    df = pd.read_csv('data/polls_games_joined.csv')

    print(f"\nTotal records: {len(df):,}")
    print(f"Unique opponents: {df['game_Opponent'].nunique():,}")

    # Check for mismatches
    df['expected_conference'] = df['game_Opponent Conf ID'].map(CORRECT_CONF_ID_MAP).fillna('Other')
    df['has_error'] = df['game_opponent_conference'] != df['expected_conference']

    errors = df[df['has_error']].copy()

    print(f"\n❌ Records with INCORRECT conference assignment: {len(errors):,}")
    print(f"✓  Records with correct conference assignment: {len(df) - len(errors):,}")
    print(f"   Error rate: {len(errors) / len(df) * 100:.2f}%")

    if len(errors) > 0:
        print("\n" + "-" * 80)
        print("SAMPLE OF INCORRECT ASSIGNMENTS (first 20):")
        print("-" * 80)

        sample = errors[['game_Opponent', 'game_Opponent Conf ID', 'game_opponent_conference', 'expected_conference']].head(20)
        for idx, row in sample.iterrows():
            opponent = str(row['game_Opponent']) if pd.notna(row['game_Opponent']) else '(unknown)'
            print(f"  Opponent: {opponent:25s} | Conf ID: {row['game_Opponent Conf ID']:5.0f} | "
                  f"Current: {row['game_opponent_conference']:15s} | Should be: {row['expected_conference']:15s}")

        # Summary by opponent
        print("\n" + "-" * 80)
        print("AFFECTED OPPONENTS (count of incorrect records):")
        print("-" * 80)

        affected = errors.groupby(['game_Opponent', 'game_Opponent Conf ID', 'game_opponent_conference', 'expected_conference']).size().reset_index(name='count')
        affected = affected.sort_values('count', ascending=False)

        for idx, row in affected.head(30).iterrows():
            print(f"  {row['game_Opponent']:25s} (ID {row['game_Opponent Conf ID']:5.0f}): "
                  f"{row['count']:4d} games | Current: {row['game_opponent_conference']:15s} → Should be: {row['expected_conference']:15s}")

        if len(affected) > 30:
            print(f"\n  ... and {len(affected) - 30} more affected opponents")

    # 3. Conference ID distribution
    print("\n" + "="*80)
    print("3. CONFERENCE ID DISTRIBUTION IN SOURCE DATA")
    print("-" * 80)

    conf_dist = df.groupby('game_Opponent Conf ID')['game_Opponent Conf ID'].count().sort_values(ascending=False)

    print(f"\nConference IDs found in data:")
    for conf_id, count in conf_dist.head(30).items():
        correct_name = CORRECT_CONF_ID_MAP.get(conf_id, '(Unknown)')
        current_name = CURRENT_INCORRECT_MAP.get(conf_id, '(Not mapped)')
        status = "✓" if conf_id in CURRENT_INCORRECT_MAP and CURRENT_INCORRECT_MAP[conf_id] == correct_name else "❌"
        print(f"  {status} ID {conf_id:3.0f}: {count:5d} games | Current: {current_name:20s} | Correct: {correct_name:20s}")

    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"\n✓ Validation complete!")
    print(f"\nAction needed:")
    print(f"  1. Update CONF_ID_MAP in scripts/process/build_polls_games_joined.py")
    print(f"  2. Regenerate polls_games_joined.csv")
    print(f"  3. Rerun validation to confirm fix")

    return len(errors) == 0


if __name__ == '__main__':
    success = validate_conference_mapping()
    sys.exit(0 if success else 1)
