"""Regression tests for scripts/process/prep_dashboard_data.py (run: pytest scripts/test)."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("prep", ROOT / "scripts/process/prep_dashboard_data.py")
prep = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prep)


class _Build:
    POLL_WEEK_WINDOWS = {"Pre": ("2026-09-01", "2026-11-08"), "11/9": ("2026-11-09", "2026-11-15")}


def test_sunday_night_game_stays_in_sunday_window():
    # 01:00Z on 11/9 is 8 pm ET on Sunday 11/8 -> Pre, not 11/9.
    games = pd.DataFrame({"date": ["2026-11-09T01:00Z", "2026-11-09T18:00Z", "2026-11-08T18:00Z"]})
    out = prep.make_eastern_assigner(_Build)(games)
    assert out["poll_week"].tolist() == ["Pre", "11/9", "Pre"]


def _frame():
    return pd.DataFrame({
        "team": ["A", "A", "A", "B", "B", "B"],
        "week_number": [0, 1, 2, 0, 1, 2],
        "game_id": [1, 2, np.nan, 1, np.nan, np.nan],
        "rank_numeric": [3, 4, 26, 26, 26, 25],
        "game_Team Rank": [3, 4, np.nan, 99, np.nan, np.nan],
        "game_Opponent Rank": [99, 99, np.nan, 3, np.nan, np.nan],
        "game_game_date_parsed": ["2026-11-02", "2026-11-10", "", "2026-11-02", "", ""],
        "running_wins": [1, 2, np.nan, 0, np.nan, np.nan],
        "running_road_win_pct": [np.nan, 1.0, np.nan, np.nan, np.nan, np.nan],
    })


def test_unranked_recode_and_labels():
    out = prep.add_prep_columns(_frame())
    a = out[out.team == "A"].set_index("week_number")
    assert pd.isna(a.loc[2, "rank_numeric_clean"]) and a.loc[2, "poll_rank_label"] == "NR"
    g = out[(out.team == "A") & (out.week_number == 0)].iloc[0]
    assert g["opp_rank_tipoff_label"] == "NR" and g["team_rank_tipoff_label"] == "#3"
    assert a.loc[1, "next_poll_rank_label"] == "NR"


def test_carry_forward_only_on_rows_without_a_game():
    out = prep.add_prep_columns(_frame())
    a = out[out.team == "A"].set_index("week_number")
    assert a.loc[2, "running_wins"] == 2 and bool(a.loc[2, "running_stats_carried"])
    assert not a.loc[1, "running_stats_carried"]
    b = out[out.team == "B"].set_index("week_number")
    assert b.loc[1, "running_wins"] == 0          # carried from week 0 game
    assert pd.isna(b.loc[1, "running_road_win_pct"])  # 0/0 stays blank


def test_one_unique_row_per_game():
    out = prep.add_prep_columns(_frame())
    g1 = out[out.game_id == 1]
    assert g1.is_unique_game.sum() == 1
    assert g1[g1.is_unique_game].iloc[0]["team"] == "A"   # higher-ranked at tip-off
