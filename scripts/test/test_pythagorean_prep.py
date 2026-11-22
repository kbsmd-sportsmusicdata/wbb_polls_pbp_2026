"""Tests for the Pythagorean join in prep_dashboard_data.py and its validation/gate metrics."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]


def _load(rel, name):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


prep = _load("scripts/process/prep_dashboard_data.py", "prep2")
dq = _load("scripts/data_quality_validation.py", "dq2")
gates = _load("scripts/process/evaluate_section_gates.py", "gates2")

WINDOWS = {"Pre": ["2026-09-01", "2026-11-08"], "11/9": ["2026-11-09", "2026-11-15"], "11/16": ["2026-11-16", "2026-11-22"]}


def _polls():
    return pd.DataFrame({
        "team": ["TCU"] * 3 + ["Duke"] * 3,
        "poll_week": ["Pre", "11/9", "11/16"] * 2,
        "week_number": [0, 1, 2] * 2,
        "run_date": ["2026-11-17"] * 6,
    })


def _ratings():
    rows = []
    for team, d, w, l, pyth in [("TCU", "2026-11-14", 3, 0, 2.4), ("TCU", "2026-11-16", 4, 0, 3.6), ("TCU", "2026-11-22", 6, 0, 5.0),
                                ("Duke", "2026-11-14", 3, 0, 1.5)]:
        rows.append({"team": team, "run_date": pd.Timestamp(d), "W": w, "L": l, "pyth_win_pct": round(pyth / (w + l), 4), "pyth_wins": pyth,
                     "wins_minus_pyth_wins": round(w - pyth, 2)})
    return pd.DataFrame(rows)


def test_snapshot_is_capped_by_window_end_and_run_date():
    out = prep.add_pythagorean_columns(_polls(), _ratings(), WINDOWS, 10.0)
    tcu = out[out.team == "TCU"].set_index("poll_week")
    assert pd.isna(tcu.loc["Pre", "pyth_wins"])                     # no snapshot before the preseason window closes
    assert tcu.loc["11/9", "pyth_snapshot_date"] == "2026-11-14"    # latest on/before 11/15
    # 11/16 window ends 11/22 but the dashboard only has data through 11/17 -> 11/16 snapshot, not 11/22
    assert tcu.loc["11/16", "pyth_snapshot_date"] == "2026-11-16" and tcu.loc["11/16", "pyth_games"] == 4


def test_labels_and_unmatched_teams_stay_blank():
    out = prep.add_pythagorean_columns(_polls(), _ratings(), WINDOWS, 10.0)
    tcu = out[(out.team == "TCU") & (out.poll_week == "11/16")].iloc[0]
    assert tcu.pyth_luck_label == "In line" and tcu.pyth_exponent == 10.0           # 4 wins vs 3.6 expected = +0.4
    duke = out[(out.team == "Duke") & (out.poll_week == "11/16")].iloc[0]
    assert duke.pyth_snapshot_date == "2026-11-14"                    # carries the latest available snapshot
    assert duke.pyth_luck_label == "Over-performing"                  # 3 wins vs 1.5 expected = +1.5


def test_missing_ratings_gives_blank_columns_not_an_error():
    out = prep.add_pythagorean_columns(_polls(), None, WINDOWS, 10.0)
    assert out["pyth_wins"].isna().all() and "pyth_luck_label" in out.columns


def test_alias_maps_texas_christian_to_tcu(tmp_path):
    pd.DataFrame({"School": ["Texas Christian"], "W": [1], "L": [0], "run_date": ["2026-11-14"], "pyth_win_pct": [0.9],
                  "pyth_wins": [0.9], "wins_minus_pyth_wins": [0.1]}).to_csv(tmp_path / "ratings_master.csv", index=False)
    r = prep.load_ratings(tmp_path)
    assert r.team.tolist() == ["TCU"]


def test_validation_catches_bad_pythagorean_values():
    d = prep.add_pythagorean_columns(_polls(), _ratings(), WINDOWS, 10.0)
    assert not [i for i in dq.check_dashboard_frame(d.assign(game_id=np.nan, is_unique_game=False, rank_numeric_clean=np.nan,
               team_rank_tipoff=np.nan, opp_rank_tipoff=np.nan, next_poll_rank_numeric=np.nan, poll_rank_label="NR",
               team_rank_tipoff_label=None, opp_rank_tipoff_label=None, next_poll_rank_label=None, is_ranked=False,
               running_stats_carried=False, running_wins=np.nan)) if "yth" in i]
    bad = d.copy(); bad.loc[bad.pyth_wins.notna(), "pyth_wins"] = 99
    issues = dq.check_dashboard_frame(bad.assign(game_id=np.nan, is_unique_game=False, rank_numeric_clean=np.nan,
               team_rank_tipoff=np.nan, opp_rank_tipoff=np.nan, next_poll_rank_numeric=np.nan, poll_rank_label="NR",
               team_rank_tipoff_label=None, opp_rank_tipoff_label=None, next_poll_rank_label=None, is_ranked=False,
               running_stats_carried=False, running_wins=np.nan))
    assert any("pyth_wins outside" in i for i in issues)


def test_gate_metrics_default_to_zero_without_pythagorean_columns():
    df = pd.DataFrame({"team": ["A"], "week_number": [0], "has_game": [False], "is_unique_game": [False], "game_Location": [None],
                       "game_is_conf_game": [None], "opp_rank_tipoff": [np.nan], "game_is_upset": [None], "game_quality_tier": [None], "game_id": [np.nan]})
    m = gates.compute_metrics(df)
    assert m["pyth_teams"] == 0 and m["pct_teams_pyth_games_ge8"] == 0.0


def test_texas_christian_is_standardized_to_tcu():
    """data/raw/team_name_standardization.csv maps the ratings name to the poll name."""
    import sys
    sys.path.insert(0, str(ROOT))
    import team_name_utils as t
    out = t.standardize_team_names(pd.DataFrame({"School": ["Texas Christian", "TCU", "Duke"]}), ["School"])
    assert out.School.tolist() == ["TCU", "TCU", "Duke"]


def test_band_scales_with_games():
    f = prep.pyth_luck_band
    assert abs(float(f(6)) - 0.98) < 0.01          # ~1 win at 6 games
    assert abs(float(f(25)) - 2.0) < 0.01          # 2 wins at 25 games
    assert float(f(1)) == prep.PYTH_BAND_FLOOR     # tiny samples use the floor
    assert float(f(14)) > float(f(6))


def test_same_gap_is_labelled_differently_early_and_late():
    ratings = pd.DataFrame({"team": ["A", "B"], "run_date": pd.Timestamp("2026-11-14"), "W": [6, 25], "L": [0, 0],
                            "pyth_win_pct": [0.8, 0.9], "pyth_wins": [4.8, 22.5],     # both +1.2 / +2.5 wins above expectation
                            "wins_minus_pyth_wins": [1.2, 2.5]})
    polls = pd.DataFrame({"team": ["A", "B"], "poll_week": "11/9", "week_number": 1, "run_date": "2026-11-17"})
    out = prep.add_pythagorean_columns(polls, ratings, WINDOWS, 10.0).set_index("team")
    assert out.loc["A", "pyth_luck_label"] == "Over-performing" and out.loc["A", "pyth_luck_band"] == 0.98
    assert out.loc["B", "pyth_luck_label"] == "Over-performing" and out.loc["B", "pyth_luck_band"] == 2.0
    ratings.loc[1, ["wins_minus_pyth_wins", "pyth_wins"]] = [1.5, 23.5]               # +1.5 at 25 games is inside the 2.0 band
    out = prep.add_pythagorean_columns(polls, ratings, WINDOWS, 10.0).set_index("team")
    assert out.loc["B", "pyth_luck_label"] == "In line"
