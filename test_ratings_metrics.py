"""Checks for the Pythagorean columns added by scrape_ratings.py. Run: pytest test_ratings_metrics.py"""
import io
import math

import pandas as pd

import scrape_ratings as sr


def _df(rows):
    return pd.DataFrame(rows, columns=["School", "W", "L", "Pts", "Opp"])


def test_matches_textbook_formula():
    out = sr.add_pythagorean_metrics(_df([["A", 5, 0, 91.0, 63.8]]))
    k = 13.91
    expected = 91.0**k / (91.0**k + 63.8**k)
    assert math.isclose(out.pyth_win_pct[0], expected, abs_tol=5e-5)
    assert math.isclose(out.pyth_wins[0], expected * 5, abs_tol=5e-3)
    assert math.isclose(out.wins_minus_pyth_wins[0], 5 - expected * 5, abs_tol=5e-3)


def test_even_scoring_is_a_coin_flip():
    out = sr.add_pythagorean_metrics(_df([["A", 3, 3, 70.0, 70.0]]))
    assert out.pyth_win_pct[0] == 0.5 and out.pyth_wins[0] == 3.0 and out.wins_minus_pyth_wins[0] == 0.0


def test_no_games_or_missing_data_is_nan_not_zero():
    out = sr.add_pythagorean_metrics(_df([["A", 0, 0, 70.0, 60.0], ["B", 2, 1, None, None]]))
    assert out[["pyth_win_pct", "pyth_wins", "wins_minus_pyth_wins"]].isna().all().all()


def test_unnamed_spacer_columns_dropped():
    df = pd.DataFrame({"School": ["A"], "Unnamed: 3_level_1": [None], "W": [1]})
    assert list(sr.drop_unnamed_columns(df).columns) == ["School", "W"]


def test_append_aligns_by_column_name_and_dedups(tmp_path):
    m = tmp_path / "master.csv"
    a = pd.DataFrame({"School": ["A"], "W": [1], "run_date": ["2026-11-10"]})
    b = pd.DataFrame({"School": ["B"], "pyth_wins": [2.0], "W": [3], "run_date": ["2026-11-13"]})  # new col, new order
    sr.append_csv_with_dedup(a, m)
    sr.append_csv_with_dedup(b, m)
    sr.append_csv_with_dedup(b, m)  # same run_date -> skipped
    out = pd.read_csv(m)
    assert len(out) == 2 and list(out.columns)[-1] == "run_date"
    assert out.loc[out.School == "B", "W"].item() == 3
