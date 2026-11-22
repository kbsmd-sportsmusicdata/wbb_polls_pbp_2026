"""Tests for the watchlist additions: config loader, synthesized rows, school types, scope flags,
ratings extras, gate metrics and validation."""
import importlib.util
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def _load(rel, name):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


prep = _load("scripts/process/prep_dashboard_data.py", "prep3")
gates = _load("scripts/process/evaluate_section_gates.py", "gates3")
dq = _load("scripts/data_quality_validation.py", "dq3")
import watchlist_config as wc  # noqa: E402


def test_loader_dedupes_keeps_order_and_tolerates_a_missing_file(tmp_path):
    f = tmp_path / "w.json"
    f.write_text(json.dumps({"schools": ["Baylor", " Rice ", "Baylor", "", "Texas Tech"]}))
    assert wc.load_watchlist(f) == ["Baylor", "Rice", "Texas Tech"]
    assert wc.load_watchlist(tmp_path / "nope.json") == []


def test_repo_watchlist_has_the_20_schools():
    w = wc.load_watchlist()
    assert len(w) == 20 and "Fairfield" in w and "Nebraska" in w and len(set(w)) == 20


def _polls():
    rows = []
    for team, ranks in [("Duke", [4, 5, 5]), ("NC State", [25, 26, 26])]:
        for wk, (name, r) in enumerate(zip(["Pre", "11/9", "11/16"], ranks)):
            rows.append({"season": 2027, "team": team, "conference": "ACC", "poll_week": name, "week_number": wk,
                         "rank": np.nan if r == 26 else float(r), "rank_numeric": r, "run_date": "2026-11-17", "table_id": 2})
    return pd.DataFrame(rows)


def _games():
    return pd.DataFrame({"Team": ["Baylor"] * 3, "Opponent": ["USC", "Rice", "Duke"], "Opponent Rank": [4, 99, 5], "Game Result": ["L", "W", "L"]})


def _ratings():
    return pd.DataFrame({"team": ["Baylor"], "Conf": ["Big 12"], "run_date": [pd.Timestamp("2026-11-14")]})


def test_watchlist_rows_mirror_an_unranked_team():
    polls = _polls().assign(**{c: 0.0 for c in ["prev_rank_numeric", "rank_change", "weeks_in_top25", "ranked_streak", "games_vs_top25", "wins_vs_top25", "losses_vs_top25", "win_pct_vs_top25"]},
                            movement_category="x", team_identity="x")
    rows, notes = prep.watchlist_poll_rows(polls, ["Duke", "Baylor", "Nobody"], _ratings(), _games())
    assert notes == ["Nobody"]                                  # no games and no ratings -> reported, not invented
    assert set(rows.team) == {"Baylor"} and len(rows) == 3      # Duke already in the polls -> skipped
    b = rows.set_index("poll_week")
    assert b.loc["Pre", "movement_category"] == "Unknown" and pd.isna(b.loc["Pre", "prev_rank_numeric"])
    assert (b["rank_numeric"] == 26).all() and b["rank"].isna().all() and (b["conference"] == "Big 12").all()
    assert b.loc["11/9", "games_vs_top25"] == 2 and b.loc["11/9", "wins_vs_top25"] == 0   # USC (#4) and Duke (#5), both losses


def _scoped():
    rows = [  # team, week, ranked?, game_id, team_rank_tipoff, opp_rank_tipoff
        ("Duke", 0, 4, 1, 4, np.nan), ("Duke", 1, 5, 2, 5, 25.0), ("Duke", 2, 5, np.nan, np.nan, np.nan),
        ("NC State", 0, 25, 3, 25, np.nan), ("NC State", 1, np.nan, np.nan, np.nan, np.nan), ("NC State", 2, np.nan, np.nan, np.nan, np.nan),
        ("Baylor", 0, np.nan, 2, np.nan, 5.0), ("Baylor", 1, np.nan, 4, np.nan, np.nan), ("Baylor", 2, np.nan, np.nan, np.nan, np.nan),
        ("Rice", 0, np.nan, 4, np.nan, np.nan), ("Rice", 1, np.nan, np.nan, np.nan, np.nan), ("Rice", 2, np.nan, np.nan, np.nan, np.nan),
    ]
    df = pd.DataFrame(rows, columns=["team", "week_number", "rank_numeric_clean", "game_id", "team_rank_tipoff", "opp_rank_tipoff"])
    df["has_game"] = df["game_id"].notna()
    return df


def test_school_types_and_scope():
    out = prep.add_school_columns(_scoped(), ["Baylor", "Rice"])
    t = out.set_index(["team", "week_number"])
    assert t.loc[("Duke", 1), "school_type"] == "Ranked"
    assert t.loc[("NC State", 0), "school_type"] == "Ranked" and t.loc[("NC State", 1), "school_type"] == "Previously Ranked"
    assert t.loc[("Baylor", 0), "school_type"] == "Watchlist" and t.loc[("Rice", 2), "school_type"] == "Watchlist"
    assert out[out.team == "Baylor"].is_poll_team.eq(False).all() and out[out.team == "NC State"].is_poll_team.all()
    # game 2 (Duke vs Baylor) involves a poll team; game 4 (Baylor vs Rice) does not
    scope = out.drop_duplicates("game_id").set_index("game_id").is_poll_scope_game
    assert bool(scope[2]) and not bool(scope[4]) and bool(scope[1]) and bool(scope[3])


def test_ratings_extras_are_carried_and_blank_without_a_snapshot():
    r = pd.DataFrame({"team": ["Baylor"], "run_date": [pd.Timestamp("2026-11-14")], "W": [5], "L": [1], "Rk": [30], "Pts": [93.5], "Opp": [56.2],
                      "MOV": [37.3], "SOS": [-4.6], "OSRS": [21.2], "DSRS": [11.5], "SRS": [32.7], "ORtg": [111.3], "DRtg": [73.0], "NRtg": [38.3],
                      "pyth_win_pct": [0.99], "pyth_wins": [5.9], "wins_minus_pyth_wins": [-0.9]})
    df = pd.DataFrame({"team": ["Baylor"] * 2, "poll_week": ["Pre", "11/9"], "run_date": "2026-11-17"})
    out = prep.add_ratings_columns(df, r, {"Pre": ["2026-09-01", "2026-11-08"], "11/9": ["2026-11-09", "2026-11-15"]}, 10.0).set_index("poll_week")
    assert out.loc["11/9", ["rt_pts", "rt_opp", "rt_mov", "rt_sos", "rt_osrs", "rt_dsrs", "rt_srs", "rt_ortg", "rt_drtg", "rt_nrtg", "rt_rk"]].notna().all()
    assert out.loc["11/9", "rt_srs"] == 32.7
    assert out.loc["Pre", ["rt_pts", "rt_srs", "rt_nrtg"]].isna().all()


def test_gate_metrics_ignore_watchlist_schools_and_out_of_scope_games():
    df = prep.add_school_columns(_scoped(), ["Baylor", "Rice"])
    df = df.assign(is_unique_game=df["has_game"] & ~df.duplicated("game_id"), game_Location="Home", game_is_conf_game=False,
                   game_is_upset="No Upset", game_quality_tier="Ranked vs Unranked")
    m = gates.compute_metrics(df)
    assert m["teams"] == 2                      # Duke and NC State only
    assert m["unique_games"] == 3               # games 1, 2, 3; game 4 (Baylor vs Rice) is out of scope
    assert m["watchlist_teams"] == 2


def test_validation_flags_bad_school_columns():
    df = prep.add_school_columns(_scoped(), ["Baylor", "Rice"])
    df = df.assign(is_ranked=df.rank_numeric_clean.notna(), is_unique_game=df.has_game & ~df.duplicated("game_id"), poll_rank_label="NR",
                   team_rank_tipoff_label=None, opp_rank_tipoff_label=None, next_poll_rank_label=None, next_poll_rank_numeric=np.nan,
                   running_stats_carried=False, running_wins=np.nan, poll_week="Pre")
    assert not [i for i in dq.check_dashboard_frame(df, watchlist=["Baylor", "Rice"]) if "school" in i.lower() or "watchlist" in i.lower() or "poll" in i.lower()]
    bad = df.assign(school_type=np.where(df.team == "Duke", "Watchlist", df.school_type))          # Duke is ranked
    assert any("'Ranked' does not match" in i for i in dq.check_dashboard_frame(bad))
    assert any("watchlist schools missing" in i for i in dq.check_dashboard_frame(df, watchlist=["Baylor", "Gonzaga"]))
