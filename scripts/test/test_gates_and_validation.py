"""Tests for evaluate_section_gates.py and the dashboard checks in data_quality_validation.py."""
import importlib.util
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]


def _load(rel, name):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


gates = _load("scripts/process/evaluate_section_gates.py", "gates")
prep = _load("scripts/process/prep_dashboard_data.py", "prep")
dq = _load("scripts/data_quality_validation.py", "dq")


def _dash():
    df = pd.DataFrame({
        "team": ["A", "A", "A", "B", "B", "B"],
        "week_number": [0, 1, 2, 0, 1, 2],
        "poll_week": ["Pre", "11/9", "11/16"] * 2,
        "game_id": [1, 2, np.nan, 1, np.nan, np.nan],
        "game_date": ["2026-11-03T18:00Z", "2026-11-10T18:00Z", None, "2026-11-03T18:00Z", None, None],
        "rank_numeric": [3, 4, 26, 26, 26, 25],
        "game_Team Rank": [3, 4, np.nan, 99, np.nan, np.nan],
        "game_Opponent Rank": [99, 99, np.nan, 3, np.nan, np.nan],
        "game_game_date_parsed": ["2026-11-03", "2026-11-10", "", "2026-11-03", "", ""],
        "running_wins": [1, 2, np.nan, 0, np.nan, np.nan],
        "game_Location": ["Home", "Away", None, "Away", None, None],
        "game_is_conf_game": [False, False, None, False, None, None],
        "game_is_upset": ["No Upset", "Upset", None, "No Upset", None, None],
        "game_quality_tier": ["Ranked vs Unranked", "Top 25 Matchup", None, "Ranked vs Unranked", None, None],
        "run_date": ["2026-11-17"] * 6,
    })
    return prep.add_prep_columns(df)


WINDOWS = {"Pre": ["2026-09-01", "2026-11-08"], "11/9": ["2026-11-09", "2026-11-15"], "11/16": ["2026-11-16", "2026-11-22"]}


def test_clean_frame_passes():
    assert dq.check_dashboard_frame(_dash(), windows=WINDOWS) == []


def test_duplicate_unique_flag_is_caught():
    d = _dash()
    d["is_unique_game"] = d["game_id"].notna()      # flags both sides of game 1
    assert any("more than once" in i for i in dq.check_dashboard_frame(d))


def test_sentinels_left_in_clean_columns_are_caught():
    d = _dash()
    d.loc[d.index[0], "team_rank_tipoff"] = 99
    d.loc[d.index[1], "rank_numeric_clean"] = 26
    issues = dq.check_dashboard_frame(d)
    assert any("team_rank_tipoff" in i for i in issues) and any("rank_numeric_clean" in i for i in issues)


def test_carried_flag_on_a_game_row_is_caught():
    d = _dash()
    d.loc[d["has_game"].idxmax(), "running_stats_carried"] = True
    assert any("AND running_stats_carried" in i for i in dq.check_dashboard_frame(d))


def test_utc_misfiled_game_is_caught_by_window_check():
    d = _dash()
    d.loc[d["game_id"] == 2, "poll_week"] = "Pre"      # Nov 10 inside the Pre window? no -> flagged
    assert any("outside their poll-week window" in i for i in dq.check_dashboard_frame(d, windows=WINDOWS))


def test_gate_status_logic():
    cfg = {"sections": [
        {"id": "a", "title": "A", "published": True, "requires": [{"metric": "teams", "op": ">=", "value": 2}]},
        {"id": "b", "title": "B", "published": False, "requires": [{"metric": "teams", "op": ">=", "value": 2}]},
        {"id": "c", "title": "C", "published": True, "requires": [{"metric": "teams", "op": ">=", "value": 9}], "unlock_note": "needs 9"},
    ]}
    out = {s["id"]: s for s in gates.evaluate(cfg, gates.compute_metrics(_dash()))}
    assert out["a"]["status"] == "live" and out["a"]["clickable"]
    assert out["b"]["status"] == "awaiting_approval" and not out["b"]["clickable"] and out["b"]["tooltip"] == "Coming Soon"
    assert out["c"]["status"] == "coming_soon" and "2 / 9" in out["c"]["tooltip"]


def test_every_configured_metric_exists():
    cfg = json.loads((ROOT / "config" / "dashboard_sections.json").read_text())
    metrics = gates.compute_metrics(_dash())
    ids = [s["id"] for s in cfg["sections"]]
    assert len(ids) == len(set(ids))
    for s in cfg["sections"]:
        for r in s["requires"]:
            assert r["metric"] in metrics and r["op"] in gates.OPS
