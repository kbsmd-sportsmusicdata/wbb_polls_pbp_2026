"""Tests for scripts/validate_game_dates_ncaa.py (no network)."""
import importlib.util
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("recon", ROOT / "scripts/validate_game_dates_ncaa.py")
recon = importlib.util.module_from_spec(spec)
spec.loader.exec_module(recon)

# 01:00Z on 11/9 is Sunday 11/8 in Eastern time; NCAA lists local dates.
DASH = pd.DataFrame({
    "is_unique_game": [True, True], "game_id": [111.0, 222.0], "team": ["USC", "Ole Miss"],
    "game_Opponent": ["Utah State", "North Alabama"], "game_Team Score": [118, 72], "game_Opponent Score": [40, 51],
    "game_date": ["2026-11-09T01:00Z", "2026-11-06T00:00Z"],
})
NCAA = pd.DataFrame({
    "contest_id": ["9001", "9002", "9003"], "game_date": ["11/08/2026", "11/05/2026", "11/05/2026"],
    "home": ["Southern California", "Ole Miss", "UNC Greensboro"], "away": ["Utah St.", "North Ala.", "Wofford"],
    "home_score": [118, 72, 72], "away_score": [40, 51, 51],
})
XWALK = pd.DataFrame({"espn_game_id": ["111"], "ncaa_contest_id": ["9001"], "yahoo_game_id": ["ncaaw.g.202611080001"]})


def test_crosswalk_id_matches_and_et_beats_utc():
    out = recon.reconcile(DASH, NCAA, XWALK).set_index("team")
    usc = out.loc["USC"]
    assert usc.match_method == "crosswalk_id" and usc.score_matches
    assert usc.et_matches_ncaa and not usc.utc_matches_ncaa and usc.yahoo_matches_et


def test_fallback_disambiguates_same_score_games_by_name():
    out = recon.reconcile(DASH, NCAA, XWALK).set_index("team")
    oleMiss = out.loc["Ole Miss"]            # not in crosswalk; two 72-51 games on 11/5
    assert oleMiss.match_method == "score_date_name" and oleMiss.ncaa_contest_id == "9002"
    assert oleMiss.et_matches_ncaa


def test_unmatched_game_is_reported_not_dropped():
    d = DASH.assign(**{"game_Team Score": [1, 2]})
    out = recon.reconcile(d, NCAA, None)
    assert len(out) == 2 and not out.matched.any()
