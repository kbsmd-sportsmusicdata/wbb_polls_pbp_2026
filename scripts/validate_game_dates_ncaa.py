#!/usr/bin/env python3
"""
Reconcile game dates in polls_games_dashboard.csv against the NCAA schedule.

Source: sportsdataverse-data release ``ncaa_wbb_schedule`` (NCAA contest ids, local
calendar dates, scores; independent of the ESPN schedule the pipeline joins on).
The two sources share no ids or team names, so games are matched on the unordered
score pair plus a date within +/-1 day and a loose team-name check. For each match
it reports whether the ESPN date as UTC and as Eastern equals the NCAA date.

Usage:
    python scripts/validate_game_dates_ncaa.py [--ncaa-file local.parquet]
Writes data/seasons/<season>/reconciliation_ncaa_dates.csv. Not run in CI (needs a download).
"""
import argparse
import re
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
from season_config import get_current_season, get_season_data_dir  # noqa: E402

URL = ("https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
       "ncaa_wbb_schedule/ncaa_wbb_schedule_{season}.parquet")
# NCAA short names that share no token with our standardized names
ALIASES = {"unc": {"north", "carolina"}, "uconn": {"connecticut"}, "penn": {"pennsylvania"},
           "usc": {"southern", "california"}, "fdu": {"fairleigh", "dickinson"}, "uni": {"northern", "iowa"}}


GENERIC = {"north", "south", "east", "west", "northern", "southern", "eastern", "western", "central",
           "state", "university", "college", "of", "the", "st", "tech", "u"}


def _tokens(name):
    t = set(re.findall(r"[a-z]+", re.sub(r"\bst\.?\b", "state", str(name).lower())))
    for k, v in ALIASES.items():
        if k in t:
            t |= v
    distinctive = t - GENERIC
    return distinctive or t


def _squash(name):
    return re.sub(r"[^a-z]", "", re.sub(r"\bst\.?(?=\s|$)", "state", str(name).lower()))


def _same_team(ours, theirs):
    a, b = _squash(ours), _squash(theirs)
    if len(a) >= 4 and len(b) >= 4 and (a in b or b in a):   # "N.C. State" == "NC State"
        return True
    # prefix match handles NCAA abbreviations ("Ala." vs "Alabama", "Mich." vs "Michigan")
    return any(a.startswith(b) or b.startswith(a) for a in _tokens(ours) for b in _tokens(theirs) if min(len(a), len(b)) >= 3)


def _names_match(team, opp, home, away):
    return (_same_team(team, home) and _same_team(opp, away)) or (_same_team(team, away) and _same_team(opp, home))


def reconcile(dashboard, ncaa):
    ncaa = ncaa.assign(ncaa_date=pd.to_datetime(ncaa["game_date"], format="%m/%d/%Y"))
    g = dashboard[dashboard["is_unique_game"].astype(bool)].copy()
    ts = pd.to_datetime(g["game_date"], utc=True)
    g["espn_utc_date"] = ts.dt.tz_localize(None).dt.normalize()
    g["espn_et_date"] = ts.dt.tz_convert("America/New_York").dt.tz_localize(None).dt.normalize()
    rows = []
    for _, x in g.iterrows():
        a, b = int(x["game_Team Score"]), int(x["game_Opponent Score"])
        c = ncaa[(((ncaa.home_score == a) & (ncaa.away_score == b)) | ((ncaa.home_score == b) & (ncaa.away_score == a)))
                 & ((ncaa.ncaa_date - x.espn_et_date).abs().dt.days <= 1)]
        if len(c) > 1:
            c = c[c.apply(lambda r: _names_match(x.team, x.game_Opponent, r.home, r.away), axis=1)]
        hit = c.iloc[0] if len(c) == 1 else None
        rows.append({
            "game_id": x.game_id, "team": x.team, "opponent": x.game_Opponent, "score": f"{a}-{b}",
            "espn_utc_date": x.espn_utc_date.date(), "espn_et_date": x.espn_et_date.date(),
            "ncaa_contest_id": None if hit is None else hit.contest_id,
            "ncaa_date": None if hit is None else hit.ncaa_date.date(),
            "ncaa_matchup": None if hit is None else f"{hit.away} @ {hit.home}",
            "matched": hit is not None,
            "et_matches_ncaa": hit is not None and hit.ncaa_date == x.espn_et_date,
            "utc_matches_ncaa": hit is not None and hit.ncaa_date == x.espn_utc_date,
        })
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ncaa-file", help="Local copy of the NCAA parquet (default: download)")
    args = ap.parse_args()
    season_dir = REPO_ROOT / get_season_data_dir()
    ncaa = pd.read_parquet(args.ncaa_file or URL.format(season=get_current_season()))
    out = reconcile(pd.read_csv(season_dir / "polls_games_dashboard.csv"), ncaa)
    path = season_dir / "reconciliation_ncaa_dates.csv"
    out.to_csv(path, index=False)
    m = out[out.matched]
    print(f"{len(out)} games; matched {len(m)}; ET==NCAA {int(m.et_matches_ncaa.sum())}; UTC==NCAA {int(m.utc_matches_ncaa.sum())}")
    if (~out.matched).any():
        print("Unmatched:\n", out[~out.matched][["team", "opponent", "score", "espn_et_date"]].to_string(index=False))
    print(f"Wrote {path}")
    return 0 if bool(m.et_matches_ncaa.all()) and bool(out.matched.all()) else 1


if __name__ == "__main__":
    sys.exit(main())
