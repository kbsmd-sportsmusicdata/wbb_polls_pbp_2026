#!/usr/bin/env python3
"""
Reconcile game dates in polls_games_dashboard.csv against the NCAA schedule.

Sources (sportsdataverse-data releases):
  * ``ncaa_wbb_schedule``  -- NCAA contest ids, local calendar dates, scores
  * ``wbb_crosswalk`` schedule crosswalk -- maps ESPN game id -> NCAA contest id,
    and carries a Yahoo game id whose digits encode a third date.
Games are matched by ID through the crosswalk (deterministic). Any game missing
from the crosswalk falls back to unordered score pair + date within +/-1 day +
team-name check. For each game the report says whether the ESPN date as UTC and as
Eastern equals the NCAA date (and the Yahoo date), and whether the scores agree.
Exit code is non-zero if any game is unmatched, has a score mismatch, or has an
Eastern date that differs from the NCAA date.

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

BASE = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
URL = BASE + "ncaa_wbb_schedule/ncaa_wbb_schedule_{season}.parquet"
XWALK_URL = BASE + "wbb_crosswalk/wbb_schedule_crosswalk_{season}.parquet"
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


def _score_ok(r, a, b):
    return bool(((r.home_score == a) & (r.away_score == b)) or ((r.home_score == b) & (r.away_score == a)))


def reconcile(dashboard, ncaa, xwalk=None):
    ncaa = ncaa.assign(ncaa_date=pd.to_datetime(ncaa["game_date"], format="%m/%d/%Y"),
                       contest=ncaa["contest_id"].astype(str)).set_index("contest", drop=False)
    xmap, ymap = {}, {}
    if xwalk is not None:
        xw = xwalk.dropna(subset=["espn_game_id", "ncaa_contest_id"]).drop_duplicates("espn_game_id")
        xmap = dict(zip(xw.espn_game_id.astype(str), xw.ncaa_contest_id.astype(str)))
        ymap = dict(zip(xw.espn_game_id.astype(str),
                        pd.to_datetime(xw.yahoo_game_id.str.extract(r"g\.(\d{8})")[0], format="%Y%m%d", errors="coerce")))
    g = dashboard[dashboard["is_unique_game"].astype(bool)].copy()
    ts = pd.to_datetime(g["game_date"], utc=True)
    g["espn_utc_date"] = ts.dt.tz_localize(None).dt.normalize()
    g["espn_et_date"] = ts.dt.tz_convert("America/New_York").dt.tz_localize(None).dt.normalize()
    rows = []
    for _, x in g.iterrows():
        a, b = int(x["game_Team Score"]), int(x["game_Opponent Score"])
        gid = str(int(x.game_id))
        hit, method = None, None
        if gid in xmap and xmap[gid] in ncaa.index:
            hit, method = ncaa.loc[xmap[gid]], "crosswalk_id"
        else:
            c = ncaa[(((ncaa.home_score == a) & (ncaa.away_score == b)) | ((ncaa.home_score == b) & (ncaa.away_score == a)))
                     & ((ncaa.ncaa_date - x.espn_et_date).abs().dt.days <= 1)]
            if len(c) > 1:
                c = c[c.apply(lambda r: _names_match(x.team, x.game_Opponent, r.home, r.away), axis=1)]
            if len(c) == 1:
                hit, method = c.iloc[0], "score_date_name"
        yd = ymap.get(gid)
        rows.append({
            "game_id": x.game_id, "team": x.team, "opponent": x.game_Opponent, "score": f"{a}-{b}",
            "espn_utc_date": x.espn_utc_date.date(), "espn_et_date": x.espn_et_date.date(),
            "ncaa_contest_id": None if hit is None else hit.contest_id,
            "ncaa_date": None if hit is None else hit.ncaa_date.date(),
            "ncaa_matchup": None if hit is None else f"{hit.away} @ {hit.home}",
            "yahoo_date": None if yd is None or pd.isna(yd) else yd.date(),
            "match_method": method, "matched": hit is not None,
            "score_matches": hit is not None and _score_ok(hit, a, b),
            "et_matches_ncaa": hit is not None and hit.ncaa_date == x.espn_et_date,
            "utc_matches_ncaa": hit is not None and hit.ncaa_date == x.espn_utc_date,
            "yahoo_matches_et": yd is not None and not pd.isna(yd) and yd == x.espn_et_date,
        })
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ncaa-file", help="Local copy of the NCAA schedule parquet (default: download)")
    ap.add_argument("--crosswalk-file", help="Local copy of the schedule crosswalk parquet (default: download)")
    args = ap.parse_args()
    season = get_current_season()
    season_dir = REPO_ROOT / get_season_data_dir()
    ncaa = pd.read_parquet(args.ncaa_file or URL.format(season=season))
    try:
        xwalk = pd.read_parquet(args.crosswalk_file or XWALK_URL.format(season=season))
    except Exception as e:  # crosswalk is optional; fall back to score+date matching
        print(f"WARNING: crosswalk unavailable ({e}); using score+date matching only")
        xwalk = None
    out = reconcile(pd.read_csv(season_dir / "polls_games_dashboard.csv"), ncaa, xwalk)
    path = season_dir / "reconciliation_ncaa_dates.csv"
    out.to_csv(path, index=False)
    m = out[out.matched]
    print(f"{len(out)} games; matched {len(m)} ({m.match_method.value_counts().to_dict()}); scores agree {int(m.score_matches.sum())}")
    print(f"Eastern date == NCAA: {int(m.et_matches_ncaa.sum())}   UTC date == NCAA: {int(m.utc_matches_ncaa.sum())}   "
          f"Yahoo date == Eastern: {int(m.yahoo_matches_et.sum())}")
    bad = out[~out.matched | ~out.score_matches | ~out.et_matches_ncaa]
    if len(bad):
        print("Needs review:\n", bad[["team", "opponent", "score", "espn_et_date", "ncaa_date", "match_method"]].to_string(index=False))
    print(f"Wrote {path}")
    return 0 if len(bad) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
