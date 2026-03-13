
#!/usr/bin/env python3
"""
build_team_season_master.py

Build a unified team-season master dataset joining:
  - Player recruit aggregates (ESPNw top-100 player rankings)
  - AP Poll historical season summaries
  - Schedule parquet data (wins / losses per team-season)
  - NET ratings (2026 only, if present)

Output columns:
    team, season, conference,
    portal_in_count, portal_out_count,       # stub — no portal data yet
    top_recruit_rank, avg_espn_grade,
    n_top10_recruits, n_top25_recruits,
    recruit_star_index,
    ap_rank_preseason, ap_rank_peak, ap_rank_final,
    weeks_in_top25,
    wins, losses, win_pct,
    net_rating                               # stub — 2026 only if available

Usage:
    python build_team_season_master.py \
        --recruits      player_recruit_aggregates.csv \
        --polls-summary polls_historical_season_summary.csv \
        --output        team_season_master.csv
"""

from __future__ import annotations

import argparse
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)


# Poll team names → schedule parquet location/name
POLL_TO_SCHEDULE_NAME: dict[str, str] = {
    "UNC":             "North Carolina",
    "St. John's (NY)": "St. John's",
    "Miami (FL)":      "Miami",
}

SCHEDULE_PARQUET_URL = (
    "https://raw.githubusercontent.com/sportsdataverse/wehoop-wbb-raw/main"
    "/wbb/schedules/parquet/wbb_schedule_{season}.parquet"
)

RECRUITING_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "recruiting"
DATA_DIR       = Path(__file__).resolve().parent.parent.parent / "data"
SCHEDULE_DIR   = DATA_DIR / "wbb_schedule"


def _resolve_recruiting(path_str: str) -> Path:
    p = Path(path_str)
    return RECRUITING_DIR / p if p.parent == Path(".") else p


def _resolve_data(path_str: str) -> Path:
    p = Path(path_str)
    return DATA_DIR / p if p.parent == Path(".") else p


def _load_schedule(season: int) -> pd.DataFrame | None:
    """Load schedule parquet from local cache or download."""
    local = SCHEDULE_DIR / f"wbb_schedule_{season}.parquet"
    if local.exists():
        try:
            return pd.read_parquet(local)
        except Exception as e:
            print(f"  Warning: could not read {local}: {e}")
            return None
    url = SCHEDULE_PARQUET_URL.format(season=season)
    try:
        df = pd.read_parquet(url)
        SCHEDULE_DIR.mkdir(parents=True, exist_ok=True)
        df.to_parquet(local)
        return df
    except Exception as e:
        print(f"  Warning: could not download schedule {season}: {e}")
        return None


def compute_wins_by_season(seasons: list[int]) -> pd.DataFrame:
    """Return DataFrame with columns: season, team, wins, losses, win_pct."""
    records: list[dict] = []

    for season in seasons:
        sched = _load_schedule(season)
        if sched is None:
            continue

        completed = sched[sched["status_type_completed"] == True].copy()
        if completed.empty:
            continue

        # Build per-game rows for each team (home + away perspective)
        home = completed[["home_location", "home_winner", "season"]].copy()
        home.columns = ["team_sched", "win", "season"]

        away = completed[["away_location", "away_winner", "season"]].copy()
        away.columns = ["team_sched", "win", "season"]

        games = pd.concat([home, away], ignore_index=True)
        games["win"] = games["win"].astype(bool)

        agg = (
            games.groupby(["season", "team_sched"])
            .agg(wins=("win", "sum"), total=("win", "count"))
            .reset_index()
        )
        agg["losses"]  = agg["total"] - agg["wins"]
        agg["win_pct"] = (agg["wins"] / agg["total"]).round(3)

        # Map schedule names → poll names where they differ
        sched_to_poll = {v: k for k, v in POLL_TO_SCHEDULE_NAME.items()}
        agg["team"] = agg["team_sched"].map(lambda t: sched_to_poll.get(str(t), str(t)))
        records.append(agg[["season", "team", "wins", "losses", "win_pct"]])

    if not records:
        return pd.DataFrame(columns=["season", "team", "wins", "losses", "win_pct"])

    return pd.concat(records, ignore_index=True)


def build_master(
    recruits: pd.DataFrame,
    polls_summary: pd.DataFrame,
    wins: pd.DataFrame,
) -> pd.DataFrame:
    # Normalise column names
    polls = polls_summary.rename(columns={
        "preseason_rank": "ap_rank_preseason",
        "best_rank":      "ap_rank_peak",
        "final_rank":     "ap_rank_final",
    })

    # Join polls + recruits
    master = polls.merge(
        recruits,
        on=["season", "team"],
        how="outer",
        suffixes=("", "_rec"),
    )

    # Join wins
    master = master.merge(wins, on=["season", "team"], how="left")

    # Stub portal columns — no data yet
    master["portal_in_count"]  = np.nan
    master["portal_out_count"] = np.nan

    # Stub NET rating column
    master["net_rating"] = np.nan

    # Select and order final columns
    out_cols = [
        "team", "season", "conference",
        "portal_in_count", "portal_out_count",
        "top_recruit_rank", "avg_espn_grade",
        "n_top10_recruits", "n_top25_recruits", "n_top50_recruits",
        "n_recruits", "recruit_star_index",
        "n_guards", "n_wings_forwards", "n_bigs",
        "avg_height_inches",
        "ap_rank_preseason", "ap_rank_peak", "ap_rank_final",
        "weeks_in_top25", "peak_streak",
        "wins", "losses", "win_pct",
        "net_rating",
    ]
    # Keep only columns that exist
    out_cols = [c for c in out_cols if c in master.columns]
    master = master[out_cols]

    return master.sort_values(["season", "team"]).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--recruits",      required=True, help="Player recruit aggregates CSV")
    parser.add_argument("--polls-summary", required=True, help="AP polls season summary CSV")
    parser.add_argument("--output",        required=True, help="Output team-season master CSV")
    args = parser.parse_args()

    recruits      = pd.read_csv(_resolve_recruiting(args.recruits))
    polls_summary = pd.read_csv(_resolve_data(args.polls_summary))
    output_path   = _resolve_recruiting(args.output)

    seasons = sorted(polls_summary["season"].dropna().unique().astype(int).tolist())
    print(f"Computing wins for seasons: {seasons}")
    wins = compute_wins_by_season(seasons)

    master = build_master(recruits, polls_summary, wins)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    master.to_csv(output_path, index=False)
    print(f"Saved team-season master to {output_path} ({len(master):,} rows)")


if __name__ == "__main__":
    main()
