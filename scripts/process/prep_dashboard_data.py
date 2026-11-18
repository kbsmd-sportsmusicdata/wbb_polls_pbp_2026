"""
Dashboard prep layer for polls_games_joined.csv.

Reads the same inputs as build_polls_games_joined.py and writes an
analysis-ready sibling file, ``polls_games_dashboard.csv``. The original joined
file (Tableau, Google Sheets, the live dashboard) is NOT modified, and
build_polls_games_joined.py is NOT edited -- this script imports it and swaps in
one corrected function.

What it changes vs. the joined file
-----------------------------------
1. Eastern-date poll-week assignment. ESPN game timestamps are UTC, and the build
   script windows on ``date[:10]`` (the UTC date). Any tip-off after ~7-8 pm ET
   therefore lands on the next calendar day, and a Sunday-night game can be filed
   under the following Monday's poll week. Here the window is matched on the
   America/New_York date. Every derived field (gw_*, running_*, trigger game,
   upset columns) is recomputed by the build module's own functions.
2. Unranked recode. rank 26 (unranked sentinel) and opponent rank 99 become NaN in
   the *_clean columns, with "NR" / "#n" display labels alongside.
3. Rank labelling. Tip-off ranks (ESPN, at game time) and poll ranks (the AP poll
   row) are kept side by side, plus ``next_poll_rank_numeric`` so a chart can show
   rank before the game, the result, and rank after.
4. Carry-forward. ``running_*`` columns are copied onto poll rows with no game
   (bye weeks, newest poll) from the team's most recent game row.
   ``running_stats_carried`` flags those rows. Preseason rows stay blank.
5. ``has_game`` and ``is_unique_game`` flags. A game between two ranked teams
   appears once per team; ``is_unique_game`` is True on exactly one of them
   (higher-ranked side at tip-off, then alphabetical) for league-level charts.

No columns are dropped, and blank-by-design columns (conference record early in
the season, win pct with 0 games) stay blank.

Usage:
    python scripts/process/prep_dashboard_data.py
    python scripts/process/prep_dashboard_data.py --polls P --games G --output O
"""

import argparse
import importlib.util
import os
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

UNRANKED_POLL = 26    # sentinel used in rank_numeric for unranked teams
UNRANKED_GAME = 99    # sentinel used in ESPN game ranks for unranked teams
LOCAL_TZ = "America/New_York"


def _load_build_module():
    path = Path(__file__).resolve().parent / "build_polls_games_joined.py"
    spec = importlib.util.spec_from_file_location("build_polls_games_joined", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_eastern_assigner(build):
    """Return a drop-in replacement for build.assign_poll_weeks using ET dates."""

    def assign_poll_weeks_et(games_df):
        ts = pd.to_datetime(games_df["date"], utc=True, errors="coerce")
        local = ts.dt.tz_convert(LOCAL_TZ)
        # Same dtype the build script produces (naive midnight timestamps).
        games_df["game_date_parsed"] = local.dt.tz_localize(None).dt.normalize()
        games_df["poll_week"] = None
        for pw, (start, end) in build.POLL_WEEK_WINDOWS.items():
            mask = (
                (games_df["game_date_parsed"] >= pd.Timestamp(start))
                & (games_df["game_date_parsed"] <= pd.Timestamp(end))
                & games_df["poll_week"].isna()
            )
            games_df.loc[mask, "poll_week"] = pw
        unassigned = games_df["poll_week"].isna().sum()
        if unassigned:
            dates = sorted(games_df.loc[games_df["poll_week"].isna(), "game_date_parsed"].unique())
            print(f"  WARNING: {unassigned} games have no poll_week assignment: {dates}")
        return games_df

    return assign_poll_weeks_et


def _rank_label(series):
    return series.map(lambda v: "NR" if pd.isna(v) else f"#{int(v)}")


def add_prep_columns(df):
    df = df.copy()
    df["has_game"] = df["game_id"].notna()

    # --- Unranked recode (poll rank, tip-off ranks) ---
    df["rank_numeric_clean"] = df["rank_numeric"].where(df["rank_numeric"] != UNRANKED_POLL)
    df["is_ranked"] = df["rank_numeric_clean"].notna()
    df["poll_rank_label"] = _rank_label(df["rank_numeric_clean"])

    df["team_rank_tipoff"] = df["game_Team Rank"].where(df["game_Team Rank"] != UNRANKED_GAME)
    df["opp_rank_tipoff"] = df["game_Opponent Rank"].where(df["game_Opponent Rank"] != UNRANKED_GAME)
    df["team_rank_tipoff_label"] = np.where(df["has_game"], _rank_label(df["team_rank_tipoff"]), None)
    df["opp_rank_tipoff_label"] = np.where(df["has_game"], _rank_label(df["opp_rank_tipoff"]), None)

    # Rank in the *next* poll (the poll that reflects this row's games).
    poll_rows = (
        df.drop_duplicates(["team", "week_number"])[["team", "week_number", "rank_numeric_clean"]]
        .assign(week_number=lambda x: x["week_number"] - 1)
        .rename(columns={"rank_numeric_clean": "next_poll_rank_numeric"})
    )
    df = df.merge(poll_rows, on=["team", "week_number"], how="left")
    df["next_poll_rank_label"] = np.where(
        df["week_number"] < df["week_number"].max(), _rank_label(df["next_poll_rank_numeric"]), None
    )

    # --- is_unique_game: one row per game_id ---
    sort_key = df["team_rank_tipoff"].fillna(UNRANKED_GAME)
    order = (
        df.assign(_k=sort_key)
        .sort_values(["game_id", "_k", "team"])
        .groupby("game_id", sort=False)
        .head(1)
        .index
    )
    df["is_unique_game"] = df.index.isin(order) & df["has_game"]

    # --- Carry-forward running_* onto rows without a game ---
    df = df.sort_values(["team", "week_number", "game_game_date_parsed"]).reset_index(drop=True)
    run_cols = [c for c in df.columns if c.startswith("running_")]
    last_game_idx = df.index.to_series().where(df["has_game"])
    last_game_idx = last_game_idx.groupby(df["team"]).ffill()
    carry = (~df["has_game"]) & last_game_idx.notna()
    src = last_game_idx[carry].astype(int)
    df.loc[carry, run_cols] = df.loc[src.values, run_cols].values
    df["running_stats_carried"] = carry
    return df


def run(polls_path, games_path, output_path):
    build = _load_build_module()
    build.assign_poll_weeks = make_eastern_assigner(build)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_out = os.path.join(tmp, "joined_et.csv")
        build.run_pipeline(polls_path, games_path, tmp_out)
        joined = pd.read_csv(tmp_out)

    out = add_prep_columns(joined)
    out.to_csv(output_path, index=False)
    print(f"\nPrep done: {output_path}  ({out.shape[0]} rows x {out.shape[1]} cols)")
    return out


def main():
    sys.path.insert(0, str(REPO_ROOT))
    from season_config import get_season_data_dir

    season_dir = get_season_data_dir()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--polls", default=str(season_dir / "polls_analytics.csv"))
    ap.add_argument("--games", default=str(REPO_ROOT / "data/wbb_schedule/schedule_filtered.csv"))
    ap.add_argument("--output", default=str(season_dir / "polls_games_dashboard.csv"))
    args = ap.parse_args()
    run(args.polls, args.games, args.output)


if __name__ == "__main__":
    os.chdir(REPO_ROOT)  # season_config / config paths are repo-relative
    main()
