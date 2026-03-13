
#!/usr/bin/env python3
"""
build_player_recruit_aggregates.py

Aggregate the player-level ESPNw recruit rankings to team-season level metrics.

Input:  data/recruiting/player_recruit_rankings_20212026.csv
Output: data/recruiting/player_recruit_aggregates.csv

Usage:
    python build_player_recruit_aggregates.py \
        --input player_recruit_rankings_20212026.csv \
        --output player_recruit_aggregates.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


# Map schedule-style team names (used in player file) → poll-style names
# (used in polls and cross-reference datasets)
SCHEDULE_TO_POLL_NAME: dict[str, str] = {
    "North Carolina": "UNC",
    "Miami":          "Miami (FL)",
    "St. John's":     "St. John's (NY)",
}

# Normalise abbreviated positions to broader groups
POSITION_GROUP_MAP: dict[str, str] = {
    "PG": "Guard",
    "G":  "Guard",
    "W":  "Wing/Forward",
    "F":  "Wing/Forward",
    "P":  "Big",
}


def position_group(pos: str) -> str:
    if pd.isna(pos):
        return "Unknown"
    return POSITION_GROUP_MAP.get(str(pos).strip().upper(), "Unknown")


def height_inches_total(feet, inches) -> float | None:
    try:
        f = float(feet)
        i = float(inches)
        return f * 12 + i
    except (TypeError, ValueError):
        return np.nan


def aggregate_player_recruits(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Team name normalisation: schedule-style → poll-style
    df["team_poll_name"] = df["TEAM"].map(lambda t: SCHEDULE_TO_POLL_NAME.get(str(t), str(t)) if pd.notna(t) else np.nan)

    # Position group
    df["position_group"] = df["POS"].apply(position_group)

    # Total height in inches
    df["height_inches_total"] = df.apply(
        lambda r: height_inches_total(r["HEIGHT_FEET"], r["HEIGHT_INCHES"]), axis=1
    )

    # Rank tier flags
    df["is_top10"]  = (df["ESPNW_PLAYER_RECRUIT_RANK"] <= 10).astype(int)
    df["is_top25"]  = (df["ESPNW_PLAYER_RECRUIT_RANK"] <= 25).astype(int)
    df["is_top50"]  = (df["ESPNW_PLAYER_RECRUIT_RANK"] <= 50).astype(int)
    df["is_guard"]         = (df["position_group"] == "Guard").astype(int)
    df["is_wing_forward"]  = (df["position_group"] == "Wing/Forward").astype(int)
    df["is_big"]           = (df["position_group"] == "Big").astype(int)

    agg = (
        df.groupby(["SEASON", "team_poll_name"], dropna=False)
        .agg(
            n_recruits=("PLAYER_NAME", "size"),
            top_recruit_rank=("ESPNW_PLAYER_RECRUIT_RANK", "min"),
            avg_espn_grade=("ESPN_GRADE", "mean"),
            median_espn_grade=("ESPN_GRADE", "median"),
            n_top10_recruits=("is_top10", "sum"),
            n_top25_recruits=("is_top25", "sum"),
            n_top50_recruits=("is_top50", "sum"),
            n_guards=("is_guard", "sum"),
            n_wings_forwards=("is_wing_forward", "sum"),
            n_bigs=("is_big", "sum"),
            avg_height_inches=("height_inches_total", "mean"),
        )
        .reset_index()
        .rename(columns={"SEASON": "season", "team_poll_name": "team"})
    )

    # Derived composite metrics
    agg["recruit_star_index"] = (
        agg["n_top10_recruits"] * 3 +
        agg["n_top25_recruits"] * 2 +
        agg["n_top50_recruits"] * 1
    )
    agg["avg_espn_grade"] = agg["avg_espn_grade"].round(2)
    agg["avg_height_inches"] = agg["avg_height_inches"].round(1)

    return agg.sort_values(["season", "top_recruit_rank"]).reset_index(drop=True)


RECRUITING_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "recruiting"


def _resolve(path_str: str) -> Path:
    p = Path(path_str)
    return RECRUITING_DIR / p if p.parent == Path(".") else p


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  required=True, help="Player recruit rankings CSV (bare filename or full path)")
    parser.add_argument("--output", required=True, help="Output team-season aggregates CSV")
    args = parser.parse_args()

    input_path  = _resolve(args.input)
    output_path = _resolve(args.output)

    df  = pd.read_csv(input_path)
    agg = aggregate_player_recruits(df)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    agg.to_csv(output_path, index=False)
    print(f"Saved player recruit aggregates to {output_path} ({len(agg):,} rows)")


if __name__ == "__main__":
    main()
