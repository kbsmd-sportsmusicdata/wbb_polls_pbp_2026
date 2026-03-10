
#!/usr/bin/env python3
"""
derive_recruiting_metrics.py

Create pre-computed recruiting class metrics from the cleaned recruiting dataset.

Usage:
    python derive_recruiting_metrics.py \
        --input recruiting_classes_cleaned.csv \
        --output recruiting_class_metrics.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def add_commit_level_metrics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Higher = better quality. Rank 1 => 100, Rank 100 => 1
    out["commit_rank_score"] = 101 - pd.to_numeric(out["Commit_Rank"], errors="coerce")
    out["top5_flag"] = (out["Commit_Rank"] <= 5).astype(int)
    out["top10_flag"] = (out["Commit_Rank"] <= 10).astype(int)
    out["top25_flag"] = (out["Commit_Rank"] <= 25).astype(int)

    # Position buckets
    out["position_group"] = np.select(
        [
            out["is_big"],
            out["is_wing_or_forward"],
            out["is_guard"],
        ],
        [
            "Big",
            "Wing/Forward",
            "Guard",
        ],
        default="Unknown",
    )

    # Flag composite skill archetypes
    out["elite_shooting_big_flag"] = (
        out["is_big"] & out["strength_shooting_3pt"]
    ).astype(int)
    out["two_way_wing_flag"] = (
        out["is_wing_or_forward"] & out["strength_shooting_3pt"] & out["strength_defense"]
    ).astype(int)
    out["lead_guard_flag"] = (
        out["is_guard"] & out["strength_playmaking_passing"]
    ).astype(int)

    return out


def aggregate_class_metrics(df: pd.DataFrame) -> pd.DataFrame:
    numeric_strength_cols = [c for c in df.columns if c.startswith("strength_")]

    group_cols = [
        "recruiting_year",
        "season",
        "team_canonical",
        "program_name_canonical",
        "Conference",
        "Recruiting_Rank",
        "Top_100_Commits",
    ]

    agg = (
        df.groupby(group_cols, dropna=False)
        .agg(
            class_size=("Highest_Ranked_Commit_Name", "size"),
            best_commit_rank=("Commit_Rank", "min"),
            mean_commit_rank=("Commit_Rank", "mean"),
            median_commit_rank=("Commit_Rank", "median"),
            total_rank_score=("commit_rank_score", "sum"),
            avg_rank_score=("commit_rank_score", "mean"),
            top5_commits=("top5_flag", "sum"),
            top10_commits=("top10_flag", "sum"),
            top25_commits=("top25_flag", "sum"),
            guard_commits=("position_group", lambda s: (s == "Guard").sum()),
            wing_forward_commits=("position_group", lambda s: (s == "Wing/Forward").sum()),
            big_commits=("position_group", lambda s: (s == "Big").sum()),
            avg_height_inches=("height_inches", "mean"),
            max_height_inches=("height_inches", "max"),
            n_elite_shooting_bigs=("elite_shooting_big_flag", "sum"),
            n_two_way_wings=("two_way_wing_flag", "sum"),
            n_lead_guards=("lead_guard_flag", "sum"),
        )
        .reset_index()
    )

    # Add strength feature shares
    strength_share = (
        df.groupby(group_cols, dropna=False)[numeric_strength_cols]
        .mean()
        .reset_index()
        .rename(columns={c: f"{c}_share" for c in numeric_strength_cols})
    )
    agg = agg.merge(strength_share, on=group_cols, how="left")

    # Derived metrics
    agg["recruiting_rank_inverse"] = 21 - pd.to_numeric(agg["Recruiting_Rank"], errors="coerce")
    agg["avg_commit_quality_index"] = agg["avg_rank_score"]
    agg["star_power_index"] = (
        agg["top10_commits"] * 3 +
        agg["top25_commits"] * 2 +
        agg["best_commit_rank"].rsub(101).fillna(0) / 25
    )
    agg["class_balance_index"] = (
        (agg["guard_commits"] > 0).astype(int) +
        (agg["wing_forward_commits"] > 0).astype(int) +
        (agg["big_commits"] > 0).astype(int)
    ) / 3.0

    agg["size_skill_index"] = (
        agg["avg_height_inches"].fillna(0) / 84 * 0.35
        + agg.get("strength_shooting_3pt_share", 0).fillna(0) * 0.25
        + agg.get("strength_playmaking_passing_share", 0).fillna(0) * 0.20
        + agg.get("strength_versatility_share", 0).fillna(0) * 0.20
    )

    agg["frontcourt_skill_score"] = (
        agg["n_elite_shooting_bigs"] * 2 +
        agg["big_commits"] +
        agg.get("strength_post_skill_share", 0).fillna(0) * 5
    )

    agg["perimeter_creation_score"] = (
        agg["n_lead_guards"] * 2 +
        agg["guard_commits"] +
        agg.get("strength_playmaking_passing_share", 0).fillna(0) * 5 +
        agg.get("strength_slashing_rim_pressure_share", 0).fillna(0) * 4
    )

    agg["two_way_wing_score"] = (
        agg["n_two_way_wings"] * 2 +
        agg.get("strength_defense_share", 0).fillna(0) * 5 +
        agg.get("strength_shooting_3pt_share", 0).fillna(0) * 4
    )

    agg["recruiting_profile"] = np.select(
        [
            agg["top10_commits"] >= 2,
            agg["class_balance_index"] >= 1.0,
            agg["perimeter_creation_score"] >= agg["frontcourt_skill_score"] + 2,
            agg["frontcourt_skill_score"] >= agg["perimeter_creation_score"] + 2,
        ],
        [
            "Blue-Chip Heavy",
            "Balanced Class",
            "Perimeter-Driven Class",
            "Frontcourt-Driven Class",
        ],
        default="Mixed Class",
    )

    return agg.sort_values(["season", "Recruiting_Rank", "team_canonical"]).reset_index(drop=True)


RECRUITING_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "recruiting"


def _resolve(path_str: str) -> Path:
    """Resolve a bare filename to RECRUITING_DIR; leave explicit paths as-is."""
    p = Path(path_str)
    return RECRUITING_DIR / p if p.parent == Path(".") else p


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to cleaned recruiting CSV")
    parser.add_argument("--output", required=True, help="Path to output class metrics CSV")
    args = parser.parse_args()

    input_path  = _resolve(args.input)
    output_path = _resolve(args.output)

    df = pd.read_csv(input_path)
    commit_level = add_commit_level_metrics(df)
    class_metrics = aggregate_class_metrics(commit_level)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    class_metrics.to_csv(output_path, index=False)
    print(f"Saved recruiting class metrics to {output_path} ({len(class_metrics):,} rows)")


if __name__ == "__main__":
    main()
