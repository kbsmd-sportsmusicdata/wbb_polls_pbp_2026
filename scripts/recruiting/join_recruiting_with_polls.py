
#!/usr/bin/env python3
"""
join_recruiting_with_polls.py

Join pre-computed recruiting class metrics to historical AP polls datasets.

Usage:
    python join_recruiting_with_polls.py \
        --recruiting recruiting_class_metrics.csv \
        --polls-weekly polls_historical_analytics.csv \
        --polls-summary polls_historical_season_summary.csv \
        --output cross_reference_recruiting_polls.csv
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import numpy as np
import pandas as pd


TEAM_ALIASES = {
    "uconn": "UConn",
    "u conn": "UConn",
    "usc": "USC",
    "ucla": "UCLA",
    "ole miss": "Ole Miss",
    "unc": "North Carolina",
    "nc state": "NC State",
    "north carolina state": "NC State",
    "smu": "SMU",
    "lsu": "LSU",
    "tcu": "TCU",
}


def canonical_team_name(name: str) -> str:
    if pd.isna(name):
        return np.nan
    raw = str(name).strip()
    key = re.sub(r"\s+", " ", raw.lower())
    return TEAM_ALIASES.get(key, raw)


def build_poll_summary(weekly: pd.DataFrame, summary: pd.DataFrame) -> pd.DataFrame:
    weekly = weekly.copy()
    summary = summary.copy()

    weekly["team_canonical"] = weekly["team"].apply(canonical_team_name)
    summary["team_canonical"] = summary["team"].apply(canonical_team_name)

    # Useful weekly-derived features by season/team
    weekly_agg = (
        weekly.groupby(["season", "team_canonical"], dropna=False)
        .agg(
            n_poll_rows=("poll_week", "size"),
            avg_rank_numeric=("rank_numeric", "mean"),
            last_rank_numeric=("rank_numeric", "last"),
            best_weekly_rank=("rank_numeric", "min"),
            worst_weekly_rank=("rank_numeric", "max"),
            n_big_rises=("movement_category", lambda s: (s == "Big Rise").sum()),
            n_drops=("movement_category", lambda s: s.isin(["Drop", "Big Drop"]).sum()),
            avg_rank_change=("rank_change", "mean"),
            max_rank_change=("rank_change", "max"),
            min_rank_change=("rank_change", "min"),
            top25_wins=("wins_vs_top25", "max"),
            top25_losses=("losses_vs_top25", "max"),
            top25_win_pct=("win_pct_vs_top25", "max"),
        )
        .reset_index()
    )

    summary_small = summary.rename(
        columns={
            "season": "polls_season",
        }
    )

    summary_small = summary_small.rename(columns={"polls_season": "season"})
    out = summary_small.merge(weekly_agg, on=["season", "team_canonical"], how="left")
    return out


def create_cross_reference(recruiting: pd.DataFrame, polls_summary: pd.DataFrame) -> pd.DataFrame:
    r = recruiting.copy()
    p = polls_summary.copy()

    r["team_canonical"] = r["team_canonical"].apply(canonical_team_name)
    p["team_canonical"] = p["team_canonical"].apply(canonical_team_name)

    merged = r.merge(
        p,
        on=["season", "team_canonical"],
        how="left",
        suffixes=("", "_polls"),
    )

    # Outcome flags / labels
    merged["ranked_preseason_flag"] = merged["preseason_rank"].notna().astype(int)
    merged["finished_ranked_flag"] = merged["final_rank"].notna().astype(int)
    merged["top10_finish_flag"] = (merged["final_rank"] <= 10).fillna(False).astype(int)
    merged["top25_longevity_flag"] = (merged["weeks_in_top25"] >= 10).fillna(False).astype(int)

    # Recruiting → polls comparison metrics
    merged["recruiting_vs_final_rank_gap"] = pd.to_numeric(merged["Recruiting_Rank"], errors="coerce") - pd.to_numeric(merged["final_rank"], errors="coerce")
    merged["recruiting_vs_preseason_rank_gap"] = pd.to_numeric(merged["Recruiting_Rank"], errors="coerce") - pd.to_numeric(merged["preseason_rank"], errors="coerce")

    merged["polls_outcome_bucket"] = np.select(
        [
            merged["top10_finish_flag"] == 1,
            merged["finished_ranked_flag"] == 1,
            merged["ranked_preseason_flag"] == 1,
        ],
        [
            "Finished Top 10",
            "Finished Ranked",
            "Preseason Ranked Only",
        ],
        default="Unranked Outcome",
    )

    merged["recruiting_expectation_bucket"] = pd.cut(
        pd.to_numeric(merged["Recruiting_Rank"], errors="coerce"),
        bins=[0, 3, 5, 10, 20, np.inf],
        labels=["Elite 1-3", "Top 5", "Top 10", "Top 20", "Outside Top 20"],
        include_lowest=True,
    )

    # Simplified cross-reference score
    merged["recruiting_to_results_index"] = (
        merged["star_power_index"].fillna(0) * 0.35 +
        merged["avg_commit_quality_index"].fillna(0) * 0.20 +
        merged["finished_ranked_flag"].fillna(0) * 10 * 0.20 +
        (26 - merged["final_rank"].fillna(26)) * 0.15 +
        merged["weeks_in_top25"].fillna(0) * 0.10
    )

    return merged.sort_values(["season", "Recruiting_Rank", "team_canonical"]).reset_index(drop=True)


RECRUITING_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "recruiting"
POLLS_DIR      = Path(__file__).resolve().parent.parent.parent / "data"


def _resolve_recruiting(path_str: str) -> Path:
    """Resolve a bare filename to RECRUITING_DIR; leave explicit paths as-is."""
    p = Path(path_str)
    return RECRUITING_DIR / p if p.parent == Path(".") else p


def _resolve_polls(path_str: str) -> Path:
    """Resolve a bare filename to POLLS_DIR; leave explicit paths as-is."""
    p = Path(path_str)
    return POLLS_DIR / p if p.parent == Path(".") else p


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--recruiting", required=True)
    parser.add_argument("--polls-weekly", required=True)
    parser.add_argument("--polls-summary", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    recruiting    = pd.read_csv(_resolve_recruiting(args.recruiting))
    polls_weekly  = pd.read_csv(_resolve_polls(args.polls_weekly))
    polls_summary = pd.read_csv(_resolve_polls(args.polls_summary))
    output_path   = _resolve_recruiting(args.output)

    poll_summary = build_poll_summary(polls_weekly, polls_summary)
    merged = create_cross_reference(recruiting, poll_summary)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_path, index=False)
    print(f"Saved cross-reference dataset to {output_path} ({len(merged):,} rows)")


if __name__ == "__main__":
    main()
