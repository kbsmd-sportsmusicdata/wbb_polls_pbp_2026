
#!/usr/bin/env python3
"""
build_roster_archetypes.py

From the team-season master dataset, assign Roster Construction Archetypes
and produce three Tableau-ready visual output CSVs.

Archetypes assigned via rule-based logic (no portal data yet; portal metrics
are stubs that will auto-populate once portal data is available):
    - "Recruit-Built"       top_recruit_rank <= 5  AND portal reliance low
    - "Top Recruit Anchor"  top_recruit_rank <= 10
    - "Balanced Class"      broad recruiting + decent ap results
    - "Veteran / Retention" no strong recruit signal, ranked in polls
    - "Under the Radar"     not top-ranked recruiting, still appeared in polls
    - "Unranked"            default

Outputs:
    data/roster_archetypes.csv          — archetype per team-season
    data/recruiting_analysis.csv        — recruiting rank vs poll correlation table
    data/tableau/tableau_recruiting_vs_polls.csv
    data/tableau/tableau_roster_archetypes.csv
    data/tableau/tableau_portal_roi.csv  (stub)

Usage:
    python build_roster_archetypes.py \
        --master team_season_master.csv \
        --output-archetypes roster_archetypes.csv \
        --output-analysis   recruiting_analysis.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


RECRUITING_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "recruiting"
DATA_DIR       = Path(__file__).resolve().parent.parent.parent / "data"
TABLEAU_DIR    = DATA_DIR / "tableau"


def _resolve_recruiting(path_str: str) -> Path:
    p = Path(path_str)
    return RECRUITING_DIR / p if p.parent == Path(".") else p


def _assign_archetypes_vectorised(master: pd.DataFrame) -> pd.Series:
    """Vectorised archetype assignment using np.select (NaN-safe)."""
    rank  = master.get("top_recruit_rank")
    peakap = master.get("ap_rank_peak")
    w25   = master.get("weeks_in_top25", pd.Series(0, index=master.index)).fillna(0)

    rank_known      = rank.notna()
    ranked_in_polls = peakap.notna() | (w25 > 0)

    conditions = [
        rank_known & (rank <= 3),
        rank_known & (rank <= 5),
        rank_known & (rank <= 10),
        ranked_in_polls & rank_known & (rank <= 20),
        ranked_in_polls & (~rank_known | (rank > 20)),
        rank_known & (rank <= 20) & ~ranked_in_polls,
    ]
    choices = [
        "Elite Recruiting Class",
        "Top-5 Recruit-Built",
        "Top Recruit Anchor",
        "Balanced Recruit + Results",
        "Veteran / Development Success",
        "High Expectations / Underperformed",
    ]
    return pd.Series(
        np.select(conditions, choices, default="Unranked / Low Recruit Visibility"),
        index=master.index,
    )


def compute_recruiting_analysis(master: pd.DataFrame) -> pd.DataFrame:
    """Correlation table: recruiting rank bucket vs poll outcomes."""
    df = master.copy()

    df["recruit_rank_bucket"] = pd.cut(
        pd.to_numeric(df["top_recruit_rank"], errors="coerce"),
        bins=[0, 3, 5, 10, 15, 20, np.inf],
        labels=["Elite 1-3", "Top 5", "Top 10", "Top 15", "Top 20", "Outside Top 20"],
        include_lowest=True,
    )

    df["finished_ranked"]  = df["ap_rank_final"].notna().astype(int)
    df["top10_finish"]     = (pd.to_numeric(df["ap_rank_final"], errors="coerce") <= 10).fillna(False).astype(int)
    df["preseason_ranked"] = df["ap_rank_preseason"].notna().astype(int)

    analysis = (
        df.groupby("recruit_rank_bucket", observed=True)
        .agg(
            n_team_seasons=("team", "size"),
            pct_preseason_ranked=("preseason_ranked", "mean"),
            pct_finished_ranked=("finished_ranked", "mean"),
            pct_top10_finish=("top10_finish", "mean"),
            avg_weeks_in_top25=("weeks_in_top25", "mean"),
            avg_ap_peak=("ap_rank_peak", "mean"),
            avg_ap_final=("ap_rank_final", "mean"),
            avg_wins=("wins", "mean"),
        )
        .reset_index()
    )

    for col in ["pct_preseason_ranked", "pct_finished_ranked", "pct_top10_finish"]:
        analysis[col] = analysis[col].round(3)
    for col in ["avg_weeks_in_top25", "avg_ap_peak", "avg_ap_final", "avg_wins"]:
        analysis[col] = analysis[col].round(2)

    return analysis


def build_tableau_recruiting_vs_polls(master: pd.DataFrame) -> pd.DataFrame:
    """Scatter-ready: one row per team-season with key recruiting + poll metrics."""
    cols = [
        "team", "season", "conference", "roster_archetype",
        "top_recruit_rank", "avg_espn_grade", "recruit_star_index",
        "n_top10_recruits", "n_top25_recruits",
        "ap_rank_preseason", "ap_rank_peak", "ap_rank_final",
        "weeks_in_top25", "wins", "losses", "win_pct",
    ]
    return master[[c for c in cols if c in master.columns]].copy()


def build_tableau_archetypes(master: pd.DataFrame) -> pd.DataFrame:
    """Archetype summary: counts and average outcomes per archetype × season."""
    df = master.copy()
    df["finished_ranked"] = df["ap_rank_final"].notna().astype(int)
    df["top10_finish"]    = (pd.to_numeric(df["ap_rank_final"], errors="coerce") <= 10).fillna(False).astype(int)

    agg = (
        df.groupby(["roster_archetype", "season"])
        .agg(
            n_teams=("team", "size"),
            pct_finished_ranked=("finished_ranked", "mean"),
            pct_top10_finish=("top10_finish", "mean"),
            avg_weeks_in_top25=("weeks_in_top25", "mean"),
            avg_wins=("wins", "mean"),
            avg_ap_peak=("ap_rank_peak", "mean"),
        )
        .reset_index()
    )
    for col in ["pct_finished_ranked", "pct_top10_finish", "avg_weeks_in_top25", "avg_wins", "avg_ap_peak"]:
        agg[col] = agg[col].round(3)
    return agg


def build_tableau_portal_roi() -> pd.DataFrame:
    """Stub portal ROI table — will be populated once transfer portal data is available."""
    return pd.DataFrame(columns=[
        "team", "season", "conference", "roster_archetype",
        "portal_in_count", "portal_out_count", "net_portal_delta",
        "ap_rank_peak", "weeks_in_top25", "wins",
        "portal_roi_score",   # stub metric
        "note",
    ]).assign(note="Portal data not yet available")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--master",             required=True, help="Team-season master CSV (bare or full path)")
    parser.add_argument("--output-archetypes",  required=True, help="Output archetypes CSV")
    parser.add_argument("--output-analysis",    required=True, help="Output recruiting analysis CSV")
    args = parser.parse_args()

    master_path     = _resolve_recruiting(args.master)
    archetypes_path = _resolve_recruiting(args.output_archetypes)
    analysis_path   = _resolve_recruiting(args.output_analysis)

    master = pd.read_csv(master_path)

    # Assign archetypes (vectorised — avoids row-by-row iteration and NaN edge cases)
    master["roster_archetype"] = _assign_archetypes_vectorised(master)

    # Save archetype-enriched master slice
    archetypes_path.parent.mkdir(parents=True, exist_ok=True)
    master.to_csv(archetypes_path, index=False)
    print(f"Saved roster archetypes to {archetypes_path} ({len(master):,} rows)")

    # Recruiting analysis table
    analysis = compute_recruiting_analysis(master)
    analysis_path.parent.mkdir(parents=True, exist_ok=True)
    analysis.to_csv(analysis_path, index=False)
    print(f"Saved recruiting analysis to {analysis_path} ({len(analysis):,} rows)")

    # Tableau outputs
    TABLEAU_DIR.mkdir(parents=True, exist_ok=True)

    t1 = build_tableau_recruiting_vs_polls(master)
    t1_path = TABLEAU_DIR / "tableau_recruiting_vs_polls.csv"
    t1.to_csv(t1_path, index=False)
    print(f"Saved Tableau scatter data to {t1_path} ({len(t1):,} rows)")

    t2 = build_tableau_archetypes(master)
    t2_path = TABLEAU_DIR / "tableau_roster_archetypes.csv"
    t2.to_csv(t2_path, index=False)
    print(f"Saved Tableau archetype summary to {t2_path} ({len(t2):,} rows)")

    t3 = build_tableau_portal_roi()
    t3_path = TABLEAU_DIR / "tableau_portal_roi.csv"
    t3.to_csv(t3_path, index=False)
    print(f"Saved Tableau portal ROI stub to {t3_path}")


if __name__ == "__main__":
    main()
