
#!/usr/bin/env python3
"""
clean_recruiting_classes.py

Clean raw WBB recruiting class rankings and parse the free-text
'Key_Player_Features' column into structured fields.

Usage:
    python clean_recruiting_classes.py \
        --input raw_2023_2026_wbb_top20_recruit_classes.csv \
        --output recruiting_classes_cleaned.csv
"""

from __future__ import annotations

import argparse
import math
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


POSITION_KEYWORDS = {
    "point_guard": [r"\bpoint guard\b", r"\bpg\b"],
    "guard": [r"\bguard\b", r"\bbig guard\b", r"\bcombo guard\b"],
    "wing": [r"\bwing\b", r"\bwing/forward\b", r"\bforward/wing\b"],
    "forward": [r"\bforward\b", r"\bface-up forward\b", r"\bpower forward\b"],
    "center": [r"\bcenter\b", r"\bpost\b", r"\bbig\b"],
}

STRENGTH_PATTERNS = {
    "shooting_3pt": [r"3-point", r"3s\b", r"3-pointer", r"from deep", r"range", r"shooter", r"shooting stroke", r"spot-up"],
    "slashing_rim_pressure": [r"slash", r"drive", r"dribble", r"rim", r"attacks? glass", r"off the bounce", r"three levels"],
    "playmaking_passing": [r"passing", r"playmaking", r"facilitating", r"pick-and-roll", r"create", r"floor general"],
    "rebounding": [r"rebound", r"glass"],
    "defense": [r"defender", r"defense", r"guard inside to perimeter", r"3-and-d", r"3-and-D"],
    "versatility": [r"versatile", r"multiple positions", r"do-everything", r"blend of size and skill"],
    "athleticism": [r"athletic", r"quick", r"physical", r"lengthy", r"long", r"explosive", r"flair"],
    "post_skill": [r"footwork in the post", r"\bpost\b", r"face-up", r"big"],
    "international_honors": [r"u19", r"world cup", r"mvp"],
}

ARCHETYPE_RULES = [
    ("Playmaking Guard", lambda r: r["is_guard"] and r["strength_playmaking_passing"] and not r["strength_post_skill"]),
    ("Scoring Guard", lambda r: r["is_guard"] and r["strength_slashing_rim_pressure"]),
    ("Shooting Wing", lambda r: r["is_wing_or_forward"] and r["strength_shooting_3pt"]),
    ("Two-Way Wing", lambda r: r["is_wing_or_forward"] and r["strength_defense"] and r["strength_shooting_3pt"]),
    ("Skilled Forward", lambda r: r["is_wing_or_forward"] and r["strength_playmaking_passing"]),
    ("Interior Big", lambda r: r["is_big"] and r["strength_post_skill"]),
    ("Versatile Big", lambda r: r["is_big"] and r["strength_playmaking_passing"]),
]


def canonical_team_name(name: str) -> str:
    if pd.isna(name):
        return np.nan
    raw = str(name).strip()
    key = re.sub(r"\s+", " ", raw.lower())
    return TEAM_ALIASES.get(key, raw)


def normalize_text(text: str) -> str:
    if pd.isna(text):
        return ""
    text = str(text)
    text = text.replace("’", "'").replace("–", "-").replace("—", "-")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_height(text: str) -> tuple[float | None, int | None]:
    text = normalize_text(text).lower()

    # Patterns like 6-3, 5-11
    m = re.search(r"\b([5-7])[- ]([0-9]{1,2})\b", text)
    if m:
        feet, inches = int(m.group(1)), int(m.group(2))
        return round(feet + inches / 12, 3), feet * 12 + inches

    # Pattern like 6-foot
    m = re.search(r"\b([5-7])[- ]foot\b", text)
    if m:
        feet = int(m.group(1))
        return float(feet), feet * 12

    return None, None


def keyword_flag(text: str, patterns: list[str]) -> bool:
    return any(re.search(p, text, flags=re.IGNORECASE) for p in patterns)


def detect_positions(text: str, pos_short: str, pos_long: str) -> dict[str, bool]:
    pos_text = " ".join(
        [normalize_text(text), normalize_text(pos_short), normalize_text(pos_long)]
    ).lower()

    out = {f"is_{k}": keyword_flag(pos_text, v) for k, v in POSITION_KEYWORDS.items()}
    out["is_big"] = out["is_center"] or ("big" in pos_text and not out["is_guard"])
    out["is_wing_or_forward"] = out["is_wing"] or out["is_forward"]
    return out


def parse_feature_flags(text: str) -> dict[str, bool]:
    text = normalize_text(text).lower()
    out = {}
    for k, patterns in STRENGTH_PATTERNS.items():
        out[f"strength_{k}"] = keyword_flag(text, patterns)
    return out


def derive_primary_secondary_strengths(row: pd.Series) -> tuple[str | None, str | None]:
    priority = [
        "shooting_3pt",
        "playmaking_passing",
        "slashing_rim_pressure",
        "defense",
        "rebounding",
        "post_skill",
        "versatility",
        "athleticism",
        "international_honors",
    ]
    active = [k for k in priority if row.get(f"strength_{k}", False)]
    primary = active[0] if active else None
    secondary = active[1] if len(active) > 1 else None
    return primary, secondary


def derive_archetype(row: pd.Series) -> str:
    for label, fn in ARCHETYPE_RULES:
        if fn(row):
            return label
    if row["is_big"]:
        return "Big / Frontcourt"
    if row["is_guard"]:
        return "Guard"
    if row["is_wing_or_forward"]:
        return "Wing / Forward"
    return "General Prospect"


def clean_recruiting(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["team_canonical"] = out["Team"].apply(canonical_team_name)
    out["program_name_canonical"] = out["Full_Program_Name"].fillna(out["team_canonical"])
    out["feature_text_clean"] = out["Key_Player_Features"].apply(normalize_text)

    # Season cleanup
    out["season"] = pd.to_numeric(out["Season"], errors="coerce").astype("Int64")
    out["recruiting_year"] = pd.to_numeric(out["Recruiting_Year"], errors="coerce").astype("Int64")

    # Infer season if missing: class year + 1
    missing_season = out["season"].isna()
    out.loc[missing_season, "season"] = (out.loc[missing_season, "recruiting_year"] + 1).astype("Int64")

    # Heights
    heights = out["feature_text_clean"].apply(parse_height)
    out["height_ft"] = [h[0] for h in heights]
    out["height_inches"] = [h[1] for h in heights]

    # Position flags
    pos_flags = out.apply(
        lambda r: detect_positions(r["feature_text_clean"], r["Commit_Position_Short"], r["Commit_Position"]),
        axis=1,
        result_type="expand",
    )
    out = pd.concat([out, pos_flags], axis=1)

    # Strength flags
    strength_flags = out["feature_text_clean"].apply(parse_feature_flags).apply(pd.Series)
    out = pd.concat([out, strength_flags], axis=1)

    # Primary + secondary strengths
    strengths = out.apply(derive_primary_secondary_strengths, axis=1)
    out["primary_strength"] = [s[0] for s in strengths]
    out["secondary_strength"] = [s[1] for s in strengths]

    # Archetype
    out["player_archetype"] = out.apply(derive_archetype, axis=1)

    # Simple feature counts
    strength_cols = [c for c in out.columns if c.startswith("strength_")]
    out["n_strength_flags"] = out[strength_cols].sum(axis=1)

    # Rank quality buckets
    out["commit_rank_bucket"] = pd.cut(
        out["Commit_Rank"],
        bins=[0, 5, 10, 20, 35, 60, 100, np.inf],
        labels=["Top 5", "6-10", "11-20", "21-35", "36-60", "61-100", "100+"],
        include_lowest=True,
    )

    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to raw recruiting CSV")
    parser.add_argument("--output", required=True, help="Path to cleaned recruiting CSV")
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    cleaned = clean_recruiting(df)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    cleaned.to_csv(args.output, index=False)
    print(f"Saved cleaned recruiting data to {args.output} ({len(cleaned):,} rows)")


if __name__ == "__main__":
    main()
