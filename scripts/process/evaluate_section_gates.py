"""
Evaluate dashboard section gates against polls_games_dashboard.csv.

Reads config/dashboard_sections.json and the season's dashboard dataset and
writes ``dashboard_gates.json`` beside the data. The dashboard loads that file on
each refresh to decide which sections are clickable and what tooltip to show.

Status per section
------------------
live               data thresholds met AND published -> clickable
awaiting_approval  data thresholds met, not yet published -> greyed out
coming_soon        data thresholds not met -> greyed out, tooltip shows progress

The public tooltip for both greyed states begins with "Coming Soon"; only the
``coming_soon`` tooltip carries progress. ``awaiting_approval`` is flagged in the
JSON so you can see what is ready to switch on without exposing it.

Usage:
    python scripts/process/evaluate_section_gates.py
"""

import json
import operator
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG = REPO_ROOT / "config" / "dashboard_sections.json"

OPS = {">=": operator.ge, ">": operator.gt, "<=": operator.le, "<": operator.lt, "==": operator.eq}


def compute_metrics(df):
    """Compute gate metrics from a polls_games_dashboard frame.

    The gates describe the poll teams and the games they play. Watchlist schools (rows
    with is_poll_team False) and games between two non-poll schools (is_poll_scope_game
    False) are left out, so adding watchlist schools never changes a threshold's meaning.
    """
    all_rows = df
    if "is_poll_team" in df.columns:
        df = df[df["is_poll_team"].astype(bool)]
    teams = df["team"].unique()
    game_rows = df[df["has_game"].astype(bool)]
    # team-games are unique per (team, game_id); a team never repeats a game
    per_team = game_rows.groupby("team")

    def counts(mask_col=None, value=None):
        sub = game_rows if mask_col is None else game_rows[game_rows[mask_col] == value]
        return sub.groupby("team").size().reindex(teams, fill_value=0)

    def pct_ge(series, n):
        return round(float((series >= n).mean()), 3) if len(series) else 0.0

    total = counts()
    home, road = counts("game_Location", "Home"), counts("game_Location", "Away")
    conf = game_rows[game_rows["game_is_conf_game"].astype(str) == "True"].groupby("team").size().reindex(teams, fill_value=0)
    top25 = (
        game_rows[game_rows["opp_rank_tipoff"].notna()].groupby("team").size().reindex(teams, fill_value=0)
    )
    # Pythagorean ratings: latest row per team that has them (blank until ratings exist)
    if "pyth_wins" in df.columns:
        py = df.dropna(subset=["pyth_wins"]).sort_values("week_number").groupby("team").tail(1)
        pyth_teams = int(len(py))
        pyth_ge8 = round(float((py["pyth_games"] >= 8).sum() / len(teams)), 3) if len(teams) else 0.0
        pyth_min = int(py["pyth_games"].min()) if pyth_teams else 0
    else:
        pyth_teams, pyth_ge8, pyth_min = 0, 0.0, 0
    in_scope = all_rows["is_unique_game"].astype(bool)
    if "is_poll_scope_game" in all_rows.columns:
        in_scope &= all_rows["is_poll_scope_game"].astype(bool)
    unique = all_rows[in_scope]
    weeks = df["week_number"].nunique()
    n_watch = int(all_rows.loc[all_rows["is_watchlist"].astype(bool), "team"].nunique()) if "is_watchlist" in all_rows.columns else 0
    return {
        "polls_released": int(weeks),
        "complete_weeks": int(max(weeks - 1, 0)),
        "teams": int(len(teams)),
        "unique_games": int(unique["game_id"].nunique()),
        "upsets": int(unique["game_is_upset"].isin(["Upset", "Major Upset"]).sum()),
        "top25_matchups": int((unique["game_quality_tier"] == "Top 25 Matchup").sum()),
        "min_games_per_team": int(total.min()) if len(total) else 0,
        "pct_teams_games_ge2": pct_ge(total, 2),
        "pct_teams_games_ge3": pct_ge(total, 3),
        "pct_teams_home_ge2": pct_ge(home, 2),
        "pct_teams_road_ge2": pct_ge(road, 2),
        "pct_teams_conf_ge2": pct_ge(conf, 2),
        "pct_teams_top25_ge2": pct_ge(top25, 2),
        "watchlist_teams": n_watch,
        "pyth_teams": pyth_teams,
        "pct_teams_pyth_games_ge8": pyth_ge8,
        "min_pyth_games": pyth_min,
    }


LABELS = {
    "polls_released": "polls", "complete_weeks": "complete poll weeks", "teams": "teams",
    "unique_games": "games", "upsets": "upsets", "top25_matchups": "Top 25 matchups",
    "min_games_per_team": "games for the team with the fewest",
    "pct_teams_games_ge2": "of teams with 2+ games", "pct_teams_games_ge3": "of teams with 3+ games",
    "pct_teams_home_ge2": "of teams with 2+ home games", "pct_teams_road_ge2": "of teams with 2+ road games",
    "pct_teams_conf_ge2": "of teams with 2+ conference games",
    "pct_teams_top25_ge2": "of teams with 2+ ranked opponents",
    "watchlist_teams": "watchlist schools", "pyth_teams": "teams with ratings", "pct_teams_pyth_games_ge8": "of teams with 8+ rated games",
    "min_pyth_games": "games for the team with the fewest",
}


def _fmt(metric, v):
    return f"{v:.0%}" if metric.startswith("pct_") else f"{v:g}"


def evaluate(config, metrics):
    out = []
    prefix = config.get("coming_soon_prefix", "Coming Soon")
    for sec in config["sections"]:
        checks = []
        for req in sec["requires"]:
            have = metrics[req["metric"]]
            checks.append({
                "metric": req["metric"], "op": req["op"], "need": req["value"], "have": have,
                "met": bool(OPS[req["op"]](have, req["value"])),
            })
        data_ready = all(c["met"] for c in checks)
        published = bool(sec.get("published", False))
        if data_ready and published:
            status, tooltip = "live", ""
        elif data_ready:
            status, tooltip = "awaiting_approval", f"{prefix}"
        else:
            unmet = [c for c in checks if not c["met"]]
            progress = "; ".join(
                f"{_fmt(c['metric'], c['have'])} / {_fmt(c['metric'], c['need'])} {LABELS[c['metric']]}"
                for c in unmet
            )
            status = "coming_soon"
            tooltip = f"{prefix} — {sec.get('unlock_note', '')} ({progress})"
        out.append({
            "id": sec["id"], "title": sec["title"], "audience": sec.get("audience"),
            "status": status, "clickable": status == "live", "published": published,
            "data_ready": data_ready, "tooltip": tooltip, "checks": checks,
        })
    return out


def main():
    sys.path.insert(0, str(REPO_ROOT))
    from season_config import get_season_data_dir

    season_dir = REPO_ROOT / get_season_data_dir()
    df = pd.read_csv(season_dir / "polls_games_dashboard.csv")
    config = json.loads(CONFIG.read_text())
    metrics = compute_metrics(df)
    sections = evaluate(config, metrics)
    run_date = str(df["run_date"].iloc[0]) if "run_date" in df else None
    payload = {"run_date": run_date, "metrics": metrics, "sections": sections}
    out_path = season_dir / "dashboard_gates.json"
    out_path.write_text(json.dumps(payload, indent=2))
    print(f"Wrote {out_path}")
    for s in sections:
        print(f"  {s['status']:18s} {s['id']:22s} {s['tooltip']}")


if __name__ == "__main__":
    main()
