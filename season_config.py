"""
Single source of truth for which NCAA WBB season this pipeline is currently
scraping. Every scraper that hits a season-specific sports-reference URL
imports get_current_season() instead of hardcoding the year, so bumping to a
new season is a one-line change in config/poll_week_windows.json.
"""
import json
from pathlib import Path

CONFIG_PATH = Path("config") / "poll_week_windows.json"


def get_current_season() -> int:
    """Return the current season year (e.g. 2026 for the 2025-26 season).

    Raises FileNotFoundError / KeyError loudly rather than falling back to a
    guessed value -- a season-specific URL built from the wrong year would
    silently scrape stale or wrong data instead of failing visibly.
    """
    with open(CONFIG_PATH) as f:
        config = json.load(f)

    if "season" not in config:
        raise KeyError(
            f"'season' key not found in {CONFIG_PATH}. "
            f'Add it at the top level, e.g. "season": 2026.'
        )

    return config["season"]
