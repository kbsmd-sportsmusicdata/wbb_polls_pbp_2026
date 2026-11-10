"""
Single source of truth for which NCAA WBB season this pipeline is currently
scraping, and where that season's data lives.

Every scraper that hits a season-specific sports-reference URL imports
get_current_season() instead of hardcoding the year, so bumping to a new
season is a one-line change in config/poll_week_windows.json. The files that
risk colliding across a season boundary (polls_long.csv, polls_analytics.csv,
polls_games_joined.csv, and the raw dated snapshots that feed them -- none of
them carry a season column, only team+poll_week/run_date) live under
get_season_data_dir() instead of data/'s top level, so a new season's rows
can never get deduped against a previous season's under the same label.
Files that dedup by date alone (ratings, NET rankings, schedule) and files
that are season-independent (team_state_mapping.csv, the multi-season
polls_historical_* archive) are untouched and stay at the top level.
"""
import json
from pathlib import Path

CONFIG_PATH = Path("config") / "poll_week_windows.json"
DATA_DIR = Path("data")

# The last season whose collision-risk files live directly under data/ (the
# original, pre-data/seasons/ layout). Any season after this one gets its own
# data/seasons/<label>/ folder instead -- see get_season_data_dir(). Bumping
# 'season' past this value is what actually switches a season over; nothing
# else needs to change for that to take effect.
LEGACY_TOP_LEVEL_SEASON = 2026


def _load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return json.load(f)


def get_current_season() -> int:
    """Return the current season year (e.g. 2026 for the 2025-26 season).

    Raises FileNotFoundError / KeyError loudly rather than falling back to a
    guessed value -- a season-specific URL built from the wrong year would
    silently scrape stale or wrong data instead of failing visibly.
    """
    config = _load_config()

    if "season" not in config:
        raise KeyError(
            f"'season' key not found in {CONFIG_PATH}. "
            f'Add it at the top level, e.g. "season": 2026.'
        )

    return config["season"]


def get_season_label() -> str:
    """Return the two-year season label (e.g. '2026-27' for season 2027)."""
    season = get_current_season()
    return f"{season - 1}-{str(season)[2:]}"


def get_season_data_dir() -> Path:
    """Return the directory where the current season's collision-risk files
    (polls_long.csv, polls_analytics.csv, polls_games_joined.csv, and the raw
    dated snapshots that feed them) live.

    For LEGACY_TOP_LEVEL_SEASON (2026, the 2025-26 season already accumulated
    at data/'s top level) this returns data/ itself, unchanged -- those files
    are not moved. For any later season it returns data/seasons/<label>/,
    created if missing, starting that season with a clean slate instead of
    appending onto a previous season's files (none of which carry a season
    column, so a naive shared file risks deduping across seasons under the
    same team+poll_week label).
    """
    season = get_current_season()
    if season <= LEGACY_TOP_LEVEL_SEASON:
        return DATA_DIR

    season_dir = DATA_DIR / "seasons" / get_season_label()
    season_dir.mkdir(parents=True, exist_ok=True)
    return season_dir
