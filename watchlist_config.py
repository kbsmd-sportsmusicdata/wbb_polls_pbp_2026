"""Loads the dashboard watchlist (config/dashboard_watchlist.json): schools outside the
AP poll that could make it or upset a ranked team. Shared by filter_schedule.py (so their
games are in the schedule) and scripts/process/prep_dashboard_data.py."""
import json
from pathlib import Path

WATCHLIST_PATH = Path(__file__).resolve().parent / "config" / "dashboard_watchlist.json"


def load_watchlist(path=WATCHLIST_PATH) -> list:
    """Return the watchlist school names (empty list if the file is missing)."""
    p = Path(path)
    if not p.exists():
        return []
    names = json.loads(p.read_text()).get("schools", [])
    seen, out = set(), []
    for n in names:                       # keep order, drop blanks and repeats
        n = str(n).strip()
        if n and n not in seen:
            seen.add(n)
            out.append(n)
    return out
