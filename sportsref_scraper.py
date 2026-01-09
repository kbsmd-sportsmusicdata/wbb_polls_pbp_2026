import io
import os
import re
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment  # make sure this is here


# ---------------------------
# Paths / constants
# ---------------------------
DATA_DIR = Path("data")
STANDINGS_SPLIT_DIR = DATA_DIR / "standings_by_conf"
STANDINGS_COMBINED_DIR = DATA_DIR / "standings_full"

# Make sure directories exist
for p in (STANDINGS_SPLIT_DIR, STANDINGS_COMBINED_DIR):
    p.mkdir(parents=True, exist_ok=True)

URL_POLLS = "https://www.sports-reference.com/cbb/seasons/women/2026-polls.html"
URL_STANDINGS = "https://www.sports-reference.com/cbb/seasons/women/2026-standings.html"

OUTPUT_DIR = "data"
MASTER_POLLS_LONG = DATA_DIR / "polls_long.csv"
MASTER_STANDINGS_LONG = DATA_DIR / "standings_long.csv"


# ---------------------------
# Helpers
# ---------------------------
def append_csv(df: pd.DataFrame, path: Path) -> None:
    """Append dataframe to CSV (create if missing). Skip if df empty."""
    path.parent.mkdir(parents=True, exist_ok=True)

    if df is None or df.empty:
        print(f"[append_csv] No rows to append for {path}. Skipping.")
        return

    if path.exists():
        df.to_csv(path, mode="a", header=False, index=False)
    else:
        df.to_csv(path, mode="w", header=True, index=False)


def strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace from string cells."""
    return df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))


def clean_tables(tables: list[pd.DataFrame]) -> list[pd.DataFrame]:
    cleaned = []
    for df in tables:
        df = strip_strings(df)
        df.dropna(how="all", inplace=True)
        cleaned.append(df)
    return cleaned


def normalize_col(c) -> str:
    """
    Sports-Reference tables often come with MultiIndex columns from pandas.read_html
    (tuples like ('Week Poll','1')). This converts any column label into a usable string.

    Strategy: if tuple/list, pick the last non-empty piece.
    """
    if isinstance(c, (tuple, list)):
        parts = [str(x).strip() for x in c if str(x).strip() and str(x).strip().lower() != "nan"]
        return parts[-1] if parts else ""
    return str(c).strip()


def find_col(cols, candidates: set[str]):
    """Return the original column label whose normalized value matches candidates."""
    for c in cols:
        if normalize_col(c).lower() in candidates:
            return c
    return None


def is_week_col(col_label) -> bool:
    """
    Determine whether a column represents a poll week (Pre or date-like).
    Works with normalized column labels.
    """
    s = normalize_col(col_label)
    s_low = s.lower()

    # Exclude known metadata labels
    if s_low in {"rk", "rank", "prev", "previous", "chng", "change", "conf", "conference", "school", "team"}:
        return False

    # Preseason
    if s_low in {"pre", "preseason"}:
        return True

    # Common date-like patterns: 11/10, 1/5, 11-10, 11.10
    if re.match(r"^\d{1,2}[\/\-.]\d{1,2}$", s):
        return True

    # Sometimes pandas gives just digits for the header row index; treat carefully
    # We only accept short strings that contain digits and aren't obviously metadata.
    if any(ch.isdigit() for ch in s) and len(s) <= 10:
        # Avoid weird stuff like "2026" if it appears as a header
        if len(re.findall(r"\d", s)) >= 2:
            return True

    return False


# ---------------------------
# Fetchers
# ---------------------------
def fetch_tables(url: str) -> list[pd.DataFrame]:
    """Generic fetch: read all <table> elements from a URL."""
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()

    html_buf = io.StringIO(resp.text)
    tables = pd.read_html(html_buf)
    return clean_tables(tables)


def fetch_standings_tables(url: str) -> list[pd.DataFrame]:
    """
    Sports-Reference sometimes hides tables inside HTML comments.
    This function:
      1) Tries normal read_html on the full page
      2) If that fails, scans HTML comments for <table> blocks and parses those.
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")

    # 1) Try normal tables first
    html = str(soup)
    try:
        tables = pd.read_html(html)
        if tables:
            print(f"Found {len(tables)} standings tables via normal parsing.")
            return clean_tables(tables)
    except ValueError:
        pass

    # 2) Look inside HTML comments for tables
    comment_tables = []
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment_str = str(comment)
        if "<table" not in comment_str:
            continue
        try:
            dfs = pd.read_html(comment_str)
            comment_tables.extend(dfs)
        except ValueError:
            continue

    if not comment_tables:
        raise ValueError("No standings tables found in page or HTML comments")

    print(f"Found {len(comment_tables)} standings tables in HTML comments.")
    return clean_tables(comment_tables)


# ---------------------------
# Saving
# ---------------------------
def save_tables(tables: list[pd.DataFrame], prefix: str) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d")
    for i, df in enumerate(tables, start=1):
        path = os.path.join(OUTPUT_DIR, f"{prefix}_{i}_{ts}.csv")
        df.to_csv(path, index=False)
        print(f"Saved: {path}")


def save_standings_tables(tables: list[pd.DataFrame], date_str: str) -> None:
    """Save each conference standings table and one combined CSV."""
    cleaned_tables = []
    for i, df in enumerate(tables, start=1):
        cleaned_tables.append(df)
        out_path = STANDINGS_SPLIT_DIR / f"standings_{i}_{date_str}.csv"
        df.to_csv(out_path, index=False)
        print(f"Saved conference {i} standings: {out_path}")

    combined = pd.concat(cleaned_tables, ignore_index=True)
    combined_path = STANDINGS_COMBINED_DIR / f"standings_all_{date_str}.csv"
    combined.to_csv(combined_path, index=False)
    print(f"Saved combined standings: {combined_path}")


# ---------------------------
# Polls -> LONG builder
# ---------------------------
def build_polls_long(polls_tables: list[pd.DataFrame], run_date: date) -> pd.DataFrame:
    """
    Melt AP-style wide poll tables into long:
    one row per team per poll_week.
    """
    long_parts = []
    run_date_str = run_date.strftime("%Y-%m-%d")

    for t_idx, df in enumerate(polls_tables, start=1):
        df = df.copy()
        cols = list(df.columns)

        print(f"[polls table {t_idx}] raw columns: {cols}")
        print(f"[polls table {t_idx}] normalized columns: {[normalize_col(c) for c in cols]}")

        school_col = find_col(cols, {"school", "team"})
        conf_col = find_col(cols, {"conf", "conference"})

        if school_col is None:
            # Not a team table
            continue

        # --- YOUR GATE: only process tables that contain actual week columns ---
        has_week_cols = any((normalize_col(c) == "Pre") or ("/" in normalize_col(c)) for c in cols)
        # More robust fallback (covers 11-10, 11.10, etc.)
        has_week_cols = has_week_cols or any(is_week_col(c) for c in cols)

        if not has_week_cols:
            print(f"[polls table {t_idx}] skipping: no week columns detected.")
            continue

        id_vars = [school_col]
        if conf_col is not None:
            id_vars.append(conf_col)

        value_vars = [c for c in cols if (c not in id_vars and is_week_col(c))]

        if not value_vars:
            print(f"[polls table {t_idx}] skipping: week columns detected but none selected for melt.")
            continue

        print(f"[polls table {t_idx}] week cols used for melt: {[normalize_col(c) for c in value_vars]}")

        melted = df.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name="poll_week",
            value_name="rank",
        )

        # Normalize poll_week into a clean string
        melted["poll_week"] = melted["poll_week"].map(normalize_col)

        # Rename id columns to canonical names
        rename_map = {school_col: "team"}
        if conf_col is not None:
            rename_map[conf_col] = "conference"
        melted.rename(columns=rename_map, inplace=True)

        # Clean ranks
        melted["rank"] = pd.to_numeric(melted["rank"], errors="coerce")

        melted["run_date"] = run_date_str
        melted["table_id"] = t_idx

        long_parts.append(melted)

    if not long_parts:
        return pd.DataFrame(columns=["team", "conference", "poll_week", "rank", "run_date", "table_id", "rank_numeric"])

    out = pd.concat(long_parts, ignore_index=True)

    # Fill conference if missing (some tables won’t include it)
    if "conference" not in out.columns:
        out["conference"] = None

    # Tableau-friendly numeric rank: unranked -> 26
    out["rank_numeric"] = out["rank"].fillna(26).astype(int)

    # Keep tidy column order
    out = out[["team", "conference", "poll_week", "rank", "run_date", "table_id", "rank_numeric"]]

    return out


# ---------------------------
# Main
# ---------------------------
def main() -> None:
    today = date.today()
    today_str = today.strftime("%Y%m%d")

    # --- POLLS ---
    print("Fetching polls data...")
    polls = fetch_tables(URL_POLLS)

    # Per-run snapshots (audit trail)
    save_tables(polls, "polls")

    # Master LONG append
    polls_long = build_polls_long(polls, run_date=today)
    append_csv(polls_long, MASTER_POLLS_LONG)
    print(f"polls_long rows built: {len(polls_long)}")

    # --- STANDINGS (optional; keep if you want both) ---
    print("Fetching standings data...")
    standings = fetch_standings_tables(URL_STANDINGS)
    save_standings_tables(standings, today_str)


if __name__ == "__main__":
    main()
