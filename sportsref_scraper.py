import io
import os
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment  # <- make sure this is here

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

def append_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        df.to_csv(path, mode="a", header=False, index=False)
    else:
        df.to_csv(path, mode="w", header=True, index=False)

def build_polls_long(polls_tables: list[pd.DataFrame], run_date: date) -> pd.DataFrame:
    """
    Your polls page returns multiple tables (you've been saving as polls_1, polls_2, ...).
    We'll melt any AP-style wide table into long rows: one row per team per poll_date.
    """
    long_parts = []
    run_date_str = run_date.strftime("%Y-%m-%d")

    for t_idx, df in enumerate(polls_tables, start=1):
        df = df.copy()

        # Find likely ID columns
        cols = [c for c in df.columns] 
        print(f"[polls table {t_idx}] columns: {list(cols)}")

        school_col = next((c for c in cols if str(c).lower() in ("school", "team")), None)
        conf_col   = next((c for c in cols if str(c).lower() in ("conf", "conference")), None)

        if school_col is None:
            continue  # not a team table
        # Only process tables that actually contain poll-week columns
        def is_week_col(x) -> bool:
            s = str(x).strip()
            if s.lower() in {"rk", "rank", "prev", "previous", "chng", "change", "conf", "conference", "school", "team"}:
                return False
            if s.lower() in {"pre", "preseason"}:
                return True
            # accept common date-like formats: 11/10, 11-10, 11.10, Nov 10, etc.
            return any(ch.isdigit() for ch in s) and len(s) <= 10

    has_week_cols = any(is_week_col(c) for c in cols)
    if not has_week_cols:
        continue


        id_vars = [school_col]
        if conf_col:
            id_vars.append(conf_col)

        # Everything else is a "poll week" column (often 'Pre', '11/10', etc.)
        # Keep only true poll-week columns (e.g., Pre, 11/10, 1/5), drop metadata columns
        drop_cols = {"rk", "rank", "prev", "previous", "chng", "change", "conference", "conf"}

        def is_week_col(x) -> bool:
            s = str(x).strip()
            if s.lower() in drop_cols or s.lower() in {"school", "team"}:
                return False
            if s.lower() in {"pre", "preseason"}:
                return True
            return any(ch.isdigit() for ch in s) and len(s) <= 10

        value_vars = [c for c in cols if (c not in id_vars and is_week_col(c))]
        print(f"[polls table {t_idx}] week cols used for melt: {list(value_vars)}")

            # accept poll-week labels: Pre, or dates like 11/10, 1/5, etc.
            if c_str == "Pre" or "/" in c_str:
                value_vars.append(c)


        melted = df.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name="poll_week",
            value_name="rank"
        )

        melted.rename(columns={school_col: "team", conf_col or "conf": "conference"}, inplace=True)

        # Clean ranks: blanks -> NaN; keep numeric
        melted["rank"] = pd.to_numeric(melted["rank"], errors="coerce")

        melted["run_date"] = run_date_str
        melted["table_id"] = t_idx  # keeps traceability (polls_1 vs polls_2)

        long_parts.append(melted)

    if not long_parts:
        return pd.DataFrame(columns=["team","conference","poll_week","rank","run_date","table_id"])

    out = pd.concat(long_parts, ignore_index=True)

    # Add rank_numeric for Tableau convenience: unranked -> 26
    out["rank_numeric"] = out["rank"].fillna(26).astype(int)

    return out


def clean_tables(tables):
    cleaned = []
    for df in tables:
        # strip strings, drop all-null rows
        df = strip_strings(df)
        df.dropna(how="all", inplace=True)
        cleaned.append(df)
    return cleaned

def strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    return df.apply(
        lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x)
    )
def fetch_tables(url):
    """Generic fetch: read all <table> elements from a URL."""
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    html_buf = io.StringIO(resp.text)
    tables = pd.read_html(html_buf)  # uses lxml/html5lib
    return clean_tables(tables)

def fetch_standings_tables(url):
    """
    Sports-Reference sometimes hides tables inside HTML comments.
    This function:
      1. Tries normal read_html on the full page
      2. If that fails, scans HTML comments for <table> blocks and parses those.
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")

    # --- 1) Try normal tables first ---
    html = str(soup)
    try:
        tables = pd.read_html(html)
        if tables:
            print(f"Found {len(tables)} standings tables via normal parsing.")
            return clean_tables(tables)
    except ValueError:
        # No tables found at top level; fall through to comment parsing
        pass

    # --- 2) Look inside HTML comments for tables ---
    comment_tables = []
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment_str = str(comment)
        if "<table" not in comment_str:
            continue
        try:
            dfs = pd.read_html(comment_str)
            comment_tables.extend(dfs)
        except ValueError:
            # This comment didn't actually contain a parseable table; skip it
            continue

    if not comment_tables:
        raise ValueError("No standings tables found in page or HTML comments")

    print(f"Found {len(comment_tables)} standings tables in HTML comments.")
    return clean_tables(comment_tables)

def save_tables(tables, prefix):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d")
    for i, df in enumerate(tables, start=1):
        path = os.path.join(OUTPUT_DIR, f"{prefix}_{i}_{ts}.csv")
        df.to_csv(path, index=False)
        print(f"Saved: {path}")

def save_standings_tables(tables, date_str: str) -> None:
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

from datetime import date

def main() -> None:
    today = date.today()
    today_str = today.strftime("%Y%m%d")

    print("Fetching polls data...")
    polls = fetch_tables(URL_POLLS)

    # keep your current per-run snapshots (nice for debugging / audit trail)
    save_tables(polls, "polls")

    # NEW: build + append to master LONG table
    polls_long = build_polls_long(polls, run_date=today)
    append_csv(polls_long, MASTER_POLLS_LONG)
    print(f"Appended polls_long rows: {len(polls_long)} -> {MASTER_POLLS_LONG}")

if __name__ == "__main__":
    main()
