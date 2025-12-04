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

def clean_tables(tables):
    cleaned = []
    for df in tables:
        # strip strings, drop all-null rows
        df = strip_strings(df)(lambda x: x.strip() if isinstance(x, str) else x)
        df.dropna(how="all", inplace=True)
        cleaned.append(df)
    return cleaned
    
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

def strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    return df.apply(
        lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x)
    )

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

def main():
    today_str = date.today().strftime("%Y%m%d")

    print("Fetching polls data...")
    polls = fetch_tables(URL_POLLS)
    save_tables(polls, "polls")

    print("Fetching standings data...")
    try:
        standings = fetch_standings_tables(URL_STANDINGS)
        if not standings:
            print("WARNING: no standings tables returned")
    else:
        save_standings_tables(standings, today_str)
   except ValueError as e:
        print(f"WARNING: could not parse standings tables: {e}")


if __name__ == "__main__":
    main()
