import io
import os
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment  # <- make sure this is here

URL_POLLS = "https://www.sports-reference.com/cbb/seasons/women/2026-polls.html"
URL_STANDINGS = "https://www.sports-reference.com/cbb/seasons/women/2026-standings.html"
OUTPUT_DIR = "data"

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

def clean_tables(tables):
    cleaned = []
    for df in tables:
        # strip strings, drop all-null rows
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df.dropna(how="all", inplace=True)
        cleaned.append(df)
    return cleaned

def save_tables(tables, prefix):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d")
    for i, df in enumerate(tables, start=1):
        path = os.path.join(OUTPUT_DIR, f"{prefix}_{i}_{ts}.csv")
        df.to_csv(path, index=False)
        print(f"Saved: {path}")

def main():
    print("Fetching polls data...")
    polls = fetch_tables(URL_POLLS)
    save_tables(polls, "polls")

    print("Fetching standings data...")
    try:
        standings = fetch_standings_tables(URL_STANDINGS)
        save_tables(standings, "standings")
    except ValueError as e:
        print(f"WARNING: could not parse standings tables: {e}")
        # Script still exits successfully so your workflow stays green

if __name__ == "__main__":
    main()
