import io
import os
from datetime import datetime

import pandas as pd
import requests

URL_POLLS = "https://www.sports-reference.com/cbb/seasons/women/2026-polls.html"
URL_STANDINGS = "https://www.sports-reference.com/cbb/seasons/women/2026-standings.html"
OUTPUT_DIR = "data"


def fetch_tables(url):
    """Fetch all HTML tables from a URL and return list of cleaned DataFrames."""
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    html_buf = io.StringIO(resp.text)
    tables = pd.read_html(html_buf)  # uses lxml/html5lib under the hood

    cleaned = []
    for df in tables:
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
        standings = fetch_tables(URL_STANDINGS)
        save_tables(standings, "standings")
    except ValueError as e:
        # pandas raises this when no <table> tags are detected
        print(f"WARNING: could not parse standings tables: {e}")
        # Do NOT re-raise, so the script exits successfully


if __name__ == "__main__":
    main()
