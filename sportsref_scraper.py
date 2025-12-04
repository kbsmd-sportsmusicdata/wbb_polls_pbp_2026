import io
import pandas as pd
import requests
from datetime import datetime
import os

URL_POLLS = "https://www.sports-reference.com/cbb/seasons/women/2026-polls.html"
URL_STANDINGS = "https://www.sports-reference.com/cbb/seasons/women/2026-standings.html"
OUTPUT_DIR = "data"

def fetch_tables(url):
    """Fetch all HTML tables from a URL and return list of cleaned DataFrames."""
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    # read_html will now use lxml as the parser
    # Wrap in StringIO to be future-proof
    html_buf = io.StringIO(resp.text)
    tables = pd.read_html(html_buf)

    cleaned = []
    for df in tables:
        # strip whitespace
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        # drop fully empty rows
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
    standings = fetch_tables(URL_STANDINGS)
    save_tables(standings, "standings")

if __name__ == "__main__":
    main()
    
