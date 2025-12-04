import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os

# URLs to scrape
URL_POLLS = "https://www.sports-reference.com/cbb/seasons/women/2026-polls.html"
URL_STANDINGS = "https://www.sports-reference.com/cbb/seasons/women/2026-standings.html"
OUTPUT_DIR = "data"

def fetch_tables(url):
    """Fetches all HTML tables from a URL and returns list of cleaned DataFrames."""
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    tables = pd.read_html(str(soup))

    cleaned_tables = []
    for table in tables:
        table = table.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        table.dropna(how='all', inplace=True)
        cleaned_tables.append(table)
    return cleaned_tables

def save_tables(tables, prefix):
    """Saves each DataFrame to CSV with timestamped filename."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d")
    for i, df in enumerate(tables):
        filename = f"{prefix}_{i+1}_{timestamp}.csv"
        filepath = os.path.join(OUTPUT_DIR, filename)
        df.to_csv(filepath, index=False)
        print(f"Saved: {filepath}")

def main():
    print("Fetching polls data...")
    polls = fetch_tables(URL_POLLS)
    save_tables(polls, "polls")

    print("Fetching standings data...")
    standings = fetch_tables(URL_STANDINGS)
    save_tables(standings, "standings")

if __name__ == "__main__":
    main()
