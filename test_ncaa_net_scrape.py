"""
Test NCAA WBB NET Rankings scraping using the wehoop R method translated to Python.

Based on: https://github.com/sportsdataverse/wehoop/blob/main/R/ncaa_wbb_data.R
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime

def scrape_ncaa_net_rankings():
    """
    Scrape NCAA WBB NET Rankings from official NCAA website.

    Returns:
        pd.DataFrame with NET rankings data
    """
    NET_url = "https://www.ncaa.com/rankings/basketball-women/d1/ncaa-womens-basketball-net-rankings"

    print(f"Fetching NET rankings from NCAA.com...")
    print(f"URL: {NET_url}")
    print()

    try:
        # Fetch the page
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(NET_url, headers=headers, timeout=30)
        response.raise_for_status()

        print(f"✓ Successfully fetched page (status: {response.status_code})")
        print()

        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the first table (like R code does)
        tables = soup.find_all('table')

        if not tables:
            print("✗ No tables found on page")
            return None

        print(f"✓ Found {len(tables)} table(s) on page")

        # Extract first table
        table = tables[0]

        # Convert to DataFrame
        df = pd.read_html(str(table))[0]

        print(f"✓ Extracted table with {len(df)} rows, {len(df.columns)} columns")
        print()

        # Clean column names (lowercase, replace spaces with underscores)
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')

        # Add metadata
        df['scraped_at'] = datetime.now().isoformat()
        df['season'] = 2026

        print("Column names:")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i}. {col}")
        print()

        print("Sample data (first 5 rows):")
        print(df.head(5).to_string())
        print()

        print("Sample data (last 5 rows):")
        print(df.tail(5).to_string())
        print()

        return df

    except requests.RequestException as e:
        print(f"✗ Request error: {e}")
        return None
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    print("=" * 70)
    print("NCAA WBB NET Rankings Scraper Test")
    print("=" * 70)
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()

    # Run the scraper
    df = scrape_ncaa_net_rankings()

    if df is not None:
        print("=" * 70)
        print("✅ SUCCESS!")
        print("=" * 70)
        print(f"Total teams: {len(df)}")
        print(f"Columns: {len(df.columns)}")

        # Save to file
        output_file = "data/net_rankings/ncaa_net_test.csv"
        import os
        os.makedirs("data/net_rankings", exist_ok=True)
        df.to_csv(output_file, index=False)
        print(f"✓ Saved to: {output_file}")
        print("=" * 70)
    else:
        print("=" * 70)
        print("✗ FAILED - No data retrieved")
        print("=" * 70)
