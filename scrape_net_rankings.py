#!/usr/bin/env python3
"""
Scrapes NCAA NET Rankings for Women's Basketball.

This script scrapes the NCAA's NET rankings page to get Top 50 team rankings,
which are used for filtering schedule data to relevant matchups.

Output:
    - data/net_rankings/net_rankings_YYYYMMDD.csv (daily snapshot)
    - data/net_rankings/net_rankings_master.csv (cumulative history)

Usage:
    python scrape_net_rankings.py
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://stats.ncaa.org/selection_rankings/nitty_gritties/48409"
DATA_DIR = Path("data/net_rankings")
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
}


def scrape_net_rankings() -> Optional[pd.DataFrame]:
    """
    Scrapes NET rankings from NCAA stats page.

    Returns:
        DataFrame with columns: net_rank, team, conference, record, or None if scraping fails
    """
    try:
        logger.info(f"Fetching NET rankings from {BASE_URL}")

        # Create session for better handling of cookies/redirects
        session = requests.Session()
        response = session.get(BASE_URL, headers=HEADERS, timeout=30)
        response.raise_for_status()

        logger.info(f"Successfully fetched page (status code: {response.status_code})")

        # Parse HTML
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the rankings table
        # NCAA typically uses tables with specific classes or IDs
        # Common patterns: class="mytable", id="rankings_table", or just the first large table
        table = soup.find('table')

        if not table:
            logger.error("Could not find rankings table on page")
            logger.debug(f"Page content preview: {soup.get_text()[:500]}")
            return None

        # Try to extract table data
        # Method 1: Use pandas read_html (most robust)
        try:
            tables = pd.read_html(str(table))
            if not tables:
                logger.error("pandas.read_html found no tables")
                return None

            df = tables[0]
            logger.info(f"Successfully parsed table with shape: {df.shape}")
            logger.debug(f"Columns: {df.columns.tolist()}")

        except Exception as e:
            logger.error(f"Failed to parse table with pandas: {e}")
            logger.info("Attempting manual parsing...")

            # Method 2: Manual parsing as fallback
            rows = []
            for tr in table.find_all('tr')[1:]:  # Skip header
                cells = tr.find_all(['td', 'th'])
                if cells:
                    row_data = [cell.get_text(strip=True) for cell in cells]
                    rows.append(row_data)

            if not rows:
                logger.error("Manual parsing found no data rows")
                return None

            # Get headers
            header_row = table.find('tr')
            headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]

            df = pd.DataFrame(rows, columns=headers if headers else None)
            logger.info(f"Manual parsing successful, shape: {df.shape}")

        # Standardize column names
        # Common NCAA column names: "Rank", "Team", "Conference", "Record", "W-L"
        df = standardize_columns(df)

        # Add metadata
        df['run_date'] = datetime.now().strftime('%Y-%m-%d')

        return df

    except requests.RequestException as e:
        logger.error(f"Network error fetching NET rankings: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error scraping NET rankings: {e}", exc_info=True)
        return None


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes column names from NCAA table to consistent format.

    Args:
        df: Raw DataFrame from NCAA site

    Returns:
        DataFrame with standardized column names
    """
    # Map common NCAA column variations to standard names
    column_mapping = {
        'Rank': 'net_rank',
        'NET Rank': 'net_rank',
        'RK': 'net_rank',
        'Team': 'team',
        'Institution': 'team',
        'School': 'team',
        'Conference': 'conference',
        'Conf': 'conference',
        'Record': 'record',
        'W-L': 'record',
        'Overall Record': 'record',
    }

    # Rename columns if they match known patterns
    df.columns = [column_mapping.get(col, col) for col in df.columns]

    # Ensure required columns exist
    required_cols = ['net_rank', 'team']
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        logger.warning(f"Missing required columns: {missing_cols}")
        logger.warning(f"Available columns: {df.columns.tolist()}")

    return df


def standardize_team_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize team names to match polls dataset conventions.

    Args:
        df: DataFrame with 'team' column

    Returns:
        DataFrame with standardized team names
    """
    TEAM_NAME_MAPPINGS = {
        'Connecticut': 'UConn',
        'Louisiana State': 'LSU',
        'Southern California': 'USC',
        'Mississippi': 'Ole Miss',
        'North Carolina': 'UNC',
    }

    if 'team' in df.columns:
        df['team'] = df['team'].replace(TEAM_NAME_MAPPINGS)

    return df


def save_data(df: pd.DataFrame) -> bool:
    """
    Saves NET rankings data to both snapshot and master files.

    Args:
        df: DataFrame containing NET rankings data

    Returns:
        True if save successful, False otherwise
    """
    try:
        # Create data directory if it doesn't exist
        DATA_DIR.mkdir(parents=True, exist_ok=True)

        # Generate snapshot filename with date
        today = datetime.now().strftime('%Y%m%d')
        snapshot_path = DATA_DIR / f"net_rankings_{today}.csv"
        master_path = DATA_DIR / "net_rankings_master.csv"

        # Save snapshot
        df.to_csv(snapshot_path, index=False)
        logger.info(f"Saved snapshot to {snapshot_path}")

        # Update master file (append if exists, otherwise create)
        if master_path.exists():
            existing_df = pd.read_csv(master_path)

            # Check for duplicates (same date)
            if 'run_date' in existing_df.columns:
                existing_dates = existing_df['run_date'].unique()
                new_date = df['run_date'].iloc[0]

                if new_date in existing_dates:
                    logger.info(f"Data for {new_date} already exists in master file - skipping append")
                    logger.info(f"To force update, delete rows with run_date={new_date} from {master_path}")
                    return True

            # Append new data
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df.to_csv(master_path, index=False)
            logger.info(f"Appended {len(df)} rows to {master_path} (total: {len(combined_df)} rows)")
        else:
            df.to_csv(master_path, index=False)
            logger.info(f"Created new master file at {master_path}")

        return True

    except Exception as e:
        logger.error(f"Error saving data: {e}")
        return False


def main():
    """Main execution function."""
    logger.info("Starting NET rankings scraper")

    # Scrape data
    df = scrape_net_rankings()

    if df is None or df.empty:
        logger.error("Failed to scrape NET rankings data")
        return False

    logger.info(f"Successfully scraped {len(df)} teams")

    # Standardize team names
    df = standardize_team_names(df)

    # Save data
    if save_data(df):
        logger.info("NET rankings scraper completed successfully")

        # Display top 10 for verification
        if 'net_rank' in df.columns and 'team' in df.columns:
            logger.info("\nTop 10 NET Rankings:")
            top10 = df.nsmallest(10, 'net_rank')[['net_rank', 'team']]
            for _, row in top10.iterrows():
                logger.info(f"  {row['net_rank']}. {row['team']}")

        return True
    else:
        logger.error("Failed to save NET rankings data")
        return False


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)
