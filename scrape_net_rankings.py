#!/usr/bin/env python3
"""
Scrapes NCAA NET Rankings for Women's Basketball.

This script scrapes the NCAA's NET rankings page to get Top 50 team rankings,
which are used for filtering schedule data to relevant matchups.

MANUAL WORKAROUND:
    If scraping fails due to bot protection (403 errors), you can manually export:
    1. Visit: https://stats.ncaa.org/selection_rankings/nitty_gritties/48409
    2. Click the "Excel" button to download CSV
    3. Save as: data/net_rankings/net_rankings_manual_YYYYMMDD.csv
    4. Run: python scrape_net_rankings.py --manual data/net_rankings/net_rankings_manual_YYYYMMDD.csv

Output:
    - data/net_rankings/net_rankings_YYYYMMDD.csv (daily snapshot)
    - data/net_rankings/net_rankings_master.csv (cumulative history)

Usage:
    python scrape_net_rankings.py                  # Auto-scrape
    python scrape_net_rankings.py --manual FILE    # Process manual export
"""

import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Import centralized team name standardization
from team_name_utils import standardize_team_names as apply_team_name_standardization

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://stats.ncaa.org/selection_rankings/nitty_gritties/48409"
DATA_DIR = Path("data/net_rankings")
TABLE_ID = "selection_rankings_nitty_gritty_data_table"  # From browser inspection

# Enhanced headers to avoid bot detection
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Cache-Control': 'max-age=0',
    'Referer': 'https://www.ncaa.org/',
}


def scrape_net_rankings(retry_count: int = 3) -> Optional[pd.DataFrame]:
    """
    Scrapes NET rankings from NCAA stats page with retry logic.

    Args:
        retry_count: Number of retries on failure

    Returns:
        DataFrame with columns: net_rank, team, conference, or None if scraping fails
    """
    for attempt in range(retry_count):
        try:
            if attempt > 0:
                delay = 2 ** attempt  # Exponential backoff: 2s, 4s, 8s
                logger.info(f"Retry attempt {attempt + 1}/{retry_count} after {delay}s delay...")
                time.sleep(delay)

            logger.info(f"Fetching NET rankings from {BASE_URL}")

            # Create session with cookies
            session = requests.Session()

            # Make request
            response = session.get(BASE_URL, headers=HEADERS, timeout=30, allow_redirects=True)

            # Check for bot protection
            if response.status_code == 403:
                logger.warning(f"Received 403 Forbidden (bot protection detected)")
                if attempt < retry_count - 1:
                    continue
                else:
                    logger.error("All retry attempts exhausted. See manual workaround in script header.")
                    return None

            response.raise_for_status()
            logger.info(f"Successfully fetched page (status code: {response.status_code})")

            # Parse HTML
            soup = BeautifulSoup(response.content, 'html.parser')

            # Find the specific rankings table by ID (from browser inspection)
            table = soup.find('table', {'id': TABLE_ID})

            if not table:
                # Fallback: find any table with "dataTable" class
                table = soup.find('table', class_='dataTable')

            if not table:
                # Last resort: find first table
                table = soup.find('table')

            if not table:
                logger.error("Could not find rankings table on page")
                logger.debug(f"Page content preview: {soup.get_text()[:500]}")
                if attempt < retry_count - 1:
                    continue
                return None

            # Try to extract table data using pandas (most robust)
            try:
                tables = pd.read_html(str(table))
                if not tables:
                    logger.error("pandas.read_html found no tables")
                    if attempt < retry_count - 1:
                        continue
                    return None

                df = tables[0]
                logger.info(f"Successfully parsed table with shape: {df.shape}")
                logger.debug(f"Columns: {df.columns.tolist()}")

            except Exception as e:
                logger.error(f"Failed to parse table with pandas: {e}")
                if attempt < retry_count - 1:
                    continue
                return None

            # Standardize column names
            df = standardize_columns(df)

            # Add metadata
            df['run_date'] = datetime.now().strftime('%Y-%m-%d')

            return df

        except requests.RequestException as e:
            logger.error(f"Network error fetching NET rankings: {e}")
            if attempt < retry_count - 1:
                continue
            return None
        except Exception as e:
            logger.error(f"Unexpected error scraping NET rankings: {e}", exc_info=True)
            if attempt < retry_count - 1:
                continue
            return None

    logger.error("Failed to scrape NET rankings after all retries")
    return None


def load_manual_export(csv_path: Path) -> Optional[pd.DataFrame]:
    """
    Loads NET rankings from a manually exported CSV file.

    Args:
        csv_path: Path to manually exported CSV from NCAA site

    Returns:
        DataFrame with standardized columns
    """
    try:
        logger.info(f"Loading manual export from {csv_path}")

        if not csv_path.exists():
            logger.error(f"File not found: {csv_path}")
            return None

        # Read CSV/TSV with auto-detected separator
        df = pd.read_csv(csv_path, sep=None, engine='python')
        logger.info(f"Loaded {len(df)} rows from manual export")

        # Standardize column names
        df = standardize_columns(df)

        # Add metadata
        df['run_date'] = datetime.now().strftime('%Y-%m-%d')

        return df

    except Exception as e:
        logger.error(f"Error loading manual export: {e}")
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


# W-L string fields to parse into numeric wins/losses/winpct columns.
# Phase 1: fields needed now for Tableau visuals (Quadrant Heatmap, SOS scatter)
PHASE_1_WL_FIELDS = {
    'DivIWL':  'DivI',
    'Q1':      'Q1',
    'Q2':      'Q2',
    'Q3':      'Q3',
    'Q4':      'Q4',
}

# Phase 2: additional fields — added after Phase 1 is validated
PHASE_2_WL_FIELDS = {
    'ConfRecord':  'Conf',
    'RoadWL':      'Road',
    'Last10Games': 'Last10',
}


def preprocess_wl_fields(df: pd.DataFrame, wl_fields: dict) -> pd.DataFrame:
    """
    Parses W-L string columns (e.g. '21-0') into numeric wins, losses, and win_pct columns.

    For each entry in wl_fields (source_column -> prefix), adds:
        {prefix}_wins      int
        {prefix}_losses    int
        {prefix}_winpct    float, rounded to 3 decimals

    Columns that don't exist in df are silently skipped.

    Args:
        df: DataFrame containing W-L string columns
        wl_fields: Dict mapping source column name -> output prefix

    Returns:
        DataFrame with new numeric columns appended
    """
    for col, prefix in wl_fields.items():
        if col not in df.columns:
            logger.warning(f"preprocess_wl_fields: column '{col}' not found, skipping")
            continue

        split = df[col].astype(str).str.split('-', expand=True, n=1)
        wins   = pd.to_numeric(split[0], errors='coerce').astype('Int64')
        losses = pd.to_numeric(split[1], errors='coerce').astype('Int64')
        total  = wins + losses

        df[f'{prefix}_wins']   = wins
        df[f'{prefix}_losses'] = losses
        df[f'{prefix}_winpct'] = (wins / total).round(3)

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


def main(manual_file: Optional[str] = None):
    """
    Main execution function.

    Args:
        manual_file: Optional path to manually exported CSV file
    """
    logger.info("Starting NET rankings scraper")

    # Load data (either scrape or from manual file)
    if manual_file:
        logger.info(f"Using manual export mode with file: {manual_file}")
        df = load_manual_export(Path(manual_file))
    else:
        logger.info("Using auto-scrape mode")
        df = scrape_net_rankings()

    if df is None or df.empty:
        logger.error("Failed to obtain NET rankings data")
        logger.info("\n" + "="*60)
        logger.info("MANUAL WORKAROUND:")
        logger.info("1. Visit: https://stats.ncaa.org/selection_rankings/nitty_gritties/48409")
        logger.info("2. Click the 'Excel' button to download CSV")
        logger.info("3. Save as: data/net_rankings/net_rankings_manual_YYYYMMDD.csv")
        logger.info("4. Run: python scrape_net_rankings.py --manual <path-to-csv>")
        logger.info("="*60 + "\n")
        return False

    logger.info(f"Successfully obtained {len(df)} teams")

    # Standardize team names using centralized function
    df = apply_team_name_standardization(df, ['team'])

    # Pre-process W-L string fields into numeric columns for Tableau
    df = preprocess_wl_fields(df, {**PHASE_1_WL_FIELDS, **PHASE_2_WL_FIELDS})

    # Save data
    if save_data(df):
        logger.info("NET rankings scraper completed successfully")

        # Display top 10 for verification
        if 'net_rank' in df.columns and 'team' in df.columns:
            logger.info("\nTop 10 NET Rankings:")
            # Convert to numeric for sorting
            df['net_rank_numeric'] = pd.to_numeric(df['net_rank'], errors='coerce')
            top10 = df.nsmallest(10, 'net_rank_numeric')[['net_rank', 'team']]
            for _, row in top10.iterrows():
                logger.info(f"  {row['net_rank']}. {row['team']}")

        return True
    else:
        logger.error("Failed to save NET rankings data")
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Scrape NCAA NET Rankings')
    parser.add_argument('--manual', type=str, help='Path to manually exported CSV file')

    args = parser.parse_args()

    success = main(manual_file=args.manual)
    sys.exit(0 if success else 1)
