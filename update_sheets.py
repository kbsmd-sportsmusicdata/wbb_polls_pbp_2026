"""
Update Google Sheets with scraped data.

This script syncs local CSV files to Google Sheets with idempotency:
- Only appends new rows that don't already exist in the Sheet
- Handles empty sheets by uploading all data
- Uses Service Account authentication for GitHub Actions

Usage:
    python update_sheets.py
"""
import os
import json
from pathlib import Path
from typing import Optional

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# Google Sheets API scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Configuration
DATA_DIR = Path("data")
SOS_MASTER_CSV = DATA_DIR / "sos" / "ratings_master.csv"
POLLS_MASTER_CSV = DATA_DIR / "polls_long.csv"

# Google Sheet configuration
SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "1DAafyFpNlFRheOABdR4960L1VqP1gH-_0bMOG9BGD6Q") # Replace with your sheet ID or load from env
SOS_TAB_NAME = "sos_data_weekly_run"
POLLS_TAB_NAME = "polls_data_weekly_run"  # Future use


def get_gspread_client() -> gspread.Client:
    """
    Authenticate and return a gspread client using Service Account credentials.

    Reads credentials from the GCP_SERVICE_ACCOUNT environment variable,
    which should contain the full JSON key as a string.

    Returns:
        gspread.Client: Authenticated gspread client

    Raises:
        ValueError: If GCP_SERVICE_ACCOUNT environment variable is not set
        json.JSONDecodeError: If the credentials JSON is malformed
    """
    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT")

    if not creds_json:
        raise ValueError(
            "GCP_SERVICE_ACCOUNT environment variable not set. "
            "Please set it to your Service Account JSON key."
        )

    # Parse JSON credentials from environment variable
    creds_dict = json.loads(creds_json)

    # Create credentials object
    credentials = Credentials.from_service_account_info(
        creds_dict,
        scopes=SCOPES
    )

    # Authorize and return client
    client = gspread.authorize(credentials)
    return client


def read_sheet_data(worksheet: gspread.Worksheet) -> pd.DataFrame:
    """
    Read all data from a Google Sheet worksheet into a DataFrame.

    Args:
        worksheet: gspread Worksheet object

    Returns:
        pd.DataFrame: Data from the worksheet, or empty DataFrame if sheet is empty
    """
    try:
        # Get all values from the sheet
        data = worksheet.get_all_values()

        if not data or len(data) == 0:
            print("   Sheet is empty")
            return pd.DataFrame()

        if len(data) == 1:
            # Only header row exists
            print("   Sheet has header but no data rows")
            return pd.DataFrame(columns=data[0])

        # Convert to DataFrame (first row is header)
        df = pd.DataFrame(data[1:], columns=data[0])

        # Strip whitespace from all string columns
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.strip()

        print(f"   Read {len(df)} existing rows from sheet")
        return df

    except Exception as e:
        print(f"   Error reading sheet data: {e}")
        return pd.DataFrame()


def find_new_rows(csv_df: pd.DataFrame, sheet_df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify rows in the CSV that don't exist in the Sheet.

    Uses all columns to determine uniqueness. A row is considered new if
    the exact combination of all column values doesn't exist in the sheet.

    Args:
        csv_df: DataFrame from local CSV
        sheet_df: DataFrame from Google Sheet

    Returns:
# ... (Keep your find_new_rows function above here) ...

def sync_csv_to_sheet(csv_path, sheet_id, tab_name, client):
    """
    Main logic to sync a CSV file to a Google Sheet tab.
    """
    print(f"Syncing {csv_path} to tab: {tab_name}")
    
    # 1. Load Local CSV
    csv_df = pd.read_csv(csv_path)
    
    # 2. Open Google Sheet
    sh = client.open_by_key(sheet_id)
    worksheet = sh.worksheet(tab_name)
    
    # 3. Load Existing Data from Sheet
    existing_data = worksheet.get_all_records()
    sheet_df = pd.DataFrame(existing_data)

    # 4. Check for new columns (Schema Mismatch logic)
    new_cols = [col for col in csv_df.columns if col not in sheet_df.columns]
    
    if new_cols:
        print(f"   Detected {len(new_cols)} new columns. Updating Sheet header...")
        combined_headers = list(sheet_df.columns) + new_cols
        worksheet.update('A1', [combined_headers])
        # Refresh sheet_df structure to include new columns
        for col in new_cols:
            sheet_df[col] = None

    # 5. Identify only the new rows to append
    # Note: Ensure find_new_rows is defined earlier in your script
    new_rows = find_new_rows(csv_df, sheet_df)
    
    if not new_rows.empty:
        # Convert NaN to empty strings for Google Sheets compatibility
        rows_to_upload = new_rows.fillna('').values.tolist()
        worksheet.append_rows(rows_to_upload)
        print(f"   ✓ Successfully synced {len(new_rows)} new rows.")
    else:
        print("   → No new data to sync.")      
