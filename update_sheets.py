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
        pd.DataFrame: Only the new rows that should be appended
    """
    if sheet_df.empty:
        print("   Sheet is empty - all CSV rows are new")
        return csv_df

    # Ensure both DataFrames have the same columns
    if not set(csv_df.columns).issubset(set(sheet_df.columns)):
        # CSV has columns that don't exist in Sheet
        # This might happen on first upload with new schema
        print("   Schema mismatch - CSV has new columns. Treating all rows as new.")
        return csv_df

    # Convert all columns to strings for comparison
    csv_str = csv_df.astype(str)
    sheet_str = sheet_df.astype(str)

    # Create a composite key from all columns for comparison
    # Create a composite key from all columns for comparison
    # Using a less common separator to avoid conflicts with cell content
    separator = "<|>"
    csv_keys = csv_str.apply(lambda row: separator.join(row.values), axis=1)
    sheet_keys = sheet_str.apply(lambda row: separator.join(row.values), axis=1)
    sheet_keys = sheet_str.apply(lambda row: '|||'.join(row.values), axis=1)

    # Find rows in CSV that are not in Sheet
    new_mask = ~csv_keys.isin(sheet_keys)
    new_rows = csv_df[new_mask]

    print(f"   Found {len(new_rows)} new rows to append (out of {len(csv_df)} total CSV rows)")
    return new_rows


def sync_csv_to_sheet(
    csv_path: Path,
    sheet_id: str,
    tab_name: str,
    client: Optional[gspread.Client] = None
) -> bool:
    """
    Sync a local CSV file to a Google Sheet with idempotency.

    This function:
    1. Reads the CSV file
    2. Reads existing data from the Google Sheet
    3. Identifies new rows not already in the Sheet
    4. Appends only the new rows to the Sheet

    Args:
        csv_path: Path to the local CSV file
        sheet_id: Google Sheets ID (from the URL)
        tab_name: Name of the worksheet/tab within the spreadsheet
        client: Optional gspread client (will create one if not provided)

    Returns:
        bool: True if sync was successful, False otherwise
    """
    try:
        print(f"\n{'='*60}")
        print(f"Syncing: {csv_path.name} → Sheet '{tab_name}'")
        print(f"{'='*60}")

        # Check if CSV exists
        if not csv_path.exists():
            print(f"✗ CSV file not found: {csv_path}")
            return False

        # Read CSV data
        print("1. Reading local CSV file...")
        csv_df = pd.read_csv(csv_path)
        print(f"   ✓ Loaded {len(csv_df)} rows from CSV")

        if csv_df.empty:
            print("   ⚠ CSV is empty - nothing to sync")
            return True

        # Get gspread client
        if client is None:
            print("2. Authenticating with Google Sheets...")
            client = get_gspread_client()
            print("   ✓ Authentication successful")

        # Open spreadsheet and worksheet
        print("3. Opening Google Sheet...")
        spreadsheet = client.open_by_key(sheet_id)

        # Try to get existing worksheet, or create it
        try:
            worksheet = spreadsheet.worksheet(tab_name)
            print(f"   ✓ Found existing tab '{tab_name}'")
        except gspread.exceptions.WorksheetNotFound:
            print(f"   ⚠ Tab '{tab_name}' not found - creating new tab")
            worksheet = spreadsheet.add_worksheet(
                title=tab_name,
                rows=1000,
                cols=len(csv_df.columns)
            )
            print(f"   ✓ Created new tab '{tab_name}'")

        # Read existing sheet data
        print("4. Reading existing data from sheet...")
        sheet_df = read_sheet_data(worksheet)

        # Find new rows
        print("5. Identifying new rows...")
        new_rows = find_new_rows(csv_df, sheet_df)

        if new_rows.empty:
            print("   ✓ No new rows to append - sheet is up to date")
            print(f"\n{'='*60}")
            return True

        # Append new rows to sheet
        print(f"6. Appending {len(new_rows)} new rows to sheet...")

        if sheet_df.empty:
            # Sheet is empty - write header and all data
            data_to_append = [new_rows.columns.tolist()] + new_rows.values.tolist()
            worksheet.update('A1', data_to_append)
            print(f"   ✓ Wrote header and {len(new_rows)} rows to empty sheet")
        else:
            # Sheet has data - append only new rows (no header)
            data_to_append = new_rows.values.tolist()
            worksheet.append_rows(data_to_append)
            print(f"   ✓ Appended {len(new_rows)} new rows")

        print(f"   ✓ Sync completed successfully")
        print(f"\n{'='*60}")
        return True

    except Exception as e:
        print(f"\n✗ Error syncing {csv_path.name}: {e}")
        import traceback
        traceback.print_exc()
        print(f"\n{'='*60}")
        return False


def main():
    """Main function to sync all data to Google Sheets."""
    print("\n" + "="*60)
    print("Google Sheets Update Script")
    print("="*60)

    success = True

    # Get a single client to reuse for all syncs
    try:
        print("\nAuthenticating with Google Sheets API...")
        client = get_gspread_client()
        print("✓ Authentication successful")
    except Exception as e:
        print(f"✗ Authentication failed: {e}")
        return

    # Sync SOS (ratings) data
    if SOS_MASTER_CSV.exists():
        result = sync_csv_to_sheet(
            csv_path=SOS_MASTER_CSV,
            sheet_id=SHEET_ID,
            tab_name=SOS_TAB_NAME,
            client=client
        )
        success = success and result
    else:
        print(f"\n⚠ Skipping SOS data - file not found: {SOS_MASTER_CSV}")

    # Future: Sync Polls data
    # Uncomment when ready to sync polls data
    # if POLLS_MASTER_CSV.exists():
    #     result = sync_csv_to_sheet(
    #         csv_path=POLLS_MASTER_CSV,
    #         sheet_id=SHEET_ID,
    #         tab_name=POLLS_TAB_NAME,
    #         client=client
    #     )
    #     success = success and result
    # else:
    #     print(f"\n⚠ Skipping Polls data - file not found: {POLLS_MASTER_CSV}")

    # Summary
    print("\n" + "="*60)
    if success:
        print("✅ All syncs completed successfully")
    else:
        print("❌ Some syncs failed - check logs above")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
