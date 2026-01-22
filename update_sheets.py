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
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path

# Configuration
DATA_DIR = Path("data")
WBB_TEAM_BOX_CSV = DATA_DIR / "wbb_team_box" / "wbb_teambox_filtered.csv"
SOS_RATINGS_CSV = DATA_DIR / "sos_data_weekly_run.csv"

# Google Sheets Configuration
# Ensure these match the Tab names in your Google Sheet
SHEET_ID = "1DAafyFpNlFRheOABdR4960L1VqP1gH-_0bMOG9BGD6Q" 
TABS = {
    "wbb_teambox": "wbb_teambox_filtered",
    "sos_ratings": "sos_data_weekly_run"
}

def get_gspread_client():
    """Authenticates and returns the gspread client."""
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    # This assumes you have set up a GitHub Secret named GCP_SERVICE_ACCOUNT
    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT")
    if not creds_json:
        raise ValueError("GCP_SERVICE_ACCOUNT environment variable not set")
    
    import json
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)

def find_new_rows(csv_df: pd.DataFrame, sheet_df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify rows in the CSV that don't exist in the Sheet.
    Uses all columns to determine uniqueness.
    """
    if sheet_df.empty:
        return csv_df
    
    # Standardize both to strings to avoid type-mismatch false positives
    csv_str = csv_df.astype(str)
    sheet_str = sheet_df.astype(str)
    
    # Create a boolean mask of rows in CSV that are NOT in the Sheet
    # This works by checking if the row values (as a tuple) exist in the sheet's set of tuples
    sheet_tuples = set(sheet_str.apply(tuple, axis=1))
    new_rows_mask = ~csv_str.apply(tuple, axis=1).isin(sheet_tuples)
    
    return csv_df[new_rows_mask]

def sync_csv_to_sheet(csv_path, sheet_id, tab_name, client):
    """
    Main logic to sync a CSV file to a Google Sheet tab.
    Corrects the indentation issues and handles schema changes.
    """
    if not csv_path.exists():
        print(f"Skipping {tab_name}: CSV file not found at {csv_path}")
        return

    print(f"Syncing {csv_path} to tab: {tab_name}")
    
    # 1. Load Local CSV
    csv_df = pd.read_csv(csv_path)
    
    # 2. Open Google Sheet
    sh = client.open_by_key(sheet_id)
    try:
        worksheet = sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"   Creating new worksheet: {tab_name}")
        worksheet = sh.add_worksheet(title=tab_name, rows="100", cols="20")

    # 3. Load Existing Data from Sheet
    existing_data = worksheet.get_all_records()
    sheet_df = pd.DataFrame(existing_data)

    # 4. Check for new columns (Schema Mismatch logic)
    new_cols = [col for col in csv_df.columns if col not in sheet_df.columns]
    
    if new_cols:
        print(f"   Detected {len(new_cols)} new columns. Updating Sheet header...")
        # Get all current headers and append the new ones
        combined_headers = list(sheet_df.columns) + new_cols
        worksheet.update('A1', [combined_headers])
        # Refresh sheet_df to include the new columns for comparison
        for col in new_cols:
            sheet_df[col] = None

    # 5. Identify only the new rows to append
    new_rows = find_new_rows(csv_df, sheet_df)
    
    if not new_rows.empty:
        # Convert NaN/NaT to empty strings for Google Sheets compatibility
        rows_to_upload = new_rows.fillna('').astype(str).values.tolist()
        worksheet.append_rows(rows_to_upload)
        print(f"   ✓ Successfully synced {len(new_rows)} new rows.")
    else:
        print("   → No new data to sync.")

def main():
    print("Starting Google Sheets Sync Process...")
    
    # Check if credentials exist before attempting to authenticate
    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT")
    
    if not creds_json:
        print("\n" + "!" * 60)
        print("NOTICE: GCP_SERVICE_ACCOUNT environment variable not set.")
        print("The script will complete the data processing steps but skip the Google Sheets sync.")
        print("!" * 60 + "\n")
        # Return gracefully instead of raising an error
        return

    try:
        client = get_gspread_client()
        
        # Sync Team Box Data
        sync_csv_to_sheet(WBB_TEAM_BOX_CSV, SHEET_ID, TABS["wbb_teambox"], client)
        
        # Sync Ratings Data
        sync_csv_to_sheet(SOS_RATINGS_CSV, SHEET_ID, TABS["sos_ratings"], client)
        
        print("Sync Process Complete.")
    except Exception as e:
        print(f"Error during sync: {e}")
        raise

if __name__ == "__main__":
    main()
