# Google Sheets Integration Setup Guide

This guide explains how to set up Google Cloud Service Account authentication for the `update_sheets.py` script.

## Overview

The `update_sheets.py` script uses a Google Cloud Service Account to authenticate and update Google Sheets. This is the recommended approach for automated scripts and GitHub Actions.

---

## Part 1: Create a Google Cloud Service Account

### Step 1: Create a Google Cloud Project

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Sign in with your Google account
3. Click on the project dropdown at the top (or "Select a project")
4. Click **"New Project"**
5. Enter a project name (e.g., `wbb-data-automation`)
6. Click **"Create"**
7. Wait for the project to be created, then select it

### Step 2: Enable Required APIs

1. In the Google Cloud Console, go to **"APIs & Services"** → **"Library"**
2. Search for and enable the following APIs:
   - **Google Sheets API** - Click "Enable"
   - **Google Drive API** - Click "Enable"

### Step 3: Create a Service Account

1. Go to **"APIs & Services"** → **"Credentials"**
2. Click **"Create Credentials"** → **"Service Account"**
3. Fill in the service account details:
   - **Service account name**: `sheets-updater` (or any name you prefer)
   - **Service account ID**: This will auto-generate (e.g., `sheets-updater@wbb-data-automation.iam.gserviceaccount.com`)
   - **Description**: "Service account for updating Google Sheets with scraped data"
4. Click **"Create and Continue"**
5. For "Grant this service account access to project":
   - You can skip this step or select "Editor" role (optional)
   - Click **"Continue"**
6. For "Grant users access to this service account":
   - Skip this step
   - Click **"Done"**

### Step 4: Create and Download the JSON Key

1. On the **Credentials** page, find your newly created service account in the "Service Accounts" section
2. Click on the service account email (e.g., `sheets-updater@wbb-data-automation.iam.gserviceaccount.com`)
3. Go to the **"Keys"** tab
4. Click **"Add Key"** → **"Create new key"**
5. Select **"JSON"** as the key type
6. Click **"Create"**
7. A JSON file will be downloaded to your computer (e.g., `wbb-data-automation-a1b2c3d4e5f6.json`)
8. **⚠️ IMPORTANT**: Keep this file secure! It contains credentials that give access to your Google Cloud resources.

The JSON file will look something like this:
```json
{
  "type": "service_account",
  "project_id": "wbb-data-automation",
  "private_key_id": "a1b2c3d4e5f6...",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  "client_email": "sheets-updater@wbb-data-automation.iam.gserviceaccount.com",
  "client_id": "123456789...",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  ...
}
```

---

## Part 2: Share the Google Sheet with the Service Account

### Step 1: Get the Service Account Email

From the JSON key file you downloaded, find the `client_email` field. It will look like:
```
sheets-updater@wbb-data-automation.iam.gserviceaccount.com
```

### Step 2: Share the Google Sheet

1. Open your Google Sheet: `https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit`
2. Click the **"Share"** button in the top-right corner
3. In the "Add people and groups" field, paste the service account email
   - Example: `sheets-updater@wbb-data-automation.iam.gserviceaccount.com`
4. Set the permission level to **"Editor"**
5. **UNCHECK** "Notify people" (the service account is not a real person)
6. Click **"Share"** or **"Send"**

✅ The service account now has permission to read and write to this Google Sheet!

---

## Part 3: Add the JSON Key to GitHub Secrets

### Step 1: Copy the JSON Content

1. Open the downloaded JSON key file in a text editor
2. Copy the **entire contents** of the file (all the JSON)

### Step 2: Add to GitHub Secrets

1. Go to your GitHub repository: `https://github.com/YOUR_ORG/YOUR_REPO`
2. Click **"Settings"** (in the repository menu)
3. In the left sidebar, click **"Secrets and variables"** → **"Actions"**
4. Click **"New repository secret"**
5. Fill in the secret details:
   - **Name**: `GCP_SERVICE_ACCOUNT`
   - **Value**: Paste the entire JSON content you copied
6. Click **"Add secret"**

✅ The secret is now available to your GitHub Actions workflows!

---

## Part 4: Testing the Setup

### Local Testing (Optional)

To test the script locally before running in GitHub Actions:

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set the environment variable with your JSON key:
   ```bash
   # Linux/Mac
   export GCP_SERVICE_ACCOUNT='{"type": "service_account", "project_id": "your-project", ...}'

   # Or load from file
   export GCP_SERVICE_ACCOUNT=$(cat path/to/your-key.json)

   # Windows (PowerShell)
   $env:GCP_SERVICE_ACCOUNT = Get-Content path\to\your-key.json -Raw
   ```

3. Run the script:
   ```bash
   python update_sheets.py
   ```

### GitHub Actions Usage

In your GitHub Actions workflow, the script will automatically use the `GCP_SERVICE_ACCOUNT` secret:

```yaml
- name: Update Google Sheets
  env:
    GCP_SERVICE_ACCOUNT: ${{ secrets.GCP_SERVICE_ACCOUNT }}
  run: |
    python update_sheets.py
```

---

## Troubleshooting

### Error: "GCP_SERVICE_ACCOUNT environment variable not set"
- Make sure you've added the secret to GitHub Secrets (Part 3)
- Make sure your workflow is passing the secret as an environment variable

### Error: "Insufficient permissions" or "403 Forbidden"
- Make sure you've shared the Google Sheet with the service account email (Part 2)
- Make sure you gave the service account "Editor" permissions

### Error: "Worksheet not found"
- The script will automatically create the tab if it doesn't exist
- Make sure the spreadsheet ID in `update_sheets.py` matches your sheet

### Error: "API not enabled"
- Go back to Google Cloud Console and enable both Google Sheets API and Google Drive API (Part 1, Step 2)

---

## Security Best Practices

1. **Never commit the JSON key file to Git** - It's already in `.gitignore`
2. **Use GitHub Secrets** for the service account credentials - Never hardcode them
3. **Rotate keys periodically** - Create new keys and delete old ones from Google Cloud Console
4. **Limit permissions** - Only share the specific Google Sheets that need access
5. **Monitor usage** - Check the Google Cloud Console for unexpected API usage

---

## Adding More Sheets

To sync additional data (like Polls data), edit `update_sheets.py`:

```python
# Uncomment in the main() function:
if POLLS_MASTER_CSV.exists():
    result = sync_csv_to_sheet(
        csv_path=POLLS_MASTER_CSV,
        sheet_id=SHEET_ID,
        tab_name=POLLS_TAB_NAME,
        client=client
    )
    success = success and result
```

Or create a new tab by calling:
```python
sync_csv_to_sheet(
    csv_path=Path("data/your_data.csv"),
    sheet_id=SHEET_ID,
    tab_name="your_new_tab_name",
    client=client
)
```

---

## Summary Checklist

- [ ] Created Google Cloud Project
- [ ] Enabled Google Sheets API and Google Drive API
- [ ] Created Service Account
- [ ] Downloaded JSON key file
- [ ] Shared Google Sheet with service account email (as Editor)
- [ ] Added JSON key to GitHub Secrets as `GCP_SERVICE_ACCOUNT`
- [ ] Tested the script (locally or in GitHub Actions)

---

If you have any issues, check the [gspread documentation](https://docs.gspread.org/) or the [Google Sheets API documentation](https://developers.google.com/sheets/api).
