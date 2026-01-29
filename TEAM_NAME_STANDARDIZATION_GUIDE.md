# Team Name Standardization Guide

## Overview

All scripts now use a **centralized team name standardization system** to ensure consistency across the entire data pipeline.

## Single Source of Truth

**File:** `data/raw/team_name_standardization.csv`

```csv
source_name,canonical_name
Connecticut,UConn
Louisiana State,LSU
Southern California,USC
Mississippi,Ole Miss
North Carolina,UNC
```

## How It Works

### 1. Centralized Utility Module

**File:** `team_name_utils.py`

- Loads mappings from CSV file
- Provides `standardize_team_names(df, columns)` function
- Used by all scrapers and processors

### 2. Scripts Updated

All scripts that process team names now use centralized standardization:

#### **scrape_net_rankings.py**
```python
from team_name_utils import TEAM_NAME_MAPPINGS
# Uses TEAM_NAME_MAPPINGS for standardization
```

#### **scrape_ratings.py**
```python
from team_name_utils import standardize_team_names as apply_team_name_standardization
# Standardizes 'School' column
```

#### **scrape_schedule.py**
```python
from team_name_utils import standardize_team_names
# Standardizes 'home_location' and 'away_location' columns
```

## Key Fixes Implemented

### 1. SOS Path Fix (scrape_schedule.py)

**Before:**
```python
SOS_RATINGS_CSV = DATA_DIR / "sos_data_weekly_run.csv"  # ❌ Wrong path
```

**After:**
```python
sos_master_csv = SOS_DIR / "ratings_master.csv"  # ✅ Correct path
```

**Result:** No more "SOS file not found" warning

### 2. Schedule Column Fix (scrape_schedule.py)

**Before:**
```python
df = clean_and_rename_teams(df, ['home_name', 'away_name'])  # ❌ Wrong columns
```

**After:**
```python
df = clean_and_rename_teams(df, ['home_location', 'away_location'])  # ✅ Correct columns
```

**Why:**
- `home_name/away_name` = mascots (Trojans, Bruins)
- `home_location/away_location` = school names (USC, UCLA)

### 3. NET Rankings Integration

**Manual Import Tested:**
```bash
python scrape_net_rankings.py --manual data/net_rankings/net_rankings_manual_20260129.csv
```

**Results:**
- ✅ 15 teams imported
- ✅ Team names standardized (Connecticut→UConn, Louisiana State→LSU, etc.)
- ✅ Saved to `data/net_rankings/net_rankings_master.csv`
- ✅ Filter schedule now includes NET rankings teams

## Impact on Data Pipeline

### Before Standardization
- **Polls:** UConn, LSU, USC
- **NET Rankings:** Connecticut, Louisiana State, Southern California
- **Schedule:** USC, LSU (already short names)
- **Ratings:** Connecticut, Louisiana State, Southern California

❌ **Problem:** Names don't match across datasets

### After Standardization
- **All sources:** UConn, LSU, USC, Ole Miss, UNC

✅ **Solution:** Consistent names everywhere

## Testing Results

### NET Rankings Import
```
Top 10 NET Rankings:
  1. South Carolina
  2. UConn          ← Standardized from "Connecticut"
  3. USC            ← Standardized from "Southern California"
  4. UCLA
  5. LSU            ← Standardized from "Louisiana State"
  6. Texas
  7. Maryland
  8. Notre Dame
  9. Duke
 10. Ohio State
```

### Schedule Filter Output
- **Before NET rankings:** 165 rows, 25 teams
- **After NET rankings:** 177 rows, 27 teams
- **Additional teams:** Georgia, Oklahoma (from NET Top 50)

### All Datasets Show Standardized Names
```python
# Example teams across all datasets:
- UConn (not Connecticut)
- LSU (not Louisiana State)
- USC (not Southern California)
- Ole Miss (not Mississippi)
- UNC (not North Carolina)
```

## Adding New Mappings

To add new team name variations:

1. **Edit CSV file:**
   ```bash
   # Add new row to data/raw/team_name_standardization.csv
   Brigham Young,BYU
   ```

2. **Mappings auto-load:**
   - All scripts use `team_name_utils.py`
   - No code changes needed
   - Just restart scripts

## Workflow Testing Checklist

When you manually run the GitHub Actions workflow:

### ✅ Expected Results:

1. **All scrapers run successfully**
   - sportsref_scraper.py ✓
   - scrape_ratings.py ✓
   - scrape_schedule.py ✓
   - scrape_net_rankings.py (may fail if no manual data - OK!)
   - filter_schedule.py ✓

2. **No "SOS file not found" warning**
   - Script finds `data/sos/ratings_master.csv`

3. **Filtered schedule includes NET teams**
   - Should show 25-50 teams (depending on NET rankings availability)
   - Should show 150-400+ game records

4. **All team names standardized**
   - Check `data/polls_analytics.csv` → UConn, LSU, USC
   - Check `data/net_rankings/net_rankings_master.csv` → UConn, LSU, USC
   - Check `data/sos/ratings_master.csv` → UConn, LSU, USC
   - Check `data/wbb_schedule/schedule_filtered.csv` → UConn, LSU, USC

5. **Data committed and pushed**
   - All updated files in `data/` directory

## Troubleshooting

### Issue: Names still not matching
**Solution:** Check which column is being standardized
- Schedule: Should standardize `home_location`, `away_location`
- Ratings: Should standardize `School`
- NET Rankings: Should standardize `Team` or `team`

### Issue: New variation not standardized
**Solution:** Add to `data/raw/team_name_standardization.csv`

### Issue: Script can't find team_name_utils.py
**Solution:** Make sure you're running from project root directory

## Files Modified

```
✅ Created:
- data/raw/team_name_standardization.csv
- team_name_utils.py
- data/net_rankings/net_rankings_master.csv (from test)

✅ Updated:
- scrape_net_rankings.py
- scrape_ratings.py
- scrape_schedule.py
- filter_schedule.py
- data/wbb_schedule/schedule_filtered.csv

✅ No changes needed:
- sportsref_scraper.py (polls already use canonical names)
- generate_analytics_table.py (uses already-standardized data)
```

## Next Steps

1. **Replace sample NET rankings data** with actual manual export from NCAA
2. **Run GitHub Actions workflow** to test end-to-end
3. **Verify Tableau connection** with standardized names
4. **Weekly:** Export NET rankings manually and import

Ready for workflow testing! 🚀
