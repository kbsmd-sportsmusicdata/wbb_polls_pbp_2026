# NCAA NET Rankings Scraper

## Overview

The NET rankings scraper (`scrape_net_rankings.py`) is designed to automatically fetch NCAA Women's Basketball NET rankings, which are used alongside AP/Coaches polls to identify Top 50 teams for schedule filtering.

**Source:** https://stats.ncaa.org/selection_rankings/nitty_gritties/48409

## Current Status: Bot Protection

The NCAA stats site has bot protection (403 Forbidden errors) that prevents automated scraping from certain environments. The scraper includes:
- ✅ Retry logic with exponential backoff
- ✅ Enhanced headers to mimic real browsers
- ✅ Multiple table detection strategies
- ⚠️ May still be blocked by firewall/proxy settings

## Usage Options

### Option 1: Auto-Scrape (Preferred)
Run directly to attempt automatic scraping:
```bash
python scrape_net_rankings.py
```

**When it works:**
- Saves to `data/net_rankings/net_rankings_YYYYMMDD.csv` (snapshot)
- Updates `data/net_rankings/net_rankings_master.csv` (cumulative)
- Works from local machines without strict firewall rules
- May work in GitHub Actions (depends on runner network)

### Option 2: Manual Export (Fallback)

If auto-scraping fails due to bot protection:

1. **Visit NCAA Site:**
   - Go to: https://stats.ncaa.org/selection_rankings/nitty_gritties/48409
   - Wait for table to load (shows ~363 teams)

2. **Export Data:**
   - Click the **"Excel"** button (top right of table)
   - OR click **"Copy"** and paste into Excel/Google Sheets
   - Save as CSV

3. **Save File:**
   ```bash
   # Save as (use current date):
   data/net_rankings/net_rankings_manual_20260129.csv
   ```

4. **Import:**
   ```bash
   python scrape_net_rankings.py --manual data/net_rankings/net_rankings_manual_20260129.csv
   ```

The script will:
- Read the manual CSV
- Standardize column names
- Add to master file
- Display Top 10 for verification

## Data Format

### Expected Columns (from NCAA site):
- **NET Rank** → net_rank (numeric ranking)
- **Team** → team (school name)
- **Conference** → conference
- Plus additional columns: Prev NET, Avg Opp NET, Records, SOS, etc.

### Output Format:
```csv
net_rank,team,conference,run_date
1,South Carolina,SEC,2026-01-29
2,UConn,Big East,2026-01-29
3,USC,Big Ten,2026-01-29
...
```

### Team Name Standardization

The script automatically standardizes team names to match polls:
- Connecticut → UConn
- Louisiana State → LSU
- Southern California → USC
- Mississippi → Ole Miss
- North Carolina → UNC

## Integration with Schedule Filter

The `filter_schedule.py` script uses NET rankings to identify Top 50 teams:

**Data Sources (combined):**
1. **NET Rankings:** Top 50 teams by NET rank
2. **Current Polls:** All ranked teams (1-25)

**Fallback Behavior:**
- If NET rankings unavailable → Uses only polls data (25 teams)
- Still functional, just narrower filter
- Consider running manual export weekly to maintain Top 50 coverage

## Automated Workflow

The GitHub Actions workflow (`manual_and_sched.yml`) includes NET scraper:

```yaml
- name: Run NET rankings scraper
  run: python scrape_net_rankings.py || echo "NET rankings scraper failed (may require manual export)"
  continue-on-error: true
```

**Workflow behavior:**
- Attempts auto-scrape on every run (Tues/Sat 9 AM PST)
- Continues even if scraper fails (won't block other tasks)
- You'll see warning in logs if it fails
- Schedule filter will use polls-only mode

## Troubleshooting

### Error: 403 Forbidden
**Cause:** Bot protection blocking requests
**Solution:** Use manual export option (Option 2 above)

### Error: Table not found
**Cause:** Page structure changed
**Solution:**
1. Check if table ID is still `selection_rankings_nitty_gritty_data_table`
2. Update `TABLE_ID` constant in `scrape_net_rankings.py`
3. Or use manual export

### Error: Columns not matching
**Cause:** NCAA changed column names
**Solution:** Update `column_mapping` dict in `standardize_columns()` function

## Alternative Data Source

If NCAA site becomes permanently inaccessible, consider:
- **Warren Nolan:** https://www.warrennolan.com/basketballw/2026/net-nitty
- Similar data, simpler format
- May require updating scraper URL and parsing logic

## Schedule

**Recommended update frequency:** Once per week
- NET rankings update daily during season
- Weekly updates sufficient for Top 50 tracking
- Manual export takes ~2 minutes

**Current automation:** Runs with workflow (Tues/Sat)
- May need occasional manual intervention
- Check workflow logs for status

## Files

```
scrape_net_rankings.py           # Main scraper script
data/net_rankings/
  ├── net_rankings_YYYYMMDD.csv  # Daily snapshots
  ├── net_rankings_master.csv    # Cumulative history
  └── net_rankings_manual_*.csv  # Manual exports (optional)
```

## Support

If both auto-scrape and manual export fail:
1. Check if NCAA site URL changed
2. Verify table structure in browser inspector
3. Consider alternative data source
4. Filter will continue working with polls-only data (25 teams)
