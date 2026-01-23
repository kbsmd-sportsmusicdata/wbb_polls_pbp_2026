# Historical Polls Scraper Documentation

## Overview

`polls_historical.py` scrapes historical WBB polls data from Sports Reference for seasons 2025 back to 2010.

## Features

### ✅ Multi-Year Scraping
- Scrapes polls data for years **2025 → 2010** (16 seasons)
- Processes each year sequentially with rate limiting

### ✅ Rate Limiting
- **3-second delay** between requests to avoid being blocked
- Polite scraping practices to respect server resources

### ✅ Robust Error Handling
- Continues scraping even if individual years fail
- Saves successfully scraped data even if some years error out
- Detailed reporting of successful and failed years

### ✅ Data Tracking
- Adds a **`year` column** to all data for easy filtering
- Adds a **`table_number` column** to distinguish multiple poll tables per year

### ✅ Idempotency
- Checks existing master file for year overlap
- Prevents duplicate data when re-running the script
- Only appends new years to master file

## Output Files

### 1. Timestamped Snapshot
**Location:** `data/polls_historical/polls_historical_YYYYMMDD.csv`

Contains all data collected in a single run.

**Columns:**
- All columns from source tables (Rk, School, Conf, weekly poll columns, etc.)
- `year` - The season year (2025, 2024, etc.)
- `table_number` - Which table on the page (usually 1-2 per year)

### 2. Master File
**Location:** `data/polls_historical/polls_historical_master.csv`

Cumulative file containing all historical data ever collected.

- First run: Creates the file
- Subsequent runs: Appends only new years (no duplicates)

## Usage

### Basic Usage

```bash
python polls_historical.py
```

### Expected Output

```
======================================================================
WBB Historical Polls Scraper
======================================================================
Run Date: 2026-01-22
Target Years: 2025 → 2010
Request Delay: 3 seconds
======================================================================

[1/16] Processing 2025...
  Fetching: https://www.sports-reference.com/cbb/seasons/women/2025-polls.html
    ✓ Found 2 table(s) with 450 total rows
    ✓ Processed 450 rows for 2025
    ⏳ Waiting 3 seconds before next request...

[2/16] Processing 2024...
  Fetching: https://www.sports-reference.com/cbb/seasons/women/2024-polls.html
    ✓ Found 2 table(s) with 425 total rows
    ✓ Processed 425 rows for 2024
    ⏳ Waiting 3 seconds before next request...

[3/16] Processing 2023...
  Fetching: https://www.sports-reference.com/cbb/seasons/women/2023-polls.html
    ✓ Found 2 table(s) with 410 total rows
    ✓ Processed 410 rows for 2023
    ⏳ Waiting 3 seconds before next request...

...

======================================================================
Processing Results
======================================================================
✓ Successfully scraped 16 years
  Total rows collected: 6,543
  Years: 2025, 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015, 2014, 2013, 2012, 2011, 2010

✓ Saved snapshot: data/polls_historical/polls_historical_20260122.csv

Updating master file...
  ✓ Created master file with 6543 rows
✓ Master file: data/polls_historical/polls_historical_master.csv

======================================================================
Summary
======================================================================
✓ Successful: 16/16 years
  2025, 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015, 2014, 2013, 2012, 2011, 2010
======================================================================
```

### If Some Years Fail

```
======================================================================
Summary
======================================================================
✓ Successful: 14/16 years
  2025, 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015, 2014, 2011, 2010
✗ Failed: 2/16 years
  2013, 2012
======================================================================
```

The script will still save the 14 successful years!

## Data Structure

### Raw Tables
Each year's polls page typically contains 2 tables:
1. **AP Poll** - Associated Press rankings by week
2. **Coaches Poll** - USA Today Coaches rankings by week

### Example Output Schema

```csv
Rk,School,Conf,Pre,11/13,11/20,...,year,table_number
1,Connecticut,Big East,1,1,1,...,2025,1
2,South Carolina,SEC,2,2,2,...,2025,1
3,UCLA,Big Ten,3,3,4,...,2025,1
...
```

## Integration with Existing Data

This script complements your existing polls data:

- **`sportsref_scraper.py`** - Scrapes **current season** polls (2026) with detailed processing
- **`polls_historical.py`** - Scrapes **historical seasons** (2025-2010) with raw table capture

You can combine them for complete historical analysis:
```python
# Current season (processed)
current = pd.read_csv("data/polls_long.csv")

# Historical (raw tables)
historical = pd.read_csv("data/polls_historical/polls_historical_master.csv")
```

## Customization

### Change Year Range

Edit the constants in `polls_historical.py`:

```python
START_YEAR = 2025  # Most recent year
END_YEAR = 2010    # Oldest year
```

### Adjust Rate Limiting

```python
REQUEST_DELAY = 3  # Seconds between requests (increase if getting blocked)
```

### Scrape Specific Years Only

```python
# Instead of range, specify exact years:
YEARS_TO_SCRAPE = [2025, 2024, 2020, 2015]  # Cherry-pick years
```

## Troubleshooting

### "Page not found (404)" errors
- Some older years may not have data available
- The script will skip these and continue

### "HTTP error" or rate limiting
- Increase `REQUEST_DELAY` to 5 or 10 seconds
- Some sites may block rapid requests

### Network timeouts
- Check your internet connection
- The script uses a 30-second timeout per request
- Temporary network issues will be reported and skipped

### No data extracted from tables
- The page structure may have changed for certain years
- Check the URL manually to verify data exists
- You may need to adjust the parsing logic

## Performance

With default settings:
- **Request delay:** 3 seconds
- **Years to scrape:** 16 (2025-2010)
- **Estimated runtime:** ~48 seconds (16 years × 3 seconds) + processing time
- **Total time:** Approximately **1-2 minutes**

## Best Practices

1. **Run during off-peak hours** to minimize server load
2. **Don't run multiple times rapidly** - use the master file instead
3. **Check the summary output** to verify all years were collected
4. **Re-run only failed years** if needed (customize `YEARS_TO_SCRAPE`)

## Future Enhancements

Potential improvements:
- Add command-line arguments for year range
- Implement exponential backoff for retries
- Add data validation and schema checks
- Create long-format output similar to `polls_long.csv`
- Merge with current season data automatically

---

**Created:** 2026-01-22
**Maintainer:** Data Automation Pipeline
**Related Scripts:** `sportsref_scraper.py`, `scrape_ratings.py`
