# Historical Polls Data Processor

## Overview

`process_polls_historical.py` transforms raw historical polls data from **wide format** (one row per team with week columns) to **long format** (one row per team per week per year) for easier analysis.

## Purpose

The historical polls scraper (`polls_historical.py`) produces data in the same wide format as it appears on Sports Reference:
- Each row = one team
- Each column = one week
- Values = rankings

This is great for visual inspection but **difficult for analysis**. The processor converts it to a normalized long format that's ideal for:
- Filtering and querying
- Time series analysis
- Trend visualization
- Statistical modeling
- Database storage

## Input → Output

### Input Format (Wide)
```csv
School,Conf,Pre,11/10,11/17,12/1,12/8,year,table_number
UConn,Big East,1,1,1,1,1,2025,1
South Carolina,SEC,2,2,2,3,3,2025,1
UCLA,Big Ten,3,3,3,4,4,2025,1
```

### Output Format (Long)
```csv
year,team,conference,poll_week,rank,rank_numeric,table_id
2025,UConn,Big East,Pre,1,1,1
2025,UConn,Big East,11/10,1,1,1
2025,UConn,Big East,11/17,1,1,1
2025,UConn,Big East,12/1,1,1,1
2025,UConn,Big East,12/8,1,1,1
2025,South Carolina,SEC,Pre,2,2,1
2025,South Carolina,SEC,11/10,2,2,1
...
```

## Schema

### Output Columns

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `year` | int | Season year | 2025 |
| `team` | str | Team name | UConn |
| `conference` | str | Conference abbreviation | Big East |
| `poll_week` | str | Poll week identifier | Pre, 11/10, 12/1 |
| `rank` | float | Actual ranking (1-25, or NaN) | 1.0 |
| `rank_numeric` | int | Ranking with 26 for unranked | 1 |
| `table_id` | int | Poll table number (1=AP, 2=Coaches) | 1 |

### Column Details

**`year`**: The basketball season year
- Example: 2025 for the 2024-25 season

**`team`**: School/team name as it appears in polls
- Standardized from "School" column
- Example: "UConn", "South Carolina", "UCLA"

**`conference`**: Conference abbreviation
- Example: "Big East", "SEC", "Big Ten", "ACC"
- Can be `None` if not available in source data

**`poll_week`**: Poll release week/date
- "Pre" or "Preseason" for preseason rankings
- Date format: "11/10", "12/1", "1/19"
- Normalized from column headers

**`rank`**: Actual poll ranking
- Float: 1.0 to 25.0 for ranked teams
- NaN for unranked teams (teams that received votes but not ranked)

**`rank_numeric`**: Integer ranking for analysis
- 1-25 for ranked teams
- 26 for unranked teams (makes filtering easier)

**`table_id`**: Which poll table the data came from
- Typically 1 = AP Poll, 2 = Coaches Poll
- Preserved from source data's `table_number` column

## Usage

### Basic Usage

```bash
python process_polls_historical.py
```

### Prerequisites

1. Run the historical polls scraper first:
   ```bash
   python polls_historical.py
   ```

2. This creates `data/polls_historical/polls_historical_master.csv`

3. Then run the processor:
   ```bash
   python process_polls_historical.py
   ```

### Expected Output

```
======================================================================
Historical Polls Data Processor
======================================================================
Process Date: 2026-01-22
======================================================================

Processing historical polls to long format...

Reading master file: data/polls_historical/polls_historical_master.csv
  ✓ Loaded 6,543 rows
  ✓ Found 16 years: 2010 to 2025
  Processing 2010 - Table 1 (385 rows)
    Found 18 week columns
  Processing 2010 - Table 2 (412 rows)
    Found 18 week columns
  Processing 2011 - Table 1 (398 rows)
    Found 19 week columns
  ...

Combining all data...
  ✓ Total rows in long format: 125,432
  ✓ Unique years: 16
  ✓ Unique teams: 187
  ✓ Unique weeks: 21

Saving long format to: data/polls_historical/polls_historical_long.csv
  ✓ Saved 125,432 rows

======================================================================
Sample Data (First 20 rows)
======================================================================
 year           team conference poll_week  rank  rank_numeric  table_id
 2025          UConn   Big East       Pre     1             1         1
 2025 South Carolina        SEC       Pre     2             2         1
 2025           UCLA    Big Ten       Pre     3             3         1
 ...

======================================================================
Summary Statistics
======================================================================

Years covered:
  2025: 8,456 rows
  2024: 8,123 rows
  2023: 7,891 rows
  ...

Top 10 teams by appearances:
  UConn: 352 weeks
  South Carolina: 348 weeks
  Stanford: 342 weeks
  ...

Weeks per season:
  21 unique poll weeks across all years

Ranking distribution:
  Ranked (1-25): 98,234 rows
  Unranked (26): 27,198 rows

======================================================================
✅ Processing Complete!
======================================================================
Output file: data/polls_historical/polls_historical_long.csv
======================================================================
```

## Analysis Examples

### Python/Pandas

```python
import pandas as pd

# Load the long format data
polls = pd.read_csv("data/polls_historical/polls_historical_long.csv")

# 1. Get UConn's ranking history for 2025
uconn_2025 = polls[
    (polls['team'] == 'UConn') &
    (polls['year'] == 2025)
]

# 2. Find teams that were #1 at any point
number_ones = polls[polls['rank'] == 1]['team'].unique()

# 3. Analyze ranking stability (teams ranked all season)
ranked_all_year = polls.groupby(['year', 'team']).agg({
    'rank_numeric': lambda x: (x <= 25).all()  # Ranked every week
})

# 4. Compare AP Poll vs Coaches Poll
ap_poll = polls[polls['table_id'] == 1]
coaches_poll = polls[polls['table_id'] == 2]

# 5. Week-by-week ranking changes
uconn_ranks = polls[
    (polls['team'] == 'UConn') &
    (polls['year'] == 2025)
].sort_values('poll_week')

# 6. Conference dominance
conf_ranked = polls[polls['rank_numeric'] <= 25].groupby(
    ['year', 'conference']
).size()

# 7. First-time ranked teams
newly_ranked = polls.groupby(['year', 'team']).agg({
    'poll_week': 'first',
    'rank': 'first'
})

# 8. End-of-season rankings (last poll week)
final_polls = polls.sort_values('poll_week').groupby(
    ['year', 'team']
).last()
```

### SQL Queries

If you load this data into a database:

```sql
-- Teams ranked in final poll of each year
SELECT year, team, rank
FROM polls_historical_long
WHERE poll_week IN (
    SELECT MAX(poll_week)
    FROM polls_historical_long
    GROUP BY year
)
AND rank_numeric <= 25
ORDER BY year DESC, rank;

-- Longest continuous ranking streaks
SELECT team, year, COUNT(*) as weeks_ranked
FROM polls_historical_long
WHERE rank_numeric <= 25
GROUP BY team, year
ORDER BY weeks_ranked DESC
LIMIT 20;

-- Teams that appeared in both AP and Coaches polls
SELECT year, team, COUNT(DISTINCT table_id) as poll_count
FROM polls_historical_long
WHERE rank_numeric <= 25
GROUP BY year, team
HAVING COUNT(DISTINCT table_id) = 2;

-- Biggest ranking jumps week-to-week
WITH ranked_polls AS (
    SELECT *,
           LAG(rank) OVER (PARTITION BY year, team ORDER BY poll_week) as prev_rank
    FROM polls_historical_long
    WHERE rank IS NOT NULL
)
SELECT year, team, poll_week, prev_rank, rank, (prev_rank - rank) as jump
FROM ranked_polls
WHERE prev_rank IS NOT NULL
ORDER BY ABS(jump) DESC
LIMIT 20;
```

### Excel/Google Sheets

Import the CSV and use pivot tables:

1. **Ranking Timeline**:
   - Rows: team
   - Columns: poll_week
   - Values: rank
   - Filter: year = 2025

2. **Conference Performance**:
   - Rows: conference
   - Values: COUNT(rank) where rank <= 25
   - Group by: year

3. **Team Comparison**:
   - Create line chart with:
     - X-axis: poll_week
     - Y-axis: rank (reversed scale)
     - Series: different teams
     - Filter: year

## Data Quality Notes

### Handling Unranked Teams

Teams can be "unranked" in two ways:

1. **Not in data at all** - Team didn't receive votes
2. **In data with NaN rank** - Team received votes but wasn't top 25

The processor handles this with `rank_numeric`:
- Ranked teams: 1-25
- Unranked but in data: 26

This makes filtering easier:
```python
# Only ranked teams
ranked = polls[polls['rank_numeric'] <= 25]

# All teams (ranked + receiving votes)
all_teams = polls  # Already includes everyone
```

### Missing Weeks

Some teams may not appear in certain weeks:
- Didn't receive votes that week
- Were unranked

To analyze continuous streaks, account for missing weeks.

### Multi-Level Columns

The original data from Sports Reference may have multi-level column headers. The processor automatically:
1. Detects multi-level columns
2. Extracts the lowest (most specific) level
3. Normalizes to simple string column names

### Week Identification

The processor identifies poll weeks using `is_week_col()`:
- ✅ Includes: "Pre", "11/10", "12/1", date patterns
- ❌ Excludes: "Rk", "School", "Conf", "Year", metadata columns

## Performance

- **Input size**: ~6,500 rows (16 years × ~400 teams/year)
- **Output size**: ~125,000 rows (6,500 rows × ~20 weeks/year)
- **Processing time**: < 10 seconds
- **Memory usage**: < 100 MB

## Comparison with Current Season

| Feature | `sportsref_scraper.py` | `process_polls_historical.py` |
|---------|------------------------|-------------------------------|
| Time period | Current season (2026) | Historical (2025-2010) |
| Source | Direct scraping | Post-processing |
| Output | `polls_long.csv` | `polls_historical_long.csv` |
| `run_date` column | ✓ (scrape date) | ✗ (not needed for historical) |
| `year` column | ✗ (always 2026) | ✓ (varies 2025-2010) |
| Processing | Built into scraper | Separate script |

### Combining Both

To analyze both current and historical data:

```python
# Current season
current = pd.read_csv("data/polls_long.csv")
current['year'] = 2026  # Add year column

# Historical
historical = pd.read_csv("data/polls_historical/polls_historical_long.csv")

# Combine (align columns first)
all_polls = pd.concat([historical, current], ignore_index=True)
```

## Troubleshooting

### "Master file not found"
```
✗ Master file not found: data/polls_historical/polls_historical_master.csv
```
**Solution**: Run `python polls_historical.py` first to scrape the data.

### "'year' column not found"
The master file is missing the year column.
**Solution**: Re-run the scraper - it should add the year column automatically.

### "No week columns found"
The processor couldn't identify poll week columns.
**Solution**: Check the master CSV structure. Week columns should be dates like "11/10" or "Pre".

### Very few rows in output
**Possible causes**:
1. Limited data in master file (re-run scraper for more years)
2. Data format doesn't match expected structure
3. Teams were filtered out due to missing School/Team column

**Solution**: Check the console output for warnings during processing.

## Next Steps

After processing:

1. **Visualize trends**:
   ```bash
   # Use any visualization tool
   # Data is ready for Tableau, PowerBI, matplotlib, etc.
   ```

2. **Database import**:
   ```bash
   # Import to PostgreSQL, MySQL, SQLite, etc.
   ```

3. **Combine with other data**:
   - Join with team box scores (`scrape_teambox.py` output)
   - Join with ratings data (`scrape_ratings.py` output)
   - Analyze poll rank vs. SOS/SRS metrics

4. **Analysis notebooks**:
   - Create Jupyter notebooks for exploratory analysis
   - Build dashboards
   - Generate reports

---

## Summary

**Input**: `polls_historical_master.csv` (wide format, ~6,500 rows)
**Output**: `polls_historical_long.csv` (long format, ~125,000 rows)
**Purpose**: Transform data for easier analysis
**Runtime**: < 10 seconds

The long format makes it trivial to:
- Filter by year, team, week, or conference
- Calculate ranking trends over time
- Compare teams across seasons
- Visualize ranking evolution
- Join with other datasets

---

**Created**: 2026-01-23
**Related Scripts**: `polls_historical.py`, `sportsref_scraper.py`
