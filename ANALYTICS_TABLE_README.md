# AP Poll Analytics Table Documentation

## Overview

The **polls_analytics.csv** file is an enhanced, analysis-ready dataset that adds calculated metrics to the raw polls data. This table is specifically designed for data visualization tools (Tableau, Power BI, Excel) and analytical workflows.

---

## Purpose

While `polls_long.csv` provides raw poll rankings in long format, `polls_analytics.csv` adds:
- **Week-over-week rank changes**
- **Movement categories** (Rising/Falling/New/Dropped)
- **Cumulative metrics** (weeks in Top 25, ranked streaks)
- **Previous week rankings** for comparison

This makes it trivial to build dashboards showing rank volatility, team trajectories, and conference performance without complex calculated fields.

---

## Schema

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `season` | int | Basketball season year | 2026 |
| `team` | str | Team name (standardized) | UConn |
| `conference` | str | Conference abbreviation | Big East |
| `poll_week` | str | Poll release week | Pre, 11/10, 1/19 |
| `rank` | float | Actual AP Poll rank (1-25) | 1.0 |
| `rank_numeric` | int | Rank with 26 for unranked | 1 |
| `prev_rank_numeric` | int | Previous week's rank_numeric | 3 |
| `rank_change` | float | Rank improvement (prev - current) | +2.0 |
| `movement_category` | str | Movement type | Rising |
| `weeks_in_top25` | int | Cumulative weeks ranked | 15 |
| `ranked_streak` | int | Consecutive weeks currently ranked | 15 |
| `run_date` | str | Date of data collection | 2026-01-22 |
| `table_id` | int | Poll table (1=AP, 2=Coaches) | 2 |

---

## Key Metrics Explained

### 1. `rank_change` - Week-over-Week Movement

**Formula:** `prev_rank_numeric - rank_numeric`

**Interpretation:**
- **Positive value** = Improvement (better rank)
- **Negative value** = Decline (worse rank)
- **Zero** = No change
- **NaN** = First appearance (no previous week)

**Examples:**
- Rank 10 → 5: `rank_change = +5` (improved 5 spots)
- Rank 5 → 10: `rank_change = -5` (dropped 5 spots)
- Rank 3 → 3: `rank_change = 0` (stayed same)

**Why this direction?**
We use `prev - current` so that **positive numbers = good news**. This is more intuitive for visualizations where "up and to the right" means improvement.

---

### 2. `movement_category` - Movement Classification

Seven categories capture all possible week-to-week transitions:

| Category | Definition | Example |
|----------|------------|---------|
| **Rising** | Improved rank (both weeks ranked) | #10 → #5 |
| **Falling** | Worsened rank (both weeks ranked) | #5 → #10 |
| **Stable** | Same rank (both weeks ranked) | #3 → #3 |
| **New** | Entered Top 25 | Unranked → #24 |
| **Dropped** | Fell out of Top 25 | #25 → Unranked |
| **Unranked** | Unranked in both weeks | Unranked → Unranked |
| **Unknown** | No previous week data (first poll) | N/A → #15 |

**Use Cases:**
- **Filter by movement type** in dashboards
- **Color code** teams (green=Rising, red=Falling)
- **Analyze volatility** (count Rising/Falling occurrences)

---

### 3. `weeks_in_top25` - Cumulative Weeks Ranked

**Definition:** Total number of weeks the team has been ranked (1-25) up to and including the current week.

**Behavior:**
- **Increments** when team is ranked (1-25)
- **Stays same** when team is unranked (26)
- **Cumulative** across the entire season

**Examples:**
- Week 1: Ranked #5 → `weeks_in_top25 = 1`
- Week 2: Ranked #3 → `weeks_in_top25 = 2`
- Week 3: Unranked → `weeks_in_top25 = 2` (no change)
- Week 4: Ranked #20 → `weeks_in_top25 = 3`

**Use Cases:**
- Identify **most consistently ranked teams**
- Compare **conference depth** (total weeks ranked per conference)
- Track **season trajectory** (teams gaining or losing ranked status)

---

### 4. `ranked_streak` - Consecutive Weeks Ranked

**Definition:** Current consecutive weeks the team has been ranked.

**Behavior:**
- **Increments** each week while ranked
- **Resets to 0** when team becomes unranked
- **Stays 0** while unranked

**Examples:**
- Ranked for 5 consecutive weeks → `ranked_streak = 5`
- Drops out in week 6 → `ranked_streak = 0`
- Returns to rankings week 7 → `ranked_streak = 1`

**Use Cases:**
- Identify **sustained success** (long streaks)
- Find **trending teams** (growing streaks)
- Highlight **program stability** vs. volatility

---

## Movement Category Distribution

Based on current season data (2,553 rows, 33 teams, 11 weeks):

| Category | Count | Percentage | Meaning |
|----------|-------|------------|---------|
| **Stable** | 1,767 | 69.2% | Most teams maintain their rank |
| **Unranked** | 550 | 21.5% | Teams outside Top 25 |
| **Rising** | 82 | 3.2% | Upward movements |
| **Falling** | 81 | 3.2% | Downward movements |
| **Unknown** | 33 | 1.3% | First poll (no previous data) |
| **Dropped** | 20 | 0.8% | Fell out of Top 25 |
| **New** | 20 | 0.8% | Entered Top 25 |

**Key Insight:** ~70% of teams maintain their rank week-to-week, showing relative stability in college basketball rankings.

---

## Usage Examples

### Tableau

#### 1. Rank Movement Over Time (Line Chart)
```
Columns: poll_week
Rows: rank (reversed axis - 1 at top)
Color: team
Filter: team (select 5-10 teams to compare)
Tooltip: Show rank_change, movement_category
```

#### 2. Week-over-Week Change (Diverging Bar)
```
Columns: rank_change
Rows: team
Color: rank_change (diverging palette: green=positive, red=negative)
Filter: poll_week = [Latest Week]
Sort: By rank_change descending
```

#### 3. Movement Category Summary (Stacked Bar)
```
Columns: COUNT(team)
Rows: poll_week
Color: movement_category
Palette: Green (Rising), Red (Falling), Blue (Stable), Yellow (New), Gray (Dropped)
```

#### 4. Conference Performance
```
Columns: conference
Rows: SUM(weeks_in_top25)
Color: conference
Tooltip: Show teams, average rank
```

---

### Python/Pandas

```python
import pandas as pd
import matplotlib.pyplot as plt

# Load analytics data
polls = pd.read_csv('data/polls_analytics.csv')

# 1. Teams with biggest jumps this week
latest_week = polls['poll_week'].max()
this_week = polls[polls['poll_week'] == latest_week]
biggest_movers = this_week.nlargest(10, 'rank_change')

print("Biggest Positive Movers:")
print(biggest_movers[['team', 'rank', 'prev_rank_numeric', 'rank_change']])

# 2. Most volatile teams (total absolute movement)
volatility = polls.groupby('team')['rank_change'].apply(
    lambda x: x.abs().sum()
).sort_values(ascending=False)

print("\nMost Volatile Teams:")
print(volatility.head(10))

# 3. Most consistent teams (longest current streak)
latest_data = polls[polls['poll_week'] == latest_week]
longest_streaks = latest_data.nlargest(10, 'ranked_streak')

print("\nLongest Current Ranked Streaks:")
print(longest_streaks[['team', 'ranked_streak', 'weeks_in_top25']])

# 4. Movement category analysis
movement_by_week = polls.groupby(['poll_week', 'movement_category']).size().unstack(fill_value=0)
movement_by_week.plot(kind='bar', stacked=True, figsize=(12, 6))
plt.title('Movement Categories by Week')
plt.xlabel('Poll Week')
plt.ylabel('Number of Teams')
plt.legend(title='Movement')
plt.tight_layout()
plt.show()

# 5. Conference dominance (total weeks ranked)
conf_performance = polls.groupby('conference')['weeks_in_top25'].sum().sort_values(ascending=False)

print("\nConference Rankings (Total Weeks in Top 25):")
print(conf_performance.head(10))
```

---

### SQL Queries

```sql
-- Teams that entered Top 25 this season
SELECT DISTINCT team, conference, poll_week as first_appearance
FROM polls_analytics
WHERE movement_category = 'New'
ORDER BY poll_week;

-- Average rank by conference (latest week)
SELECT
    conference,
    AVG(rank_numeric) as avg_rank,
    COUNT(*) as teams_ranked
FROM polls_analytics
WHERE poll_week = (SELECT MAX(poll_week) FROM polls_analytics)
  AND rank_numeric <= 25
GROUP BY conference
ORDER BY avg_rank;

-- Teams that have never been unranked
SELECT
    team,
    MAX(weeks_in_top25) as total_weeks,
    MAX(ranked_streak) as current_streak
FROM polls_analytics
GROUP BY team
HAVING MIN(rank_numeric) <= 25  -- Always ranked
ORDER BY total_weeks DESC;

-- Biggest single-week jumps (positive and negative)
SELECT
    team,
    poll_week,
    prev_rank_numeric as from_rank,
    rank_numeric as to_rank,
    rank_change,
    movement_category
FROM polls_analytics
WHERE rank_change IS NOT NULL
ORDER BY ABS(rank_change) DESC
LIMIT 20;

-- Conference movement summary
SELECT
    conference,
    SUM(CASE WHEN movement_category = 'Rising' THEN 1 ELSE 0 END) as rising,
    SUM(CASE WHEN movement_category = 'Falling' THEN 1 ELSE 0 END) as falling,
    SUM(CASE WHEN movement_category = 'Stable' THEN 1 ELSE 0 END) as stable,
    SUM(CASE WHEN movement_category = 'New' THEN 1 ELSE 0 END) as new_entries
FROM polls_analytics
WHERE poll_week = (SELECT MAX(poll_week) FROM polls_analytics)
GROUP BY conference
ORDER BY (rising + new_entries) DESC;
```

---

## Data Quality Notes

### First Week (Unknown Category)

The first poll week for each team will have:
- `prev_rank_numeric` = NaN
- `rank_change` = NaN
- `movement_category` = 'Unknown'

This is expected behavior since there's no previous week to compare against.

### Unranked Teams (Rank 26)

Teams receiving votes but not in Top 25:
- `rank` = NaN (no actual rank)
- `rank_numeric` = 26 (for easier filtering/sorting)
- Can still have `rank_change` calculated
- `movement_category` might be 'Unranked', 'Dropped', or 'New'

### Multiple Run Dates per Week

The raw polls data may have multiple `run_date` values for the same `poll_week` (due to multiple scrapes). This is preserved in the analytics table:
- Each row represents one team/week/run_date combination
- For analysis, typically **filter to latest run_date per week**
- Or use: `df.sort_values('run_date').groupby(['team', 'poll_week']).last()`

---

## Generation Workflow

The analytics table is generated from `polls_long.csv`:

```bash
python generate_analytics_table.py
```

**Process:**
1. Reads `data/polls_long.csv`
2. Sorts by team and poll_week
3. Calculates `prev_rank_numeric` using pandas shift
4. Computes `rank_change` (prev - current)
5. Determines `movement_category` based on rank transitions
6. Counts cumulative `weeks_in_top25`
7. Tracks consecutive `ranked_streak`
8. Saves to `data/polls_analytics.csv`

**Runtime:** < 5 seconds for full season

---

## Comparison with Other Files

| File | Purpose | Format | Best For |
|------|---------|--------|----------|
| `polls_long.csv` | Raw poll data | Long format | Historical analysis, raw data needs |
| **`polls_analytics.csv`** | **Enhanced with metrics** | **Long format** | **Dashboards, trend analysis** |
| `ratings_master.csv` | Team ratings/SOS | Wide format | Statistical modeling, advanced metrics |

**Recommendation:** Use `polls_analytics.csv` for 95% of visualization and analysis needs.

---

## Future Enhancements

Potential additions to analytics table:

1. **Momentum Index**: 3-week rolling average of rank_change
2. **Rank Volatility Score**: Standard deviation of rank over time
3. **Peak Rank**: Best rank achieved by team in season
4. **Weeks at #1**: Count of weeks ranked first
5. **Conference Rank**: Rank within conference
6. **Tournament Implications**: Projected seed based on current rank
7. **First/Last Poll Comparison**: Season start vs. current rank
8. **Rank Percentile**: Rank as percentile (better for unranked teams)

---

## Troubleshooting

### Issue: Rank changes seem reversed

**Check:** Are you sorting by rank ascending or descending?
- Rank 1 is BEST, rank 25 is WORST
- For charts, reverse the Y-axis so #1 appears at top

### Issue: Too many "Unknown" categories

**Cause:** First poll of season or team's first appearance
**Solution:** Filter out `movement_category = 'Unknown'` for week-to-week analysis

### Issue: Duplicate rows for same team/week

**Cause:** Multiple scrapes on different `run_date` values
**Solution:** Filter to latest run_date:
```python
latest = df.sort_values('run_date').groupby(['team', 'poll_week']).last()
```

### Issue: Weeks_in_top25 seems too high

**Check:** This is CUMULATIVE across all poll_weeks in the dataset
- If dataset has multiple run_dates per week, it counts each occurrence
- Filter to one run_date per week for accurate counts

---

## Summary

**polls_analytics.csv** is your go-to file for:
- ✅ Tableau dashboards
- ✅ Trend analysis
- ✅ Movement tracking
- ✅ Conference comparisons
- ✅ Volatility studies

**Key advantage:** Pre-calculated metrics mean you can build visualizations in minutes, not hours.

---

**Created:** 2026-01-29
**Generator Script:** `generate_analytics_table.py`
**Source Data:** `data/polls_long.csv`
**Output:** `data/polls_analytics.csv`
