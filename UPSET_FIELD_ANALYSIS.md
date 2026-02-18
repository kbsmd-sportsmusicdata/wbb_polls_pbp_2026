# Upset Field Logic Analysis
**Date:** 2026-02-18
**Analyst:** Claude Code

## Executive Summary

The current upset classification logic in `build_polls_games_joined.py` has a **significant conceptual limitation**: it only flags upsets when a ranked team loses to an unranked team (or vice versa for wins). It does **NOT** consider cases where both teams are ranked but there's a significant rank difference.

This means games like **#2 Texas losing to #12 LSU** (10-rank difference) are classified as "No Upset" despite being arguably upset-worthy.

---

## Current Logic Implementation

### 1. `game_is_upset` Classification (Lines 180-191)

```python
gdf['game_is_upset'] = np.where(
    (gdf['game_Team Rank'].notna()) & (gdf['game_Team Rank'] <= 25) &
    ((gdf['game_Opponent Rank'].isna()) | (gdf['game_Opponent Rank'] > 25)) &
    (gdf['game_Game Result'] == 'L'),
    'Upset Loss',
    np.where(
        ((gdf['game_Team Rank'].isna()) | (gdf['game_Team Rank'] > 25)) &
        (gdf['game_Opponent Rank'].notna()) & (gdf['game_Opponent Rank'] <= 25) &
        (gdf['game_Game Result'] == 'W'),
        'Upset Win', 'No Upset'
    )
)
```

**Logic:**
- **Upset Loss**: Ranked team (≤25) loses to unranked team (>25 or null)
- **Upset Win**: Unranked team (>25 or null) beats ranked team (≤25)
- **No Upset**: Everything else (including ranked vs ranked)

### 2. `upset_magnitude` Calculation (Lines 480-485)

```python
def _magnitude(row):
    if pd.isna(row.get('game_Team Rank')) or pd.isna(row.get('game_Opponent Rank')):
        return None
    tr = row['game_Team Rank'] if row['game_Team Rank'] <= 25 else 26
    opr = row['game_Opponent Rank'] if row['game_Opponent Rank'] <= 25 else 26
    return abs(tr - opr)
```

**Logic:**
- Calculates absolute rank difference for ANY two ranked teams
- Returns `None` if either team is unranked

### 3. `upset_rank_label` Generation (Lines 487-505)

```python
def _label(row):
    upset_type = row.get('game_is_upset')
    if upset_type not in ('Upset Loss', 'Upset Win'):
        return None
    # ... creates label string ...
```

**Logic:**
- Only creates a label if `game_is_upset` is "Upset Loss" or "Upset Win"
- Returns `None` for "No Upset" games

---

## The Problem: Conceptual Mismatch

There's a **three-way inconsistency**:

1. `game_is_upset` → Only flags ranked-vs-unranked upsets
2. `upset_magnitude` → Calculates rank difference for ALL ranked-vs-ranked games
3. `upset_rank_label` → Only generates labels for games flagged in #1

**Result:** Games with significant rank differences (e.g., #2 vs #12) have:
- ✅ `upset_magnitude` calculated (10)
- ❌ `game_is_upset` = "No Upset"
- ❌ `upset_rank_label` = null

---

## Specific Cases: Texas Examples

### Line 721: Poll Week 1/5
- **Game:** #2 Texas lost to #12 LSU (Away, 65-70)
- **Rank Difference:** 10
- **Classification:** "No Upset"
- **Issue:** A top-5 team losing to a mid-ranked team by 10 spots is not flagged

### Line 729: Poll Week 2/9
- **Game:** #4 Texas lost to #5 Vanderbilt (Away, 70-86)
- **Rank Difference:** 1
- **Classification:** "No Upset"
- **Issue:** This is arguably correct (very close in rankings)

---

## Dataset-Wide Impact

### Ranked-vs-Ranked Games with Large Rank Differences

**Query:** Games where both teams ranked ≤25, team lost, rank diff > 5

**Results:** **61 games** classified as "No Upset" despite significant rank differences

**Sample Cases:**
| Team | Team Rank | Opponent | Opp Rank | Magnitude | Classification |
|------|-----------|----------|----------|-----------|----------------|
| Alabama | #24 | LSU | #6 | 18 | No Upset |
| Alabama | #23 | Oklahoma | #10 | 13 | No Upset |
| Baylor | #7 | Iowa | #19 | 12 | No Upset |
| Texas | #2 | LSU | #12 | 10 | No Upset |
| Iowa | #11 | UConn | #1 | 10 | No Upset |
| Duke | #7 | Baylor | #16 | 9 | No Upset |

### What IS Currently Classified as Upset

**Upset Losses (44 total):** Ranked team loses to unranked team
- Example: #21 Alabama lost to Unranked Auburn
- Example: #15 Baylor lost to Unranked Texas Tech

**Upset Wins (19 total):** Unranked team beats ranked team
- Example: Unranked Georgia beat #5 Vanderbilt
- Example: Unranked Duke beat #18 Notre Dame

---

## Root Cause Analysis

The current logic appears to be designed for a **binary ranked/unranked upset model**, which is common in sports analytics but may not capture the full picture for AP Poll analysis.

**Possible Original Intent:**
- Simple definition: "upset = ranked loses to unranked"
- Easy to understand and visualize
- Avoids subjective thresholds for rank differences

**Current Limitation:**
- Doesn't capture "quality upsets" (e.g., #1 losing to #15)
- `upset_magnitude` field exists but isn't used in classification
- Users may be confused by magnitude values with "No Upset" classification

---

## Recommendations

### Option 1: Expand Upset Definition (Recommended)
Add ranked-vs-ranked upsets with configurable threshold:

```python
# Define upset threshold
RANKED_UPSET_THRESHOLD = 5  # Rank difference to consider upset

gdf['game_is_upset'] = np.where(
    # Original: Ranked loses to unranked
    (gdf['game_Team Rank'].notna()) & (gdf['game_Team Rank'] <= 25) &
    ((gdf['game_Opponent Rank'].isna()) | (gdf['game_Opponent Rank'] > 25)) &
    (gdf['game_Game Result'] == 'L'),
    'Upset Loss',
    np.where(
        # NEW: Higher-ranked team loses to lower-ranked team by threshold+
        (gdf['game_Team Rank'].notna()) & (gdf['game_Team Rank'] <= 25) &
        (gdf['game_Opponent Rank'].notna()) & (gdf['game_Opponent Rank'] <= 25) &
        (gdf['game_Game Result'] == 'L') &
        ((gdf['game_Opponent Rank'] - gdf['game_Team Rank']) >= RANKED_UPSET_THRESHOLD),
        'Upset Loss (Ranked)',
        np.where(
            # Original: Unranked beats ranked
            ((gdf['game_Team Rank'].isna()) | (gdf['game_Team Rank'] > 25)) &
            (gdf['game_Opponent Rank'].notna()) & (gdf['game_Opponent Rank'] <= 25) &
            (gdf['game_Game Result'] == 'W'),
            'Upset Win',
            np.where(
                # NEW: Lower-ranked team beats higher-ranked team by threshold+
                (gdf['game_Team Rank'].notna()) & (gdf['game_Team Rank'] <= 25) &
                (gdf['game_Opponent Rank'].notna()) & (gdf['game_Opponent Rank'] <= 25) &
                (gdf['game_Game Result'] == 'W') &
                ((gdf['game_Team Rank'] - gdf['game_Opponent Rank']) >= RANKED_UPSET_THRESHOLD),
                'Upset Win (Ranked)',
                'No Upset'
            )
        )
    )
)
```

**Pros:**
- Captures significant ranked-vs-ranked upsets
- Makes use of existing `upset_magnitude` data
- More comprehensive upset tracking

**Cons:**
- Requires choosing threshold (subjective)
- More complex logic
- May need separate categories for ranked vs unranked upsets

### Option 2: Multi-Tier Upset Classification
Create upset categories based on magnitude:

- **Major Upset**: Rank diff ≥ 10
- **Moderate Upset**: Rank diff 5-9
- **Minor Upset**: Rank diff 1-4 or ranked vs unranked
- **No Upset**: Close games, expected outcomes

### Option 3: Keep Current Logic, Add Documentation
If current logic is intentional, add clear documentation:

```python
# --- Upset classification ---
# NOTE: This only flags ranked-vs-unranked upsets.
# Ranked-vs-ranked games are NOT classified as upsets regardless of rank difference.
# Use 'upset_magnitude' field for rank difference analysis.
```

---

## Impact on Tableau Visualizations

**Current State:**
- "Upset Radar" dashboard likely only shows ranked-vs-unranked upsets
- 61 potentially significant upsets are invisible
- `upset_magnitude` field exists but may be underutilized

**If Fixed:**
- More comprehensive upset tracking
- Better insights into "surprising" results
- More accurate "strength of schedule" narratives

---

## Next Steps

1. **Validate with stakeholder:** Is current ranked-vs-unranked logic intentional?
2. **Choose approach:** Expand definition, multi-tier, or document limitation
3. **Test threshold:** If expanding, determine appropriate rank difference threshold (5? 7? 10?)
4. **Update Tableau:** Ensure dashboards reflect new classification
5. **Backfill data:** Re-run pipeline to update historical classifications

---

## Technical Notes

### Files to Update
- `scripts/process/build_polls_games_joined.py` (lines 176-191, 475-508)
- Documentation/README if keeping current logic

### Testing Queries
```python
# Count upsets by type
df.groupby('game_is_upset').size()

# Find large rank-diff games currently marked "No Upset"
df[(df['upset_magnitude'] > 5) & (df['game_is_upset'] == 'No Upset')]

# Distribution of upset magnitudes
df[df['game_is_upset'].str.contains('Upset')]['upset_magnitude'].describe()
```

### Configuration Recommendation
Add to `config/poll_week_windows.json` or new config file:

```json
{
  "upset_thresholds": {
    "ranked_vs_unranked": 0,
    "ranked_vs_ranked": 5
  }
}
```
