# Team Name Standardization Report

**Date:** 2026-01-23
**Purpose:** Cross-reference team names across datasets to identify naming inconsistencies

---

## Executive Summary

This report identifies team name mismatches between different datasets in the WBB data pipeline. The primary reference dataset is `data/polls_long.csv`, which uses standardized abbreviations for team names.

### Datasets Analyzed
1. ✅ **ratings_20260122.csv** (from GitHub) - Ratings/SOS data
2. ⏳ **polls_historical_long.csv** - Not yet available (will be created after running scrapers)
3. ✅ **polls_long.csv** - Reference dataset (current season polls)

---

## Team Name Mismatches

### Ratings Dataset vs. Polls Long Dataset

The following teams appear with different names in `ratings_20260122.csv` compared to the standardized names in `polls_long.csv`:

| Source Name (Ratings) | Dataset | Target Name (Polls Long) | Verified |
|----------------------|---------|--------------------------|----------|
| **Connecticut** | ratings_20260122.csv | **UConn** | ✅ |
| **Louisiana State** | ratings_20260122.csv | **LSU** | ✅ |
| **Southern California** | ratings_20260122.csv | **USC** | ✅ |
| **Mississippi** | ratings_20260122.csv | **Ole Miss** | ✅ |
| **North Carolina** | ratings_20260122.csv | **UNC** | ✅ |

**Total Mismatches:** 5

---

## Verification Details

### 1. Connecticut → UConn
- **Official Name:** University of Connecticut Huskies
- **Common Usage:** UConn (universally used)
- **Sports Reference Page:** [UConn Women's Basketball](https://www.sports-reference.com/cbb/schools/connecticut/women/2026.html)
- **Verification:** Confirmed via [UConn Athletics](https://uconnhuskies.com/sports/womens-basketball)

### 2. Louisiana State → LSU
- **Official Name:** Louisiana State University Tigers
- **Common Usage:** LSU (standard abbreviation)
- **Note:** LSU is the universally recognized abbreviation for Louisiana State University

### 3. Southern California → USC
- **Official Name:** University of Southern California Trojans
- **Common Usage:** USC (standard abbreviation)
- **Note:** USC is the official abbreviation used by the university

### 4. Mississippi → Ole Miss
- **Official Name:** University of Mississippi Rebels
- **Common Usage:** Ole Miss (official nickname)
- **Sports Page:** [Ole Miss Women's Basketball](https://olemisssports.com/sports/womens-basketball)
- **Verification:** Confirmed via [Ole Miss Athletics](https://olemisssports.com/sports/womens-basketball)
- **Note:** "Ole Miss" is the official nickname of the University of Mississippi

### 5. North Carolina → UNC
- **Official Name:** University of North Carolina Tar Heels
- **Common Usage:** UNC (to distinguish from NC State)
- **Note:** Important distinction - "North Carolina" refers to UNC Chapel Hill, not NC State

---

## Teams That Cannot Be Matched

**None** - All team names from ranked teams in the ratings dataset were successfully matched to their standardized equivalents in the polls dataset.

### Important Note on Dataset Scope

The `ratings_20260122.csv` dataset contains **~360 Division I teams**, while `polls_long.csv` only contains **~33 ranked teams** (Top 25 + those receiving votes).

**This is expected behavior.** The following teams appear in ratings but NOT in polls:
- Minnesota, Oregon, Georgia, Stanford, Villanova, Syracuse, Virginia, Miami (FL), Virginia Tech, Utah, Brigham Young, Florida, Clemson, South Dakota State, Colorado, Arizona State, and many more...

These are NOT mismatches - they simply weren't ranked in the polls during the analyzed time period.

---

## Historical Polls Dataset Analysis

### Status: Not Yet Available ⏳

The `polls_historical_long.csv` file does not exist yet. It will be created after running:
```bash
python polls_historical.py  # Scrape historical data
python process_polls_historical.py  # Process to long format
```

### Expected Analysis When Available

Once the historical polls data is available for years 2020-2025, check for the same naming patterns:

**Likely Mismatches to Look For:**
1. **Connecticut** vs **UConn**
2. **Louisiana State** vs **LSU**
3. **Southern California** vs **USC**
4. **Mississippi** vs **Ole Miss**
5. **North Carolina** vs **UNC**

**Additional teams to watch** (based on common variations):
- **Texas Christian** vs **TCU**
- **Brigham Young** vs **BYU**
- **Central Florida** vs **UCF**
- **Southern Methodist** vs **SMU**

---

## Recommendations

### 1. For Data Analysis

When joining datasets, use a standardization function:

```python
def standardize_team_name(name):
    """Standardize team names across datasets."""
    mappings = {
        'Connecticut': 'UConn',
        'Louisiana State': 'LSU',
        'Southern California': 'USC',
        'Mississippi': 'Ole Miss',
        'North Carolina': 'UNC',
        # Add more as needed
    }
    return mappings.get(name, name)

# Apply to dataframe
df['team'] = df['team'].apply(standardize_team_name)
```

### 2. For Data Pipeline

Consider adding a standardization step to the scrapers:

**Option A:** Add to `scrape_ratings.py`
- Standardize team names immediately after scraping
- Ensures consistency at the source

**Option B:** Create a separate standardization script
- `standardize_team_names.py` - Runs after scraping
- Updates all datasets to use consistent naming

**Option C:** Create a lookup table
- `team_name_mappings.csv` - Master reference
- All scripts reference this table for standardization

### 3. For Polls Historical Data

When the historical data becomes available:
1. Run the same comparison
2. Update the mappings dictionary
3. Apply standardization before or after processing to long format

---

## Data Quality Notes

### Why Different Names?

Different data sources use different naming conventions:

1. **Sports Reference** (ratings) - Uses full official names
   - Example: "Louisiana State", "Connecticut"

2. **AP/Coaches Polls** (polls) - Uses common abbreviations
   - Example: "LSU", "UConn"

3. **NCAA Official** - Varies by context
   - Some reports use full names, others use abbreviations

### Best Practice

For consistency across your analysis:
- ✅ **Use the polls naming convention** (UConn, LSU, USC, Ole Miss, UNC)
- ✅ These are more concise and widely recognized
- ✅ Easier to read in charts and tables

---

## Implementation Checklist

- [x] Identified mismatches in ratings dataset
- [x] Verified official names via web search
- [x] Created standardization mapping
- [ ] Apply standardization to ratings scraper
- [ ] Test with historical polls data (when available)
- [ ] Update team name mappings as needed
- [ ] Document any new variations discovered

---

## Sources

### Web Search Verification
- [UConn Women's Basketball](https://uconnhuskies.com/sports/womens-basketball) - University of Connecticut Athletics
- [UConn 2025-26 Team](https://en.wikipedia.org/wiki/2025%E2%80%9326_UConn_Huskies_women's_basketball_team) - Wikipedia
- [Ole Miss Women's Basketball](https://olemisssports.com/sports/womens-basketball) - Ole Miss Athletics
- [Ole Miss Rebels](https://en.wikipedia.org/wiki/Ole_Miss_Rebels_women's_basketball) - Wikipedia

---

**Report Generated:** 2026-01-23
**Script Used:** `compare_team_names.py`
**Next Update:** After polls_historical_long.csv becomes available
