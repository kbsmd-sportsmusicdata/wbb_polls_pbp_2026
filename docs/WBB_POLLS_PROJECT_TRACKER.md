# WBB AP Poll - Project Tracker

**Timeline:** 4 weeks • ~25 hours total
**Last Updated:** 2026-01-28

---

## Overall Progress: 0/38 tasks (0%)

| Phase | Progress | Status |
|-------|----------|--------|
| Phase 1: Data Pipeline | 0/12 (0%) | ⬜ Not Started |
| Phase 2: Tableau Build | 0/16 (0%) | ⬜ Not Started |
| Phase 3: Automation & QA | 0/6 (0%) | ⬜ Not Started |
| Phase 4: Publishing | 0/4 (0%) | ⬜ Not Started |

---

## Phase 1: Data Pipeline (Week 1) — 8 hrs

### Repository Setup

- [ ] **Set Up Repository Structure** `HIGH`
  - Create wbb_polls_pbp_2026/ with subdirectories
  - _data/, scripts/, docs/, tableau/_

- [ ] **Install Dependencies**
  - Set up Python environment
  - _pandas, requests, beautifulsoup4, lxml_

- [ ] **Create .gitignore**
  - Exclude raw CSVs, credentials, temp files

### Scraper Development

- [ ] **Build AP Poll Scraper Script** `HIGH`
  - Scrape Sports-Reference AP Poll pages
  - `scripts/scrape_polls.py`

- [ ] **Store Raw Poll Data**
  - Save as `data/polls_1_YYYYMMDD.csv`, `data/polls_2_YYYYMMDD.csv`
  - _One file per poll release_

- [ ] **Build Analytical Table Generator** `HIGH`
  - Create `polls_long.csv` (one row per team per poll week)
  - `scripts/build_polls_long.py`

### Calculated Fields

- [ ] **Implement Current Rank Logic**
  - Numeric rank, unranked = 26
  - `scripts/metrics.py`

- [ ] **Implement Previous Week Rank**
  - Table calculation / lag logic
  - `scripts/metrics.py`

- [ ] **Implement Rank Change (WoW)**
  - Week-over-week rank delta
  - `scripts/metrics.py`

- [ ] **Implement Movement Category**
  - Big Rise (≥+5), Small Rise (+1 to +4), Flat (0), Drop (-1 to -4), Big Drop (≤-5)
  - `scripts/metrics.py`

- [ ] **Implement Ranked/Unranked Flags**
  - Boolean flags for filtering
  - `scripts/metrics.py`

- [ ] **Implement Conference Grouping**
  - Conference assignment per team
  - `scripts/metrics.py`

---

## Phase 2: Tableau Build (Week 2-3) — 12 hrs

### Core Visuals

- [ ] **Snapshot Table View** `HIGH`
  - Team, Conference, Current Rank, Rank Change, Movement Category
  - _Sort by current rank, color by movement category_

- [ ] **Rank Change Bar Chart** `HIGH`
  - Diverging bars centered on zero
  - _Positive = rank improvement, Negative = rank decline_

- [ ] **Volatility View** `HIGH`
  - Std Dev of Rank across weeks
  - _Scatter or bar ranking teams by stability_

- [ ] **Conference Lens View**
  - Median rank by conference over time
  - _Rank distribution per poll week_

### Filters & Interactivity

- [ ] **Poll Week Selector**
  - Dropdown to select which poll week to display
  - _Show date in selector_

- [ ] **Conference Filter**
  - Filter by conference grouping
  - _Multi-select enabled_

- [ ] **Movement Category Filter**
  - Filter by Big Rise, Drop, etc.
  - _Quick highlight of movers_

- [ ] **Team Search/Highlight**
  - Search or select specific teams
  - _Highlight across all views_

### Design Standards

- [ ] **Movement Color Encoding**
  - Green for rises, red for drops, gray for flat
  - _Consistent across all views_

- [ ] **Diverging Bar Styling**
  - Zero line centered consistently
  - _Movement bands visually distinct_

- [ ] **Clear Visual Hierarchy**
  - Title > Section > Chart > Annotation
  - _Font sizes: 24pt → 18pt → 12pt → 10pt_

- [ ] **Mobile-Friendly Layout**
  - Test on phone viewport
  - _Consider vertical stacking_

### Annotations & Storytelling

- [ ] **Tooltip Strategy** `HIGH`
  - Explain *why* movement matters, not just what moved
  - _Use relative language (conference context)_

- [ ] **Caption Guidelines**
  - Keep captions under 2 lines
  - _Tableau Public readability_

- [ ] **Movement Context Annotations**
  - Call out notable rises/drops
  - _Conference context where relevant_

- [ ] **Volatility Insights**
  - Highlight most/least stable teams
  - _Narrative for portfolio value_

---

## Phase 3: Automation & QA (Week 4) — 3 hrs

### Pipeline QA

- [ ] **Scraper Error Handling** `HIGH`
  - Scraper runs without errors
  - _Handle missing weeks, network failures_

- [ ] **Append Logic Validation**
  - New poll weeks append correctly
  - _No duplicate rows_

- [ ] **Unranked Team Handling**
  - Consistent treatment (rank = 26)
  - _Verify in all views_

### Tableau QA

- [ ] **Filter Integrity Check**
  - Filters do not break table calcs
  - _Test all filter combinations_

- [ ] **Zero Line Verification**
  - Zero line centered consistently in bar chart
  - _Check edge cases_

- [ ] **Cross-View Consistency**
  - Same teams, same colors, same logic across views
  - _No contradictions_

---

## Phase 4: Publishing (Week 4) — 2 hrs

- [ ] **Publish to Tableau Public** `HIGH`
  - Upload dashboard with public sharing enabled
  - _Test all interactions live_

- [ ] **Update Dashboard Description**
  - Clear description for Tableau Public listing
  - _Include data source attribution_

- [ ] **Update README** `HIGH`
  - Align README with dashboard narrative
  - _Include screenshots_

- [ ] **Portfolio Integration**
  - Link to portfolio site
  - _Add to project showcase_

---

## Success Criteria

### Technical
- [ ] Scraper runs without manual intervention
- [ ] New poll weeks append without duplicates
- [ ] Unranked teams handled consistently (rank = 26)
- [ ] All Tableau filters work correctly

### Portfolio Value
- [ ] Non-technical viewer understands rank movement in <30s
- [ ] Demonstrates calculated fields (not just raw ranks)
- [ ] Shows conference context for every team
- [ ] Dashboard tells a story, not just displays data

---

## Future Expansion Ideas

- [ ] Rank momentum index (rolling avg change)
- [ ] Tournament seeding projection overlay
- [ ] Conference depth score
- [ ] Historical season comparisons

---

## Notes

_Add session notes, blockers, and decisions here:_

### 2026-01-28
- Project tracker created
- Outline reviewed and tasks mapped to 4 phases

