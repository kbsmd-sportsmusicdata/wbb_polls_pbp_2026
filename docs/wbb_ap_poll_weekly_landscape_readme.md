# WBB AP Poll — Weekly Landscape

## Overview
This project analyzes **weekly AP Top 25 polls in NCAA Women’s Basketball** to surface movement, volatility, and conference context across the season. The goal is to move beyond static rankings and instead tell a **time‑aware story**: who is rising, who is falling, which movements are signal vs noise, and how conferences stack up in depth—not just headline teams.

This project is designed as a **portfolio‑ready Tableau dashboard** supported by a reproducible Python scraping pipeline and long‑format analytical tables.

---

## Core Questions
- How do AP rankings change week to week—and which teams move the most?
- Which teams are stable vs volatile across the season?
- Are conference reputations supported by depth and consistency, or driven by a few elite teams?
- How does a single team’s trajectory compare to the rest of the field over time?

---

## Data Sources
- **Sports‑Reference.com**
  - AP Women’s Basketball Polls (weekly)
  - NCAA Women’s Basketball standings

Data is scraped automatically via Python and refreshed through **GitHub Actions**.

---

## Data Pipeline
**Python (GitHub Actions)**
- Scrapes weekly AP poll tables
- Preserves raw snapshot CSVs per run (audit trail)
- Builds a **long‑format master table** (`polls_long.csv`)

**Key Fields Created**
- `team`
- `conference`
- `poll_week`
- `rank`
- `rank_numeric` (unranked → 26)
- `run_date`

---

## Dashboard Pages

### Page 1 — Weekly Landscape
**Purpose:** Orientation + macro context
- Snapshot Table (rank, movement, conference)
- Poll Rank Bump Chart (week‑over‑week movement)
- Upset Radar (unexpected jumps/drops)

### Page 2 — Momentum & Volatility
**Purpose:** Diagnose movement quality
- Biggest Movers (diverging bars)
- Volatility Index (consistency vs chaos scatter)

### Page 3 — Conference Context
**Purpose:** Structural comparison
- Conference Median Rank Over Time
- Rank Distribution / Depth View

### Page 4 — Team Profile
**Purpose:** Deep dive
- Team rank trajectory
- Mini stats panel (best, worst, average, volatility)
- Optional contextual tables (future expansion)

---

## Key Metrics & Concepts
- **Rank Change (WoW):** Current rank minus previous poll rank
- **Volatility:** Standard deviation of rank across weeks
- **Median Conference Rank:** Structural strength vs outliers
- **Unranked Handling:** Treated as rank 26 for continuity

---

## Design Principles
- Movement‑first storytelling (centered on zero)
- Consistent conference color encoding
- Long‑format data for flexible slicing
- Minimal text, insight‑forward annotations

---

## Tools Used
- Python (pandas, BeautifulSoup)
- GitHub Actions (automation)
- Tableau Public (visualization)

---

## Portfolio Notes
This project emphasizes:
- Reproducible data pipelines
- Analytical framing beyond raw rankings
- Dashboard narrative structure suitable for analysts, media, and front‑office audiences

---

## Future Enhancements
- Team quality tiers (Elo/NET‑like lens)
- Game result overlays on rank movement
- Conference strength normalization
- Tournament outcome simulations

---

## Author
**Krystal Beasley**  
Women’s Sports Analytics | NCAA WBB • WNBA • Softball

