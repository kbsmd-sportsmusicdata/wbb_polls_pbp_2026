# WBB AP Poll — Project Outline & Task Map

This document serves as a **working reference and planning scaffold** for development, QA, and future expansion of the WBB AP Poll analytics project.

---

## 1. Objectives
- Transform weekly AP polls into a time‑aware analytical dataset
- Highlight rank movement, volatility, and conference structure
- Deliver a clean, narrative‑driven Tableau Public dashboard
- Maintain full reproducibility via automated scraping

---

## 2. Data Architecture

### 2.1 Raw Data
- Source: Sports‑Reference AP Poll pages
- Storage:
  - `data/polls_1_YYYYMMDD.csv`
  - `data/polls_2_YYYYMMDD.csv`

### 2.2 Analytical Tables
- `polls_long.csv`
  - One row per team per poll week
  - Includes derived rank handling for unranked teams

---

## 3. Key Calculated Fields (Tableau)

### Rank Logic
- Current Rank (numeric, unranked = 26)
- Previous Week Rank (table calculation)
- Rank Change (WoW)

### Classification Fields
- Movement Category
- Ranked vs Unranked Flags
- Conference Grouping

---

## 4. Core Visuals

### 4.1 Snapshot Table
**Purpose:** Fast situational awareness
- Team
- Conference
- Current Rank
- Rank Change (WoW)
- Movement Category

Design notes:
- Sort by current rank
- Color by movement category
- Optional conference icons

---

### 4.2 Rank Change Bar Chart
**Purpose:** Emphasize magnitude + direction
- Diverging bars centered on zero
- Positive = rank improvement
- Negative = rank decline

Movement bands:
- Big Rise (≥ +5)
- Small Rise (+1 to +4)
- Flat (0)
- Drop (−1 to −4)
- Big Drop (≤ −5)

---

### 4.3 Volatility View
- Std Dev of Rank across weeks
- Scatter or bar ranking teams by stability

---

### 4.4 Conference Lens
- Median rank by conference over time
- Rank distribution per poll week

---

## 5. Tooltip & Annotation Strategy
- Explain *why* movement matters, not just what moved
- Use relative language (field percentile, conference context)
- Keep captions under 2 lines for Tableau Public readability

---

## 6. Automation & QA Checklist

### Pipeline
- [ ] Scraper runs without errors
- [ ] New poll weeks append correctly
- [ ] No duplicate rows

### Tableau
- [ ] Filters do not break table calcs
- [ ] Zero line centered consistently
- [ ] Unranked teams handled consistently

---

## 7. Publishing Checklist
- Dashboard description updated
- Annotations reviewed for clarity
- Data source linked
- README aligned with dashboard narrative

---

## 8. Future Expansion Ideas
- Rank momentum index (rolling avg change)
- Tournament seeding projection overlay
- Conference depth score
- Historical season comparisons

---

## 9. Reference
This outline complements the README and is intended for iterative use during development, QA, and portfolio reviews.

