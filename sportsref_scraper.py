import io
import os
import re
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment

# Paths / constants
DATA_DIR = Path("data")
STANDINGS_SPLIT_DIR = DATA_DIR / "standings_by_conf"
STANDINGS_COMBINED_DIR = DATA_DIR / "standings_full"

for p in (STANDINGS_SPLIT_DIR, STANDINGS_COMBINED_DIR):
    p.mkdir(parents=True, exist_ok=True)

URL_POLLS = "https://www.sports-reference.com/cbb/seasons/women/2026-polls.html"
URL_STANDINGS = "https://www.sports-reference.com/cbb/seasons/women/2026-standings.html"

OUTPUT_DIR = "data"
MASTER_POLLS_LONG = DATA_DIR / "polls_long.csv"
MASTER_STANDINGS_LONG = DATA_DIR / "standings_long.csv"

def append_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if df is None or df.empty:
        print(f"[append_csv] No rows to append for {path}. Skipping.")
        return
    if path.exists():
        df.to_csv(path, mode="a", header=False, index=False)
    else:
        df.to_csv(path, mode="w", header=True, index=False)

def strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    return df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))

def clean_tables(tables: list[pd.DataFrame]) -> list[pd.DataFrame]:
    cleaned = []
    for df in tables:
        df = strip_strings(df)
        df.dropna(how="all", inplace=True)
        cleaned.append(df)
    return cleaned

def normalize_col(c) -> str:
    if isinstance(c, (tuple, list)):
        parts = [str(x).strip() for x in c if str(x).strip() and str(x).strip().lower() != "nan"]
        return parts[-1] if parts else ""
    return str(c).strip()

def find_col(cols, candidates: set[str]):
    for c in cols:
        if normalize_col(c).lower() in candidates:
            return c
    return None

def is_week_col(col_label) -> bool:
    s = normalize_col(col_label)
    s_low = s.lower()
    if s_low in {"rk", "rank", "prev", "previous", "chng", "change", "conf", "conference", "school", "team"}:
        return False
    if s_low in {"pre", "preseason"}:
        return True
    if re.match(r"^\d{1,2}[\/\-.]\d{1,2}$", s):
        return True
    if any(ch.isdigit() for ch in s) and len(s) <= 10:
        if len(re.findall(r"\d", s)) >= 2:
            return True
    return False

def fetch_tables(url: str) -> list[pd.DataFrame]:
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    html_buf = io.StringIO(resp.text)
    tables = pd.read_html(html_buf)
    return clean_tables(tables)

def fetch_standings_tables(url: str) -> list[pd.DataFrame]:
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    html = str(soup)
    try:
        tables = pd.read_html(html)
        if tables:
            return clean_tables(tables)
    except ValueError:
        pass
    comment_tables = []
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment_str = str(comment)
        if "<table" not in comment_str:
            continue
        try:
            dfs = pd.read_html(comment_str)
            comment_tables.extend(dfs)
        except ValueError:
            continue
    if not comment_tables:
        raise ValueError("No standings tables found in page or HTML comments")
    return clean_tables(comment_tables)

def save_tables(tables: list[pd.DataFrame], prefix: str) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d")
    for i, df in enumerate(tables, start=1):
        path = os.path.join(OUTPUT_DIR, f"{prefix}_{i}_{ts}.csv")
        df.to_csv(path, index=False)
        print(f"Saved: {path}")

def save_standings_tables(tables: list[pd.DataFrame], date_str: str) -> None:
    cleaned_tables = []
    for i, df in enumerate(tables, start=1):
        cleaned_tables.append(df)
        out_path = STANDINGS_SPLIT_DIR / f"standings_{i}_{date_str}.csv"
        df.to_csv(out_path, index=False)
        print(f"Saved conference {i} standings: {out_path}")
    combined = pd.concat(cleaned_tables, ignore_index=True)
    combined_path = STANDINGS_COMBINED_DIR / f"standings_all_{date_str}.csv"
    combined.to_csv(combined_path, index=False)
    print(f"Saved combined standings: {combined_path}")

def build_polls_long(polls_tables: list[pd.DataFrame], run_date: date) -> pd.DataFrame:
    long_parts = []
    run_date_str = run_date.strftime("%Y-%m-%d")
    for t_idx, df in enumerate(polls_tables, start=1):
        df = df.copy()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(-1)
        df.columns = [str(c).strip() for c in df.columns]
        cols = list(df.columns)
        school_col = find_col(cols, {"school", "team"})
        conf_col = find_col(cols, {"conf", "conference"})
        if school_col is None:
            continue
        df = df[df[school_col].notna()]
        df = df[df[school_col].str.lower() != "school"]
        has_week_cols = any(is_week_col(c) for c in cols)
        if not has_week_cols:
            continue
        id_vars = [school_col]
        if conf_col is not None:
            id_vars.append(conf_col)
        value_vars = [c for c in cols if (c not in id_vars and is_week_col(c))]
        if not value_vars:
            continue
        melted = df.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name="poll_week",
            value_name="rank",
        )
        melted["poll_week"] = melted["poll_week"].map(normalize_col)
        rename_map = {school_col: "team"}
        if conf_col is not None:
            rename_map[conf_col] = "conference"
        melted.rename(columns=rename_map, inplace=True)
        melted["rank"] = pd.to_numeric(melted["rank"], errors="coerce")
        melted["run_date"] = run_date_str
        melted["table_id"] = t_idx
        long_parts.append(melted)
    if not long_parts:
        return pd.DataFrame(columns=["team","conference","poll_week","rank","run_date","table_id","rank_numeric"])
    out = pd.concat(long_parts, ignore_index=True)
    if "conference" not in out.columns:
        out["conference"] = None
    out["rank_numeric"] = out["rank"].fillna(26).astype(int)
    out = out[["team","conference","poll_week","rank","run_date","table_id","rank_numeric"]]
    return out

def main() -> None:
    today = date.today()
    today_str = today.strftime("%Y%m%d")
    print("Fetching polls data...")
    polls = fetch_tables(URL_POLLS)
    save_tables(polls, "polls")
    polls_long = build_polls_long(polls, run_date=today)
    append_csv(polls_long, MASTER_POLLS_LONG)
    print(f"polls_long rows built: {len(polls_long)}")
    print("Fetching standings data...")
    standings = fetch_standings_tables(URL_STANDINGS)
    save_standings_tables(standings, today_str)

if __name__ == "__main__":
    main()
