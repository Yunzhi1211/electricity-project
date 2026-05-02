# -*- coding: utf-8 -*-
"""
1a_crawl_national_demand.py - National Demand Crawler

This script crawls monthly electricity demand bulletins and extracts
structured demand metrics from semi-structured webpage text.

Main workflow:
1. Read source file and auto-detect header row containing date and url.
2. Fetch webpage content with requests and parse plain text with BeautifulSoup.
3. Extract monthly and cumulative values with regex rules.
4. Backfill monthly values using cumulative differencing when direct monthly
    values are missing.
5. Export a normalized CSV with demand fields.
"""
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path

# Base configuration
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "0_raw_data"
OUTPUT_DIR = BASE_DIR / "4_output_anylogic"

YEAR_START = 2010
YEAR_END = 2025

HEADERS = {"User-Agent": "Mozilla/5.0"}

FIELDS = {
    "total_demand": ["\u5168\u793e\u4f1a\u7528\u7535\u91cf"],
    "primary_demand": ["\u7b2c\u4e00\u4ea7\u4e1a\u7528\u7535\u91cf", "\u7b2c\u4e00\u4ea7\u4e1a"],
    "secondary_demand": ["\u7b2c\u4e8c\u4ea7\u4e1a\u7528\u7535\u91cf", "\u7b2c\u4e8c\u4ea7\u4e1a"],
    "tertiary_demand": ["\u7b2c\u4e09\u4ea7\u4e1a\u7528\u7535\u91cf", "\u7b2c\u4e09\u4ea7\u4e1a"],
    "residential_demand": ["\u57ce\u4e61\u5c45\u6c11\u751f\u6d3b\u7528\u7535\u91cf", "\u57ce\u4e61\u5c45\u6c11\u751f\u6d3b", "\u5c45\u6c11\u751f\u6d3b\u7528\u7535\u91cf", "\u57ce\u4e61\u5c45\u6c11\u7528\u7535\u91cf"]
}


def read_source_file(path):
    if path.lower().endswith(".csv"):
        df = pd.read_csv(path)
        df.columns = [str(c).strip().lower() for c in df.columns]
        return df

    raw = pd.read_excel(path, header=None)
    header_row = None
    for i in range(len(raw)):
        row = [str(x).strip().lower() for x in raw.iloc[i].tolist() if pd.notna(x)]
        if {"date", "url"}.issubset(set(row)):
            header_row = i
            break
    if header_row is None:
        raise ValueError("Header row not found (requires at least date and url)")

    df = pd.read_excel(path, header=header_row)
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def fetch_text(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text("\n")
        text = re.sub(r"[ \t\r\xa0]+", " ", text)
        text = re.sub(r"\n+", "\n", text)
        return text.strip()
    except Exception as e:
        print(f"[ERROR] {url} -> {e}")
        return ""


def normalize_num(x, unit):
    v = float(str(x).replace(",", "").replace("，", ""))
    return round(v * 10000, 3) if unit == "\u4e07\u4ebf" else round(v, 3)


def extract_field(text, aliases):
    for alias in aliases:
        m = re.search(rf"{re.escape(alias)}[^0-9]{{0,8}}([0-9]+(?:\.[0-9]+)?)\s*(\u4e07\u4ebf|\u4ebf)\u5343\u74e6\u65f6", text)
        if m:
            return normalize_num(m.group(1), m.group(2))
    return None


def extract_all_fields(text):
    out = {}
    for k, aliases in FIELDS.items():
        out[k] = extract_field(text, aliases)
    return out


def get_windows(text, prefixes, window=260):
    blocks = []
    for p in prefixes:
        for m in re.finditer(p, text):
            start = m.start()
            blocks.append(text[start:start + window])
    return blocks


def extract_monthly_direct(text, month):
    # Extract direct monthly values for total and category fields.
    prefixes = [
        rf"{month}\u6708\u4efd",
        rf"{month}\u6708",
        r"\u5f53\u6708",
        r"\u672c\u6708"
    ]
    blocks = get_windows(text, prefixes, window=320)

    best = {k: None for k in FIELDS}

    for b in blocks:
        # If the block looks cumulative, avoid taking category values.
        has_cum = bool(re.search(rf"1[-—~\u81f3\u5230]{month}\u6708|\u7d2f\u8ba1|\u5168\u5e74", b))

        # Prefer extracting total demand first.
        if best["total_demand"] is None:
            best["total_demand"] = extract_field(b, FIELDS["total_demand"])

        # Extract category values only in likely non-cumulative blocks.
        if not has_cum:
            for k in ["primary_demand", "secondary_demand", "tertiary_demand", "residential_demand"]:
                if best[k] is None:
                    best[k] = extract_field(b, FIELDS[k])

    return best


def extract_cumulative(text, month):
    prefixes = [
        rf"1[-—~\u81f3\u5230]{month}\u6708\u7d2f\u8ba1",
        rf"1[-—~\u81f3\u5230]{month}\u6708",
    ]
    if month == 12:
        prefixes += [r"\u5168\u5e74", r"1-12\u6708", r"1—12\u6708", r"1\u81f312\u6708"]

    blocks = get_windows(text, prefixes, window=420)
    best = {k: None for k in FIELDS}

    for b in blocks:
        vals = extract_all_fields(b)
        for k in FIELDS:
            if best[k] is None and vals[k] is not None:
                best[k] = vals[k]

    return best


def diff_data(curr, prev):
    out = {}
    for k in FIELDS:
        a = curr.get(k)
        b = prev.get(k) if prev else None
        out[k] = round(a - b, 3) if a is not None and b is not None else None
    return out


def crawl_power_data(source_file, output_csv="power_monthly_clean.csv", sleep_sec=0.3):
    src = read_source_file(source_file)
    print("Detected columns:", src.columns.tolist())

    if "type" not in src.columns:
        src["type"] = "\u5f53\u6708"

    src = src[~src["date"].isna()].copy()
    src["date"] = pd.to_datetime(src["date"], errors="coerce")
    src = src[~src["date"].isna()].copy()
    src = src.sort_values("date").reset_index(drop=True)

    rows = []
    cumulative_cache = {}   # Yearly cache for cumulative values.

    for i, r in src.iterrows():
        date = r["date"]
        year = date.year
        month = date.month
        url = str(r["url"]).strip() if pd.notna(r["url"]) else ""

        row = {
            "date": date.strftime("%Y-%m-%d"),
            "total_demand": None,
            "primary_demand": None,
            "secondary_demand": None,
            "tertiary_demand": None,
            "residential_demand": None
        }

        if not url or url.lower() == "nan" or url == "\u65e0":
            rows.append(row)
            continue

        print(f"[{i+1}/{len(src)}] {row['date']}")

        text = fetch_text(url)
        if not text:
            rows.append(row)
            continue

        # 1) Try direct monthly extraction first.
        monthly_direct = extract_monthly_direct(text, month)

        # 2) Then extract cumulative values.
        cumulative = extract_cumulative(text, month)

        # 3) Start with direct monthly total demand.
        row["total_demand"] = monthly_direct["total_demand"]

        # 4) For category values, prefer direct monthly values; otherwise use cumulative differencing.
        prev_cum = cumulative_cache.get(year)

        monthly_from_cum = diff_data(cumulative, prev_cum) if (prev_cum and cumulative["total_demand"] is not None) else {k: None for k in FIELDS}

        for k in ["primary_demand", "secondary_demand", "tertiary_demand", "residential_demand"]:
            row[k] = monthly_direct[k] if monthly_direct[k] is not None else monthly_from_cum[k]

        # 5) If direct total is missing, backfill from cumulative differencing.
        if row["total_demand"] is None and monthly_from_cum["total_demand"] is not None:
            row["total_demand"] = monthly_from_cum["total_demand"]

        # 6) Update yearly cumulative cache.
        if cumulative["total_demand"] is not None:
            cumulative_cache[year] = cumulative

        rows.append(row)
        time.sleep(sleep_sec)

    df = pd.DataFrame(rows)[[
        "date",
        "total_demand",
        "primary_demand",
        "secondary_demand",
        "tertiary_demand",
        "residential_demand"
    ]]

    # Keep only the AnyLogic modeling window (2010-2025).
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()].copy()
    df = df[(df["date"].dt.year >= YEAR_START) & (df["date"].dt.year <= YEAR_END)].copy()
    df = df.sort_values("date").reset_index(drop=True)
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"saved -> {output_csv}")
    return df


if __name__ == "__main__":
    # Use the URL source file under 0_raw_data.
    source_file = DATA_DIR / "nea_urls_manual.csv.xlsx"
    output_file = OUTPUT_DIR / "demand_crawl.csv"
    
    if source_file.exists():
        df = crawl_power_data(str(source_file), str(output_file))
        print(df.head(20))
    else:
        print(f"Source file not found: {source_file}")
