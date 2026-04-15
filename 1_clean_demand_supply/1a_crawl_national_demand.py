# -*- coding: utf-8 -*-
"""
1a_crawl_national_demand.py - 国家能源局用电量数据爬取

面向国家能源月度用电公告的半结构化文本爬虫脚本，核心是：
(1)读取源文件：
自动识别输入文件中的表头，要求至少包含date, url
(2)抓网页正文：
使用 requests 请求网页，再用 BeautifulSoup 提取纯文本内容。
(3)正则提取当月/累计用电量：
根据预设关键词如"全社会用电量""第一产业用电量"等，在网页正文中搜索对应数值，并识别单位。若是"万亿"，统一换算成"亿千瓦时"。
(4)用累计差分补齐月度值：
先在"X月份 / 当月 / 本月"等上下文附近提取当月数据。
若网页没有直接给出当月分项数据，则尝试提取"1—X月累计"数据，并用：本月值 = 本月累计值 - 上月累计值的方式反推出当月数据
(5)输出标准化表格：
将整理后的结果保存为标准化 CSV，
字段包括：date, total_demand, primary_demand, secondary_demand, tertiary_demand, residential_demand
"""
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path

# 基础配置
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "0_raw_data"
OUTPUT_DIR = BASE_DIR / "cleaned_data"

HEADERS = {"User-Agent": "Mozilla/5.0"}

FIELDS = {
    "total_demand": ["全社会用电量"],
    "primary_demand": ["第一产业用电量", "第一产业"],
    "secondary_demand": ["第二产业用电量", "第二产业"],
    "tertiary_demand": ["第三产业用电量", "第三产业"],
    "residential_demand": ["城乡居民生活用电量", "城乡居民生活", "居民生活用电量", "城乡居民用电量"]
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
        raise ValueError("没找到表头行（至少需要 date 和 url）")

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
    return round(v * 10000, 3) if unit == "万亿" else round(v, 3)


def extract_field(text, aliases):
    for alias in aliases:
        m = re.search(rf"{re.escape(alias)}[^0-9]{{0,8}}([0-9]+(?:\.[0-9]+)?)\s*(万亿|亿)千瓦时", text)
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
    # 当月总量 / 当月分项
    prefixes = [
        rf"{month}月份",
        rf"{month}月",
        r"当月",
        r"本月"
    ]
    blocks = get_windows(text, prefixes, window=320)

    best = {k: None for k in FIELDS}

    for b in blocks:
        # 如果块里明确出现累计，就只拿总量，不轻易拿分项
        has_cum = bool(re.search(rf"1[-—~至到]{month}月|累计|全年", b))

        # 总量优先抓
        if best["total_demand"] is None:
            best["total_demand"] = extract_field(b, FIELDS["total_demand"])

        # 分项只有在明显不是累计块时才抓
        if not has_cum:
            for k in ["primary_demand", "secondary_demand", "tertiary_demand", "residential_demand"]:
                if best[k] is None:
                    best[k] = extract_field(b, FIELDS[k])

    return best


def extract_cumulative(text, month):
    prefixes = [
        rf"1[-—~至到]{month}月累计",
        rf"1[-—~至到]{month}月",
    ]
    if month == 12:
        prefixes += [r"全年", r"1-12月", r"1—12月", r"1至12月"]

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
    print("识别到的列名：", src.columns.tolist())

    if "type" not in src.columns:
        src["type"] = "当月"

    src = src[~src["date"].isna()].copy()
    src["date"] = pd.to_datetime(src["date"], errors="coerce")
    src = src[~src["date"].isna()].copy()
    src = src.sort_values("date").reset_index(drop=True)

    rows = []
    cumulative_cache = {}   # 每年累计缓存

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

        if not url or url.lower() == "nan" or url == "无":
            rows.append(row)
            continue

        print(f"[{i+1}/{len(src)}] {row['date']}")

        text = fetch_text(url)
        if not text:
            rows.append(row)
            continue

        # 1) 先抓当月直接值
        monthly_direct = extract_monthly_direct(text, month)

        # 2) 再抓累计值
        cumulative = extract_cumulative(text, month)

        # 3) 先放当月总量
        row["total_demand"] = monthly_direct["total_demand"]

        # 4) 分项：优先当月直接值；否则用累计差分
        prev_cum = cumulative_cache.get(year)

        monthly_from_cum = diff_data(cumulative, prev_cum) if (prev_cum and cumulative["total_demand"] is not None) else {k: None for k in FIELDS}

        for k in ["primary_demand", "secondary_demand", "tertiary_demand", "residential_demand"]:
            row[k] = monthly_direct[k] if monthly_direct[k] is not None else monthly_from_cum[k]

        # 5) 如果总量当月没抓到，也允许用累计差分补
        if row["total_demand"] is None and monthly_from_cum["total_demand"] is not None:
            row["total_demand"] = monthly_from_cum["total_demand"]

        # 6) 更新累计缓存
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

    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"saved -> {output_csv}")
    return df


if __name__ == "__main__":
    # 使用 0_raw_data 中的 URL 文件
    source_file = DATA_DIR / "nea_urls_manual.csv.xlsx"
    output_file = OUTPUT_DIR / "demand_crawl.csv"
    
    if source_file.exists():
        df = crawl_power_data(str(source_file), str(output_file))
        print(df.head(20))
    else:
        print(f"源文件不存在: {source_file}")
