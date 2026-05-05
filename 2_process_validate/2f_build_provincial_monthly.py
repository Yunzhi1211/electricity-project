#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
2f_build_provincial_monthly.py
Build provincial monthly generation series and extend to 2035.

Historical disaggregation:
  share(p,k,y) = AnnualProv(p,k,y) / sum_p AnnualProv(p,k,y)
  Prov(p,k,y,m) = share(p,k,y) * CN(k,y,m)

Forecast extension (2025-2035):
  1) Forecast provincial annual shares by linear trend on 2015-2024 shares.
  2) Normalize shares by year so they sum to 1 across provinces.
  3) Multiply by national annual targets (total from forecast_supply.csv;
     thermal/hydro inferred from national historical share trends).
  4) Distribute annual provincial values to months using:
     - total_gen: national monthly weights from historical+forecast total.
     - thermal/hydro: monthly climatology from 2018-2024.

Output:
  - 4_output_anylogic/province_generation_monthly_2010_2035.csv
"""

from __future__ import annotations

from pathlib import Path
import csv
import re

import numpy as np
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "0_raw_data"
OUT_DIR = BASE_DIR / "4_output_anylogic"
NATIONAL_MONTHLY_FILE = (
    BASE_DIR
    / "5_anylogic_model"
    / "ElectricityTrial_-_Version 7_-_Sources"
    / "1_generation_data.xlsx"
)
TOTAL_FORECAST_FILE = OUT_DIR / "forecast_supply.csv"

ANNUAL_FILES = {
    "total_gen_annual": RAW_DIR / "发电量分省年度数据.csv",
    "thermal_gen_annual": RAW_DIR / "火力分省年度数据.csv",
    "hydro_gen_annual": RAW_DIR / "水力发电量分省年度数据.csv",
}

HIST_YEAR_START = 2010
HIST_YEAR_END = 2024
FORECAST_YEAR_START = 2025
FORECAST_YEAR_END = 2035
OUTPUT_FILE = OUT_DIR / f"province_generation_monthly_{HIST_YEAR_START}_{FORECAST_YEAR_END}.csv"
ANYLOGIC_OUTPUT_FILE = (
    BASE_DIR
    / "5_anylogic_model"
    / "ElectricityTrial_-_Version 7_-_Sources"
    / OUTPUT_FILE.name
)

PROVINCE_ISO = {
    "北京市": "CN-11",
    "天津市": "CN-12",
    "河北省": "CN-13",
    "山西省": "CN-14",
    "内蒙古自治区": "CN-15",
    "辽宁省": "CN-21",
    "吉林省": "CN-22",
    "黑龙江省": "CN-23",
    "上海市": "CN-31",
    "江苏省": "CN-32",
    "浙江省": "CN-33",
    "安徽省": "CN-34",
    "福建省": "CN-35",
    "江西省": "CN-36",
    "山东省": "CN-37",
    "河南省": "CN-41",
    "湖北省": "CN-42",
    "湖南省": "CN-43",
    "广东省": "CN-44",
    "广西壮族自治区": "CN-45",
    "海南省": "CN-46",
    "重庆市": "CN-50",
    "四川省": "CN-51",
    "贵州省": "CN-52",
    "云南省": "CN-53",
    "西藏自治区": "CN-54",
    "陕西省": "CN-61",
    "甘肃省": "CN-62",
    "青海省": "CN-63",
    "宁夏回族自治区": "CN-64",
    "新疆维吾尔自治区": "CN-65",
}


def _to_float(raw: str) -> float | None:
    s = str(raw).replace("\t", "").replace(",", "").strip()
    if s in {"", "-", "--", "…"}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _trend_predict(years: np.ndarray, values: np.ndarray, target_year: int) -> float:
    """Simple robust trend predictor used for share extrapolation."""
    msk = np.isfinite(values)
    years = years[msk]
    values = values[msk]
    if len(values) == 0:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    if len(values) < 4:
        return float(values[-1])
    x = years - years.min()
    a, b = np.polyfit(x, values, 1)
    x_new = target_year - years.min()
    return float(a * x_new + b)


def read_provincial_annual_csv(path: Path, value_col: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing annual file: {path}")

    lines = path.read_text(encoding="utf-8-sig").splitlines()
    header_idx = next((i for i, line in enumerate(lines) if "地区" in line), None)
    if header_idx is None:
        raise ValueError(f"Cannot find header row with '地区' in: {path}")

    reader = csv.reader(lines[header_idx:], delimiter=",")
    rows = list(reader)
    if not rows:
        raise ValueError(f"No rows parsed from: {path}")

    header = [c.replace("\t", "").strip() for c in rows[0]]
    year_pos: list[tuple[int, int]] = []
    for idx, col in enumerate(header):
        m = re.search(r"(20\d{2})", col)
        if m:
            year_pos.append((idx, int(m.group(1))))

    records: list[dict] = []
    for row in rows[1:]:
        if not row:
            continue
        province = (row[0] if len(row) > 0 else "").replace("\t", "").strip()
        if not province:
            continue
        if "数据来源" in province:
            break

        for idx, year in year_pos:
            value_raw = row[idx] if idx < len(row) else ""
            records.append(
                {
                    "province_name_cn": province,
                    "year": int(year),
                    value_col: _to_float(value_raw),
                }
            )

    out = pd.DataFrame(records)
    out = out[(out["year"] >= HIST_YEAR_START) & (out["year"] <= HIST_YEAR_END)].copy()
    return out


def read_national_monthly(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing national monthly file: {path}")

    df = pd.read_excel(path)
    need = ["date", "total_gen", "thermal_gen", "hydro_gen"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ValueError(f"National monthly file missing columns: {missing}")

    out = df[need].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"]).copy()
    out["year"] = out["date"].dt.year
    out["month"] = out["date"].dt.month
    out = out[(out["year"] >= HIST_YEAR_START) & (out["year"] <= 2025)].copy()
    out = (
        out.groupby(["year", "month"], as_index=False)
        .agg({"date": "min", "total_gen": "mean", "thermal_gen": "mean", "hydro_gen": "mean"})
        .sort_values(["year", "month"])
        .reset_index(drop=True)
    )
    return out


def read_total_monthly_target(national_monthly: pd.DataFrame) -> pd.DataFrame:
    hist = national_monthly[["date", "year", "month", "total_gen"]].copy()

    if not TOTAL_FORECAST_FILE.exists():
        raise FileNotFoundError(
            f"Missing total forecast file: {TOTAL_FORECAST_FILE}. Run 2e_forecast.py first."
        )
    fc = pd.read_csv(TOTAL_FORECAST_FILE)
    fc["date"] = pd.to_datetime(fc["date"], errors="coerce")
    fc = fc.dropna(subset=["date"]).copy()
    fc["year"] = fc["date"].dt.year
    fc["month"] = fc["date"].dt.month
    fc = fc.rename(columns={"forecast": "total_gen"})
    fc = fc[(fc["year"] >= 2026) & (fc["year"] <= FORECAST_YEAR_END)][["date", "year", "month", "total_gen"]]

    out = pd.concat([hist, fc], ignore_index=True)
    out = out[(out["year"] >= HIST_YEAR_START) & (out["year"] <= FORECAST_YEAR_END)].copy()
    return out.sort_values(["year", "month"]).reset_index(drop=True)


def annual_from_monthly(monthly: pd.DataFrame, col: str) -> pd.DataFrame:
    return (
        monthly.groupby("year", as_index=False)[col]
        .sum()
        .rename(columns={col: f"{col}_annual"})
    )


def forecast_national_thermal_hydro_annual(
    national_monthly: pd.DataFrame,
    annual_total_target: pd.DataFrame,
) -> pd.DataFrame:
    hist_annual = (
        national_monthly.groupby("year", as_index=False)[["total_gen", "thermal_gen", "hydro_gen"]]
        .sum()
        .rename(
            columns={
                "total_gen": "total_gen_annual_hist",
                "thermal_gen": "thermal_gen_annual_hist",
                "hydro_gen": "hydro_gen_annual_hist",
            }
        )
    )
    hist_annual["thermal_share"] = (
        hist_annual["thermal_gen_annual_hist"] / hist_annual["total_gen_annual_hist"]
    )
    hist_annual["hydro_share"] = (
        hist_annual["hydro_gen_annual_hist"] / hist_annual["total_gen_annual_hist"]
    )

    years = hist_annual["year"].to_numpy(dtype=float)
    thermal_s = hist_annual["thermal_share"].to_numpy(dtype=float)
    hydro_s = hist_annual["hydro_share"].to_numpy(dtype=float)

    rows = []
    for year in range(FORECAST_YEAR_START, FORECAST_YEAR_END + 1):
        tgt_total = float(annual_total_target.loc[annual_total_target["year"] == year, "total_gen_annual"].iloc[0])
        th = _trend_predict(years, thermal_s, year)
        hy = _trend_predict(years, hydro_s, year)
        th = max(0.10, min(0.95, th))
        hy = max(0.01, min(0.80, hy))
        if th + hy > 0.98:
            scale = 0.98 / (th + hy)
            th *= scale
            hy *= scale
        rows.append(
            {
                "year": year,
                "total_gen_annual": tgt_total,
                "thermal_gen_annual": tgt_total * th,
                "hydro_gen_annual": tgt_total * hy,
            }
        )
    return pd.DataFrame(rows)


def forecast_provincial_annual_from_shares(
    annual_hist: pd.DataFrame,
    national_annual_targets: pd.DataFrame,
) -> pd.DataFrame:
    out = []
    for metric in ["total_gen_annual", "thermal_gen_annual", "hydro_gen_annual"]:
        hist = annual_hist[["province_name_cn", "year", metric]].copy()
        hist = hist.merge(
            national_annual_targets[["year", metric]].rename(columns={metric: f"national_{metric}"}),
            on="year",
            how="left",
        )
        hist["share"] = hist[metric] / hist[f"national_{metric}"].replace(0, np.nan)

        for year in range(FORECAST_YEAR_START, FORECAST_YEAR_END + 1):
            preds = []
            for prov in PROVINCE_ISO.keys():
                s = hist[hist["province_name_cn"] == prov].sort_values("year")
                s = s[s["year"] >= 2015]
                pred = _trend_predict(
                    s["year"].to_numpy(dtype=float),
                    s["share"].to_numpy(dtype=float),
                    year,
                )
                preds.append({"province_name_cn": prov, "pred_share": max(0.0, pred)})

            pred_df = pd.DataFrame(preds)
            total = pred_df["pred_share"].sum()
            if total <= 0:
                pred_df["share"] = 1.0 / len(pred_df)
            else:
                pred_df["share"] = pred_df["pred_share"] / total

            nat_val = float(
                national_annual_targets.loc[national_annual_targets["year"] == year, metric].iloc[0]
            )
            pred_df["year"] = year
            pred_df[metric] = pred_df["share"] * nat_val
            out.append(pred_df[["province_name_cn", "year", metric]])

    total_df = pd.concat([x for x in out if "total_gen_annual" in x.columns], ignore_index=True)
    thermal_df = pd.concat([x for x in out if "thermal_gen_annual" in x.columns], ignore_index=True)
    hydro_df = pd.concat([x for x in out if "hydro_gen_annual" in x.columns], ignore_index=True)
    fc = total_df.merge(thermal_df, on=["province_name_cn", "year"], how="outer").merge(
        hydro_df, on=["province_name_cn", "year"], how="outer"
    )
    return fc


def build_national_monthly_targets_full(
    national_monthly: pd.DataFrame,
    total_monthly_target: pd.DataFrame,
    national_annual_forecast: pd.DataFrame,
) -> pd.DataFrame:
    # Historical monthly national values from observed national monthly data.
    hist = national_monthly[["year", "month", "total_gen", "thermal_gen", "hydro_gen"]].copy()

    # Future total_gen monthly target from forecasted monthly total series.
    tot_f = total_monthly_target[(total_monthly_target["year"] >= 2026)].copy()
    tot_f = tot_f[["year", "month", "total_gen"]].copy()

    # Future thermal/hydro monthly targets from 2018-2024 national monthly climatology.
    hist_w = national_monthly.copy()
    for k in ["thermal_gen", "hydro_gen"]:
        den = hist_w.groupby("year")[k].transform("sum")
        hist_w[f"w_{k}"] = hist_w[k] / den
        hist_w.loc[den <= 0, f"w_{k}"] = 1.0 / 12.0
    clim_base = hist_w[(hist_w["year"] >= 2018) & (hist_w["year"] <= 2024)][
        ["month", "w_thermal_gen", "w_hydro_gen"]
    ].copy()
    clim = (
        clim_base.groupby("month", as_index=False)[["w_thermal_gen", "w_hydro_gen"]]
        .mean()
        .sort_values("month")
    )
    # Renormalize for numerical safety.
    clim["w_thermal_gen"] = clim["w_thermal_gen"] / clim["w_thermal_gen"].sum()
    clim["w_hydro_gen"] = clim["w_hydro_gen"] / clim["w_hydro_gen"].sum()

    fut_rows = []
    for year in range(2026, FORECAST_YEAR_END + 1):
        yr_tot = tot_f[tot_f["year"] == year][["month", "total_gen"]].copy()
        tmp = yr_tot.merge(clim, on="month", how="left")
        annual_row = national_annual_forecast[national_annual_forecast["year"] == year].iloc[0]
        tmp["thermal_gen"] = tmp["w_thermal_gen"] * float(annual_row["thermal_gen_annual"])
        tmp["hydro_gen"] = tmp["w_hydro_gen"] * float(annual_row["hydro_gen_annual"])
        tmp["year"] = year
        fut_rows.append(tmp[["year", "month", "total_gen", "thermal_gen", "hydro_gen"]])
    fut = pd.concat(fut_rows, ignore_index=True)

    out = pd.concat([hist, fut], ignore_index=True)
    out = out[(out["year"] >= HIST_YEAR_START) & (out["year"] <= FORECAST_YEAR_END)].copy()
    out = out.sort_values(["year", "month"]).reset_index(drop=True)
    return out


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1) Provincial annual historical (2010-2024).
    total_annual = read_provincial_annual_csv(ANNUAL_FILES["total_gen_annual"], "total_gen_annual")
    thermal_annual = read_provincial_annual_csv(ANNUAL_FILES["thermal_gen_annual"], "thermal_gen_annual")
    hydro_annual = read_provincial_annual_csv(ANNUAL_FILES["hydro_gen_annual"], "hydro_gen_annual")
    annual_hist = (
        total_annual.merge(thermal_annual, on=["province_name_cn", "year"], how="outer")
        .merge(hydro_annual, on=["province_name_cn", "year"], how="outer")
        .sort_values(["province_name_cn", "year"])
        .reset_index(drop=True)
    )
    annual_hist = annual_hist[annual_hist["province_name_cn"].isin(PROVINCE_ISO.keys())].copy()
    for c in ["total_gen_annual", "thermal_gen_annual", "hydro_gen_annual"]:
        annual_hist[c] = annual_hist[c].fillna(0.0)

    # 2) National monthly historical + total monthly target to 2035.
    national_monthly = read_national_monthly(NATIONAL_MONTHLY_FILE)
    total_monthly_target = read_total_monthly_target(national_monthly)

    # 3) Build national annual targets for 2025-2035.
    annual_total_target = annual_from_monthly(total_monthly_target, "total_gen").rename(
        columns={"total_gen_annual": "total_gen_annual"}
    )
    annual_total_target = annual_total_target[
        (annual_total_target["year"] >= FORECAST_YEAR_START)
        & (annual_total_target["year"] <= FORECAST_YEAR_END)
    ].copy()
    annual_k_forecast = forecast_national_thermal_hydro_annual(national_monthly, annual_total_target)

    # National annual table for historical+forecast years.
    national_annual_hist = (
        national_monthly.groupby("year", as_index=False)[["total_gen", "thermal_gen", "hydro_gen"]]
        .sum()
        .rename(
            columns={
                "total_gen": "total_gen_annual",
                "thermal_gen": "thermal_gen_annual",
                "hydro_gen": "hydro_gen_annual",
            }
        )
    )
    national_annual_all = pd.concat(
        [
            national_annual_hist[(national_annual_hist["year"] >= HIST_YEAR_START) & (national_annual_hist["year"] <= HIST_YEAR_END)],
            annual_k_forecast,
        ],
        ignore_index=True,
    ).sort_values("year")

    # 4) Provincial annual forecast (2025-2035) from share trend.
    annual_fc = forecast_provincial_annual_from_shares(annual_hist, national_annual_all)
    annual_all = pd.concat([annual_hist, annual_fc], ignore_index=True).sort_values(
        ["province_name_cn", "year"]
    )
    annual_all["province_iso"] = annual_all["province_name_cn"].map(PROVINCE_ISO)
    annual_all["data_flag"] = np.where(
        annual_all["year"] <= HIST_YEAR_END,
        "historical_disaggregated",
        "forecast_share_based",
    )

    # 5) National monthly targets 2010-2035.
    national_monthly_targets = build_national_monthly_targets_full(
        national_monthly,
        total_monthly_target,
        annual_k_forecast,
    )

    # 6) Annual provincial share -> monthly provincial value.
    #    Prov(p,k,y,m) = AnnualProv(p,k,y) / sum_p AnnualProv(p,k,y) * CN(k,y,m)
    provincial_totals = (
        annual_all.groupby("year", as_index=False)[
            ["total_gen_annual", "thermal_gen_annual", "hydro_gen_annual"]
        ]
        .sum()
        .rename(
            columns={
                "total_gen_annual": "prov_total_gen_annual_sum",
                "thermal_gen_annual": "prov_thermal_gen_annual_sum",
                "hydro_gen_annual": "prov_hydro_gen_annual_sum",
            }
        )
    )
    annual_all = annual_all.merge(provincial_totals, on="year", how="left")
    annual_all["share_total_gen"] = annual_all["total_gen_annual"] / annual_all[
        "prov_total_gen_annual_sum"
    ].replace(0, np.nan)
    annual_all["share_thermal_gen"] = annual_all["thermal_gen_annual"] / annual_all[
        "prov_thermal_gen_annual_sum"
    ].replace(0, np.nan)
    annual_all["share_hydro_gen"] = annual_all["hydro_gen_annual"] / annual_all[
        "prov_hydro_gen_annual_sum"
    ].replace(0, np.nan)
    for c in ["share_total_gen", "share_thermal_gen", "share_hydro_gen"]:
        annual_all[c] = annual_all[c].fillna(0.0)

    month_df = annual_all.merge(national_monthly_targets, on="year", how="inner")
    month_df["total_gen"] = month_df["share_total_gen"] * month_df["total_gen"]
    month_df["thermal_gen"] = month_df["share_thermal_gen"] * month_df["thermal_gen"]
    month_df["hydro_gen"] = month_df["share_hydro_gen"] * month_df["hydro_gen"]
    month_df["date"] = pd.to_datetime(
        month_df["year"].astype(int).astype(str)
        + "-"
        + month_df["month"].astype(int).astype(str).str.zfill(2)
        + "-01"
    )

    result = month_df[
        [
            "date",
            "year",
            "month",
            "province_iso",
            "province_name_cn",
            "total_gen",
            "thermal_gen",
            "hydro_gen",
            "data_flag",
        ]
    ].copy()
    result = result.sort_values(["province_iso", "date"]).reset_index(drop=True)
    result.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig", float_format="%.6f")
    ANYLOGIC_OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(ANYLOGIC_OUTPUT_FILE, index=False, encoding="utf-8-sig", float_format="%.6f")

    print(f"[2f] Provincial monthly file written: {OUTPUT_FILE}")
    print(f"[2f] AnyLogic copy written: {ANYLOGIC_OUTPUT_FILE}")
    print(f"[2f] Rows: {len(result)}")
    print(f"[2f] Provinces: {result['province_iso'].nunique()}")
    print(f"[2f] Date range: {result['date'].min().date()} to {result['date'].max().date()}")
    print(result["data_flag"].value_counts().to_string())


if __name__ == "__main__":
    main()

