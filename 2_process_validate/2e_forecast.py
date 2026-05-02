#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
2e_forecast.py
SARIMA-based demand and supply forecast using 2010-2025 historical data.

Forecast horizon: configurable (default 120 months = 10 years).
Outputs:
  - 4_output_anylogic/forecast_demand.csv  (future demand with 80/95% CI)
  - 4_output_anylogic/forecast_supply.csv  (future total supply with 80/95% CI)
  - 4_output_anylogic/forecast_combined.xlsx  (multi-sheet)
  - 3_output_check_report/output_catalog.txt  (forecast assessment appended)

Credibility decreases with forecast horizon:
  - Years 1-3: HIGH confidence — seasonal + trend patterns well-captured
  - Years 4-5: MODERATE confidence — CI still meaningful for planning
  - Years 6-10: LOW confidence — treat as scenario range; CIs widen significantly
  - 50-year: NOT statistically valid; CI would span ±100s% of the mean.
    The SARIMA model cannot capture structural breaks, technology shifts,
    or policy changes at that horizon. DO NOT use for decision-making.
"""

import warnings
from pathlib import Path

import numpy as np
import pandas as pd

# statsmodels is required; falls back to simple linear+seasonal if not present.
try:
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    from statsmodels.tsa.stattools import adfuller
    HAS_STATSMODELS = True
except ImportError:
    HAS_STATSMODELS = False
    warnings.warn("statsmodels not installed — using fallback linear+seasonal model.")

warnings.filterwarnings("ignore")

# ── paths ──────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent.parent
AL_DIR     = BASE_DIR / "4_output_anylogic"
REPORT_DIR = BASE_DIR / "3_output_check_report"

DEMAND_FILE = AL_DIR / "demand_filled.xlsx"
SUPPLY_FILE = AL_DIR / "supply_filled.xlsx"

FORECAST_MONTHS = 120  # 10 years; credibility decreases with horizon (see assessment)

# Best-guess SARIMA orders; AIC search kept simple to avoid long runtime.
# (p,d,q)(P,D,Q,s) with s=12 for monthly seasonality.
SARIMA_ORDER         = (1, 1, 1)
SARIMA_SEASONAL_ORDER = (1, 1, 1, 12)


# ── helpers ────────────────────────────────────────────────────────────────────
def _last_date(series: pd.Series) -> pd.Timestamp:
    return pd.to_datetime(series.iloc[-1])


def sarima_forecast(ts: pd.Series, steps: int, label: str) -> pd.DataFrame:
    """Fit SARIMA and return forecast DataFrame with CI columns."""
    ts = ts.dropna()
    print(f"  [{label}] fitting SARIMA{SARIMA_ORDER}x{SARIMA_SEASONAL_ORDER} "
          f"on {len(ts)} obs …")
    model = SARIMAX(
        ts,
        order=SARIMA_ORDER,
        seasonal_order=SARIMA_SEASONAL_ORDER,
        enforce_stationarity=False,
        enforce_invertibility=False,
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fit = model.fit(disp=False, maxiter=200)

    fc = fit.get_forecast(steps=steps)
    mean = fc.predicted_mean
    ci80 = fc.conf_int(alpha=0.20)   # 80% CI
    ci95 = fc.conf_int(alpha=0.05)   # 95% CI

    future_index = pd.date_range(
        start=ts.index[-1] + pd.offsets.MonthBegin(1),
        periods=steps,
        freq="MS",
    )
    result = pd.DataFrame(
        {
            "forecast": mean.values,
            "ci80_lower": ci80.iloc[:, 0].values,
            "ci80_upper": ci80.iloc[:, 1].values,
            "ci95_lower": ci95.iloc[:, 0].values,
            "ci95_upper": ci95.iloc[:, 1].values,
        },
        index=future_index,
    )
    result.index.name = "date"
    print(f"  [{label}] AIC={fit.aic:.1f}  first 5-yr mean={result['forecast'].mean():.1f}")
    return result


def fallback_forecast(ts: pd.Series, steps: int) -> pd.DataFrame:
    """Simple linear trend + seasonal pattern when statsmodels is unavailable."""
    ts = ts.dropna()
    x = np.arange(len(ts))
    coeffs = np.polyfit(x, ts.values, 1)  # linear trend

    # seasonal factors from last 24 months average by month-of-year
    ts_monthly = ts.copy()
    ts_monthly.index = pd.to_datetime(ts_monthly.index)
    seasonal = (
        ts_monthly.groupby(ts_monthly.index.month)
        .mean()
        / ts_monthly.mean()
    )

    future_index = pd.date_range(
        start=ts_monthly.index[-1] + pd.offsets.MonthBegin(1),
        periods=steps,
        freq="MS",
    )
    forecasts, std_dev = [], float(ts.std())
    for i, dt in enumerate(future_index):
        trend_val = np.polyval(coeffs, len(ts) + i)
        sf = seasonal.get(dt.month, 1.0)
        forecasts.append(trend_val * sf)

    forecasts = np.array(forecasts)
    result = pd.DataFrame(
        {
            "forecast": forecasts,
            "ci80_lower": forecasts - 1.28 * std_dev,
            "ci80_upper": forecasts + 1.28 * std_dev,
            "ci95_lower": forecasts - 1.96 * std_dev,
            "ci95_upper": forecasts + 1.96 * std_dev,
        },
        index=future_index,
    )
    result.index.name = "date"
    return result


def write_assessment(demand_fc: pd.DataFrame, supply_fc: pd.DataFrame) -> None:
    """Append forecast assessment into the shared output catalog report."""
    # Compute per-year credibility: measure 95% CI half-width relative to forecast mean
    fc_years = []
    for yr in range(1, FORECAST_MONTHS // 12 + 1):
        start = (yr - 1) * 12
        end   = yr * 12
        hw = ((demand_fc['ci95_upper'].iloc[start:end] - demand_fc['ci95_lower'].iloc[start:end]) / 2).mean()
        mn = demand_fc['forecast'].iloc[start:end].mean()
        rel = hw / mn * 100 if mn > 0 else 0
        if rel < 15:
            credibility = "HIGH"
        elif rel < 30:
            credibility = "MODERATE"
        elif rel < 60:
            credibility = "LOW"
        else:
            credibility = "VERY LOW — treat as scenario range"
        fc_years.append(f"  Year {yr:2d}: CI half-width={hw:.0f}  ({rel:.0f}% of mean)  → {credibility}")

    lines = [
        "",
        "=" * 70,
        "FORECAST CONFIDENCE INTERVAL ASSESSMENT",
        "=" * 70,
        f"Training data:   2010-01 to 2025-12 (up to 180 monthly observations)",
        f"Forecast horizon: {FORECAST_MONTHS} months ({FORECAST_MONTHS // 12} years)",
        f"Model:           SARIMA{SARIMA_ORDER}x{SARIMA_SEASONAL_ORDER} (monthly seasonality)",
        "",
        "─── Credibility by year (demand, 95% CI) ─────────────────────────────",
        "  NOTE: Confidence decreases with distance from training data.",
        "  Near-term forecasts (years 1-3) are significantly more reliable.",
    ] + fc_years + [
        "",
        "─── 10-year (120-month) forecast overall ─────────────────────────────",
        f"  Demand 95% CI half-width (all-period mean): "
        f"{((demand_fc['ci95_upper'] - demand_fc['ci95_lower']) / 2).mean():.1f}",
        f"  Supply 95% CI half-width (all-period mean): "
        f"{((supply_fc['ci95_upper'] - supply_fc['ci95_lower']) / 2).mean():.1f}",
        "  Years 1-5: use for planning.  Years 6-10: use as scenario bounds only.",
        "",
        "─── 50-year (600-month) forecast ─────────────────────────────────────",
        "  Validity: NOT STATISTICALLY MEANINGFUL.",
        "  Reasons:",
        "  1. SARIMA CIs grow as O(√h) — after 600 steps the CI spans multiple",
        "     times the historical mean.",
        "  2. The model cannot capture structural breaks (e.g., energy transition,",
        "     carbon neutrality policy, major recessions).",
        "  3. China's electricity demand growth rate has itself changed several",
        "     times over 2010-2025; extrapolating 50 years is unreliable.",
        "  Recommendation: Use scenario-based analysis (e.g., IEA NZE, STEPS,",
        "  APS pathways) for horizons beyond 10 years.",
        "=" * 70,
    ]
    catalog_path = REPORT_DIR / "output_catalog.txt"
    existing = catalog_path.read_text(encoding="utf-8") if catalog_path.exists() else ""
    marker = "\n======================================================================\nFORECAST CONFIDENCE INTERVAL ASSESSMENT\n"
    if marker in existing:
        existing = existing.split(marker, 1)[0].rstrip() + "\n"
    catalog_path.write_text(existing.rstrip() + "\n" + "\n".join(lines) + "\n", encoding="utf-8")

    old_path = REPORT_DIR / "forecast_assessment.txt"
    if old_path.exists():
        old_path.unlink()

    print(f"[2e] Assessment appended to {catalog_path}")


# ── main ───────────────────────────────────────────────────────────────────────
def main() -> None:
    if not DEMAND_FILE.exists() or not SUPPLY_FILE.exists():
        raise FileNotFoundError("demand_filled.xlsx or supply_filled.xlsx not found in 4_output_anylogic/")

    demand_df = pd.read_excel(DEMAND_FILE, parse_dates=["date"]).set_index("date")
    supply_df = pd.read_excel(SUPPLY_FILE, parse_dates=["date"]).set_index("date")

    print(f"[2e] Forecasting {FORECAST_MONTHS} months ahead …")

    if HAS_STATSMODELS:
        dem_fc  = sarima_forecast(demand_df["total_demand"], FORECAST_MONTHS, "demand")
        sup_fc  = sarima_forecast(supply_df["total_supply"], FORECAST_MONTHS, "supply")
    else:
        dem_fc = fallback_forecast(demand_df["total_demand"], FORECAST_MONTHS)
        sup_fc = fallback_forecast(supply_df["total_supply"], FORECAST_MONTHS)

    # ── outputs ────────────────────────────────────────────────────────────────
    dem_fc.reset_index().to_csv(AL_DIR / "forecast_demand.csv", index=False, encoding="utf-8-sig")
    sup_fc.reset_index().to_csv(AL_DIR / "forecast_supply.csv", index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(AL_DIR / "forecast_combined.xlsx", engine="openpyxl") as xw:
        dem_fc.reset_index().to_excel(xw, sheet_name="demand_forecast", index=False)
        sup_fc.reset_index().to_excel(xw, sheet_name="supply_forecast", index=False)

        # Append historical for context
        demand_df[["total_demand"]].reset_index().to_excel(
            xw, sheet_name="historical_demand", index=False)
        supply_df[["total_supply"]].reset_index().to_excel(
            xw, sheet_name="historical_supply", index=False)

    print(f"[2e] Forecast CSVs and Excel written to {AL_DIR}")

    write_assessment(dem_fc, sup_fc)


if __name__ == "__main__":
    main()
