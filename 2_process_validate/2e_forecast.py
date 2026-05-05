#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
2e_forecast.py
SARIMA-based demand and supply forecast using 2010-2025 historical data.

Forecast horizon: configurable (default 120 months = 10 years).
Outputs:
  - 4_output_anylogic/forecast_demand.csv  (date + point forecast only)
  - 4_output_anylogic/forecast_supply.csv  (same)
  - 4_output_anylogic/forecast_combined.xlsx  (multi-sheet)
  - 3_output_check_report/output_catalog.txt  (qualitative forecast note appended)

The AnyLogic model reads only the ``forecast`` column. Interval bands are omitted
deliberately; uncertainty is documented qualitatively in ``write_assessment``.
"""

import warnings
from pathlib import Path

import pandas as pd

try:
    from statsmodels.tsa.statespace.sarimax import SARIMAX
except ImportError as exc:
    raise ImportError(
        "statsmodels is required for 2e_forecast.py. "
        "Install with: pip install statsmodels"
    ) from exc

warnings.filterwarnings("ignore")

# ── paths ──────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent.parent
AL_DIR     = BASE_DIR / "4_output_anylogic"
REPORT_DIR = BASE_DIR / "3_output_check_report"

DEMAND_FILE = AL_DIR / "demand_filled.xlsx"
SUPPLY_FILE = AL_DIR / "supply_filled.xlsx"

FORECAST_MONTHS = 120  # 10 years

# Best-guess SARIMA orders; AIC search kept simple to avoid long runtime.
# (p,d,q)(P,D,Q,s) with s=12 for monthly seasonality.
SARIMA_ORDER         = (1, 1, 1)
SARIMA_SEASONAL_ORDER = (1, 1, 1, 12)


# ── helpers ────────────────────────────────────────────────────────────────────
def sarima_forecast(ts: pd.Series, steps: int, label: str) -> pd.DataFrame:
    """Fit SARIMA and return a single-column point forecast."""
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

    future_index = pd.date_range(
        start=ts.index[-1] + pd.offsets.MonthBegin(1),
        periods=steps,
        freq="MS",
    )
    result = pd.DataFrame({"forecast": mean.values}, index=future_index)
    result.index.name = "date"
    print(f"  [{label}] AIC={fit.aic:.1f}  first 5-yr mean={result['forecast'].mean():.1f}")
    return result


def write_assessment(demand_fc: pd.DataFrame, supply_fc: pd.DataFrame) -> None:
    """Append a qualitative forecast note (no predictive intervals in CSV outputs)."""
    d = demand_fc["forecast"].to_numpy(dtype=float)
    mn1 = float(d[:12].mean()) if len(d) >= 12 else float(d.mean())
    mn_last = float(d[-12:].mean()) if len(d) >= 12 else float(d.mean())
    s = supply_fc["forecast"].to_numpy(dtype=float)
    sn1 = float(s[:12].mean()) if len(s) >= 12 else float(s.mean())
    sn_last = float(s[-12:].mean()) if len(s) >= 12 else float(s.mean())

    lines = [
        "",
        "=" * 70,
        "FORECAST NOTE (POINT PROJECTION ONLY)",
        "=" * 70,
        f"Training span: monthly series through the latest date in filled demand/supply tables.",
        f"Forecast horizon: {FORECAST_MONTHS} months ({FORECAST_MONTHS // 12} years).",
        f"Model: SARIMAX with SARIMA{SARIMA_ORDER}x{SARIMA_SEASONAL_ORDER} (seasonal period 12).",
        "",
        "Outputs are **mean forecasts only** (columns: date, forecast).",
        "Predictive-interval columns were removed — the ABM consumes the point trajectory.",
        "",
        "─── Sanity check on level shift (means of forecast path) ────────────",
        f"  Demand:  avg year-1 slice ≈ {mn1:.1f}  →  avg final-year slice ≈ {mn_last:.1f}",
        f"  Supply:  avg year-1 slice ≈ {sn1:.1f}  →  avg final-year slice ≈ {sn_last:.1f}",
        "",
        "Limitations: SARIMAX cannot guarantee future structural breaks, policy shocks,",
        "or technology mix changes; distant years are exploratory trend/season extrapolation.",
        "=" * 70,
    ]
    catalog_path = REPORT_DIR / "output_catalog.txt"
    existing = catalog_path.read_text(encoding="utf-8") if catalog_path.exists() else ""
    marker = "\n======================================================================\nFORECAST NOTE (POINT PROJECTION ONLY)\n"
    if marker in existing:
        existing = existing.split(marker, 1)[0].rstrip() + "\n"
    # Also strip legacy CI assessment block title if present
    legacy = "\n======================================================================\nFORECAST CONFIDENCE INTERVAL ASSESSMENT\n"
    if legacy in existing:
        existing = existing.split(legacy, 1)[0].rstrip() + "\n"

    catalog_path.write_text(existing.rstrip() + "\n" + "\n".join(lines) + "\n", encoding="utf-8")

    old_path = REPORT_DIR / "forecast_assessment.txt"
    if old_path.exists():
        old_path.unlink()

    print(f"[2e] Forecast note appended to {catalog_path}")


# ── main ───────────────────────────────────────────────────────────────────────
def main() -> None:
    if not DEMAND_FILE.exists() or not SUPPLY_FILE.exists():
        raise FileNotFoundError("demand_filled.xlsx or supply_filled.xlsx not found in 4_output_anylogic/")

    demand_df = pd.read_excel(DEMAND_FILE, parse_dates=["date"]).set_index("date")
    supply_df = pd.read_excel(SUPPLY_FILE, parse_dates=["date"]).set_index("date")

    print(f"[2e] Forecasting {FORECAST_MONTHS} months ahead …")

    dem_fc = sarima_forecast(demand_df["total_demand"], FORECAST_MONTHS, "demand")
    sup_fc = sarima_forecast(supply_df["total_supply"], FORECAST_MONTHS, "supply")

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
