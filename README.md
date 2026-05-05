# China Electricity ABM — Data Pipeline & AnyLogic Model

[English](README.md) | [简体中文](README_zh.md)

---

## Project Overview

End-to-end Agent-Based Model (ABM) for China's electricity market (2010–2025+). The Python pipeline cleans raw NBS/NEA data, produces AnyLogic database inputs, runs configurable SARIMA point forecasts (`2e_forecast.py`, default 120 months), builds provincial map CSV (`2f_build_provincial_monthly.py`), and optionally converts calibration spreadsheets for the Optimization experiment (`2g_*`).

---

## Team

| Name | Email |
|---|---|
| Yunzhi Jiang | yunzhi.j@foxmail.com |
| Zhuohan Tian | u3661579@connect.hku.hk |
| Qiyue Cheng | 13815473040@163.com |
| Yiye Liu | u3661498@connect.hku.hk |

---

## Repository layout

```
wholepackage/
├── 0_raw_data/                        # Source Excel/CSV (NBS, provincial annuals, optional calibration XLSX)
├── 1_clean_demand_supply/             # 1a–1c: crawl + clean demand/supply
├── 2_process_validate/                # 2a–2g: merge, validate, DB rebuild, forecast, provincial CSV, calibration export
├── 3_output_check_report/             # Merged tables, indicators, output_catalog.txt
├── 4_output_anylogic/                 # Workbooks + forecast trajectories + provincial CSV for the model
├── 5_anylogic_model/
│   └── ElectricityTrial_-_Version 7_-_Sources/
│       ├── ElectricityTrial.alp
│       ├── database/db.script
│       └── (CSV copies used at runtime, e.g. forecasts / province series / calibration)
├── 0_log/                             # Timestamped pipeline logs (created on run)
└── main_pipeline.py                    # Orchestrator
```

**Main generated files (under `4_output_anylogic/` unless noted):** `demand_filled.xlsx`, `supply_filled.xlsx`, `scenario_parameters.csv` (if present), `forecast_demand.csv`, `forecast_supply.csv`, `forecast_combined.xlsx`, `province_generation_monthly_2010_2035.csv` (also copied next to the `.alp` by `2f`). **Reports:** `3_output_check_report/output_catalog.txt`. **DB:** `5_anylogic_model/.../database/db.script` rebuilt by `2d`. **Optional calibration:** `calibration_observed_annual.csv` beside the model from `2g` when the source XLSX exists.

---

## Quick Start

```bash
python main_pipeline.py
```

Runs stages in sequence: cleaning → 2a–2c → embedded report bundle → `2d` (HSQLDB) → `2e` (forecasts) → `2f` (provincial CSV) → `2g` only if `0_raw_data/calibration_observed_price.xlsx` exists.

Cloud and local use the same command; ensure `0_raw_data/` and demand/supply inputs are present in the runtime workspace.

---

## AnyLogic Model (`5_anylogic_model/`)

### Agent Classes

| Class | Role |
|---|---|
| **Main** | Orchestrator: loads data, runs monthly timer, holds all arrays |
| **GeneratorAgent** | Superclass for all five generator types |
| **DemandAgent** | Sector demand node (Primary / Secondary / Tertiary / Residential) |
| **GridAgent** | Clears the market: price = basePrice × tightness × priceSensitivity |
| **GovernmentAgent** | Carries scenario parameters (demandGrowthAdj, priceSensitivity, …) |

**Generator sub-instances in Main** (all are `GeneratorAgent`):
`thermalGen`, `hydroGen`, `nuclearGen`, `windGen`, `solarGen`

**Demand sub-instances**: `primaryDem`, `secondaryDem`, `tertiaryDem`, `residentialDem`

### Database Tables

| Table | Rows | Purpose |
|---|---|---|
| GENERATION | 169 | Monthly generation by source (TWh), 2010-01 → 2025-12 |
| DEMAND | 193 | Monthly demand by sector (TWh), 2010-01 → 2025-12 |
| TECH | 10 | Generator parameters (priority, variable cost, carbon factor, bid markup, min/max share) |
| SCENARIO | 4 | Scenario multipliers for growth and pricing |

### Simulation Flow (monthly timer, 1-month recurrence)

```
Startup → loadGenerationData() + loadDemandData() + loadForecastTrajectoryData() + …
       → applyHistoricalMonth() when useHistoricalData

Each month (timer fires):
  ├── if useHistoricalData:
  │     step through months; at last historical month:
  │       • Simulation / CompareRuns: switch to forecast (useHistoricalData=false), reset index
  │       • Optimization: stop after SSE (calibrationFinishAfterHistorical=true via experiment detect)
  └── else (forecast phase):
        grow/dispatch using forecast trajectory or endogenous dynamics until forecast horizon ends
```

### What Is Stochastic vs. Deterministic

| Element | Stochastic? | Detail |
|---|---|---|
| Historical demand/supply loading | **No** | Direct table lookup from db.script |
| Monthly timer, data index stepping | **No** | Deterministic |
| **Shock occurrence** | **Yes** | Bernoulli draw each month: `uniform(1.0) < shockProbList[k]` |
| **Shock intensity** | **Yes** | Truncated normal: `normal(mean, std)`, clipped to [1%, 50%] |
| **Hydro factor during shock** | **Yes** | `shockHydroFactor` is shock-scenario dependent |
| **Shock recovery** | **Yes** | Gradual decay: intensity × 0.80 per month until < threshold |
| Market price | Partly | Deterministic formula, but inputs are shock-modified demand |
| Bid prices | **No** | Formula-based (variableCost + carbonTax × carbonFactor + markup) |
| Generator profit | **No** | Deterministic calculation |
| Scenario parameters | **No** | Loaded from SCENARIO table at startup |

**Shock config (4 shock scenarios × 3 params each)**:
- Scenario 0 (baseline): P=5%/month, intensity mean=15%, std=5%
- Scenario 1 (policy): P=3%/month, mean=8%, std=3%
- Scenario 2 (carbon): P=4%/month, mean=12%, std=4%
- Scenario 3 (extreme weather): P=6%/month, mean=18%, std=6%

---

## Forecast (script default: 10 years / 120 months)

Script: `2_process_validate/2e_forecast.py`

- Model: SARIMAX / SARIMA(1,1,1)(1,1,1)₁₂ on national monthly demand and supply
- Outputs: `4_output_anylogic/forecast_demand.csv`, `forecast_supply.csv`, `forecast_combined.xlsx`; qualitative notes appended to `3_output_check_report/output_catalog.txt`
- AnyLogic consumes the point `forecast` column only

Horizon caveat: beyond ~5–7 years paths are exploratory scenarios, not calibrated prediction intervals.

## Provincial GIS map CSV

Script: `2_process_validate/2f_build_provincial_monthly.py`

- Builds `province_generation_monthly_2010_2035.csv` in `4_output_anylogic/` and copies beside `ElectricityTrial.alp`.
- Requires provincial annual CSVs under `0_raw_data/` (see script header). Uses national totals from merged history + `forecast_supply.csv`.

## Calibration export (optional)

Script: `2_process_validate/2g_calibration_xlsx_to_anylogic_csv.py` (automatic in `main_pipeline.py` **only when** `0_raw_data/calibration_observed_price.xlsx` exists)

- Writes `calibration_observed_annual.csv` next to the model (`Main.calibrationAnnualWideCsvPath`).

## Optimization experiment

- **Optimization**: minimize `calibrationPriceSeriesSSE`; each trial stops after historical months (fast calibration).
- **Simulation / CompareRuns**: run history then forecast to the map and charts.

Set **`Main.nMonths`** to the **common aligned month count** produced by `2d_update_anylogic_database.py` (matching the merged Excel / DB row span) so historical indices never read past loaded data.

---

## Python extras (forecasting)

Forecasting requires **statsmodels**:

```bash
pip install statsmodels
```

## Appendix — manual script order

For ad-hoc runs (same order as `main_pipeline.py` after staging 1b/1c have produced Excel in `4_output_anylogic/`):

```bash
python 2_process_validate/2a_merge_datasets.py
python 2_process_validate/2b_calculate_indicators.py
python 2_process_validate/2c_validate_consistency.py
python main_pipeline.py   # easiest: rerun full orchestrator after data exist
```

Or call `2d_update_anylogic_database.py`, then `2e_forecast.py`, then `2f_build_provincial_monthly.py`, then `2g_calibration_xlsx_to_anylogic_csv.py` (last only when `0_raw_data/calibration_observed_price.xlsx` exists).

Logs: `0_log/` (timestamped per run).

## Requirements

Python 3.8+ recommended.

```bash
pip install pandas numpy openpyxl xlrd scikit-learn statsmodels
```

## Update log

### 2026-05-06
- English README: single repository layout section; removed “Two ABM Scenarios (Professor Feedback)” subsection; tightened overview text.

### 2026-05-05
- `main_pipeline.py` calls `2f_build_provincial_monthly.py` and optional `2g_*`; removes broken `2f_3d_visualization.py`; preserves forecasts and provincial CSV in `4_output_anylogic/`.
- README de-duplicated: provincial map CSV, calibration export, optimization vs simulation.

### 2026-05-02
- Restructured `3_output_check_report/` + `4_output_anylogic/`; bilingual README pair; `.gitignore` refreshed.

