# China Electricity ABM — Data Pipeline & AnyLogic Model

[English](README.md) | [简体中文](README_zh.md)

---

## Project Overview

End-to-end Agent-Based Model (ABM) for China's electricity market (2010–2025+).
The Python pipeline cleans raw NBS/NEA data, produces AnyLogic database inputs, runs a
5-year SARIMA forecast, and generates 3-D interactive visualizations.

---

## Team

| Name | Email |
|---|---|
| Yunzhi Jiang | yunzhi.j@foxmail.com |
| Zhuohan Tian | u3661579@connect.hku.hk |
| Qiyue Cheng | 13815473040@163.com |
| Yiye Liu | u3661498@connect.hku.hk |

---

## Folder Structure

```
wholepackage/
├── 0_raw_data/                        # Raw Excel files from NBS
├── 1_clean_demand_supply/             # Data cleaning scripts
│   ├── 1a_crawl_national_demand.py    # Scrape NEA demand data
│   ├── 1b_clean_demand_data.py        # Clean and fill demand gaps
│   └── 1c_clean_supply_data.py        # Clean generation data (RF imputation)
├── 2_process_validate/                # Processing, validation, modelling
│   ├── 2a_merge_datasets.py           # Merge supply + demand
│   ├── 2b_calculate_indicators.py     # YoY, shares, supply-demand ratio
│   ├── 2c_validate_consistency.py     # Quality checks
│   ├── 2d_update_anylogic_database.py # Rebuild db.script from Excel files
│   ├── 2e_forecast.py                 # SARIMA 5-year demand/supply forecast
│   └── 2f_3d_visualization.py         # Plotly 3-D interactive charts
├── 3_output_check_report/             # Intermediate CSVs, reports, HTML charts
├── 4_output_anylogic/                 # AnyLogic input files (Excel/CSV)
│   ├── demand_filled.xlsx             # 2010-2025 monthly demand (193 rows)
│   ├── supply_filled.xlsx             # 2010-2025 monthly generation (169 rows)
│   ├── scenario_parameters.csv        # Two ABM scenarios
│   ├── forecast_demand.csv            # demand point forecast (date, forecast)
│   ├── forecast_supply.csv            # 5-year supply forecast with 80/95% CI
│   └── db_update_info.txt             # Row counts; nMonths hint for AnyLogic
├── 5_anylogic_model/                  # AnyLogic project
│   └── ElectricityTrial_-_Version 7_-_Sources/
│       ├── ElectricityTrial.alp       # Model file (XML)
│       └── database/db.script         # HSQLDB plain-text database
└── main_pipeline.py                   # One-command pipeline runner
```

---

## Quick Start

```bash
python main_pipeline.py
```

Runs all 6 stages in sequence:
1. Data acquisition & cleaning
2. Processing & validation
3. AnyLogic CSV/Excel outputs
4. Rebuild AnyLogic HSQLDB (`db.script`)
5. 5-year SARIMA forecast
6. 3-D interactive charts (Plotly)

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
Startup → loadGenerationData() + loadDemandData() + loadTechConfig()
       → loadScenarioConfig() + loadShockConfig()
       → applyHistoricalMonth() (index 0)

Each month (timer fires):
  ├── if useHistoricalData:
  │     currentIndex++
  │     applyHistoricalMonth()          ← sets demand & generation from arrays
  │     sampleShock()                   ← stochastic shock (see below)
  │     grid.clearMarket()              ← price = basePrice × tightness × priceSensitivity
  │     [if currentIndex >= nMonths-1]  → finishSimulation()  ← auto-stops
  └── else (ABM growth mode):
        demand *= (1 + growthRate × demandGrowthAdj)
        grid.clearMarket()
        generators.updateProfit()
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

### Two ABM Scenarios (Professor Feedback)

| Scenario | ID | Shock type | Key parameters |
|---|---|---|---|
| **Policy Volatility (Carbon Tax)** | `policy_volatility_carbon_tax` | policy | carbon_tax_base=80, volatility=30%, shock_duration=12 mo |
| **Extreme Weather** | `extreme_weather_demand_hydro` | weather | demand_shock=+18%, hydro_factor=0.70, duration=6 mo |

---

## Forecast (5-Year)

Script: `2_process_validate/2e_forecast.py`

- Model: SARIMA(1,1,1)(1,1,1)₁₂ fitted on 2010-2025 (up to 180 monthly obs)
- Output: `4_output_anylogic/forecast_demand.csv` and `forecast_supply.csv`
  - Columns: `date`, `forecast` (point projection for AnyLogic)

**Confidence interval validity**:

| Horizon | Assessment |
|---|---|
| 5 years (60 mo) | **Reasonable** — seasonal + trend well-captured; CI widths typically <30% of mean |
| 10 years (120 mo) | Caution — treat as scenario range, not probabilistic |
| 50 years (600 mo) | **Not valid** — CI spans multiples of the mean; structural breaks cannot be modelled |

---

## 3-D Visualizations

Script: `2_process_validate/2f_3d_visualization.py`
Requires: `pip install plotly openpyxl`

Outputs in `3_output_check_report/`:

| File | Content |
|---|---|
| `3d_generation_mix.html` | 3-D ribbon: month × generation source × volume |
| `3d_supply_demand.html` | 3-D scatter: supply vs demand, colored by gap |
| `3d_demand_sectors.html` | 3-D surface: time × sector × demand |
| `3d_forecast_fan.html` | Historical + 5-year forecast with CI bounds |

Open any `.html` file in a browser for fully interactive rotation, zoom, and hover tooltips.

---

## Pipeline Output Summary

After running `main_pipeline.py`, the following files are ready:

| Location | File | Use |
|---|---|---|
| `4_output_anylogic/` | `supply_filled.xlsx` | AnyLogic GENERATION data |
| `4_output_anylogic/` | `demand_filled.xlsx` | AnyLogic DEMAND data |
| `4_output_anylogic/` | `scenario_parameters.csv` | Scenario config |
| `4_output_anylogic/` | `forecast_demand.csv` / `forecast_supply.csv` | 10-yr forecast |
| `5_anylogic_model/.../database/` | `db.script` | **Auto-rebuilt** — open model directly in AnyLogic |
| `3_output_check_report/` | `output_catalog.txt` | Validation + forecast assessment report |
| `3_output_check_report/` | `3d_*.html` | 3-D interactive charts |

> **Note**: `2d_update_anylogic_database.py` now writes GENERATION/DEMAND by **common months only**
> (intersection of supply and demand dates). Set `Main.nMonths` to the value in
> `4_output_anylogic/db_update_info.txt` (`COMMON aligned months`) so the historical phase never
> reads uninitialized array slots.



## Project Overview

This project implements a complete automated workflow for energy data acquisition, cleaning, processing, and analysis, ultimately generating input files for AnyLogic modeling software.

## Project Structure

```
wholepackage/
├── 0_raw_data/                    # Raw data (Excel files)
├── 1_clean_demand_supply/         # Data cleaning scripts
│   ├── 1a_crawl_national_demand.py    # Scrape demand data
│   ├── 1b_clean_demand_data.py        # Clean demand data
│   └── 1c_clean_supply_data.py        # Clean power generation data 
├── 2_process_validate/            # Data processing & validation scripts
│   ├── 2a_merge_datasets.py
│   ├── 2b_calculate_indicators.py
│   └── 2c_validate_consistency.py
├── 3_output_check_report/         # Validation reports and intermediate outputs
│   ├── 1_merged_energy_data.csv
│   ├── 2_energy_indicators.csv
│   ├── 3_1_demand_monthly.csv
│   ├── 3_2_generation_monthly.csv
│   ├── 3_3_energy_balance.csv
│   ├── 4_model_inputs.xlsx
│   └── output_catalog.txt
├── 4_output_anylogic/             # AnyLogic input files
│   ├── demand_crawl.csv
│   ├── demand_filled.xlsx
│   ├── supply_filled.xlsx
│   └── scenario_parameters.csv
├── main_pipeline.py               # Full Pipeline main entry point
└── README.md                      # This file
```

## Data Sources

### Power Generation Data
- **Hydropower**: NBS Hydropower Generation 2010-Monthly Data.xls (198 rows × 5 cols)
- **Nuclear Power**: NBS Nuclear Power Generation 2010-Monthly Data.xls (198 rows × 5 cols)
- **Wind Power**: NBS Wind Power Generation 2010-Monthly Data.xls (198 rows × 5 cols)
- **Solar Power**: NBS Solar Power Generation 2010-Monthly Data.xls (198 rows × 5 cols)

**Format**: Vertical format (dates as rows)
**Range**: January 2010 ~ February 2026 (169 months)

### Demand Data
Acquired via web crawler from the National Energy Administration (NEA).

## Pipeline Workflow

### Stage 1: Data Acquisition and Cleaning

#### 1a. Scrape Demand Data
```bash
python 1_clean_demand_supply/1a_crawl_national_demand.py
```
- Scrapes electricity consumption data from the official NEA website.
- **Output**: `cleaned_data/demand_crawled.csv`

#### 1b. Clean Demand Data
```bash
python 1_clean_demand_supply/1b_clean_demand_data.py
```
- Formats demand data.
- Handles missing values and data anomalies.
- **Output**: `cleaned_data/demand_cleaned.csv`

#### 1c. Clean Power Generation Data 
```bash
python 1_clean_demand_supply/1c_clean_supply_data.py
```
- Reads 4 power generation Excel files (vertical format).
- Merges data by date.
- **Accounting Identity Imputation**: `Total Supply = Hydro + Nuclear + Wind + Solar`.
- **Random Forest Imputation**: Handles remaining missing data.
- Consistency validation and non-negativity correction.
- **Output**: `cleaned_data/supply_filled.xlsx` (169 rows × 7 columns)

**Output Columns**:
- `date`: Date (datetime)
- `total_supply`: Total generation (MWh)
- `thermal_supply`: Thermal power (NaN - data not provided)
- `hydro_supply`: Hydropower (MWh)
- `nuclear_supply`: Nuclear power (MWh)
- `wind_supply`: Wind power (MWh)
- `solar_supply`: Solar power (MWh)

### Stage 2: Data Processing and Validation

#### 2a. Merge Datasets
```bash
python 2_process_validate/2a_merge_datasets.py
```
- Merges demand and generation datasets.
- **Output**: `2_process_validate/merged_data.csv`

#### 2b. Calculate Energy Indicators
```bash
python 2_process_validate/2b_calculate_indicators.py
```
- Calculates supply-demand ratios.
- Calculates generation mix proportions.
- Calculates Year-on-Year (YoY) growth rates.
- **Output**: `2_process_validate/energy_indicators.csv`

#### 2c. Consistency Validation
```bash
python 2_process_validate/2c_validate_consistency.py
```
- Validates accounting identities.
- Checks data quality.
- **Output**: `2_process_validate/validation_report.txt`

### Stage 3: Validation Reports (`3_output_check_report/`)

#### 3a. Merged Energy Data
```
3_output_check_report/1_merged_energy_data.csv
```
- Merged demand and supply data.

#### 3b. Energy Indicators
```
3_output_check_report/2_energy_indicators.csv
```
- Calculated YoY growth rates and derived indicators.

#### 3c. Monthly Demand
```
3_output_check_report/3_1_demand_monthly.csv
```

#### 3d. Monthly Generation
```
3_output_check_report/3_2_generation_monthly.csv
```
- Columns: date, total_supply, thermal_supply, hydro_supply, nuclear_supply, wind_supply, solar_supply.

#### 3e. Energy Balance
```
3_output_check_report/3_3_energy_balance.csv
```

#### 3f. Model Inputs
```
3_output_check_report/4_model_inputs.xlsx
```
- **Sheet1**: energy_data
- **Sheet2**: scenarios

### Stage 4: AnyLogic Input Files (`4_output_anylogic/`)

#### 4a. Demand Crawl
```
4_output_anylogic/demand_crawl.csv
```

#### 4b. Demand Filled
```
4_output_anylogic/demand_filled.xlsx
```

#### 4c. Supply Filled
```
4_output_anylogic/supply_filled.xlsx
```

#### 4d. Scenario Parameters
```
4_output_anylogic/scenario_parameters.csv
```
Contains 4 scenarios:
- **Baseline**: demand_growth = 5%
- **High Growth**: demand_growth = 8%
- **Low Growth**: demand_growth = 2%
- **Renewable Focus**: renewable_target = 70%

## Quick Start

### Run the Full Pipeline
```bash
python main_pipeline.py
```

### Run Generation Cleaning Separately 
```bash
python 1_clean_demand_supply/1c_clean_supply_data.py
```
This generates `cleaned_data/supply_filled.xlsx`.

### View Logs
```bash
tail -f pipeline.log
```

## Requirements

### Python Packages
- pandas >= 1.3.0
- numpy >= 1.21.0
- openpyxl >= 3.0.0
- xlrd >= 2.0.0
- scikit-learn >= 1.0.0

### Install Dependencies
```bash
pip install pandas numpy openpyxl xlrd scikit-learn
```

### Python Version
- Python 3.8 or higher

## Key Function Descriptions

### 1c_clean_supply_data.py

#### `load_and_prepare(file_path)`
Main entry function that performs the following:
1. Reads 4 vertical format Excel files.
2. Merges data by date (outer join).
3. Calculates `total_supply = hydro + nuclear + wind + solar`.
4. Handles missing values and outliers.
5. Outputs `supply_filled.xlsx`.

#### `_read_vertical_file(filepath, col_name)`
Reads a single vertical format Excel file:
- Rows: Date (Format: "2026年2月")
- Columns: Date, Generation Value
- Returns: DataFrame with [date, value] columns.

#### `_convert_cn_date(date_series)`
Converts Chinese date strings to datetime objects:
- Input: "2026年2月" (Series or scalar)
- Output: datetime(2026, 2, 15)

#### `fill_by_accounting(df)`
Imputation based on accounting identity:
- Total = Hydro + Nuclear + Wind + Solar.
- If Total is missing but all components exist, calculate Total.
- If one component is missing but Total and other components exist, calculate the missing component.

#### `fill_by_random_forest(df, target_cols)`
Random Forest imputation:
- Uses lag features (1, 2, 3, 6, 12 months).
- Uses rolling average features (3, 6, 12 month moving averages).
- Trains a Random Forest model to predict missing values.

#### `adjust_consistency(df)`
Consistency adjustment:
- If `hydro + nuclear + wind + solar != total_supply`:
- Adjusts component values proportionally to match the total.

## Update Log

### 2026-05-02
- ✓ Restructured output folders: `3_output_check_report/` and `4_output_anylogic/`.
- ✓ Added bilingual documentation (`README.md` + `README_zh.md`).
- ✓ Updated `.gitignore` to match actual project layout.

### 2026-04-16
- ✓ Regenerated `main_pipeline.py` to fix encoding issues.
- ✓ Updated `README.md` to reflect actual project structure.
- ✓ Generation cleaning script (1c) fully operational.
- ✓ Output directory changed to `cleaned_data/`.

### 2026-04-15
- ✓ Generation data merging and imputation completed.
- ✓ Output `supply_filled.xlsx` (169 × 7) generated.

### 2026-03-10
- ✓ Project structure refactored.
- ✓ Environment dependencies finalized.
