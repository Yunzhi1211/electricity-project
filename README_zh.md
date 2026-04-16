# National Energy Data Processing Pipeline

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
├── 3_output_anylogic/             # AnyLogic output files
│   ├── demand_crawl.csv
│   ├── demand_filled.csv
│   ├── supply_filled.csv
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

### Stage 3: Generate AnyLogic Input Files

#### 3a. Monthly Demand File
```
3_output_anylogic/demand_monthly.csv
```
- Columns: date, total_demand, etc.
- Format: 1 header row + N data rows.

#### 3b. Monthly Generation File 
```
3_output_anylogic/generation_monthly.csv
```
- Columns: date, total_supply, thermal_supply, hydro_supply, nuclear_supply, wind_supply, solar_supply.
- Format: 1 header row + 169 data rows.

#### 3c. Energy Balance Sheet
```
3_output_anylogic/energy_balance.csv
```
- Columns: date, total_supply, thermal_supply, etc.
- Contains all supply-side data.

#### 3d. Scenario Parameters
```
3_output_anylogic/scenario_parameters.csv
```
Contains 4 scenarios:
- **Baseline**: demand_growth = 5%
- **High Growth**: demand_growth = 8%
- **Low Growth**: demand_growth = 2%
- **Renewable Focus**: renewable_target = 70%

#### 3e. Integrated Input File
```
3_output_anylogic/model_inputs.xlsx
```
- **Sheet1**: energy_data (Raw/processed data)
- **Sheet2**: scenarios (Scenario parameters)

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
