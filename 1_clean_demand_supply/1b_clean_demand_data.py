# -*- coding: utf-8 -*-
"""
1b_clean_demand_data.py - Demand Data Cleaning and Imputation

Imputation uses two stages:
1. Accounting-rule completion with total = sum(parts), including reverse
    inference for single missing part columns.
2. Random-forest prediction for remaining gaps using temporal features such as
    year/month, lags, and rolling averages.

Finally, values are adjusted for consistency so total demand aligns with the
sum of demand categories as closely as possible.
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from pathlib import Path

# Base configuration
BASE_DIR = Path(__file__).parent.parent
INPUT_DIR = BASE_DIR / "4_output_anylogic"
OUTPUT_DIR = BASE_DIR / "4_output_anylogic"

YEAR_START = 2010
YEAR_END = 2025

# Parameters
FILE_PATH = INPUT_DIR / "demand_crawl.csv"
OUTPUT_PATH = OUTPUT_DIR / "demand_filled.xlsx"

# Date and target columns
DATE_COL = 'date'
TOTAL_COL = 'total_demand'
PART_COLS = [
    'primary_demand',      # Electricity demand in the primary sector
    'secondary_demand',    # Electricity demand in the secondary sector
    'tertiary_demand',     # Electricity demand in the tertiary sector
    'residential_demand',  # Residential electricity demand
]

# Keep only the first 7 columns (date + 6 numeric columns) when True.
KEEP_FIRST_7_COLS = True


def load_and_prepare(file_path):
    """Load and pre-clean input demand data."""
    df = pd.read_csv(file_path)

    # Keep a fixed schema width if configured.
    if KEEP_FIRST_7_COLS:
        df = df.iloc[:, :7].copy()

    # Drop description rows that contain a literal "date" label.
    df = df[df[DATE_COL] != 'date'].copy()

    # Parse date column.
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors='coerce')

    # Drop rows with invalid dates.
    df = df[df[DATE_COL].notna()].copy()

    # Target columns.
    targets = [TOTAL_COL] + PART_COLS

    # Convert targets to numeric.
    for col in targets:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Sort by date.
    df = df.sort_values(DATE_COL).reset_index(drop=True)

    # Keep only the modeling window (2010-2025).
    df = df[
        (df[DATE_COL].dt.year >= YEAR_START) &
        (df[DATE_COL].dt.year <= YEAR_END)
    ].copy()
    df = df.sort_values(DATE_COL).reset_index(drop=True)
    
    return df


def add_temporal_features(df):
    """Add temporal features used for imputation."""
    df['year'] = df[DATE_COL].dt.year
    df['month'] = df[DATE_COL].dt.month
    df['quarter'] = df[DATE_COL].dt.quarter

    # Cyclical month encoding.
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    
    return df


def fill_by_accounting(df, total_col, part_cols):
    """Fill values using accounting identity: total = sum(parts)."""
    changed = True

    while changed:
        changed = False

        # If total is missing but all parts exist, infer total.
        mask_total = df[total_col].isna() & df[part_cols].notna().all(axis=1)
        if mask_total.any():
            df.loc[mask_total, total_col] = df.loc[mask_total, part_cols].sum(axis=1)
            changed = True

        # If one part is missing and total + other parts exist, infer that part.
        for col in part_cols:
            other_parts = [c for c in part_cols if c != col]
            mask_part = (
                df[col].isna() &
                df[total_col].notna() &
                df[other_parts].notna().all(axis=1)
            )
            if mask_part.any():
                df.loc[mask_part, col] = (
                    df.loc[mask_part, total_col] -
                    df.loc[mask_part, other_parts].sum(axis=1)
                )
                changed = True

    return df


def create_temp_filled(df, col):
    """Create temporary filled series for lag/rolling feature construction."""
    temp = df[col].copy()

    # Linear interpolation.
    temp = temp.interpolate(method='linear')

    # Fill remaining gaps by month-wise historical mean.
    month_mean = df.groupby('month')[col].transform('mean')
    temp = temp.fillna(month_mean)

    # Fall back to column median.
    temp = temp.fillna(df[col].median())

    return temp


def add_series_features(df, col):
    """Add lag and rolling features for one target column."""
    temp_col = f'{col}_temp'
    df[temp_col] = create_temp_filled(df, col)

    # Lag terms.
    df[f'{col}_lag_1'] = df[temp_col].shift(1)
    df[f'{col}_lag_2'] = df[temp_col].shift(2)
    df[f'{col}_lag_3'] = df[temp_col].shift(3)
    df[f'{col}_lag_6'] = df[temp_col].shift(6)
    df[f'{col}_lag_12'] = df[temp_col].shift(12)

    # Rolling means shifted by 1 month to avoid data leakage.
    df[f'{col}_roll_mean_3'] = df[temp_col].shift(1).rolling(3).mean()
    df[f'{col}_roll_mean_6'] = df[temp_col].shift(1).rolling(6).mean()
    df[f'{col}_roll_mean_12'] = df[temp_col].shift(1).rolling(12).mean()

    return df


def ml_fill_column(df, target_col):
    """Fill missing values for one column using Random Forest."""
    feature_cols = [
        'year', 'month', 'quarter', 'month_sin', 'month_cos',
        f'{target_col}_lag_1',
        f'{target_col}_lag_2',
        f'{target_col}_lag_3',
        f'{target_col}_lag_6',
        f'{target_col}_lag_12',
        f'{target_col}_roll_mean_3',
        f'{target_col}_roll_mean_6',
        f'{target_col}_roll_mean_12'
    ]

    train_df = df[df[target_col].notna()].copy()
    pred_df = df[df[target_col].isna()].copy()

    # Return immediately when no missing values exist.
    if pred_df.empty:
        return df

    # For very small training sets, use month mean / median fallback.
    if len(train_df) < 12:
        month_mean_map = train_df.groupby('month')[target_col].mean()
        fill_vals = pred_df['month'].map(month_mean_map)
        fill_vals = fill_vals.fillna(train_df[target_col].median())
        df.loc[df[target_col].isna(), target_col] = fill_vals.values
        return df

    # Fill feature NaNs with training medians.
    for col in feature_cols:
        med = train_df[col].median()
        train_df[col] = train_df[col].fillna(med)
        pred_df[col] = pred_df[col].fillna(med)

    X_train = train_df[feature_cols]
    y_train = train_df[target_col]
    X_pred = pred_df[feature_cols]

    model = RandomForestRegressor(
        n_estimators=500,
        max_depth=8,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1
    )

    model.fit(X_train, y_train)
    pred_values = model.predict(X_pred)

    df.loc[df[target_col].isna(), target_col] = pred_values

    return df


def consistency_adjustment(df, total_col, part_cols):
    """Scale parts so their sum aligns with total demand."""
    sum_parts = df[part_cols].sum(axis=1)

    # Avoid division-by-zero.
    ratio = np.where(sum_parts > 0, df[total_col] / sum_parts, 1)

    for col in part_cols:
        df[col] = df[col] * ratio
    
    return df


def clean_demand_data(input_file, output_file):
    """Main flow: load -> clean -> impute -> save."""
    print(f"Loading data: {input_file}")
    df = load_and_prepare(input_file)
    
    print("Adding temporal features...")
    df = add_temporal_features(df)
    
    print("Applying accounting-rule completion...")
    df = fill_by_accounting(df, TOTAL_COL, PART_COLS)
    
    print("Building time-series features...")
    targets = [TOTAL_COL] + PART_COLS
    for col in targets:
        df = add_series_features(df, col)
    
    print("Running random-forest imputation...")
    for col in targets:
        df = ml_fill_column(df, col)
    
    print("Applying consistency adjustment...")
    df = consistency_adjustment(df, TOTAL_COL, PART_COLS)
    
    # Output only date + five demand columns.
    output_df = df[[DATE_COL] + [TOTAL_COL] + PART_COLS].copy()
    
    print(f"Saving output to: {output_file}")
    output_df.to_excel(output_file, index=False)
    
    return output_df


if __name__ == "__main__":
    if FILE_PATH.exists():
        print(f"Processing file: {FILE_PATH}")
        result = clean_demand_data(str(FILE_PATH), str(OUTPUT_PATH))
        print("Preview of first 20 rows:")
        print(result.head(20))
    else:
        print(f"Input file not found: {FILE_PATH}")
        print("Run 1a_crawl_national_demand.py first to fetch source demand data")
