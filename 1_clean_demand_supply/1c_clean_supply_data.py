# -*- coding: utf-8 -*-
"""
1c_clean_supply_data.py - Supply Data Cleaning and Imputation

This script cleans monthly generation data and imputes missing values for
total and component supply series.

Method overview:
1. Load source generation files and align by date.
2. Apply accounting-rule completion where possible.
3. Build temporal lag/rolling features.
4. Use Random Forest to impute remaining missing values.
5. Apply consistency and non-negative corrections.
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from pathlib import Path
import logging
import re

# Base configuration
BASE_DIR = Path(__file__).parent.parent
INPUT_DIR = BASE_DIR / "0_raw_data"
OUTPUT_DIR = BASE_DIR / "4_output_anylogic"
LOG_DIR = BASE_DIR / "0_log"
LOG_DIR.mkdir(exist_ok=True)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Supply column definitions
TARGETS = ['total_supply', 'thermal_supply', 'hydro_supply', 'nuclear_supply', 'wind_supply', 'solar_supply']
PART_COLS = ['thermal_supply', 'hydro_supply', 'nuclear_supply', 'wind_supply', 'solar_supply']


def load_and_prepare(file_path):
    """Load and pre-clean supply data from mixed (vertical/horizontal) files."""
    logger.info(f"Reading data: {file_path}")
    
    file_path = Path(file_path)
    
    if not file_path.is_dir():
        logger.error("Input must be a directory path")
        return None
    
    logger.info("Reading supply datasets (auto-detect vertical/horizontal formats)...")

    dfs = []
    file_specs = [
        ("\u56fd\u5bb6\u7edf\u8ba1\u5c40\u53d1\u7535\u91cf2010-\u6708\u5ea6\u6570\u636e.xls", "total_supply", "Total generation"),
        ("\u56fd\u5bb6\u7edf\u8ba1\u5c40\u706b\u529b\u53d1\u7535\u91cf2010-\u6708\u5ea6\u6570\u636e.xls", "thermal_supply", "Thermal power"),
        ("\u56fd\u5bb6\u7edf\u8ba1\u5c40\u6c34\u529b\u53d1\u7535\u91cf2010-\u6708\u5ea6\u6570\u636e.xls", "hydro_supply", "Hydropower"),
        ("\u56fd\u5bb6\u7edf\u8ba1\u5c40\u6838\u80fd\u53d1\u7535\u91cf2010-\u6708\u5ea6\u6570\u636e.xls", "nuclear_supply", "Nuclear power"),
        ("\u56fd\u5bb6\u7edf\u8ba1\u5c40\u98ce\u529b\u53d1\u7535\u91cf2010-\u6708\u5ea6\u6570\u636e.xls", "wind_supply", "Wind power"),
        ("\u56fd\u5bb6\u7edf\u8ba1\u5c40\u592a\u9633\u80fd\u53d1\u7535\u91cf2010-\u6708\u5ea6\u6570\u636e.xls", "solar_supply", "Solar power"),
    ]

    for filename, col_name, label in file_specs:
        src = file_path / filename
        if not src.exists():
            continue
        df = _read_supply_file(src, col_name)
        if df is not None and not df.empty:
            dfs.append(df)
            logger.info(f"  {label}: {df.shape}")
    
    if not dfs:
        logger.error("No readable supply datasets were found")
        return None
    
    # Merge component datasets by date.
    logger.info(f"Merging {len(dfs)} datasets...")
    df = dfs[0]
    for other_df in dfs[1:]:
        df = df.merge(other_df, on='date', how='outer')
    
    logger.info(f"Merge complete, shape: {df.shape}")
    
    for col in TARGETS:
        if col not in df.columns:
            df[col] = np.nan

    # If total_supply is unavailable for a month, use component sum as fallback.
    part_sum = df[PART_COLS].sum(axis=1, min_count=1)
    df['total_supply'] = df['total_supply'].where(df['total_supply'].notna(), part_sum)
    
    # Output column order.
    df = df[['date', 'total_supply', 'thermal_supply', 'hydro_supply', 'nuclear_supply', 'wind_supply', 'solar_supply']].copy()
    
    # Basic data cleanup.
    if df is None or df.empty:
        logger.error("Loaded dataset is empty")
        return None
    
    # Parse dates.
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Drop invalid date rows.
    df = df[df['date'].notna()].copy()

    # Convert non-date columns to numeric.
    for col in df.columns:
        if col != 'date':
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Sort and deduplicate by date.
    df = df.sort_values('date').reset_index(drop=True)
    df = df.drop_duplicates(subset=['date'], keep='first')

    logger.info(f"Data load complete, shape: {df.shape}")
    return df


def _read_supply_file(filepath, col_hint):
    """Read one supply file with format auto-detection."""
    try:
        raw = pd.read_excel(filepath, header=None)
        # Horizontal format usually has one row with many month labels (e.g. 2026年2月).
        month_pattern = re.compile(r"^\d{4}\u5e74\d{1,2}\u6708$")
        max_month_hits = 0
        for row_idx in range(min(10, len(raw))):
            row_vals = raw.iloc[row_idx].astype(str).str.strip()
            month_hits = row_vals.apply(lambda x: bool(month_pattern.match(str(x).strip()))).sum()
            max_month_hits = max(max_month_hits, int(month_hits))

        if max_month_hits >= 3:
            return _read_horizontal_file(raw, filepath, col_hint)
        return _read_vertical_file(filepath, col_hint)
    except Exception as e:
        logger.warning(f"Failed to read file {filepath}: {e}")
        return None


def _read_horizontal_file(df_raw, filepath, col_hint):
    """Read horizontal-format file (rows=indicators, columns=months)."""
    try:
        df_raw = df_raw.dropna(how='all').dropna(axis=1, how='all').reset_index(drop=True)
        if df_raw.empty:
            return None

        month_pattern = re.compile(r"^\d{4}\u5e74\d{1,2}\u6708$")
        month_row = None
        month_cols = []
        for row_idx in range(min(12, len(df_raw))):
            row_vals = df_raw.iloc[row_idx].astype(str).str.strip()
            cols = [i for i, v in enumerate(row_vals) if month_pattern.match(str(v).strip())]
            if len(cols) >= 3:
                month_row = row_idx
                month_cols = cols
                break

        if month_row is None:
            logger.warning(f"No month-header row found in horizontal file: {filepath}")
            return None

        label_col = 0
        label_series = df_raw.iloc[:, label_col].astype(str).str.strip()
        row_mask = label_series.str.contains("\u5f53\u671f\u503c", na=False)
        if not row_mask.any():
            logger.warning(f"No '\u5f53\u671f\u503c' row found in file: {filepath}")
            return None

        value_row = df_raw.loc[row_mask].iloc[0]
        records = []
        for col_idx in month_cols:
            date_text = str(df_raw.iat[month_row, col_idx]).strip()
            value = pd.to_numeric(value_row.iloc[col_idx], errors='coerce')
            records.append({"date": _convert_cn_date(date_text), col_hint: value})

        out = pd.DataFrame(records).dropna(subset=['date'])
        out[col_hint] = pd.to_numeric(out[col_hint], errors='coerce')
        out = out.dropna(subset=[col_hint])
        return out
    except Exception as e:
        logger.warning(f"Failed to process horizontal format ({filepath.name}): {e}")
        return None


def _read_vertical_file(filepath, col_hint):
    """Read one vertical-format Excel file (rows=time, columns=fields).
    
    Args:
        filepath: Excel file path.
        col_hint: Output column name for extracted values.
    
    Returns:
        A DataFrame containing date and one numeric value column.
    """
    try:
        df = pd.read_excel(filepath)
        
        # In this source format, row 2 contains headers.
        if df.shape[0] < 2:
            logger.warning(f"Too few rows in file: {filepath}")
            return None
        
        # Extract header row.
        header = df.iloc[1, :].tolist()
        
        # Data starts from row 3.
        df_data = df.iloc[2:, :].copy()
        df_data.columns = header
        
        # First column is date.
        date_col = df_data.columns[0]
        
        # Use the second column as value column.
        value_col = df_data.columns[1] if len(df_data.columns) > 1 else None
        
        if value_col is None:
            logger.warning(f"Value column not found: {filepath}")
            return None
        
        # Keep date and value columns.
        result = df_data[[date_col, value_col]].copy()
        result.columns = ['date', col_hint]
        
        # Convert Chinese date format such as "2026年2月".
        result['date'] = result['date'].apply(_convert_cn_date)
        
        # Convert numeric values.
        result[col_hint] = pd.to_numeric(result[col_hint], errors='coerce')
        
        # Drop invalid rows.
        result = result.dropna(subset=['date', col_hint])
        
        return result
        
    except Exception as e:
        logger.warning(f"Failed to read file {filepath}: {e}")
        return None


def _process_vertical_format(df, filename):
    """Process vertical-format data (rows=months, columns=fields)."""
    try:
        # Row 2 is the header row in this file format.
        header_row = 1
        
        # Apply headers.
        new_header = df.iloc[header_row, :].tolist()
        df_clean = df.iloc[header_row+1:, :].reset_index(drop=True)
        df_clean.columns = new_header
        
        # First column is date.
        date_col = df_clean.columns[0]
        df_clean = df_clean.rename(columns={date_col: 'date'})
        
        # Convert dates.
        df_clean['date'] = _convert_cn_date(df_clean['date'])
        
        # Value column is usually the second column.
        if len(df_clean.columns) > 1:
            numeric_col = df_clean.columns[1]
            df_result = df_clean[['date', numeric_col]].copy()
        else:
            return None
        
        # Infer component name from file name.
        if '\u592a\u9633' in filename:
            df_result = df_result.rename(columns={numeric_col: 'solar_supply'})
        elif '\u6c34' in filename:
            df_result = df_result.rename(columns={numeric_col: 'hydro_supply'})
        elif '\u6838' in filename:
            df_result = df_result.rename(columns={numeric_col: 'nuclear_supply'})
        elif '\u98ce' in filename:
            df_result = df_result.rename(columns={numeric_col: 'wind_supply'})
        
        return df_result
        
    except Exception as e:
        logger.warning(f"Failed to process vertical format ({filename}): {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return None


def _convert_cn_date(date_series):
    """Convert Chinese date format (e.g., 2026年2月) to datetime."""
    def convert_single(date_str):
        if pd.isna(date_str):
            return pd.NaT
        
        date_str = str(date_str).strip()
        
        try:
            # Handle format like "2026年2月".
            if '\u5e74' in date_str and '\u6708' in date_str:
                date_str = date_str.replace('\u5e74', '-').replace('\u6708', '')
                # Use day 15 as a mid-month placeholder.
                if len(date_str.split('-')) == 2:
                    date_str += '-15'
                return pd.to_datetime(date_str)
            
            # Fall back to generic parser.
            return pd.to_datetime(date_str)
        except:
            return pd.NaT
    
    # Handle both Series and scalar input.
    if isinstance(date_series, pd.Series):
        return date_series.apply(convert_single)
    else:
        return convert_single(date_series)
    
    return date_series.apply(convert_single)


def add_temporal_features(df):
    """Add temporal features."""
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['quarter'] = df['date'].dt.quarter

    # Cyclical month encoding.
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)

    return df


def fill_by_accounting(df):
    """Apply accounting-rule completion: total_supply = sum(parts)."""
    logger.info("Applying accounting-rule completion...")
    changed = True
    iterations = 0

    while changed and iterations < 100:
        changed = False
        iterations += 1

        # Infer total when all components are present.
        mask_total = df['total_supply'].isna() & df[PART_COLS].notna().all(axis=1)
        if mask_total.any():
            df.loc[mask_total, 'total_supply'] = df.loc[mask_total, PART_COLS].sum(axis=1)
            changed = True

        # Infer one missing component from total and other components.
        for col in PART_COLS:
            other_parts = [c for c in PART_COLS if c != col]
            mask_part = (
                df[col].isna() &
                df['total_supply'].notna() &
                df[other_parts].notna().all(axis=1)
            )
            if mask_part.any():
                df.loc[mask_part, col] = (
                    df.loc[mask_part, 'total_supply'] -
                    df.loc[mask_part, other_parts].sum(axis=1)
                )
                changed = True

    logger.info(f"Accounting-rule completion finished after {iterations} iterations")
    return df


def create_temp_filled(df, col):
    """Create temporary filled series for feature engineering."""
    temp = df[col].copy()

    # Linear interpolation.
    temp = temp.interpolate(method='linear')

    # Fill by month-wise historical mean.
    month_mean = df.groupby('month')[col].transform('mean')
    temp = temp.fillna(month_mean)

    # Fill remaining NaNs with median.
    temp = temp.fillna(df[col].median())

    return temp


def add_series_features(df, col):
    """Add lag and rolling features for a target column."""
    temp_col = f'{col}_temp'
    df[temp_col] = create_temp_filled(df, col)

    # Lag terms.
    df[f'{col}_lag_1'] = df[temp_col].shift(1)
    df[f'{col}_lag_2'] = df[temp_col].shift(2)
    df[f'{col}_lag_3'] = df[temp_col].shift(3)
    df[f'{col}_lag_12'] = df[temp_col].shift(12)

    # Shifted rolling means to avoid leakage.
    df[f'{col}_roll_mean_3'] = df[temp_col].shift(1).rolling(3).mean()
    df[f'{col}_roll_mean_6'] = df[temp_col].shift(1).rolling(6).mean()
    df[f'{col}_roll_mean_12'] = df[temp_col].shift(1).rolling(12).mean()

    return df


def ml_fill_column(df, target_col):
    """Impute one target column using Random Forest."""
    feature_cols = [
        'year', 'month', 'quarter', 'month_sin', 'month_cos',
        f'{target_col}_lag_1',
        f'{target_col}_lag_2',
        f'{target_col}_lag_3',
        f'{target_col}_lag_12',
        f'{target_col}_roll_mean_3',
        f'{target_col}_roll_mean_6',
        f'{target_col}_roll_mean_12'
    ]

    train_df = df[df[target_col].notna()].copy()
    pred_df = df[df[target_col].isna()].copy()

    # Return if no missing values.
    if pred_df.empty:
        return df

    # Fallback for very small training sets.
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


def consistency_adjustment(df):
    """Scale parts so that their sum aligns with total supply."""
    logger.info("Applying consistency adjustment...")
    sum_parts = df[PART_COLS].sum(axis=1)

    # Avoid division-by-zero.
    ratio = np.where(sum_parts > 0, df['total_supply'] / sum_parts, 1)

    for col in PART_COLS:
        df[col] = df[col] * ratio

    return df


def non_negative_correction(df):
    """Clip negative values to zero."""
    logger.info("Applying non-negative correction...")
    for col in TARGETS:
        df[col] = df[col].clip(lower=0)

    return df


def clean_supply_data(input_file, output_file):
    """Main flow: load -> clean -> impute -> save."""
    logger.info(f"Starting supply data processing: {input_file}")
    
    df = load_and_prepare(input_file)
    
    if df is None:
        logger.error("Data loading failed, aborting supply cleaning.")
        return None
    
    logger.info("Adding temporal features...")
    df = add_temporal_features(df)
    
    logger.info("Running accounting-rule completion...")
    df = fill_by_accounting(df)
    
    logger.info("Building time-series features...")
    for col in TARGETS:
        df = add_series_features(df, col)
    
    logger.info("Running random-forest imputation...")
    for col in TARGETS:
        logger.info(f"  Imputing {col}...")
        df = ml_fill_column(df, col)
    
    logger.info("Running consistency adjustment...")
    df = consistency_adjustment(df)
    
    logger.info("Running non-negative correction...")
    df = non_negative_correction(df)
    
    # Keep final output columns only.
    output_df = df[['date'] + TARGETS].copy()
    
    logger.info(f"Saving output to: {output_file}")
    output_df.to_excel(output_file, index=False)
    
    return output_df


if __name__ == "__main__":
    print("="*50)
    print("1c_clean_supply_data started")
    print("="*50)
    
    # Find input source file or directory.
    possible_paths = [
        INPUT_DIR,
        INPUT_DIR / "supply_data.xlsx",
        INPUT_DIR / "generation_data.xlsx",
        Path("D:/\u4e00\u4e2a\u6587\u4ef6\u5939/\u5b66\u4e60/\u5b66\u4e60/hku/sem2/MSDA7102/project/\u5efa\u6a21\u6570\u636e"),
        Path(r"D:\\u4e00\u4e2a\u6587\u4ef6\u5939\\u5b66\u4e60\\u5b66\u4e60\hku\sem2\MSDA7102\project\\u5efa\u6a21\u6570\u636e"),
    ]

    input_file = None
    for path in possible_paths:
        if Path(path).exists():
            input_file = path
            print(f"Found data source: {path}")
            break

    if input_file is None:
        print("ERROR: no data source found. Tried paths:")
        for path in possible_paths:
            print(f"  {path}")
    else:
        output_file = OUTPUT_DIR / "supply_filled.xlsx"
        print("Processing data...")
        result = clean_supply_data(str(input_file), str(output_file))
        if result is not None:
            print("Preview of first 20 rows:")
            print(result.head(20))
        print("Processing completed")

