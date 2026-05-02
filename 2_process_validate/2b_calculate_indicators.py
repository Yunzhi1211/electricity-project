#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
2b_calculate_indicators.py - Energy Indicator Calculation

Purpose:
- Compute supply-demand balance metrics
- Compute structural share metrics
- Compute growth metrics
- Compute seasonality metrics

Input: 1_merged_energy_data.csv
Output: 2_energy_indicators.csv

Author: Yunzhi
Created: 2026-04-15
"""

import pandas as pd
import numpy as np
from pathlib import Path

# Directory configuration
BASE_DIR = Path(__file__).parent.parent
INPUT_DIR = BASE_DIR / "3_output_check_report"
OUTPUT_DIR = BASE_DIR / "3_output_check_report"


def load_merged_data() -> pd.DataFrame:
    """Load merged dataset."""
    input_file = INPUT_DIR / "1_merged_energy_data.csv"

    if not input_file.exists():
        raise FileNotFoundError(f"Merged data file not found: {input_file}")

    df = pd.read_csv(input_file)
    df['date'] = pd.to_datetime(df['date'])

    print(f"Loaded merged data: {df.shape}")
    return df


def calculate_balance_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate supply-demand balance indicators."""
    df = df.copy()

    # Supply-demand ratio.
    if 'total_supply' in df.columns and 'total_demand' in df.columns:
        df['supply_demand_ratio'] = df['total_supply'] / df['total_demand']

        # Balance state categories.
        df['balance_status'] = pd.cut(
            df['supply_demand_ratio'],
            bins=[0, 0.95, 1.05, float('inf')],
            labels=['undersupply', 'balanced', 'oversupply']
        )

    # Peak-valley spread is omitted because this is monthly data.

    print("Balance indicators calculated")
    return df


def calculate_structure_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate structural share indicators."""
    df = df.copy()

    # Generation structure shares.
    # Use non-negative clipped components and normalize by the clipped component sum,
    # which is more robust when raw source data contains occasional negative corrections.
    gen_cols = ['thermal_supply', 'hydro_supply', 'nuclear_supply', 'wind_supply', 'solar_supply']
    if all(col in df.columns for col in gen_cols):
        gen_components = df[gen_cols].clip(lower=0)
        gen_total = gen_components.sum(axis=1).replace(0, np.nan)
        for col in gen_cols:
            df[f'{col}_share'] = (gen_components[col] / gen_total).fillna(0)

    # Demand structure shares.
    demand_cols = ['primary_demand', 'secondary_demand', 'tertiary_demand', 'residential_demand']
    if all(col in df.columns for col in demand_cols):
        demand_components = df[demand_cols].clip(lower=0)
        demand_total = demand_components.sum(axis=1).replace(0, np.nan)
        for col in demand_cols:
            df[f'{col}_share'] = (demand_components[col] / demand_total).fillna(0)

    print("Structure indicators calculated")
    return df


def calculate_growth_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate growth indicators."""
    df = df.copy()

    # Numeric columns.
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    target_cols = [col for col in numeric_cols if col not in ['date', 'year', 'month', 'quarter', 'month_sin', 'month_cos']]

    # Winsorize each target series before computing growth to limit outlier influence.
    winsorized = {}
    for col in target_cols:
        series = df[col]
        valid = series.dropna()
        if len(valid) < 24:
            winsorized[col] = series
            continue
        lower = valid.quantile(0.01)
        upper = valid.quantile(0.99)
        winsorized[col] = series.clip(lower=lower, upper=upper)

    # Year-over-year growth (YoY).
    for col in target_cols:
        if col in winsorized:
            df[f'{col}_yoy'] = winsorized[col].pct_change(12)

    # Month-over-month growth (MoM).
    for col in target_cols:
        if col in winsorized:
            df[f'{col}_mom'] = winsorized[col].pct_change(1)

    # 3-month moving average of YoY growth.
    for col in target_cols:
        if col in df.columns:
            df[f'{col}_yoy_ma3'] = df[f'{col}_yoy'].rolling(3).mean()

    print("Growth indicators calculated")
    return df


def calculate_seasonal_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate seasonality indicators."""
    df = df.copy()

    # Ensure month column exists.
    if 'month' not in df.columns:
        df['month'] = df['date'].dt.month

    # Seasonal index relative to yearly mean.
    numeric_cols = ['total_demand', 'total_supply', 'thermal_supply', 'hydro_supply',
                   'nuclear_supply', 'wind_supply', 'solar_supply']

    for col in numeric_cols:
        if col in df.columns:
            # Compute yearly mean.
            yearly_mean = df.groupby(df['date'].dt.year)[col].transform('mean')
            df[f'{col}_seasonal'] = df[col] / yearly_mean

    # Seasonal strength index.
    for col in numeric_cols:
        if f'{col}_seasonal' in df.columns:
            # Monthly seasonal benchmark.
            monthly_seasonal = df.groupby('month')[f'{col}_seasonal'].transform('mean')
            df[f'{col}_seasonal_strength'] = df[f'{col}_seasonal'] / monthly_seasonal

    print("Seasonality indicators calculated")
    return df


def calculate_efficiency_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate efficiency indicators."""
    df = df.copy()

    # Simplified system efficiency indicator.
    if 'total_supply' in df.columns and 'total_demand' in df.columns:
        # Assumes 10% system loss for a coarse estimate.
        df['system_efficiency'] = df['total_demand'] / (df['total_supply'] * 1.1)

    # Renewable share.
    renewable_cols = ['hydro_supply', 'nuclear_supply', 'wind_supply', 'solar_supply']
    if all(col in df.columns for col in renewable_cols) and 'total_supply' in df.columns:
        df['renewable_share'] = df[renewable_cols].sum(axis=1) / df['total_supply']

    # Clean energy share.
    clean_cols = ['hydro_supply', 'nuclear_supply', 'wind_supply', 'solar_supply']
    if all(col in df.columns for col in clean_cols) and 'total_supply' in df.columns:
        df['clean_energy_share'] = df[clean_cols].sum(axis=1) / df['total_supply']

    print("Efficiency indicators calculated")
    return df


def validate_indicators(df: pd.DataFrame) -> None:
    """Run basic sanity checks on computed indicators."""
    print("\n=== Indicator Validation ===")

    # Check supply-demand ratio sanity.
    if 'supply_demand_ratio' in df.columns:
        ratio_stats = df['supply_demand_ratio'].describe()
        print(f"Supply-demand ratio stats: mean={ratio_stats['mean']:.3f}, range=[{ratio_stats['min']:.3f}, {ratio_stats['max']:.3f}]")

        # Detect outliers.
        outliers = df[(df['supply_demand_ratio'] < 0.5) | (df['supply_demand_ratio'] > 2.0)]
        if len(outliers) > 0:
            print(f"Warning: found {len(outliers)} abnormal supply-demand ratios")

    # Check whether shares sum to 1.
    share_cols = [col for col in df.columns if col.endswith('_share')]
    for base_col in ['total_supply', 'total_demand']:
        related_shares = [col for col in share_cols if base_col.replace('total_', '') in col]
        if related_shares:
            share_sum = df[related_shares].sum(axis=1)
            max_deviation = abs(share_sum - 1).max()
            print(f"Max share-sum deviation for {base_col}: {max_deviation:.6f}")

    # Check growth-rate sanity.
    yoy_cols = [col for col in df.columns if col.endswith('_yoy')]
    for col in yoy_cols:
        if df[col].notna().any():
            yoy_stats = df[col].describe()
            extreme_growth = df[abs(df[col]) > 1.0]
            if len(extreme_growth) > 0:
                print(f"Warning: {col} has {len(extreme_growth)} extreme growth values")


def save_indicators(df: pd.DataFrame) -> None:
    """Save calculated indicators."""
    output_file = OUTPUT_DIR / "2_energy_indicators.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\nIndicator data saved: {output_file}")


def main():
    """Main entry point."""
    print("=== Energy Indicator Calculation ===")

    # Load data.
    df = load_merged_data()

    # Calculate all indicators.
    df = calculate_balance_indicators(df)
    df = calculate_structure_indicators(df)
    df = calculate_growth_indicators(df)
    df = calculate_seasonal_indicators(df)
    df = calculate_efficiency_indicators(df)

    # Validate indicators.
    validate_indicators(df)

    # Save output.
    save_indicators(df)

    print(f"\nFinal dataset shape: {df.shape}")
    print("=== Indicator Calculation Completed ===")


if __name__ == "__main__":
    main()
