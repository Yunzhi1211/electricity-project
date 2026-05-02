#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
2a_merge_datasets.py - Dataset Merge

Purpose:
- Merge cleaned demand and generation datasets
- Align records by date
- Produce a unified energy dataset

Input:
- demand_filled.xlsx  (from 4_output_anylogic)
- supply_filled.xlsx  (from 4_output_anylogic)

Output: 1_merged_energy_data.csv

Author: Yunzhi
Created: 2026-04-15
"""

import pandas as pd
from pathlib import Path

# Directory configuration
BASE_DIR = Path(__file__).parent.parent
INPUT_DIR = BASE_DIR / "4_output_anylogic"
OUTPUT_DIR = BASE_DIR / "3_output_check_report"


def load_cleaned_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load cleaned demand and supply datasets from 4_output_anylogic."""
    demand_file = INPUT_DIR / "demand_filled.xlsx"
    supply_file = INPUT_DIR / "supply_filled.xlsx"

    demand_df = None
    generation_df = None

    if demand_file.exists():
        demand_df = pd.read_excel(demand_file)
        # Normalize to month granularity so sources with different day-of-month can align.
        demand_df['date'] = pd.to_datetime(demand_df['date']).dt.to_period('M').dt.to_timestamp()
        demand_df = demand_df.sort_values('date').drop_duplicates(subset=['date'], keep='last').reset_index(drop=True)
        print(f"Loaded demand data: {demand_df.shape}")
    else:
        print(f"Warning: demand file not found: {demand_file}")

    if supply_file.exists():
        generation_df = pd.read_excel(supply_file)
        # Normalize to month granularity so sources with different day-of-month can align.
        generation_df['date'] = pd.to_datetime(generation_df['date']).dt.to_period('M').dt.to_timestamp()
        generation_df = generation_df.sort_values('date').drop_duplicates(subset=['date'], keep='last').reset_index(drop=True)
        print(f"Loaded supply data: {generation_df.shape}")
    else:
        print(f"Warning: supply file not found: {supply_file}")

    return demand_df, generation_df


def merge_datasets(demand_df: pd.DataFrame, generation_df: pd.DataFrame) -> pd.DataFrame:
    """Merge available datasets by date."""
    if demand_df is None and generation_df is None:
        raise ValueError("At least one dataset is required")

    if demand_df is not None and generation_df is not None:
        # Merge when both datasets exist.
        merged = pd.merge(
            demand_df,
            generation_df,
            on='date',
            how='outer',
            suffixes=('_demand', '_gen')
        )
        print(f"Merged two datasets: {merged.shape}")

    elif demand_df is not None:
        merged = demand_df.copy()
        print("Only demand data is available")

    else:
        merged = generation_df.copy()
        print("Only generation data is available")

    # Sort by date.
    merged = merged.sort_values('date').reset_index(drop=True)

    return merged


def validate_merge(merged_df: pd.DataFrame) -> None:
    """Run basic checks on the merged dataset."""
    print("\n=== Merge Validation ===")

    # Basic information.
    print(f"Row count: {len(merged_df)}")
    print(f"Date range: {merged_df['date'].min()} to {merged_df['date'].max()}")

    # Missing value summary.
    print("\nMissing values:")
    missing_stats = merged_df.isnull().sum()
    for col, missing in missing_stats.items():
        if missing > 0:
            percentage = (missing / len(merged_df)) * 100
            print(f"  {col}: {missing} ({percentage:.1f}%)")

    # Date continuity check.
    date_gaps = merged_df['date'].diff().dt.days
    max_gap = date_gaps.max()
    print(f"\nMaximum date gap: {max_gap} days")

    if max_gap > 31:
        print("Warning: large date gap detected; missing periods may exist")

    # Numeric sanity summary.
    numeric_cols = merged_df.select_dtypes(include=['float64', 'int64']).columns
    print(f"\nNumeric column summary ({len(numeric_cols)} columns):")
    for col in numeric_cols:
        if col != 'date':
            stats = merged_df[col].describe()
            print(f"  {col}: mean={stats['mean']:.2f}, min={stats['min']:.2f}, max={stats['max']:.2f}")


def save_merged_data(merged_df: pd.DataFrame) -> None:
    """Save merged dataset to CSV."""
    output_file = OUTPUT_DIR / "1_merged_energy_data.csv"
    merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\nMerged dataset saved: {output_file}")


def main():
    """Main entry point."""
    print("=== Dataset Merge ===")

    # Load data.
    demand_df, generation_df = load_cleaned_data()

    if demand_df is None and generation_df is None:
        print("Error: no input dataset found")
        return

    # Merge datasets.
    merged_df = merge_datasets(demand_df, generation_df)

    # Validate merged output.
    validate_merge(merged_df)

    # Save result.
    save_merged_data(merged_df)

    print("=== Dataset Merge Completed ===")


if __name__ == "__main__":
    main()
