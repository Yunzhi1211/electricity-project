#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
2c_validate_consistency.py - Data Consistency Validation

Purpose:
- Validate total vs component relationships
- Validate supply-demand balance reasonableness
- Validate temporal continuity
- Generate a validation report

Input: 2_energy_indicators.csv
Output: validation_report.txt

Author: Yunzhi
Created: 2026-04-15
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List

# Directory configuration
BASE_DIR = Path(__file__).parent.parent
INPUT_DIR = BASE_DIR / "3_output_check_report"
OUTPUT_DIR = BASE_DIR / "3_output_check_report"


def load_indicator_data() -> pd.DataFrame:
    """Load indicator dataset."""
    input_file = INPUT_DIR / "2_energy_indicators.csv"

    if not input_file.exists():
        raise FileNotFoundError(f"Indicator file not found: {input_file}")

    df = pd.read_csv(input_file)
    df['date'] = pd.to_datetime(df['date'])

    print(f"Loaded indicator data: {df.shape}")
    return df


def validate_generation_consistency(df: pd.DataFrame) -> Dict:
    """Validate generation consistency."""
    results = {}

    # Support both historical *_gen names and current *_supply names.
    if 'total_supply' in df.columns:
        total_col = 'total_supply'
        gen_cols = ['thermal_supply', 'hydro_supply', 'nuclear_supply', 'wind_supply', 'solar_supply']
    else:
        total_col = 'total_gen'
        gen_cols = ['thermal_gen', 'hydro_gen', 'nuclear_gen', 'wind_gen', 'solar_gen']

    if not all(col in df.columns for col in gen_cols + [total_col]):
        results['generation'] = "Missing required columns, skipped"
        return results

    # Sum component generation.
    sum_parts = df[gen_cols].sum(axis=1)

    # Compute deviations.
    deviation = abs(df[total_col] - sum_parts)
    max_deviation = deviation.max()
    mean_deviation = deviation.mean()
    deviation_pct = (deviation / df[total_col].replace(0, np.nan)).mean() * 100

    # Business threshold: component-sum deviation above 2% of total is abnormal.
    rel_dev = deviation / df[total_col].replace(0, np.nan)
    abnormal_count = (rel_dev > 0.02).sum()

    results['generation'] = {
        'max_deviation': max_deviation,
        'mean_deviation': mean_deviation,
        'mean_deviation_pct': deviation_pct,
        'abnormal_count': abnormal_count,
        'status': 'PASS' if abnormal_count == 0 else 'WARN'
    }

    return results


def validate_demand_consistency(df: pd.DataFrame) -> Dict:
    """Validate demand consistency."""
    results = {}

    demand_cols = ['primary_demand', 'secondary_demand', 'tertiary_demand', 'residential_demand']

    if not all(col in df.columns for col in demand_cols + ['total_demand']):
        results['demand'] = "Missing required columns, skipped"
        return results

    # Sum component demand.
    sum_parts = df[demand_cols].sum(axis=1)

    # Compute deviations.
    deviation = abs(df['total_demand'] - sum_parts)
    max_deviation = deviation.max()
    mean_deviation = deviation.mean()
    deviation_pct = (deviation / df['total_demand'].replace(0, np.nan)).mean() * 100

    # Business threshold: allow up to 2% months with component-sum deviation >10%.
    rel_dev = deviation / df['total_demand'].replace(0, np.nan)
    abnormal_count = (rel_dev > 0.10).sum()
    abnormal_rate = abnormal_count / len(df) if len(df) else 0

    results['demand'] = {
        'max_deviation': max_deviation,
        'mean_deviation': mean_deviation,
        'mean_deviation_pct': deviation_pct,
        'abnormal_count': abnormal_count,
        'status': 'PASS' if abnormal_rate <= 0.02 else 'WARN'
    }

    return results


def validate_balance_consistency(df: pd.DataFrame) -> Dict:
    """Validate supply-demand balance consistency."""
    results = {}

    if 'supply_demand_ratio' not in df.columns:
        results['balance'] = "Supply-demand ratio is missing, skipped"
        return results

    ratio = df['supply_demand_ratio']

    # Basic stats.
    stats = ratio.describe()

    # Monthly business range for this project's demand/supply statistical scope.
    normal_range = ((ratio >= 0.10) & (ratio <= 0.45)).sum()
    normal_pct = normal_range / len(ratio) * 100

    # Severe imbalance points and allowed frequency.
    severe_imbalance = ((ratio < 0.05) | (ratio > 0.65)).sum()
    severe_rate = severe_imbalance / len(ratio) if len(ratio) else 0

    results['balance'] = {
        'mean': stats['mean'],
        'min': stats['min'],
        'max': stats['max'],
        'normal_range_pct': normal_pct,
        'severe_imbalance_count': severe_imbalance,
        'status': 'PASS' if severe_rate <= 0.15 else 'WARN'
    }

    return results


def validate_temporal_consistency(df: pd.DataFrame) -> Dict:
    """Validate temporal consistency."""
    results = {}

    # Date continuity.
    date_diffs = df['date'].diff().dt.days
    max_gap = date_diffs.max()
    gaps_over_month = (date_diffs > 31).sum()

    # Data completeness.
    total_rows = len(df)
    date_range = df['date'].max() - df['date'].min()
    expected_months = (date_range.days // 30) + 1
    completeness = total_rows / expected_months * 100

    results['temporal'] = {
        'max_time_gap': max_gap,
        'gaps_over_month': gaps_over_month,
        'completeness_pct': completeness,
        'expected_months': expected_months,
        'actual_months': total_rows,
        'status': 'PASS' if gaps_over_month == 0 else 'WARN'
    }

    return results


def validate_share_consistency(df: pd.DataFrame) -> Dict:
    """Validate share consistency."""
    results = {}

    # Supply shares from 2b are named *_supply_share.
    supply_share_cols = [col for col in df.columns if col.endswith('_supply_share')]
    demand_share_cols = [col for col in df.columns if col.endswith('_demand_share')]

    if not supply_share_cols and not demand_share_cols:
        results['shares'] = "No share columns found, skipped"
        return results

    supply_dev = np.nan
    demand_dev = np.nan

    if supply_share_cols:
        supply_share_sum = df[supply_share_cols].sum(axis=1)
        valid_supply_rows = supply_share_sum > 0
        if valid_supply_rows.any():
            supply_dev = abs(supply_share_sum[valid_supply_rows] - 1).max()
        else:
            supply_dev = 0.0

    if demand_share_cols:
        demand_share_sum = df[demand_share_cols].sum(axis=1)
        valid_demand_rows = demand_share_sum > 0
        if valid_demand_rows.any():
            demand_dev = abs(demand_share_sum[valid_demand_rows] - 1).max()
        else:
            demand_dev = 0.0

    # One unified SHARES check to keep the six-check structure.
    threshold = 0.05
    warn_flag = False
    if not np.isnan(supply_dev) and supply_dev >= threshold:
        warn_flag = True
    if not np.isnan(demand_dev) and demand_dev >= threshold:
        warn_flag = True

    results['shares'] = {
        'supply_max_deviation': None if np.isnan(supply_dev) else float(supply_dev),
        'demand_max_deviation': None if np.isnan(demand_dev) else float(demand_dev),
        'status': 'PASS' if not warn_flag else 'WARN'
    }

    return results


def validate_growth_rates(df: pd.DataFrame) -> Dict:
    """Validate growth-rate reasonableness."""
    results = {}

    base_growth_cols = [
        'total_demand_yoy', 'primary_demand_yoy', 'secondary_demand_yoy',
        'tertiary_demand_yoy', 'residential_demand_yoy', 'total_supply_yoy',
        'thermal_supply_yoy', 'hydro_supply_yoy', 'nuclear_supply_yoy',
        'wind_supply_yoy', 'solar_supply_yoy'
    ]
    yoy_cols = [col for col in base_growth_cols if col in df.columns]

    if not yoy_cols:
        results['growth'] = "No growth-rate columns found, skipped"
        return results

    extreme_growth = {}
    for col in yoy_cols:
        # Business threshold: extreme monthly YoY change above +/-300%.
        extreme = df[abs(df[col]) > 3.0]
        if len(extreme) > 0:
            extreme_growth[col] = len(extreme)

    extreme_ratio = (len(extreme_growth) / len(yoy_cols)) if yoy_cols else 0
    results['growth'] = {
        'extreme_growth_column_count': len(extreme_growth),
        'status': 'PASS' if extreme_ratio <= 0.20 else 'WARN',
        'details': extreme_growth
    }

    return results


def generate_validation_report(all_results: Dict) -> str:
    """Generate a human-readable validation report."""
    report = []
    report.append("=" * 60)
    report.append("        Energy Data Consistency Report")
    report.append("=" * 60)
    report.append("")

    # Overall summary.
    all_status = []
    for _category, results in all_results.items():
        if isinstance(results, dict) and 'status' in results:
            all_status.append(results['status'])

    passed = all_status.count('PASS')
    warnings = all_status.count('WARN')
    total = len(all_status)

    report.append(f"Overall status: {passed}/{total} checks passed, {warnings} warnings")
    report.append("")

    # Detailed results.
    for category, results in all_results.items():
        report.append(f"【{category.upper()}】")

        if isinstance(results, str):
            report.append(f"  {results}")
        else:
            for sub_category, sub_results in results.items():
                report.append(f"  {sub_category}:")
                if isinstance(sub_results, dict):
                    for key, value in sub_results.items():
                        if key == 'status':
                            status_icon = "OK" if value == 'PASS' else "WARN"
                            report.append(f"    {status_icon} {key}: {value}")
                        else:
                            report.append(f"    - {key}: {value}")
                else:
                    report.append(f"    {sub_results}")
        report.append("")

    report.append("=" * 60)
    return "\n".join(report)


def save_validation_report(report: str) -> None:
    """Save validation report to disk."""
    output_file = OUTPUT_DIR / "validation_report.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"\nValidation report saved: {output_file}")


def main():
    """Main entry point."""
    print("=== Data Consistency Validation ===")

    # Load data.
    df = load_indicator_data()

    # Execute all checks.
    all_results = {}

    all_results.update(validate_generation_consistency(df))
    all_results.update(validate_demand_consistency(df))
    all_results.update(validate_balance_consistency(df))
    all_results.update(validate_temporal_consistency(df))
    all_results.update(validate_share_consistency(df))
    all_results.update(validate_growth_rates(df))

    # Generate report.
    report = generate_validation_report(all_results)
    print(report)

    # Save report.
    save_validation_report(report)

    print("=== Consistency Validation Completed ===")


if __name__ == "__main__":
    main()


