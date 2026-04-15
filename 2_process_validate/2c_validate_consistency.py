#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
2c_validate_consistency.py - 数据一致性校验

功能：
- 校验总量与分项之和的关系
- 校验供需平衡的合理性
- 校验时间序列的连续性
- 生成校验报告

输入：energy_indicators.csv
输出：validation_report.txt

作者：Yunzhi
创建时间：2026-04-15
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List

# 目录配置
BASE_DIR = Path(__file__).parent.parent
INPUT_DIR = BASE_DIR / "2_process_validate"
OUTPUT_DIR = BASE_DIR / "2_process_validate"


def load_indicator_data() -> pd.DataFrame:
    """加载指标数据"""
    input_file = INPUT_DIR / "energy_indicators.csv"

    if not input_file.exists():
        raise FileNotFoundError(f"找不到指标数据文件: {input_file}")

    df = pd.read_csv(input_file)
    df['date'] = pd.to_datetime(df['date'])

    print(f"加载指标数据: {df.shape}")
    return df


def validate_generation_consistency(df: pd.DataFrame) -> Dict:
    """校验发电量数据一致性"""
    results = {}

    gen_cols = ['thermal_gen', 'hydro_gen', 'nuclear_gen', 'wind_gen', 'solar_gen']

    if not all(col in df.columns for col in gen_cols + ['total_gen']):
        results['generation'] = "缺少必要列，跳过校验"
        return results

    # 计算分项之和
    sum_parts = df[gen_cols].sum(axis=1)

    # 计算偏差
    deviation = abs(df['total_gen'] - sum_parts)
    max_deviation = deviation.max()
    mean_deviation = deviation.mean()
    deviation_pct = (deviation / df['total_gen'].replace(0, np.nan)).mean() * 100

    # 统计异常点
    threshold = df['total_gen'].mean() * 0.01  # 1%阈值
    abnormal_count = (deviation > threshold).sum()

    results['generation'] = {
        '最大偏差': max_deviation,
        '平均偏差': mean_deviation,
        '平均偏差百分比': deviation_pct,
        '异常点数量': abnormal_count,
        '校验状态': '通过' if abnormal_count == 0 else '警告'
    }

    return results


def validate_demand_consistency(df: pd.DataFrame) -> Dict:
    """校验用电量数据一致性"""
    results = {}

    demand_cols = ['primary_demand', 'secondary_demand', 'tertiary_demand', 'residential_demand']

    if not all(col in df.columns for col in demand_cols + ['total_demand']):
        results['demand'] = "缺少必要列，跳过校验"
        return results

    # 计算分项之和
    sum_parts = df[demand_cols].sum(axis=1)

    # 计算偏差
    deviation = abs(df['total_demand'] - sum_parts)
    max_deviation = deviation.max()
    mean_deviation = deviation.mean()
    deviation_pct = (deviation / df['total_demand'].replace(0, np.nan)).mean() * 100

    # 统计异常点
    threshold = df['total_demand'].mean() * 0.01  # 1%阈值
    abnormal_count = (deviation > threshold).sum()

    results['demand'] = {
        '最大偏差': max_deviation,
        '平均偏差': mean_deviation,
        '平均偏差百分比': deviation_pct,
        '异常点数量': abnormal_count,
        '校验状态': '通过' if abnormal_count == 0 else '警告'
    }

    return results


def validate_balance_consistency(df: pd.DataFrame) -> Dict:
    """校验供需平衡一致性"""
    results = {}

    if 'supply_demand_ratio' not in df.columns:
        results['balance'] = "缺少供需比数据，跳过校验"
        return results

    ratio = df['supply_demand_ratio']

    # 基本统计
    stats = ratio.describe()

    # 检查合理范围（0.8-1.2之间为正常）
    normal_range = ((ratio >= 0.8) & (ratio <= 1.2)).sum()
    normal_pct = normal_range / len(ratio) * 100

    # 严重不平衡点
    severe_imbalance = ((ratio < 0.5) | (ratio > 2.0)).sum()

    results['balance'] = {
        '均值': stats['mean'],
        '最小值': stats['min'],
        '最大值': stats['max'],
        '正常范围占比': normal_pct,
        '严重不平衡点数': severe_imbalance,
        '校验状态': '通过' if severe_imbalance == 0 else '警告'
    }

    return results


def validate_temporal_consistency(df: pd.DataFrame) -> Dict:
    """校验时间序列一致性"""
    results = {}

    # 检查日期连续性
    date_diffs = df['date'].diff().dt.days
    max_gap = date_diffs.max()
    gaps_over_month = (date_diffs > 31).sum()

    # 检查数据完整性
    total_rows = len(df)
    date_range = df['date'].max() - df['date'].min()
    expected_months = (date_range.days // 30) + 1
    completeness = total_rows / expected_months * 100

    results['temporal'] = {
        '最大时间间隔': max_gap,
        '超月间隔数量': gaps_over_month,
        '数据完整性': completeness,
        '预期月数': expected_months,
        '实际月数': total_rows,
        '校验状态': '通过' if gaps_over_month == 0 else '警告'
    }

    return results


def validate_share_consistency(df: pd.DataFrame) -> Dict:
    """校验占比数据一致性"""
    results = {}

    # 检查发电结构占比
    gen_share_cols = [col for col in df.columns if col.endswith('_gen_share')]
    if gen_share_cols:
        gen_share_sum = df[gen_share_cols].sum(axis=1)
        gen_share_dev = abs(gen_share_sum - 1).max()
        results['gen_shares'] = {
            '最大偏差': gen_share_dev,
            '校验状态': '通过' if gen_share_dev < 0.01 else '警告'
        }

    # 检查用电结构占比
    demand_share_cols = [col for col in df.columns if col.endswith('_demand_share')]
    if demand_share_cols:
        demand_share_sum = df[demand_share_cols].sum(axis=1)
        demand_share_dev = abs(demand_share_sum - 1).max()
        results['demand_shares'] = {
            '最大偏差': demand_share_dev,
            '校验状态': '通过' if demand_share_dev < 0.01 else '警告'
        }

    return results


def validate_growth_rates(df: pd.DataFrame) -> Dict:
    """校验增长率合理性"""
    results = {}

    yoy_cols = [col for col in df.columns if col.endswith('_yoy')]

    if not yoy_cols:
        results['growth'] = "无增长率数据，跳过校验"
        return results

    extreme_growth = {}
    for col in yoy_cols:
        # 检查极端增长率（超过200%或低于-50%）
        extreme = df[abs(df[col]) > 2.0]
        if len(extreme) > 0:
            extreme_growth[col] = len(extreme)

    results['growth'] = {
        '极端增长率列数': len(extreme_growth),
        '校验状态': '通过' if len(extreme_growth) == 0 else '警告',
        '详情': extreme_growth
    }

    return results


def generate_validation_report(all_results: Dict) -> str:
    """生成校验报告"""
    report = []
    report.append("=" * 60)
    report.append("        能源数据一致性校验报告")
    report.append("=" * 60)
    report.append("")

    # 总体状态
    all_status = []
    for category, results in all_results.items():
        if isinstance(results, dict):
            for sub_category, sub_results in results.items():
                if isinstance(sub_results, dict) and '校验状态' in sub_results:
                    all_status.append(sub_results['校验状态'])

    passed = all_status.count('通过')
    warnings = all_status.count('警告')
    total = len(all_status)

    report.append(f"总体状态: {passed}/{total} 项通过，{warnings} 项警告")
    report.append("")

    # 详细结果
    for category, results in all_results.items():
        report.append(f"【{category.upper()}】")

        if isinstance(results, str):
            report.append(f"  {results}")
        else:
            for sub_category, sub_results in results.items():
                report.append(f"  {sub_category}:")
                if isinstance(sub_results, dict):
                    for key, value in sub_results.items():
                        if key == '校验状态':
                            status_icon = "✓" if value == '通过' else "⚠"
                            report.append(f"    {status_icon} {key}: {value}")
                        else:
                            report.append(f"    • {key}: {value}")
                else:
                    report.append(f"    {sub_results}")
        report.append("")

    report.append("=" * 60)
    return "\n".join(report)


def save_validation_report(report: str) -> None:
    """保存校验报告"""
    output_file = OUTPUT_DIR / "validation_report.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"\n校验报告已保存: {output_file}")


def main():
    """主函数"""
    print("=== 数据一致性校验 ===")

    # 加载数据
    df = load_indicator_data()

    # 执行各项校验
    all_results = {}

    all_results.update(validate_generation_consistency(df))
    all_results.update(validate_demand_consistency(df))
    all_results.update(validate_balance_consistency(df))
    all_results.update(validate_temporal_consistency(df))
    all_results.update(validate_share_consistency(df))
    all_results.update(validate_growth_rates(df))

    # 生成报告
    report = generate_validation_report(all_results)
    print(report)

    # 保存报告
    save_validation_report(report)

    print("=== 一致性校验完成 ===")


if __name__ == "__main__":
    main()


