#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
2b_calculate_indicators.py - 能源指标计算

功能：
- 计算供需平衡指标
- 计算结构占比指标
- 计算增长率指标
- 计算季节性指标

输入：merged_energy_data.csv
输出：energy_indicators.csv

作者：Yunzhi
创建时间：2026-04-15
"""

import pandas as pd
import numpy as np
from pathlib import Path

# 目录配置
BASE_DIR = Path(__file__).parent.parent
INPUT_DIR = BASE_DIR / "2_process_validate"
OUTPUT_DIR = BASE_DIR / "2_process_validate"


def load_merged_data() -> pd.DataFrame:
    """加载合并后的数据"""
    input_file = INPUT_DIR / "merged_energy_data.csv"

    if not input_file.exists():
        raise FileNotFoundError(f"找不到合并数据文件: {input_file}")

    df = pd.read_csv(input_file)
    df['date'] = pd.to_datetime(df['date'])

    print(f"加载合并数据: {df.shape}")
    return df


def calculate_balance_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """计算供需平衡指标"""
    df = df.copy()

    # 供需平衡比
    if 'total_gen' in df.columns and 'total_demand' in df.columns:
        df['supply_demand_ratio'] = df['total_gen'] / df['total_demand']

        # 供需平衡状态
        df['balance_status'] = pd.cut(
            df['supply_demand_ratio'],
            bins=[0, 0.95, 1.05, float('inf')],
            labels=['供不应求', '基本平衡', '供过于求']
        )

    # 峰谷差率（如果有日数据可以计算）
    # 这里暂时跳过，因为是月度数据

    print("供需平衡指标计算完成")
    return df


def calculate_structure_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """计算结构占比指标"""
    df = df.copy()

    # 发电结构占比
    gen_cols = ['thermal_gen', 'hydro_gen', 'nuclear_gen', 'wind_gen', 'solar_gen']
    if all(col in df.columns for col in gen_cols) and 'total_gen' in df.columns:
        for col in gen_cols:
            df[f'{col}_share'] = df[col] / df['total_gen']

    # 用电结构占比
    demand_cols = ['primary_demand', 'secondary_demand', 'tertiary_demand', 'residential_demand']
    if all(col in df.columns for col in demand_cols) and 'total_demand' in df.columns:
        for col in demand_cols:
            df[f'{col}_share'] = df[col] / df['total_demand']

    print("结构占比指标计算完成")
    return df


def calculate_growth_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """计算增长率指标"""
    df = df.copy()

    # 数值列
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    target_cols = [col for col in numeric_cols if col not in ['date', 'year', 'month', 'quarter', 'month_sin', 'month_cos']]

    # 计算同比增长率 (YoY)
    for col in target_cols:
        if col in df.columns:
            df[f'{col}_yoy'] = df[col].pct_change(12)

    # 计算环比增长率 (MoM)
    for col in target_cols:
        if col in df.columns:
            df[f'{col}_mom'] = df[col].pct_change(1)

    # 计算3个月移动平均增长率
    for col in target_cols:
        if col in df.columns:
            df[f'{col}_yoy_ma3'] = df[f'{col}_yoy'].rolling(3).mean()

    print("增长率指标计算完成")
    return df


def calculate_seasonal_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """计算季节性指标"""
    df = df.copy()

    # 添加月份信息（如果还没有）
    if 'month' not in df.columns:
        df['month'] = df['date'].dt.month

    # 计算季节性指数（以年均值为基准）
    numeric_cols = ['total_demand', 'total_gen', 'thermal_gen', 'hydro_gen',
                   'nuclear_gen', 'wind_gen', 'solar_gen']

    for col in numeric_cols:
        if col in df.columns:
            # 计算年均值
            yearly_mean = df.groupby(df['date'].dt.year)[col].transform('mean')
            df[f'{col}_seasonal'] = df[col] / yearly_mean

    # 计算季节性强度
    for col in numeric_cols:
        if f'{col}_seasonal' in df.columns:
            # 计算每个月的季节性平均值
            monthly_seasonal = df.groupby('month')[f'{col}_seasonal'].transform('mean')
            df[f'{col}_seasonal_strength'] = df[f'{col}_seasonal'] / monthly_seasonal

    print("季节性指标计算完成")
    return df


def calculate_efficiency_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """计算效率指标"""
    df = df.copy()

    # 发电效率指标（简化计算）
    if 'total_gen' in df.columns and 'total_demand' in df.columns:
        # 假设的系统效率（实际应该有更精确的计算）
        df['system_efficiency'] = df['total_demand'] / (df['total_gen'] * 1.1)  # 考虑10%损耗

    # 可再生能源占比
    renewable_cols = ['hydro_gen', 'nuclear_gen', 'wind_gen', 'solar_gen']
    if all(col in df.columns for col in renewable_cols) and 'total_gen' in df.columns:
        df['renewable_share'] = df[renewable_cols].sum(axis=1) / df['total_gen']

    # 清洁能源占比（不含煤电）
    clean_cols = ['hydro_gen', 'nuclear_gen', 'wind_gen', 'solar_gen']
    if all(col in df.columns for col in clean_cols) and 'total_gen' in df.columns:
        df['clean_energy_share'] = df[clean_cols].sum(axis=1) / df['total_gen']

    print("效率指标计算完成")
    return df


def validate_indicators(df: pd.DataFrame) -> None:
    """验证计算的指标"""
    print("\n=== 指标验证 ===")

    # 检查供需平衡比的合理性
    if 'supply_demand_ratio' in df.columns:
        ratio_stats = df['supply_demand_ratio'].describe()
        print(f"供需比统计: 均值={ratio_stats['mean']:.3f}, 范围=[{ratio_stats['min']:.3f}, {ratio_stats['max']:.3f}]")

        # 检查异常值
        outliers = df[(df['supply_demand_ratio'] < 0.5) | (df['supply_demand_ratio'] > 2.0)]
        if len(outliers) > 0:
            print(f"警告：发现 {len(outliers)} 个异常供需比值")

    # 检查占比之和是否为1
    share_cols = [col for col in df.columns if col.endswith('_share')]
    for base_col in ['total_gen', 'total_demand']:
        related_shares = [col for col in share_cols if base_col.replace('total_', '') in col]
        if related_shares:
            share_sum = df[related_shares].sum(axis=1)
            max_deviation = abs(share_sum - 1).max()
            print(f"{base_col} 占比之和最大偏差: {max_deviation:.6f}")

    # 检查增长率的合理性
    yoy_cols = [col for col in df.columns if col.endswith('_yoy')]
    for col in yoy_cols:
        if df[col].notna().any():
            yoy_stats = df[col].describe()
            extreme_growth = df[abs(df[col]) > 1.0]  # 增长率超过100%
            if len(extreme_growth) > 0:
                print(f"警告：{col} 发现 {len(extreme_growth)} 个极端增长率值")


def save_indicators(df: pd.DataFrame) -> None:
    """保存计算后的指标"""
    output_file = OUTPUT_DIR / "energy_indicators.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n指标数据已保存: {output_file}")


def main():
    """主函数"""
    print("=== 能源指标计算 ===")

    # 加载数据
    df = load_merged_data()

    # 计算各类指标
    df = calculate_balance_indicators(df)
    df = calculate_structure_indicators(df)
    df = calculate_growth_indicators(df)
    df = calculate_seasonal_indicators(df)
    df = calculate_efficiency_indicators(df)

    # 验证指标
    validate_indicators(df)

    # 保存结果
    save_indicators(df)

    print(f"\n最终数据集形状: {df.shape}")
    print("=== 指标计算完成 ===")


if __name__ == "__main__":
    main()</content>
<parameter name="filePath">d:\一个文件夹\学习\学习\hku\sem2\MSDA7102\project\project\wholepackage\1_process_data\2b_calculate_indicators.py
