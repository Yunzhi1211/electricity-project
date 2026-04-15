#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
2a_merge_datasets.py - 数据集合并

功能：
- 合并清洗后的用电量和发电量数据
- 按日期对齐数据
- 生成统一的能源数据集

输入：
- demand_cleaned.csv
- generation_cleaned.csv

输出：merged_energy_data.csv

作者：Yunzhi
创建时间：2026-04-15
"""

import pandas as pd
from pathlib import Path

# 目录配置
BASE_DIR = Path(__file__).parent.parent
INPUT_DIR = BASE_DIR / "1_clean_demand_supply"
OUTPUT_DIR = BASE_DIR / "2_process_validate"


def load_cleaned_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """加载清洗后的数据"""
    demand_file = INPUT_DIR / "demand_cleaned.csv"
    generation_file = INPUT_DIR / "generation_cleaned.csv"

    demand_df = None
    generation_df = None

    if demand_file.exists():
        demand_df = pd.read_csv(demand_file)
        demand_df['date'] = pd.to_datetime(demand_df['date'])
        print(f"加载用电量数据: {demand_df.shape}")
    else:
        print(f"警告：找不到用电量数据文件 {demand_file}")

    if generation_file.exists():
        generation_df = pd.read_csv(generation_file)
        generation_df['date'] = pd.to_datetime(generation_df['date'])
        print(f"加载发电量数据: {generation_df.shape}")
    else:
        print(f"警告：找不到发电量数据文件 {generation_file}")

    return demand_df, generation_df


def merge_datasets(demand_df: pd.DataFrame, generation_df: pd.DataFrame) -> pd.DataFrame:
    """合并数据集"""
    if demand_df is None and generation_df is None:
        raise ValueError("至少需要一个数据集")

    if demand_df is not None and generation_df is not None:
        # 两个数据集都存在，进行合并
        merged = pd.merge(
            demand_df,
            generation_df,
            on='date',
            how='outer',
            suffixes=('_demand', '_gen')  # 处理重名列
        )
        print(f"合并两个数据集: {merged.shape}")

    elif demand_df is not None:
        merged = demand_df.copy()
        print("只有用电量数据")

    else:
        merged = generation_df.copy()
        print("只有发电量数据")

    # 按日期排序
    merged = merged.sort_values('date').reset_index(drop=True)

    return merged


def validate_merge(merged_df: pd.DataFrame) -> None:
    """验证合并结果"""
    print("\n=== 合并数据验证 ===")

    # 基本信息
    print(f"数据行数: {len(merged_df)}")
    print(f"日期范围: {merged_df['date'].min()} 至 {merged_df['date'].max()}")

    # 检查缺失值
    print("\n缺失值统计:")
    missing_stats = merged_df.isnull().sum()
    for col, missing in missing_stats.items():
        if missing > 0:
            percentage = (missing / len(merged_df)) * 100
            print(f"  {col}: {missing} ({percentage:.1f}%)")

    # 检查数据完整性
    date_gaps = merged_df['date'].diff().dt.days
    max_gap = date_gaps.max()
    print(f"\n最大日期间隔: {max_gap} 天")

    if max_gap > 31:
        print("警告：存在较长的日期间隔，可能存在数据缺失")

    # 检查数值合理性
    numeric_cols = merged_df.select_dtypes(include=['float64', 'int64']).columns
    print(f"\n数值列统计 ({len(numeric_cols)} 列):")
    for col in numeric_cols:
        if col != 'date':
            stats = merged_df[col].describe()
            print(f"  {col}: 均值={stats['mean']:.2f}, 最小={stats['min']:.2f}, 最大={stats['max']:.2f}")


def save_merged_data(merged_df: pd.DataFrame) -> None:
    """保存合并后的数据"""
    output_file = OUTPUT_DIR / "merged_energy_data.csv"
    merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n合并数据已保存: {output_file}")


def main():
    """主函数"""
    print("=== 数据集合并 ===")

    # 加载数据
    demand_df, generation_df = load_cleaned_data()

    if demand_df is None and generation_df is None:
        print("错误：没有找到任何数据文件")
        return

    # 合并数据
    merged_df = merge_datasets(demand_df, generation_df)

    # 验证结果
    validate_merge(merged_df)

    # 保存结果
    save_merged_data(merged_df)

    print("=== 数据集合并完成 ===")


if __name__ == "__main__":
    main()</content>
<parameter name="filePath">d:\一个文件夹\学习\学习\hku\sem2\MSDA7102\project\project\wholepackage\1_process_data\2a_merge_datasets.py
