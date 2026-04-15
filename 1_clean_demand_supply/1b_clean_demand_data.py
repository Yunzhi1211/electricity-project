# -*- coding: utf-8 -*-
"""
1b_clean_demand_data.py - 用电量数据清洗和补值

插补方法思路分两步：
第一，利用总用电量与各类型用电量之间的加总关系，对可直接反推的缺失值进行填补；
第二，对仍然缺失的月份，构造年份、月份、滞后值和滚动均值等时间序列特征，
使用随机森林模型预测缺失值。最后再做一致性调整，使总量与分项之和尽量匹配。
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from pathlib import Path

# 基础配置
BASE_DIR = Path(__file__).parent.parent
INPUT_DIR = BASE_DIR / "cleaned_data"
OUTPUT_DIR = BASE_DIR / "cleaned_data"

# 参数区
FILE_PATH = INPUT_DIR / "demand_crawl.csv"
OUTPUT_PATH = OUTPUT_DIR / "demand_filled.xlsx"

# 你的日期列名
DATE_COL = 'date'
TOTAL_COL = 'total_demand'
PART_COLS = [
    'primary_demand',      # 第一产业用电量
    'secondary_demand',    # 第二产业用电量
    'tertiary_demand',     # 第三产业用电量
    'residential_demand',  # 居民生活用电量
]

# 数据只想保留前7列（日期 + 6列数值），设置为 True
KEEP_FIRST_7_COLS = True


def load_and_prepare(file_path):
    """读取和基本清理数据"""
    df = pd.read_csv(file_path)

    # 只保留前7列
    if KEEP_FIRST_7_COLS:
        df = df.iloc[:, :7].copy()

    # 如果第一行/某些行有"日期"这样的说明文字，删掉
    df = df[df[DATE_COL] != '日期'].copy()

    # 日期转换
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors='coerce')

    # 删除日期为空的异常行
    df = df[df[DATE_COL].notna()].copy()

    # 所有目标列
    targets = [TOTAL_COL] + PART_COLS

    # 数值转换
    for col in targets:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 按日期升序
    df = df.sort_values(DATE_COL).reset_index(drop=True)
    
    return df


def add_temporal_features(df):
    """添加时间特征"""
    df['year'] = df[DATE_COL].dt.year
    df['month'] = df[DATE_COL].dt.month
    df['quarter'] = df[DATE_COL].dt.quarter

    # 月份周期特征
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    
    return df


def fill_by_accounting(df, total_col, part_cols):
    """会计恒等式补值：total = sum(parts)"""
    changed = True

    while changed:
        changed = False

        # 如果总量缺失，但所有分项都已知 -> 总量 = 分项和
        mask_total = df[total_col].isna() & df[part_cols].notna().all(axis=1)
        if mask_total.any():
            df.loc[mask_total, total_col] = df.loc[mask_total, part_cols].sum(axis=1)
            changed = True

        # 如果某个分项缺失，但总量和其余分项都已知 -> 反推该分项
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
    """临时补值函数，用于构造滞后/滚动特征时避免特征本身为缺失"""
    temp = df[col].copy()

    # 线性插值
    temp = temp.interpolate(method='linear')

    # 同月份历史均值填补
    month_mean = df.groupby('month')[col].transform('mean')
    temp = temp.fillna(month_mean)

    # 仍缺失则用中位数
    temp = temp.fillna(df[col].median())

    return temp


def add_series_features(df, col):
    """构造时间序列特征"""
    temp_col = f'{col}_temp'
    df[temp_col] = create_temp_filled(df, col)

    # 滞后项
    df[f'{col}_lag_1'] = df[temp_col].shift(1)
    df[f'{col}_lag_2'] = df[temp_col].shift(2)
    df[f'{col}_lag_3'] = df[temp_col].shift(3)
    df[f'{col}_lag_6'] = df[temp_col].shift(6)
    df[f'{col}_lag_12'] = df[temp_col].shift(12)

    # 滚动均值（注意都先shift(1)，防止用到当期信息）
    df[f'{col}_roll_mean_3'] = df[temp_col].shift(1).rolling(3).mean()
    df[f'{col}_roll_mean_6'] = df[temp_col].shift(1).rolling(6).mean()
    df[f'{col}_roll_mean_12'] = df[temp_col].shift(1).rolling(12).mean()

    return df


def ml_fill_column(df, target_col):
    """随机森林逐列补值"""
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

    # 没有缺失就直接返回
    if pred_df.empty:
        return df

    # 如果训练样本过少，则直接用月份均值/中位数填补
    if len(train_df) < 12:
        month_mean_map = train_df.groupby('month')[target_col].mean()
        fill_vals = pred_df['month'].map(month_mean_map)
        fill_vals = fill_vals.fillna(train_df[target_col].median())
        df.loc[df[target_col].isna(), target_col] = fill_vals.values
        return df

    # 特征缺失用训练集特征中位数填补
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
    """一致性调整：让分项之和尽量等于总量"""
    sum_parts = df[part_cols].sum(axis=1)

    # 避免除零
    ratio = np.where(sum_parts > 0, df[total_col] / sum_parts, 1)

    for col in part_cols:
        df[col] = df[col] * ratio
    
    return df


def clean_demand_data(input_file, output_file):
    """主流程：读取 -> 清洗 -> 填补 -> 保存"""
    print(f"开始读取数据: {input_file}")
    df = load_and_prepare(input_file)
    
    print("添加时间特征...")
    df = add_temporal_features(df)
    
    print("会计恒等式补值...")
    df = fill_by_accounting(df, TOTAL_COL, PART_COLS)
    
    print("构造时间序列特征...")
    targets = [TOTAL_COL] + PART_COLS
    for col in targets:
        df = add_series_features(df, col)
    
    print("随机森林补值...")
    for col in targets:
        df = ml_fill_column(df, col)
    
    print("一致性调整...")
    df = consistency_adjustment(df, TOTAL_COL, PART_COLS)
    
    # 只保留前7列（日期 + 6列数值）
    output_df = df[[DATE_COL] + [TOTAL_COL] + PART_COLS].copy()
    
    print(f"保存结果到: {output_file}")
    output_df.to_excel(output_file, index=False)
    
    return output_df


if __name__ == "__main__":
    if FILE_PATH.exists():
        print(f"处理文件: {FILE_PATH}")
        result = clean_demand_data(str(FILE_PATH), str(OUTPUT_PATH))
        print(f"前20行预览:")
        print(result.head(20))
    else:
        print(f"输入文件不存在: {FILE_PATH}")
        print("请先运行 1a_crawl_national_demand.py 获取数据")
