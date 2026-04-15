# -*- coding: utf-8 -*-
"""
1c_clean_supply_data.py - 供电量数据清洗和补值

数据来源：
- 使用国家统计局发电量数据
- 主要包括
  - 总供电量 (total_supply) 
  - 分项供电量 (thermal_supply, hydro_supply, nuclear_supply, wind_supply, solar_supply)
- 但由于存在缺失值和不一致问题，需要进行系统的清洗和补值处理。

缺失值填补方法选择随机森林原因：
- 随机森林能够同时利用年份、月份、滞后项和滚动均值等特征，更好地刻画月度供电量的趋势性和季节性
- 相比线性回归，随机森林无需假设变量之间为线性关系
- 相比 ARIMA 时间序列模型，随机森林更易在多变量、非线性和缺失模式较复杂的场景下实施

缺失值填补思路分两步：
第一，利用总供电量与各类型供电量之间的加总关系，对可直接反推的缺失值进行填补
第二，对仍然缺失的月份，构造年份、月份、滞后值和滚动均值等时间序列特征，使用随机森林模型预测缺失值
最后再做一致性调整，使总量与分项之和尽量匹配
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from pathlib import Path
import logging

# 基础配置
BASE_DIR = Path(__file__).parent.parent
INPUT_DIR = BASE_DIR / "0_raw_data"
OUTPUT_DIR = BASE_DIR / "cleaned_data"

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('1c_clean_supply.log', encoding='utf-8-sig')
    ]
)
logger = logging.getLogger(__name__)

# 供电数据列定义
TARGETS = ['total_supply', 'thermal_supply', 'hydro_supply', 'nuclear_supply', 'wind_supply', 'solar_supply']
PART_COLS = ['thermal_supply', 'hydro_supply', 'nuclear_supply', 'wind_supply', 'solar_supply']


def load_and_prepare(file_path):
    """读取和基本清理数据 - 只处理纵向格式的分项供电数据"""
    logger.info(f"读取数据: {file_path}")
    
    file_path = Path(file_path)
    
    if not file_path.is_dir():
        logger.error("需要文件夹路径")
        return None
    
    # 先读取4个纵向格式的分项数据
    logger.info("读取纵向格式供电数据...")
    
    dfs = []
    
    # 水力发电量
    hydro_file = file_path / "国家统计局水力发电量2010-月度数据.xls"
    if hydro_file.exists():
        df = _read_vertical_file(hydro_file, "hydro_supply")
        if df is not None:
            dfs.append(df)
            logger.info(f"  水力发电: {df.shape}")
    
    # 核能发电量
    nuclear_file = file_path / "国家统计局核能发电量2010-月度数据.xls"
    if nuclear_file.exists():
        df = _read_vertical_file(nuclear_file, "nuclear_supply")
        if df is not None:
            dfs.append(df)
            logger.info(f"  核能发电: {df.shape}")
    
    # 风力发电量
    wind_file = file_path / "国家统计局风力发电量2010-月度数据.xls"
    if wind_file.exists():
        df = _read_vertical_file(wind_file, "wind_supply")
        if df is not None:
            dfs.append(df)
            logger.info(f"  风力发电: {df.shape}")
    
    # 太阳能发电量
    solar_file = file_path / "国家统计局太阳能发电量2010-月度数据.xls"
    if solar_file.exists():
        df = _read_vertical_file(solar_file, "solar_supply")
        if df is not None:
            dfs.append(df)
            logger.info(f"  太阳能发电: {df.shape}")
    
    if not dfs:
        logger.error("无法读取任何供电数据文件")
        return None
    
    # 按日期合并所有分项数据
    logger.info(f"合并 {len(dfs)} 个数据集...")
    df = dfs[0]
    for other_df in dfs[1:]:
        df = df.merge(other_df, on='date', how='outer')
    
    logger.info(f"合并完成，形状: {df.shape}")
    
    # 添加总供电量（分项之和）
    supply_cols = ['hydro_supply', 'nuclear_supply', 'wind_supply', 'solar_supply']
    df['total_supply'] = df[supply_cols].sum(axis=1)
    
    # 添加火力发电量（缺失，先用 NaN）
    df['thermal_supply'] = np.nan
    
    # 最终列顺序
    df = df[['date', 'total_supply', 'thermal_supply', 'hydro_supply', 'nuclear_supply', 'wind_supply', 'solar_supply']].copy()
    
    # 数据清理
    if df is None or df.empty:
        logger.error("数据加载结果为空")
        return None
    
    # 日期转换
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # 删除日期为空的异常行
    df = df[df['date'].notna()].copy()

    # 数值转换（除了日期列）
    for col in df.columns:
        if col != 'date':
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 按日期升序并去重
    df = df.sort_values('date').reset_index(drop=True)
    df = df.drop_duplicates(subset=['date'], keep='first')

    logger.info(f"数据加载完成，形状: {df.shape}")
    return df


def _read_vertical_file(filepath, col_hint):
    """读取纵向格式Excel文件（行=时间, 列=指标）
    
    Args:
        filepath: Excel 文件路径
        col_hint: 列名关键词，用于在列中查找对应的数值列
    
    Returns:
        包含 'date' 和指标值列的 DataFrame
    """
    try:
        df = pd.read_excel(filepath)
        
        # 根据中国统计部门的标准格式，第2行（index=1）是列标题
        if df.shape[0] < 2:
            logger.warning(f"文件行数过少: {filepath}")
            return None
        
        # 获取列标题
        header = df.iloc[1, :].tolist()
        
        # 从第3行开始是数据
        df_data = df.iloc[2:, :].copy()
        df_data.columns = header
        
        # 第1列是日期
        date_col = df_data.columns[0]
        
        # 查找包含数值的列（通常是第2列，但如果有多列则取第2列）
        value_col = df_data.columns[1] if len(df_data.columns) > 1 else None
        
        if value_col is None:
            logger.warning(f"无法找到数值列: {filepath}")
            return None
        
        # 提取日期和数值列
        result = df_data[[date_col, value_col]].copy()
        result.columns = ['date', col_hint]
        
        # 转换日期（中文格式如"2026年2月"）
        result['date'] = result['date'].apply(_convert_cn_date)
        
        # 转换数值
        result[col_hint] = pd.to_numeric(result[col_hint], errors='coerce')
        
        # 删除无效行
        result = result.dropna(subset=['date', col_hint])
        
        return result
        
    except Exception as e:
        logger.warning(f"读取文件失败 {filepath}: {e}")
        return None


def _process_vertical_format(df, filename):
    """处理纵向格式（行=月份, 列=指标）"""
    try:
        # 国家统计局的数据格式约定：第2行（index=1）是列标题
        header_row = 1
        
        # 用该行作为列名
        new_header = df.iloc[header_row, :].tolist()
        df_clean = df.iloc[header_row+1:, :].reset_index(drop=True)
        df_clean.columns = new_header
        
        # 提取日期列（第1列）
        date_col = df_clean.columns[0]
        df_clean = df_clean.rename(columns={date_col: 'date'})
        
        # 转换日期
        df_clean['date'] = _convert_cn_date(df_clean['date'])  # 这里会自动调用Series版本
        
        # 找数值列（第2列通常就是）
        if len(df_clean.columns) > 1:
            numeric_col = df_clean.columns[1]
            df_result = df_clean[['date', numeric_col]].copy()
        else:
            return None
        
        # 重命名列名：根据文件名推断分项
        if '太阳' in filename:
            df_result = df_result.rename(columns={numeric_col: 'solar_supply'})
        elif '水' in filename:
            df_result = df_result.rename(columns={numeric_col: 'hydro_supply'})
        elif '核' in filename:
            df_result = df_result.rename(columns={numeric_col: 'nuclear_supply'})
        elif '风' in filename:
            df_result = df_result.rename(columns={numeric_col: 'wind_supply'})
        
        return df_result
        
    except Exception as e:
        logger.warning(f"纵向格式处理失败 ({filename}): {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return None


def _convert_cn_date(date_series):
    """转换中文日期格式（如'2026年2月'）到datetime"""
    def convert_single(date_str):
        if pd.isna(date_str):
            return pd.NaT
        
        date_str = str(date_str).strip()
        
        try:
            # 处理 "2026年2月" 格式
            if '年' in date_str and '月' in date_str:
                date_str = date_str.replace('年', '-').replace('月', '')
                # 添加15日作为月中日期
                if len(date_str.split('-')) == 2:
                    date_str += '-15'
                return pd.to_datetime(date_str)
            
            # 尝试标准格式
            return pd.to_datetime(date_str)
        except:
            return pd.NaT
    
    # 处理 Series 和标量
    if isinstance(date_series, pd.Series):
        return date_series.apply(convert_single)
    else:
        return convert_single(date_series)
    
    return date_series.apply(convert_single)


def add_temporal_features(df):
    """添加时间特征"""
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['quarter'] = df['date'].dt.quarter

    # 月份周期特征
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)

    return df


def fill_by_accounting(df):
    """会计恒等式补值：total_supply = sum(parts)"""
    logger.info("执行会计恒等式补值...")
    changed = True
    iterations = 0

    while changed and iterations < 100:
        changed = False
        iterations += 1

        # 如果总量缺失，但所有分项都已知 -> 总量 = 分项和
        mask_total = df['total_supply'].isna() & df[PART_COLS].notna().all(axis=1)
        if mask_total.any():
            df.loc[mask_total, 'total_supply'] = df.loc[mask_total, PART_COLS].sum(axis=1)
            changed = True

        # 如果某个分项缺失，但总量和其余分项都已知 -> 反推该分项
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

    logger.info(f"会计恒等式补值完成（迭代{iterations}次）")
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


def consistency_adjustment(df):
    """一致性调整：让分项之和尽量等于总量"""
    logger.info("执行一致性调整...")
    sum_parts = df[PART_COLS].sum(axis=1)

    # 避免除零
    ratio = np.where(sum_parts > 0, df['total_supply'] / sum_parts, 1)

    for col in PART_COLS:
        df[col] = df[col] * ratio

    return df


def non_negative_correction(df):
    """非负修正"""
    logger.info("执行非负修正...")
    for col in TARGETS:
        df[col] = df[col].clip(lower=0)

    return df


def clean_supply_data(input_file, output_file):
    """主流程：读取 -> 清洗 -> 填补 -> 保存"""
    logger.info(f"开始处理供电量数据: {input_file}")
    
    df = load_and_prepare(input_file)
    
    logger.info("添加时间特征...")
    df = add_temporal_features(df)
    
    logger.info("会计恒等式补值...")
    df = fill_by_accounting(df)
    
    logger.info("构造时间序列特征...")
    for col in TARGETS:
        df = add_series_features(df, col)
    
    logger.info("随机森林补值...")
    for col in TARGETS:
        logger.info(f"  补值 {col}...")
        df = ml_fill_column(df, col)
    
    logger.info("一致性调整...")
    df = consistency_adjustment(df)
    
    logger.info("非负修正...")
    df = non_negative_correction(df)
    
    # 只保留前7列（日期 + 6列数值）
    output_df = df[['date'] + TARGETS].copy()
    
    logger.info(f"保存结果到: {output_file}")
    output_df.to_excel(output_file, index=False)
    
    return output_df


if __name__ == "__main__":
    print("="*50)
    print("1c_clean_supply_data 开始执行！")
    print("="*50)
    
    # 查找输入文件或文件夹
    possible_paths = [
        INPUT_DIR,  # 优先读取整个文件夹（自动合并多个 Excel 文件）
        INPUT_DIR / "supply_data.xlsx",
        INPUT_DIR / "generation_data.xlsx",  # 兼容旧文件名
        Path("D:/一个文件夹/学习/学习/hku/sem2/MSDA7102/project/建模数据"),
        Path(r"D:\一个文件夹\学习\学习\hku\sem2\MSDA7102\project\建模数据"),
    ]

    input_file = None
    for path in possible_paths:
        if Path(path).exists():
            input_file = path
            print(f"找到数据源: {path}")
            break

    if input_file is None:
        print("ERROR: 找不到数据源，尝试的路径:")
        for path in possible_paths:
            print(f"  {path}")
    else:
        output_file = OUTPUT_DIR / "supply_filled.xlsx"
        print(f"处理数据...")
        result = clean_supply_data(str(input_file), str(output_file))
        if result is not None:
            print(f"前20行预览:")
            print(result.head(20))
        print("处理完成！")

