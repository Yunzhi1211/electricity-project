# 全国能源数据完整处理Pipeline

## 项目概述

本项目实现了一套完整的能源数据获取、清洗、处理和分析的自动化流程，最终为 AnyLogic 建模软件生成输入文件。

## 项目结构

```
wholepackage/
├── 0_raw_data/                    # 原始数据（Excel文件）
├── 1_clean_demand_supply/         # 数据清洗脚本
│   ├── 1a_crawl_national_demand.py    # 抓取需求数据
│   ├── 1b_clean_demand_data.py        # 清洗需求数据
│   └── 1c_clean_supply_data.py        # 清洗发电量数据 
├── 2_process_validate/            # 数据处理验证脚本
│   ├── 2a_merge_datasets.py
│   ├── 2b_calculate_indicators.py
│   └── 2c_validate_consistency.py
├── 3_output_anylogic/             # AnyLogic 输出文件
│   ├── demand_crawl.csv
│   ├── demand_filled.csv
│   ├── supply_filled.csv
│   └── scenario_parameters.csv
├── main_pipeline.py               # 完整 Pipeline 主程序
└── README.md                      # 本文件
```

## 数据来源

### 发电量数据
- **水力发电**: 国家统计局水力发电量2010-月度数据.xls (198行 × 5列)
- **核能发电**: 国家统计局核能发电量2010-月度数据.xls (198行 × 5列)
- **风力发电**: 国家统计局风力发电量2010-月度数据.xls (198行 × 5列)
- **太阳能发电**: 国家统计局太阳能发电量2010-月度数据.xls (198行 × 5列)

数据格式: 竖向格式（dates as rows）
数据范围: 2010年01月 ~ 2026年02月（169个月）

### 需求数据
国家能源局爬虫获取

## Pipeline 工作流程

### Stage 1: 数据获取和清洗

#### 1a. 抓取需求数据
```bash
python 1_clean_demand_supply/1a_crawl_national_demand.py
```
- 从国家能源局官网抓取用电量数据
- 输出: `cleaned_data/demand_crawled.csv`

#### 1b. 清洗需求数据
```bash
python 1_clean_demand_supply/1b_clean_demand_data.py
```
- 清洗需求数据格式
- 处理缺失值和数据异常
- 输出: `cleaned_data/demand_cleaned.csv`

#### 1c. 清洗发电量数据 
```bash
python 1_clean_demand_supply/1c_clean_supply_data.py
```
- 读取4个发电量Excel文件（竖向格式）
- 按日期合并数据
- 会计恒等式补值: `总发电量 = 水力 + 核能 + 风力 + 太阳能`
- 随机森林补值处理缺失数据
- 一致性验证和非负修正
- **输出**: `cleaned_data/supply_filled.xlsx` (169行 × 7列)

**输出列**:
- `date`: 日期 (datetime)
- `total_supply`: 总发电量 (MWh)
- `thermal_supply`: 火力发电 (NaN - 数据未提供)
- `hydro_supply`: 水力发电 (MWh)
- `nuclear_supply`: 核能发电 (MWh)
- `wind_supply`: 风力发电 (MWh)
- `solar_supply`: 太阳能发电 (MWh)

### Stage 2: 数据处理和验证

#### 2a. 合并数据集
```bash
python 2_process_validate/2a_merge_datasets.py
```
- 合并需求和发电量数据
- 输出: `2_process_validate/merged_data.csv`

#### 2b. 计算能源指标
```bash
python 2_process_validate/2b_calculate_indicators.py
```
- 计算供需比
- 计算发电结构占比
- 计算同比增长率
- 输出: `2_process_validate/energy_indicators.csv`

#### 2c. 一致性验证
```bash
python 2_process_validate/2c_validate_consistency.py
```
- 验证会计恒等式
- 检查数据质量
- 输出: `2_process_validate/validation_report.txt`

### Stage 3: 生成AnyLogic输入文件

#### 3a. 月度用电量文件
```
3_output_anylogic/demand_monthly.csv
```
- 列: date, total_demand, ...
- 格式: 1行标题 + N行数据

#### 3b. 月度发电量文件 
```
3_output_anylogic/generation_monthly.csv
```
- 列: date, total_supply, thermal_supply, hydro_supply, nuclear_supply, wind_supply, solar_supply
- 格式: 1行标题 + 169行数据

#### 3c. 能源平衡表
```
3_output_anylogic/energy_balance.csv
```
- 列: date, total_supply, thermal_supply, ...
- 包含供给侧所有数据

#### 3d. 情景参数
```
3_output_anylogic/scenario_parameters.csv
```
4个场景:
- 基准情景 (demand_growth=5%)
- 高速增长 (demand_growth=8%)
- 低速增长 (demand_growth=2%)
- 可再生重点 (renewable_target=70%)

#### 3e. 综合输入文件
```
3_output_anylogic/model_inputs.xlsx
```
- Sheet1: energy_data (原始数据)
- Sheet2: scenarios (情景参数)

## 快速开始

### 运行完整 Pipeline
```bash
python main_pipeline.py
```

### 单独运行发电量清洗 
```bash
python 1_clean_demand_supply/1c_clean_supply_data.py
```
这会生成 `cleaned_data/supply_filled.xlsx`

### 查看日志
```bash
tail -f pipeline.log
```

## 环境要求

### Python 包
- pandas >= 1.3.0
- numpy >= 1.21.0
- openpyxl >= 3.0.0
- xlrd >= 2.0.0
- scikit-learn >= 1.0.0

### 安装依赖
```bash
pip install pandas numpy openpyxl xlrd scikit-learn
```

### Python 版本
- Python 3.8 或更高版本

## 关键函数说明

### 1c_clean_supply_data.py

#### `load_and_prepare(file_path)`
主入口函数，完成以下流程:
1. 读取4个竖向格式Excel文件
2. 按日期合并数据 (外连接)
3. 计算 total_supply = hydro + nuclear + wind + solar
4. 处理缺失值和异常值
5. 输出 `supply_filled.xlsx`

#### `_read_vertical_file(filepath, col_name)`
读取单个竖向格式Excel文件:
- 行: 日期 (格式: "2026年2月")
- 列: 日期, 发电量
- 返回: DataFrame with [date, value] columns

#### `_convert_cn_date(date_series)`
将中文日期格式转换为 datetime:
- 输入: "2026年2月" (Series或标量)
- 输出: datetime(2026, 2, 15)

#### `fill_by_accounting(df)`
会计恒等式补值:
- 总发电量 = 水力 + 核能 + 风力 + 太阳能
- 若总量缺失但分项都有，则计算总量
- 若分项缺失但总量和其他分项都有，则计算该分项

#### `fill_by_random_forest(df, target_cols)`
随机森林补值:
- 使用滞后特征 (lag 1,2,3,6,12 months)
- 使用滚动平均特征 (3,6,12 month moving average)
- 训练随机森林模型预测缺失值

#### `adjust_consistency(df)`
一致性调整:
- 若 hydro + nuclear + wind + solar != total_supply
- 则按比例调整分项值


## 更新日志

### 2026-04-16
- ✓ 重新生成 main_pipeline.py，修复乱码问题
- ✓ 更新 README.md 反映实际项目结构
- ✓ 发电量清洗脚本完全正常工作（1c）
- ✓ 输出目录改为 cleaned_data/

### 2026-04-15
- ✓ 发电量数据合并和补值完成
- ✓ 输出 supply_filled.xlsx (169 × 7)

### 2026-03-10
- ✓ 项目结构重构
- ✓ 环境依赖完善
