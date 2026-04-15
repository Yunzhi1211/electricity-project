#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
全国能源数据完整Pipeline: 抓取 + 清洗 + 验证 + AnyLogic输出

完整工作流程:
├── Stage 0: 数据获取和清洗
│   ├── 1a: 抓取需求数据 (0_raw_data/)
│   ├── 1b: 清洗用电量数据 (cleaned_data/)
│   └── 1c: 清洗发电量数据 (cleaned_data/supply_filled.xlsx)
├── Stage 1: 数据处理和验证
│   ├── 2a: 合并数据集
│   ├── 2b: 计算能源指标
│   └── 2c: 一致性验证
└── Stage 2: 生成AnyLogic输出文件
    ├── demand_monthly.csv (月度用电量)
    ├── generation_monthly.csv (月度发电量)
    ├── energy_balance.csv (能源平衡表)
    ├── scenario_parameters.csv (情景参数)
    └── model_inputs.xlsx (综合输入文件)

作者: Yunzhi
创建: 2026-04-16
"""

import logging
import sys
import subprocess
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# 目录配置
BASE_DIR = Path(__file__).parent
RAW_DATA_DIR = BASE_DIR / "0_raw_data"
CLEANED_DATA_DIR = BASE_DIR / "cleaned_data"
CLEAN_DEMAND_SUPPLY_DIR = BASE_DIR / "1_clean_demand_supply"
PROCESS_VALIDATE_DIR = BASE_DIR / "2_process_validate"
OUTPUT_ANYLOGIC_DIR = BASE_DIR / "3_output_anylogic"

# 创建目录
for dir_path in [RAW_DATA_DIR, CLEANED_DATA_DIR, CLEAN_DEMAND_SUPPLY_DIR, PROCESS_VALIDATE_DIR, OUTPUT_ANYLOGIC_DIR]:
    dir_path.mkdir(exist_ok=True)


class EnergyDataPipeline:
    """全国能源数据完整处理Pipeline"""

    def __init__(self):
        self.demand_data = None
        self.supply_data = None
        self.merged_data = None

    def run_full_pipeline(self) -> None:
        """运行完整pipeline"""
        logger.info("="*60)
        logger.info("启动全国能源数据处理Pipeline")
        logger.info("="*60)

        try:
            # 阶段0: 数据获取和清洗
            logger.info("阶段0: 数据获取和清洗")
            self._stage_0_data_acquisition()

            # 阶段1: 数据处理和验证
            logger.info("阶段1: 数据处理和验证")
            self._stage_1_data_processing()

            # 阶段2: AnyLogic输出
            logger.info("阶段2: 生成AnyLogic输入文件")
            self._stage_2_anylogic_outputs()

            logger.info("="*60)
            logger.info("Pipeline执行完成")
            logger.info(f"输出文件保存到: {OUTPUT_ANYLOGIC_DIR}")
            logger.info("="*60)

        except Exception as e:
            logger.error(f"Pipeline执行失败: {str(e)}")
            raise

    def _stage_0_data_acquisition(self) -> None:
        """阶段0: 数据获取和清洗"""
        try:
            # 1a: 需求数据获取
            logger.info("1a: 检查需求数据...")
            demand_file = CLEANED_DATA_DIR / "demand_cleaned.csv"
            if demand_file.exists():
                self.demand_data = pd.read_csv(demand_file, encoding='utf-8-sig')
                logger.info(f"加载需求数据: {self.demand_data.shape}")
            else:
                logger.warning("需求数据文件不存在")

            # 1b: 清洗需求数据
            logger.info("1b: 检查需求数据清洗...")
            if not demand_file.exists():
                logger.info("执行需求数据清洗脚本...")
                self._run_demand_cleaning()

            # 1c: 清洗发电量数据
            logger.info("1c: 执行发电量数据清洗...")
            self._clean_supply_data()

        except Exception as e:
            logger.error(f"阶段0失败: {str(e)}")
            raise

    def _stage_1_data_processing(self) -> None:
        """阶段1: 数据处理和验证"""
        try:
            # 2a: 合并数据集
            logger.info("2a: 合并数据集...")
            self._merge_datasets()

            # 2b: 计算能源指标
            logger.info("2b: 计算能源指标...")
            self._calculate_indicators()

            # 2c: 一致性验证
            logger.info("2c: 执行一致性验证...")
            self._validate_consistency()

        except Exception as e:
            logger.error(f"阶段1失败: {str(e)}")
            raise

    def _stage_2_anylogic_outputs(self) -> None:
        """阶段2: 生成AnyLogic输入文件"""
        try:
            # 加载能源指标数据
            energy_indicators_file = PROCESS_VALIDATE_DIR / "energy_indicators.csv"
            if not energy_indicators_file.exists():
                logger.warning(f"能源指标文件不存在: {energy_indicators_file}")
                logger.info("请先执行阶段1数据处理")
                return

            energy_df = pd.read_csv(energy_indicators_file, encoding='utf-8-sig')
            logger.info(f"加载能源指标: {energy_df.shape}")

            # 3a: 月度用电量
            logger.info("3a: 生成月度用电量文件...")
            self._create_demand_monthly_csv(energy_df)

            # 3b: 月度发电量
            logger.info("3b: 生成月度发电量文件...")
            self._create_generation_monthly_csv(energy_df)

            # 3c: 能源平衡表
            logger.info("3c: 生成能源平衡表...")
            self._create_energy_balance_csv(energy_df)

            # 3d: 情景参数
            logger.info("3d: 生成情景参数文件...")
            self._create_scenario_parameters_csv()

            # 3e: 综合输入文件
            logger.info("3e: 生成综合输入文件...")
            self._create_model_inputs_excel(energy_df)

        except Exception as e:
            logger.error(f"阶段2失败: {str(e)}")
            raise

    # ============ 具体实现方法 ============

    def _run_demand_cleaning(self) -> None:
        """运行需求数据清洗脚本"""
        try:
            demand_script = CLEAN_DEMAND_SUPPLY_DIR / "1b_clean_demand_data.py"
            if demand_script.exists():
                logger.info(f"运行需求清洗脚本: {demand_script}")
                subprocess.run(
                    ["python", str(demand_script)],
                    cwd=str(BASE_DIR),
                    check=False,
                    capture_output=False
                )
            else:
                logger.warning(f"需求清洗脚本不存在: {demand_script}")
        except Exception as e:
            logger.error(f"运行需求清洗脚本失败: {e}")

    def _clean_supply_data(self) -> None:
        """执行发电量数据清洗 (调用1c_clean_supply_data.py)"""
        try:
            supply_script = CLEAN_DEMAND_SUPPLY_DIR / "1c_clean_supply_data.py"
            if supply_script.exists():
                logger.info(f"运行发电量清洗脚本: {supply_script}")
                subprocess.run(
                    ["python", str(supply_script)],
                    cwd=str(BASE_DIR),
                    check=False,
                    capture_output=False
                )
                
                # 检查输出文件
                supply_file = CLEANED_DATA_DIR / "supply_filled.xlsx"
                if supply_file.exists():
                    logger.info(f"发电量数据清洗完成: {supply_file}")
                    self.supply_data = pd.read_excel(supply_file)
                    logger.info(f"加载发电量数据: {self.supply_data.shape}")
            else:
                logger.warning(f"发电量清洗脚本不存在: {supply_script}")
        except Exception as e:
            logger.error(f"执行发电量清洗出错: {str(e)}")

    def _merge_datasets(self) -> None:
        """合并数据集"""
        try:
            supply_file = CLEANED_DATA_DIR / "supply_filled.xlsx"
            if not supply_file.exists():
                logger.warning(f"供电数据文件不存在: {supply_file}")
                return

            self.supply_data = pd.read_excel(supply_file)
            self.merged_data = self.supply_data.copy()
            
            logger.info(f"数据集合并完成: {self.merged_data.shape}")

            # 保存合并结果
            merged_file = PROCESS_VALIDATE_DIR / "merged_data.csv"
            self.merged_data.to_csv(merged_file, index=False, encoding='utf-8-sig')
            logger.info(f"合并数据已保存: {merged_file}")

        except Exception as e:
            logger.error(f"数据合并失败: {str(e)}")

    def _calculate_indicators(self) -> None:
        """计算能源指标"""
        try:
            if self.merged_data is None or self.merged_data.empty:
                logger.warning("合并数据为空, 跳过指标计算")
                return

            df = self.merged_data.copy()

            # 确保date列是datetime
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])

            # 计算基本统计指标
            logger.info("计算能源指标...")

            # 供应指标
            supply_cols = ['total_supply', 'thermal_supply', 'hydro_supply', 
                          'nuclear_supply', 'wind_supply', 'solar_supply']
            available_supply_cols = [col for col in supply_cols if col in df.columns]

            if available_supply_cols:
                # 计算各类型占比
                for col in available_supply_cols:
                    if col != 'total_supply' and 'total_supply' in df.columns:
                        df[f'{col}_share'] = (df[col] / df['total_supply']).fillna(0)

                # 计算同比增长
                df['total_supply_yoy'] = df['total_supply'].pct_change(12)

            # 保存处理结果
            output_file = PROCESS_VALIDATE_DIR / "energy_indicators.csv"
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"能源指标已保存: {output_file}")

        except Exception as e:
            logger.error(f"指标计算失败: {str(e)}")

    def _validate_consistency(self) -> None:
        """一致性验证"""
        try:
            energy_file = PROCESS_VALIDATE_DIR / "energy_indicators.csv"
            if not energy_file.exists():
                logger.warning(f"能源指标文件不存在: {energy_file}")
                return

            df = pd.read_csv(energy_file, encoding='utf-8-sig')
            logger.info(f"一致性验证数据: {df.shape}")

            # 基本数据质量检查
            missing_count = df.isnull().sum().sum()
            logger.info(f"缺失值总数: {missing_count}")

            if 'total_supply' in df.columns:
                logger.info(f"总供电量范围: {df['total_supply'].min():.2f} - {df['total_supply'].max():.2f}")

            logger.info("数据一致性验证完成")

        except Exception as e:
            logger.error(f"一致性验证失败: {str(e)}")

    def _create_demand_monthly_csv(self, df: pd.DataFrame) -> None:
        """生成月度用电量文件"""
        try:
            output_file = OUTPUT_ANYLOGIC_DIR / "demand_monthly.csv"
            
            if 'date' not in df.columns:
                logger.warning("缺少date列")
                return

            demand_df = df[['date']].copy() if 'date' in df.columns else pd.DataFrame()
            
            if demand_df.empty:
                logger.warning("无法生成用电量文件")
                return

            demand_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"月度用电量文件已生成: {output_file}")

        except Exception as e:
            logger.error(f"生成用电量文件失败: {str(e)}")

    def _create_generation_monthly_csv(self, df: pd.DataFrame) -> None:
        """生成月度发电量文件"""
        try:
            output_file = OUTPUT_ANYLOGIC_DIR / "generation_monthly.csv"
            
            supply_cols = ['date', 'total_supply', 'thermal_supply', 'hydro_supply',
                          'nuclear_supply', 'wind_supply', 'solar_supply']
            available_cols = [col for col in supply_cols if col in df.columns]

            if not available_cols:
                logger.warning("无发电量数据列")
                return

            gen_df = df[available_cols].copy()
            gen_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"月度发电量文件已生成: {output_file}")

        except Exception as e:
            logger.error(f"生成发电量文件失败: {str(e)}")

    def _create_energy_balance_csv(self, df: pd.DataFrame) -> None:
        """生成能源平衡表"""
        try:
            output_file = OUTPUT_ANYLOGIC_DIR / "energy_balance.csv"
            
            if 'date' not in df.columns:
                logger.warning("缺少date列")
                return

            balance_df = df[['date']].copy()
            
            # 添加可用的供应数据
            supply_cols = ['total_supply', 'thermal_supply', 'hydro_supply',
                          'nuclear_supply', 'wind_supply', 'solar_supply']
            for col in supply_cols:
                if col in df.columns:
                    balance_df[col] = df[col]

            balance_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"能源平衡表已生成: {output_file}")

        except Exception as e:
            logger.error(f"生成能源平衡表失败: {str(e)}")

    def _create_scenario_parameters_csv(self) -> None:
        """生成情景参数文件"""
        try:
            scenarios = [
                {
                    'scenario_id': 'baseline',
                    'name': '基准情景',
                    'description': '基于历史趋势的延续',
                    'demand_growth': 0.05,
                    'renewable_target': 0.50,
                    'policy_strength': 1.0
                },
                {
                    'scenario_id': 'high_growth',
                    'name': '高速增长',
                    'description': '经济快速发展',
                    'demand_growth': 0.08,
                    'renewable_target': 0.60,
                    'policy_strength': 1.2
                },
                {
                    'scenario_id': 'low_growth',
                    'name': '低速增长',
                    'description': '经济放缓',
                    'demand_growth': 0.02,
                    'renewable_target': 0.40,
                    'policy_strength': 0.8
                },
                {
                    'scenario_id': 'renewable_focus',
                    'name': '可再生重点',
                    'description': '绿色转型优先',
                    'demand_growth': 0.06,
                    'renewable_target': 0.70,
                    'policy_strength': 1.5
                }
            ]

            scenarios_df = pd.DataFrame(scenarios)
            output_file = OUTPUT_ANYLOGIC_DIR / "scenario_parameters.csv"
            scenarios_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"情景参数文件已生成: {output_file}")

        except Exception as e:
            logger.error(f"生成情景参数失败: {str(e)}")

    def _create_model_inputs_excel(self, df: pd.DataFrame) -> None:
        """生成综合输入文件"""
        try:
            output_file = OUTPUT_ANYLOGIC_DIR / "model_inputs.xlsx"

            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Sheet1: 原始数据
                df.to_excel(writer, sheet_name='energy_data', index=False)

                # Sheet2: 情景参数
                scenarios = [
                    {'scenario': 'baseline', 'demand_growth': 0.05, 'renewable_target': 0.50},
                    {'scenario': 'high_growth', 'demand_growth': 0.08, 'renewable_target': 0.60},
                    {'scenario': 'low_growth', 'demand_growth': 0.02, 'renewable_target': 0.40}
                ]
                scenarios_df = pd.DataFrame(scenarios)
                scenarios_df.to_excel(writer, sheet_name='scenarios', index=False)

            logger.info(f"综合输入文件已生成: {output_file}")

        except Exception as e:
            logger.error(f"生成综合输入文件失败: {str(e)}")


def main():
    """主函数"""
    logger.info("Pipeline初始化开始...")
    
    pipeline = EnergyDataPipeline()
    pipeline.run_full_pipeline()


if __name__ == "__main__":
    main()
