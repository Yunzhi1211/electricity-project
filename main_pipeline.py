#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
National Energy Data Pipeline: Crawl + Clean + Validate + AnyLogic Outputs

Workflow:
- Stage 0: Data acquisition and cleaning
- Stage 1: Data processing and validation
- Stage 2: AnyLogic input generation

Author: Yunzhi
Created: 2026-04-16
"""

import logging
import sys
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

# Base directory must be defined before logging so the log path is absolute.
_BASE_DIR = Path(__file__).parent
_LOG_DIR = _BASE_DIR / "0_log"
_LOG_DIR.mkdir(exist_ok=True)
_RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")
_PIPELINE_LOG = _LOG_DIR / f"{_RUN_TS}_pipeline.log"

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(str(_PIPELINE_LOG), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Directory configuration
BASE_DIR = _BASE_DIR
LOG_DIR = _LOG_DIR
RAW_DATA_DIR = BASE_DIR / "0_raw_data"
CLEAN_DEMAND_SUPPLY_DIR = BASE_DIR / "1_clean_demand_supply"
PROCESS_VALIDATE_DIR = BASE_DIR / "2_process_validate"
CHECK_REPORT_DIR = BASE_DIR / "3_output_check_report"
OUTPUT_ANYLOGIC_DIR = BASE_DIR / "4_output_anylogic"

# Ensure required directories exist.
for dir_path in [RAW_DATA_DIR, CLEAN_DEMAND_SUPPLY_DIR, PROCESS_VALIDATE_DIR, CHECK_REPORT_DIR, OUTPUT_ANYLOGIC_DIR]:
    dir_path.mkdir(exist_ok=True)


class EnergyDataPipeline:
    """End-to-end national energy data pipeline."""

    def __init__(self):
        self.demand_data = None
        self.supply_data = None
        self.merged_data = None

    def run_full_pipeline(self) -> None:
        """Run the full pipeline."""
        logger.info("="*60)
        logger.info("Starting national energy data pipeline")
        logger.info("="*60)

        try:
            # Stage 0: data acquisition and cleaning.
            logger.info("Stage 0: data acquisition and cleaning")
            self._stage_0_data_acquisition()

            # Stage 1: data processing and validation.
            logger.info("Stage 1: data processing and validation")
            self._stage_1_data_processing()

            # Stage 2: AnyLogic outputs.
            logger.info("Stage 2: generate AnyLogic input files")
            self._stage_2_anylogic_outputs()

            logger.info("="*60)
            logger.info("Pipeline execution completed")
            logger.info(f"Output files saved to: {OUTPUT_ANYLOGIC_DIR}")
            logger.info("="*60)

        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            raise

    def _stage_0_data_acquisition(self) -> None:
        """Stage 0: data acquisition and cleaning."""
        try:
            # 1a: demand data availability.
            logger.info("1a: checking demand data...")
            demand_file = OUTPUT_ANYLOGIC_DIR / "demand_filled.xlsx"
            if demand_file.exists():
                self.demand_data = pd.read_excel(demand_file)
                logger.info(f"Demand data loaded: {self.demand_data.shape}")
            else:
                logger.warning("Demand data file does not exist")

            # 1b: demand cleaning.
            logger.info("1b: checking demand cleaning...")
            if not demand_file.exists():
                logger.info("Running demand cleaning script...")
                self._run_demand_cleaning()

            # 1c: supply cleaning.
            logger.info("1c: running supply cleaning...")
            self._clean_supply_data()

        except Exception as e:
            logger.error(f"Stage 0 failed: {str(e)}")
            raise

    def _stage_1_data_processing(self) -> None:
        """Stage 1: data processing and validation."""
        try:
            # 2a: merge datasets.
            logger.info("2a: merging datasets...")
            self._run_processing_script(PROCESS_VALIDATE_DIR / "2a_merge_datasets.py")

            # 2b: calculate indicators.
            logger.info("2b: calculating energy indicators...")
            self._run_processing_script(PROCESS_VALIDATE_DIR / "2b_calculate_indicators.py")

            # 2c: consistency validation.
            logger.info("2c: running consistency validation...")
            self._run_processing_script(PROCESS_VALIDATE_DIR / "2c_validate_consistency.py")

        except Exception as e:
            logger.error(f"Stage 1 failed: {str(e)}")
            raise

    def _stage_2_anylogic_outputs(self) -> None:
        """Stage 2: generate AnyLogic input files."""
        try:
            self._cleanup_anylogic_output_dir()

            # Load energy indicators.
            energy_indicators_file = CHECK_REPORT_DIR / "2_energy_indicators.csv"
            if not energy_indicators_file.exists():
                logger.warning(f"Energy indicator file not found: {energy_indicators_file}")
                logger.info("Run Stage 1 processing first")
                return

            energy_df = pd.read_csv(energy_indicators_file, encoding='utf-8-sig')
            logger.info(f"Energy indicators loaded: {energy_df.shape}")

            # 3a: monthly demand output.
            logger.info("3a: generating monthly demand file...")
            self._create_demand_monthly_csv(energy_df)

            # 3b: monthly generation output.
            logger.info("3b: generating monthly generation file...")
            self._create_generation_monthly_csv(energy_df)

            # 3c: energy balance output.
            logger.info("3c: generating energy balance file...")
            self._create_energy_balance_csv(energy_df)

            # 3d: scenario parameters.
            logger.info("3d: generating scenario parameter file...")
            self._create_scenario_parameters_csv()

            # 3e: consolidated model inputs.
            logger.info("3e: generating consolidated model input file...")
            self._create_model_inputs_excel(energy_df)

            # 3f: output documentation.
            logger.info("3f: generating output documentation...")
            self._create_output_documentation(energy_df)

            # 3g: remove standalone validation report after consolidating it into output_catalog.
            logger.info("3g: removing standalone validation report...")
            self._remove_validation_report_file()

        except Exception as e:
            logger.error(f"Stage 2 failed: {str(e)}")
            raise

    def _cleanup_anylogic_output_dir(self) -> None:
        """Keep only AnyLogic-facing files in 4_output_anylogic."""
        allowed = {"demand_crawl.csv", "demand_filled.xlsx", "supply_filled.xlsx", "scenario_parameters.csv"}
        for file_path in OUTPUT_ANYLOGIC_DIR.glob("*"):
            if file_path.is_file() and file_path.name not in allowed:
                file_path.unlink(missing_ok=True)

    # ============ Implementation Methods ============

    def _run_processing_script(self, script_path: Path) -> None:
        """Run a processing script via subprocess."""
        if not script_path.exists():
            logger.warning(f"Script not found: {script_path}")
            return
        logger.info(f"Running script: {script_path}")
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(BASE_DIR),
            capture_output=False
        )
        if result.returncode != 0:
            raise RuntimeError(f"Script failed with exit code {result.returncode}: {script_path}")

    def _run_demand_cleaning(self) -> None:
        """Run the demand cleaning script."""
        try:
            demand_script = CLEAN_DEMAND_SUPPLY_DIR / "1b_clean_demand_data.py"
            if demand_script.exists():
                logger.info(f"Running demand cleaning script: {demand_script}")
                subprocess.run(
                    [sys.executable, str(demand_script)],
                    cwd=str(BASE_DIR),
                    check=False,
                    capture_output=False
                )
            else:
                logger.warning(f"Demand cleaning script not found: {demand_script}")
        except Exception as e:
            logger.error(f"Failed to run demand cleaning script: {e}")

    def _clean_supply_data(self) -> None:
        """Run supply cleaning (calls 1c_clean_supply_data.py)."""
        try:
            supply_script = CLEAN_DEMAND_SUPPLY_DIR / "1c_clean_supply_data.py"
            if supply_script.exists():
                logger.info(f"Running supply cleaning script: {supply_script}")
                result = subprocess.run(
                    [sys.executable, str(supply_script)],
                    cwd=str(BASE_DIR),
                    check=False,
                    capture_output=False
                )
                if result.returncode != 0:
                    raise RuntimeError(f"Supply cleaning script failed with exit code {result.returncode}")
                
                # Check output file.
                supply_file = OUTPUT_ANYLOGIC_DIR / "supply_filled.xlsx"
                if supply_file.exists():
                    logger.info(f"Supply cleaning completed: {supply_file}")
                    self.supply_data = pd.read_excel(supply_file)
                    logger.info(f"Supply data loaded: {self.supply_data.shape}")
                    if 'thermal_supply' in self.supply_data.columns:
                        nonzero_count = int((self.supply_data['thermal_supply'] > 0).sum())
                        logger.info(f"Thermal supply non-zero months: {nonzero_count}")
                    else:
                        raise RuntimeError("thermal_supply column missing in supply_filled.xlsx")
                else:
                    raise RuntimeError(f"Supply output file not found after cleaning: {supply_file}")
            else:
                logger.warning(f"Supply cleaning script not found: {supply_script}")
        except Exception as e:
            logger.error(f"Error while running supply cleaning: {str(e)}")

    def _merge_datasets(self) -> None:
        """Merge datasets."""
        try:
            supply_file = OUTPUT_ANYLOGIC_DIR / "supply_filled.xlsx"
            if not supply_file.exists():
                logger.warning(f"Supply data file not found: {supply_file}")
                return

            self.supply_data = pd.read_excel(supply_file)
            self.merged_data = self.supply_data.copy()
            
            logger.info(f"Dataset merge completed: {self.merged_data.shape}")

            # Save merged output.
            merged_file = CHECK_REPORT_DIR / "merged_data.csv"
            self.merged_data.to_csv(merged_file, index=False, encoding='utf-8-sig')
            logger.info(f"Merged data saved: {merged_file}")

        except Exception as e:
            logger.error(f"Dataset merge failed: {str(e)}")

    def _calculate_indicators(self) -> None:
        """Calculate energy indicators."""
        try:
            if self.merged_data is None or self.merged_data.empty:
                logger.warning("Merged dataset is empty, skipping indicator calculation")
                return

            df = self.merged_data.copy()

            # Ensure date column is datetime.
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])

            # Calculate basic indicator set.
            logger.info("Calculating energy indicators...")

            # Supply-side indicators.
            supply_cols = ['total_supply', 'thermal_supply', 'hydro_supply', 
                          'nuclear_supply', 'wind_supply', 'solar_supply']
            available_supply_cols = [col for col in supply_cols if col in df.columns]

            if available_supply_cols:
                # Share by source type.
                for col in available_supply_cols:
                    if col != 'total_supply' and 'total_supply' in df.columns:
                        df[f'{col}_share'] = (df[col] / df['total_supply']).fillna(0)

                # Year-over-year growth.
                df['total_supply_yoy'] = df['total_supply'].pct_change(12)

            # Save indicators.
            output_file = CHECK_REPORT_DIR / "2_energy_indicators.csv"
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"Energy indicators saved: {output_file}")

        except Exception as e:
            logger.error(f"Indicator calculation failed: {str(e)}")

    def _validate_consistency(self) -> None:
        """Run consistency validation."""
        try:
            energy_file = CHECK_REPORT_DIR / "2_energy_indicators.csv"
            if not energy_file.exists():
                logger.warning(f"Energy indicator file not found: {energy_file}")
                return

            df = pd.read_csv(energy_file, encoding='utf-8-sig')
            logger.info(f"Validation input loaded: {df.shape}")

            # Basic quality checks.
            missing_count = df.isnull().sum().sum()
            logger.info(f"Total missing values: {missing_count}")

            if 'total_supply' in df.columns:
                logger.info(f"Total supply range: {df['total_supply'].min():.2f} - {df['total_supply'].max():.2f}")

            logger.info("Consistency validation completed")

        except Exception as e:
            logger.error(f"Consistency validation failed: {str(e)}")

    def _create_demand_monthly_csv(self, df: pd.DataFrame) -> None:
        """Create monthly demand CSV."""
        try:
            output_file = CHECK_REPORT_DIR / "3_1_demand_monthly.csv"

            if 'date' not in df.columns:
                logger.warning("Missing date column")
                return

            demand_cols = [
                'date', 'total_demand', 'primary_demand', 'secondary_demand',
                'tertiary_demand', 'residential_demand'
            ]
            available_cols = [col for col in demand_cols if col in df.columns]
            demand_df = df[available_cols].copy() if available_cols else pd.DataFrame()
            
            if demand_df.empty:
                logger.warning("Unable to generate demand file")
                return

            demand_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"Monthly demand file generated: {output_file}")

        except Exception as e:
            logger.error(f"Failed to generate demand file: {str(e)}")

    def _create_generation_monthly_csv(self, df: pd.DataFrame) -> None:
        """Create monthly generation CSV."""
        try:
            output_file = CHECK_REPORT_DIR / "3_2_generation_monthly.csv"
            
            supply_cols = ['date', 'total_supply', 'thermal_supply', 'hydro_supply',
                          'nuclear_supply', 'wind_supply', 'solar_supply']
            available_cols = [col for col in supply_cols if col in df.columns]

            if not available_cols:
                logger.warning("No generation columns available")
                return

            gen_df = df[available_cols].copy()
            gen_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"Monthly generation file generated: {output_file}")

        except Exception as e:
            logger.error(f"Failed to generate generation file: {str(e)}")

    def _create_energy_balance_csv(self, df: pd.DataFrame) -> None:
        """Create energy balance CSV."""
        try:
            output_file = CHECK_REPORT_DIR / "3_3_energy_balance.csv"
            
            if 'date' not in df.columns:
                logger.warning("Missing date column")
                return

            balance_df = df[['date']].copy()

            # Add available demand and supply columns.
            demand_cols = ['total_demand', 'primary_demand', 'secondary_demand', 'tertiary_demand', 'residential_demand']
            for col in demand_cols:
                if col in df.columns:
                    balance_df[col] = df[col]

            supply_cols = ['total_supply', 'thermal_supply', 'hydro_supply',
                          'nuclear_supply', 'wind_supply', 'solar_supply']
            for col in supply_cols:
                if col in df.columns:
                    balance_df[col] = df[col]

            derived_cols = ['supply_demand_ratio', 'balance_status']
            for col in derived_cols:
                if col in df.columns:
                    balance_df[col] = df[col]

            balance_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"Energy balance file generated: {output_file}")

        except Exception as e:
            logger.error(f"Failed to generate energy balance file: {str(e)}")

    def _create_scenario_parameters_csv(self) -> None:
        """Create scenario parameter CSV."""
        try:
            scenarios = [
                {
                    'scenario_id': 'policy_volatility_carbon_tax',
                    'name': 'Scenario 1 - Policy Volatility (Carbon Tax)',
                    'description': 'Introduce staged carbon-tax volatility to observe ABM adaptation under policy uncertainty.',
                    'shock_type': 'policy',
                    'demand_shock_pct': 0.00,
                    'hydro_capacity_factor': 1.00,
                    'carbon_tax_base': 80,
                    'carbon_tax_volatility': 0.30,
                    'shock_duration_months': 12,
                    'abm_adaptation_speed': 0.60
                },
                {
                    'scenario_id': 'extreme_weather_demand_hydro',
                    'name': 'Scenario 2 - Extreme Weather (Demand Surge / Hydro Constraint)',
                    'description': 'Simulate demand spikes and hydropower constraints to highlight ABM resilience during disruptions.',
                    'shock_type': 'weather',
                    'demand_shock_pct': 0.18,
                    'hydro_capacity_factor': 0.70,
                    'carbon_tax_base': 80,
                    'carbon_tax_volatility': 0.10,
                    'shock_duration_months': 6,
                    'abm_adaptation_speed': 0.85
                }
            ]

            scenarios_df = pd.DataFrame(scenarios)
            output_file = OUTPUT_ANYLOGIC_DIR / "scenario_parameters.csv"
            scenarios_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"Scenario parameter file generated: {output_file}")

        except Exception as e:
            logger.error(f"Failed to generate scenario parameters: {str(e)}")

    def _create_model_inputs_excel(self, df: pd.DataFrame) -> None:
        """Create consolidated model input workbook with indicator columns only."""
        try:
            output_file = CHECK_REPORT_DIR / "4_model_inputs.xlsx"

            # Keep only date + calculated indicator columns.
            indicator_patterns = ['_ratio', '_share', '_yoy', '_mom', '_seasonal', '_efficiency']
            indicator_cols = ['date'] + [
                col for col in df.columns
                if any(col.endswith(p) or p in col for p in indicator_patterns)
            ]
            available_cols = [col for col in indicator_cols if col in df.columns]
            indicators_df = df[available_cols].copy()

            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                indicators_df.to_excel(writer, sheet_name='indicators', index=False)

            logger.info(f"Consolidated model input file generated: {output_file}")

        except Exception as e:
            logger.error(f"Failed to generate consolidated model inputs: {str(e)}")

    def _create_output_documentation(self, df: pd.DataFrame) -> None:
        """Create plain-text documentation for output folders without breaking CSV structure."""
        try:
            check_doc = CHECK_REPORT_DIR / "output_catalog.txt"

            report_file = CHECK_REPORT_DIR / "validation_report.txt"
            report_excerpt = "Validation report not found."
            if report_file.exists():
                report_excerpt = report_file.read_text(encoding='utf-8')

            # Compute volatility summary from mom columns.
            def _top_volatile(cols):
                present = {c: float(df[c].abs().std()) for c in cols if c in df.columns}
                if not present:
                    return "n/a"
                top = max(present, key=present.get)
                return f"{top} (std={present[top]:.4f})"

            dem_vol  = _top_volatile(['total_demand_mom','primary_demand_mom','secondary_demand_mom',
                                      'tertiary_demand_mom','residential_demand_mom'])
            sup_vol  = _top_volatile(['total_supply_mom','thermal_supply_mom','hydro_supply_mom',
                                      'nuclear_supply_mom','wind_supply_mom','solar_supply_mom'])
            bal_vol  = _top_volatile(['supply_demand_ratio_mom'])

            check_doc.write_text(
                "3_output_check_report usage\n"
                "\n"
                "1_merged_energy_data.csv\n"
                "Purpose: merged monthly demand and supply dataset for validation and indicator calculation.\n"
                "Core operation: outer merge by normalized month date.\n"
                "\n"
                "2_energy_indicators.csv\n"
                "Purpose: derived indicator table for consistency checks and downstream analysis.\n"
                "Key formulas:\n"
                "- supply_demand_ratio = total_supply / total_demand\n"
                "- source_share = source_supply / sum(nonnegative_supply_components)\n"
                "- demand_share = sector_demand / sum(nonnegative_demand_components)\n"
                "- yoy = winsorized_value_t / winsorized_value_t-12 - 1\n"
                "- mom = winsorized_value_t / winsorized_value_t-1 - 1\n"
                "\n"
                "3_Volatility summary (highest month-on-month volatility by group)\n"
                f"  3_1_demand_monthly   : {dem_vol}\n"
                f"  3_2_generation_monthly: {sup_vol}\n"
                f"  3_3_energy_balance   : {bal_vol}\n"
                "\n"
                "4_model_inputs.xlsx\n"
                "Purpose: consolidated indicator workbook (date + all derived indicator columns only).\n"
                "\n"
                "Interpretation of validation checks and warnings (6 checks):\n"
                "- GENERATION: compares total_supply with the sum of component supplies; warning means component accounting mismatch exceeds threshold.\n"
                "- DEMAND: compares total_demand with sector-demand sum; warning means sector accounting mismatch is frequent or large.\n"
                "- BALANCE: evaluates supply_demand_ratio range and severe-imbalance frequency; warning means sustained over/under-supply risk.\n"
                "- TEMPORAL: checks date continuity and completeness; warning means missing months or abnormal time gaps.\n"
                "- SHARES: checks whether supply-share and demand-share sums are close to 1; warning means share composition inconsistency.\n"
                "- GROWTH: checks extreme YoY changes across core business series; warning means multiple columns show unusually large growth shocks.\n"
                "Note: if a check is PASS in the current report, it will not be considered a warning for this run.\n"
                "Evidence-based explanation for warning causes:\n"
                "- BALANCE warning reflects sustained supply-demand imbalance: China's power system exhibits regional excess thermal generation capacity driven by over-investment in coal-fired units alongside demand-side volatility from industrial cycles and weather-sensitive load (Lin et al., 2018; Ming et al., 2017).\n"
                "- Demand-side growth warnings are consistent with macro cycle shocks, structural industrial transition, and climate-driven load variability; electricity consumption decouples from GDP growth during business-cycle downturns and structural reform phases (Lin & Liu, 2016); rising temperatures significantly increase peak load, with each 1\u00b0C increase adding approximately 0.385 GW to provincial peak demand on average (Chen et al., 2021).\n"
                "- Supply-side growth warnings are consistent with hydrology uncertainty, renewable intermittency, and rapid capacity expansion; wind, solar, and hydro curtailment resulting from grid integration constraints and dispatch barriers is a persistent feature of China's power system (Li et al., 2015; Liu et al., 2018).\n"
                "\n"
                "Why this report leads to the following AnyLogic design:\n"
                "- Because GROWTH shows persistent extreme columns, demand and supply agents should include shock regimes with heterogeneous adaptation speeds.\n"
                "- Because BALANCE still has severe imbalance months, dispatch logic should explicitly model priority rules, outage states, and demand-response behavior.\n"
                "- Because volatility peaks in demand and generation MoM indicators, scenario experiments should prioritize demand shock intensity and source-specific output fluctuation (including thermal and renewables).\n"
                "- Implement shocks via scenario_parameters.csv (demand_shock_pct, hydro_capacity_factor, carbon_tax_volatility, shock_duration_months) and evaluate unmet demand, supply-demand ratio, renewable share, and adaptation speed over time.\n"
                "Current report:\n"
                "============================================================\n"
                "        Energy Data Consistency Report\n"
                "============================================================\n"
                "\n"
                f"{report_excerpt}\n"
                "\n"
                "============================================================\n"
                "\n"
                "Lin, J., Kahrl, F., Liu, X., 2018. A regional analysis of excess capacity in China's power systems. Resources, Conservation and Recycling 129, 93–101. https://doi.org/10.1016/j.resconrec.2017.10.009.\n"
                "Ming, Z., Ping, Z., Shunkun, Y., Hui, L., 2017. Overall review of the overcapacity situation in China's power systems. Renewable and Sustainable Energy Reviews 76, 768–774. https://doi.org/10.1016/j.rser.2017.03.084.\n"
                "Lin, B., Liu, C., 2016. Why is electricity consumption inconsistent with economic growth in China? Energy Policy 88, 310–316. https://doi.org/10.1016/j.enpol.2015.10.031.\n"
                "Chen, H., Yan, H., Gong, K., Yuan, X.-C., 2021. Comprehensive assessment of the impact of temperature on electricity consumption in China. Journal of Cleaner Production 322, 129080. https://doi.org/10.1016/j.jclepro.2021.129080.\n"
                "Li, C., Shi, H., Cao, Y., Wang, J., Kuang, Y., Tan, Y., Wei, J., 2015. A review of wind curtailment and abandonment in China. Renewable and Sustainable Energy Reviews 41, 1067–1079. https://doi.org/10.1016/j.rser.2014.09.009.\n"
                "Liu, S., Bie, Z., Lin, J., Wang, X., 2018. Renewable energy integration and grid stability in China. Energy Policy 123, 494–502. https://doi.org/10.1016/j.enpol.2018.09.007.\n",
                encoding='utf-8'
            )

            logger.info("Output documentation file generated: 3_output_check_report/output_catalog.txt")
        except Exception as e:
            logger.error(f"Failed to generate output documentation: {str(e)}")

    def _remove_validation_report_file(self) -> None:
        """Remove standalone validation_report.txt after its contents are copied into output_catalog."""
        try:
            report_file = CHECK_REPORT_DIR / "validation_report.txt"
            if report_file.exists():
                report_file.unlink()
                logger.info(f"Removed standalone validation report: {report_file}")
        except Exception as e:
            logger.error(f"Failed to remove validation report: {str(e)}")


def main():
    """Main entry point."""
    logger.info("Pipeline initialization started...")
    
    pipeline = EnergyDataPipeline()
    pipeline.run_full_pipeline()


if __name__ == "__main__":
    main()
