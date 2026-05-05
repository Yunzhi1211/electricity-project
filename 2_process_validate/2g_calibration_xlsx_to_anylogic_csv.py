#!/usr/bin/env python3
"""
Turn 0_raw_data/calibration_observed_price.xlsx into ONE annual CSV AnyLogic reads
(Main.calibrationAnnualWideCsvPath, default calibration_observed_annual.csv).

Workbook layout (required):
  - Row above data: ignored native/non-export row (often row 0).
  - English header row (default pandas header=1): **strict** identifiers only —
    cell text must match one of ALLOWED_HEADERS (ASCII, case-insensitive after
    strip; see below). Partial / fuzzy names like "coal" or "hydro" are rejected.

Exactly one column must be ``year`` or ``date`` (calendar year integers).
You may include any subset of the price columns; missing columns are emitted
empty in the CSV.

Allowed English headers (recommended spelling = export column snake_case):

  year   OR   date
  national_avg_yuan_per_mwh
  coal_benchmark_yuan_per_mwh
  nuclear_yuan_per_mwh
  thermal_wholesale_yuan_per_mwh
  hydro_yuan_per_mwh
  solar_yuan_per_mwh
  wind_yuan_per_mwh
  gas_ccgt_yuan_per_mwh
  biomass_yuan_per_mwh
  pv_fit_yuan_per_mwh

Unit handling: same as before (元/kWh → ×1000 when |x|<2, etc.).

Output: single CSV with columns fixed order OUTPUT_COLUMNS_IN_ORDER.
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import pandas as pd

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent

OUTPUT_COLUMNS_IN_ORDER = [
    "year",
    "national_avg_yuan_per_mwh",
    "coal_benchmark_yuan_per_mwh",
    "nuclear_yuan_per_mwh",
    "thermal_wholesale_yuan_per_mwh",
    "hydro_yuan_per_mwh",
    "solar_yuan_per_mwh",
    "wind_yuan_per_mwh",
    "gas_ccgt_yuan_per_mwh",
    "biomass_yuan_per_mwh",
    "pv_fit_yuan_per_mwh",
]

# Strict whitelist: normalized key (strip + lower) -> semantic role (= CSV field name except year/date -> year).
STRICT_HEADER_TO_ROLE: dict[str, str] = {
    "year": "year",
    "date": "year",
    "national_avg_yuan_per_mwh": "national_avg_yuan_per_mwh",
    "coal_benchmark_yuan_per_mwh": "coal_benchmark_yuan_per_mwh",
    "nuclear_yuan_per_mwh": "nuclear_yuan_per_mwh",
    "thermal_wholesale_yuan_per_mwh": "thermal_wholesale_yuan_per_mwh",
    "hydro_yuan_per_mwh": "hydro_yuan_per_mwh",
    "solar_yuan_per_mwh": "solar_yuan_per_mwh",
    "wind_yuan_per_mwh": "wind_yuan_per_mwh",
    "gas_ccgt_yuan_per_mwh": "gas_ccgt_yuan_per_mwh",
    "biomass_yuan_per_mwh": "biomass_yuan_per_mwh",
    "pv_fit_yuan_per_mwh": "pv_fit_yuan_per_mwh",
}


def _normalize_header_key(cell: object) -> str:
    """Normalize for lookup: BOM strip + trim + lowercase (no hyphen/underscore folding)."""
    return str(cell).replace("\ufeff", "").strip().lower()


def classify_english_column(raw_header: object) -> str | None:
    """Exact-match only to STRICT_HEADER_TO_ROLE keys (after normalize)."""
    return STRICT_HEADER_TO_ROLE.get(_normalize_header_key(raw_header))


def allowed_headers_prompt() -> str:
    keys = sorted(STRICT_HEADER_TO_ROLE.keys())
    return ", ".join(keys)


def _to_yuan_per_mwh(v: float | None, national_hint_yuan_per_mwh: float) -> float | None:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    x = float(v)
    med = national_hint_yuan_per_mwh
    if not math.isfinite(med):
        med = 400.0

    if 0 < abs(x) < 2:
        x *= 1000.0
    elif 5 < abs(x) < 2000 and med < 80:
        x /= 1000.0
        if 0 < abs(x) < 2:
            x *= 1000.0
    elif abs(x) > 2500:
        x /= 1000.0
    return x


def _row_national_hint(num_vals_pref: list[float | None]) -> float:
    cands = []
    for v in num_vals_pref:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            continue
        x = float(v)
        if 0 < abs(x) < 2:
            cands.append(x * 1000.0)
        elif 80 < abs(x) < 900:
            cands.append(abs(x))
    if not cands:
        return 400.0
    return sorted(cands)[len(cands) // 2]


def validate_english_headers(df_cols: pd.Index) -> None:
    """Every column header exact-match whitelist; exactly one year role; no duplicate roles."""
    from collections import Counter

    canonicals: list[str] = []
    for col in df_cols:
        canon = classify_english_column(col)
        raw = str(col).replace("\ufeff", "").strip()
        if canon is None:
            raise ValueError(
                f"Unknown column header {raw!r}. Strict mode requires one of:\n"
                f"  {allowed_headers_prompt()}\n"
                "(match is case-insensitive; no extra spaces inside the token)."
            )
        canonicals.append(canon)
    if "year" not in canonicals:
        raise ValueError(
            'Require exactly one column named "year" or "date" (strict spelling; case-insensitive).'
        )
    dup_roles = [k for k, v in Counter(canonicals).items() if v > 1]
    if dup_roles:
        raise ValueError(
            "Each semantic role once only — duplicated: "
            + ", ".join(sorted(dup_roles))
            + '. Use "year" OR "date", not both.'
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--xlsx",
        type=Path,
        default=_REPO_ROOT / "0_raw_data" / "calibration_observed_price.xlsx",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=_REPO_ROOT
        / "5_anylogic_model"
        / "ElectricityTrial_-_Version 7_-_Sources",
        help="Directory for calibration_observed_annual.csv.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="calibration_observed_annual.csv",
        help="Single output filename (annual wide table for Main.calibrationAnnualWideCsvPath).",
    )
    parser.add_argument(
        "--english-header-row",
        type=int,
        default=1,
        help="0-based Excel row index for the English header row (default: 1 = second row).",
    )
    args = parser.parse_args()

    xlsx = Path(args.xlsx)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / args.output

    df_raw = pd.read_excel(xlsx, sheet_name=0, header=int(args.english_header_row))

    validate_english_headers(df_raw.columns)
    excel_headers = list(df_raw.columns)
    year_excel_col = next(
        str(ex)
        for ex in excel_headers
        if classify_english_column(ex) == "year"
    )

    records: list[dict[str, object]] = []

    for _, row in df_raw.iterrows():
        year_raw = row[year_excel_col]
        if pd.isna(year_raw):
            continue
        try:
            year = int(round(float(str(year_raw).replace(",", "."))))
        except (ValueError, TypeError):
            continue

        vals_raw: dict[str, float | None] = {}
        for ex_name in excel_headers:
            canon = classify_english_column(ex_name)
            if canon == "year":
                continue
            cell = row[ex_name]
            if pd.isna(cell):
                vals_raw[canon] = None
                continue
            try:
                vals_raw[canon] = float(cell)
            except (ValueError, TypeError):
                vals_raw[canon] = None

        hint = _row_national_hint(
            [
                vals_raw.get("national_avg_yuan_per_mwh"),
                vals_raw.get("coal_benchmark_yuan_per_mwh"),
            ]
        )

        rec = {"year": year}
        for canon in OUTPUT_COLUMNS_IN_ORDER[1:]:
            rec[canon] = _to_yuan_per_mwh(vals_raw.get(canon), hint)
        records.append(rec)

    wide = pd.DataFrame.from_records(records).sort_values("year")
    num_cols = [c for c in OUTPUT_COLUMNS_IN_ORDER if c != "year"]
    wide[num_cols] = wide[num_cols].apply(pd.to_numeric, errors="coerce").round(4)
    wide = wide[OUTPUT_COLUMNS_IN_ORDER]
    wide.to_csv(out_csv, index=False, encoding="utf-8-sig")

    nat_nz = wide["national_avg_yuan_per_mwh"].notna().sum()
    print(f"Rows annual: {len(wide)}, non-null national rows: {int(nat_nz)}")
    print(f"Wrote: {out_csv}")


if __name__ == "__main__":
    main()
