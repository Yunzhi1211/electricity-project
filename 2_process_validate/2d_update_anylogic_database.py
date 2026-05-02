#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
2d_update_anylogic_database.py
Rebuild the AnyLogic HSQLDB script (db.script) from the pipeline output Excel files
in 4_output_anylogic/.  Run automatically by main_pipeline.py after Stage 2.

What it does:
  - Reads supply_filled.xlsx and demand_filled.xlsx from 4_output_anylogic/
  - Rebuilds the INSERT INTO GENERATION and INSERT INTO DEMAND blocks in db.script
  - Updates the AL_ID RESTART WITH counters to match the new row counts
  - Preserves all other db.script content unchanged (except refreshed GENERATION/DEMAND blocks)
"""

from pathlib import Path
import pandas as pd
import re

# ── paths ──────────────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).parent.parent
ANYLOGIC_DIR  = BASE_DIR / "4_output_anylogic"
DB_SCRIPT     = (BASE_DIR / "5_anylogic_model"
                 / "ElectricityTrial_-_Version 7_-_Sources"
                 / "database" / "db.script")

SUPPLY_FILE   = ANYLOGIC_DIR / "supply_filled.xlsx"
DEMAND_FILE   = ANYLOGIC_DIR / "demand_filled.xlsx"

SUPPLY_COLUMNS = [
    "date",
    "total_supply",
    "thermal_supply",
    "hydro_supply",
    "nuclear_supply",
    "wind_supply",
    "solar_supply",
]

DEMAND_COLUMNS = [
    "date",
    "total_demand",
    "primary_demand",
    "secondary_demand",
    "tertiary_demand",
    "residential_demand",
]


# ── helpers ────────────────────────────────────────────────────────────────────
def _fmt(v: float) -> str:
    """Format a float in HSQLDB scientific notation (e.g. 1234.5E0)."""
    return f"{v}E0"


def _validate_columns(df: pd.DataFrame, required_cols: list[str], label: str) -> None:
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")


def _normalize_monthly(df: pd.DataFrame, required_cols: list[str], label: str) -> tuple[pd.DataFrame, int]:
    """Keep required columns, normalize date to month start, deduplicate by month."""
    _validate_columns(df, required_cols, label)
    out = df[required_cols].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    raw_rows = len(out)
    out = out.dropna(subset=["date"]).copy()
    out["date"] = out["date"].dt.to_period("M").dt.to_timestamp()
    out = out.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    return out, raw_rows


def align_common_months(supply_df: pd.DataFrame, demand_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Return supply/demand frames aligned on common monthly dates only."""
    supply_norm, supply_raw_rows = _normalize_monthly(supply_df, SUPPLY_COLUMNS, "supply_filled.xlsx")
    demand_norm, demand_raw_rows = _normalize_monthly(demand_df, DEMAND_COLUMNS, "demand_filled.xlsx")

    common_months = pd.DataFrame(
        {"date": sorted(set(supply_norm["date"]).intersection(set(demand_norm["date"])))}
    )
    if common_months.empty:
        raise ValueError("No overlapping monthly dates between supply and demand data.")

    supply_aligned = (
        common_months.merge(supply_norm, on="date", how="left")
        .sort_values("date")
        .reset_index(drop=True)
    )
    demand_aligned = (
        common_months.merge(demand_norm, on="date", how="left")
        .sort_values("date")
        .reset_index(drop=True)
    )

    # Drop obviously abnormal months before writing db.script.
    # This protects the simulation from singular outliers (e.g., tiny supply vs normal demand).
    ratio_df = (
        common_months
        .merge(supply_norm[["date", "total_supply"]], on="date", how="left")
        .merge(demand_norm[["date", "total_demand"]], on="date", how="left")
    )
    ratio_df["supply_demand_ratio"] = ratio_df["total_supply"] / ratio_df["total_demand"].replace(0, pd.NA)
    invalid_mask = ratio_df["supply_demand_ratio"].lt(0.2) | ratio_df["supply_demand_ratio"].gt(5.0)
    dropped_months = ratio_df.loc[invalid_mask, "date"].dt.strftime("%Y-%m").tolist()

    if dropped_months:
        keep_dates = ratio_df.loc[~invalid_mask, "date"]
        supply_aligned = supply_aligned[supply_aligned["date"].isin(keep_dates)].reset_index(drop=True)
        demand_aligned = demand_aligned[demand_aligned["date"].isin(keep_dates)].reset_index(drop=True)

    stats = {
        "supply_raw_rows": supply_raw_rows,
        "demand_raw_rows": demand_raw_rows,
        "supply_unique_months": len(supply_norm),
        "demand_unique_months": len(demand_norm),
        "common_months": len(common_months),
        "dropped_anomaly_months": len(dropped_months),
        "dropped_anomaly_month_list": dropped_months,
        "final_months": len(supply_aligned),
        "common_start": common_months["date"].iloc[0].strftime("%Y-%m"),
        "common_end": common_months["date"].iloc[-1].strftime("%Y-%m"),
    }
    return supply_aligned, demand_aligned, stats


def build_generation_inserts(df: pd.DataFrame) -> list[str]:
    """Return INSERT INTO GENERATION lines from supply_filled dataframe."""
    lines = []
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    for i, row in df.iterrows():
        dt = row["date"].strftime("%Y-%m-%d 00:00:00.000000")
        vals = (
            i,
            f"'{dt}'",
            _fmt(row["total_supply"]),
            _fmt(row["thermal_supply"]),
            _fmt(row["hydro_supply"]),
            _fmt(row["nuclear_supply"]),
            _fmt(row["wind_supply"]),
            _fmt(row["solar_supply"]),
        )
        lines.append(f"INSERT INTO GENERATION VALUES({','.join(str(v) for v in vals)})")
    return lines


def build_demand_inserts(df: pd.DataFrame) -> list[str]:
    """Return INSERT INTO DEMAND lines from demand_filled dataframe."""
    lines = []
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    for i, row in df.iterrows():
        dt = row["date"].strftime("%Y-%m-%d 00:00:00.000000")
        vals = (
            i,
            f"'{dt}'",
            _fmt(row["total_demand"]),
            _fmt(row["primary_demand"]),
            _fmt(row["secondary_demand"]),
            _fmt(row["tertiary_demand"]),
            _fmt(row["residential_demand"]),
        )
        lines.append(f"INSERT INTO DEMAND VALUES({','.join(str(v) for v in vals)})")
    return lines


def update_db_script(db_path: Path, gen_lines: list[str], dem_lines: list[str]) -> None:
    """Rebuild db.script replacing GENERATION and DEMAND INSERT blocks."""
    text = db_path.read_text(encoding="utf-8")

    # ── update ALTER TABLE RESTART WITH counters ──────────────────────────────
    n_gen = len(gen_lines)
    n_dem = len(dem_lines)
    text = re.sub(
        r"ALTER TABLE PUBLIC\.GENERATION ALTER COLUMN AL_ID RESTART WITH \d+",
        f"ALTER TABLE PUBLIC.GENERATION ALTER COLUMN AL_ID RESTART WITH {n_gen}",
        text,
    )
    text = re.sub(
        r"ALTER TABLE PUBLIC\.DEMAND ALTER COLUMN AL_ID RESTART WITH \d+",
        f"ALTER TABLE PUBLIC.DEMAND ALTER COLUMN AL_ID RESTART WITH {n_dem}",
        text,
    )

    # ── replace INSERT blocks ─────────────────────────────────────────────────
    # Remove all existing GENERATION inserts, then all DEMAND inserts.
    text = re.sub(r"INSERT INTO GENERATION VALUES\([^\n]+\)\n?", "", text)
    text = re.sub(r"INSERT INTO DEMAND VALUES\([^\n]+\)\n?", "", text)

    # Find insertion point: before optional legacy TECH/SCENARIO inserts (or end of file).
    anchor_pattern = re.compile(r"(INSERT INTO (TECH|SCENARIO) VALUES\()", re.MULTILINE)
    match = anchor_pattern.search(text)

    new_block = (
        "\n".join(gen_lines)
        + "\n"
        + "\n".join(dem_lines)
        + "\n"
    )

    if match:
        insert_pos = match.start()
        text = text[:insert_pos] + new_block + text[insert_pos:]
    else:
        # Fallback: append before the last line.
        text = text.rstrip() + "\n" + new_block

    db_path.write_text(text, encoding="utf-8")
    print(f"[2d] db.script updated: {n_gen} generation rows, {n_dem} demand rows")


# ── main ───────────────────────────────────────────────────────────────────────
def main() -> None:
    if not SUPPLY_FILE.exists():
        raise FileNotFoundError(f"supply_filled.xlsx not found: {SUPPLY_FILE}")
    if not DEMAND_FILE.exists():
        raise FileNotFoundError(f"demand_filled.xlsx not found: {DEMAND_FILE}")
    if not DB_SCRIPT.exists():
        raise FileNotFoundError(f"db.script not found: {DB_SCRIPT}")

    supply_df = pd.read_excel(SUPPLY_FILE)
    demand_df = pd.read_excel(DEMAND_FILE)

    # Use common monthly dates only so GENERATION/DEMAND are index-aligned.
    supply_aligned, demand_aligned, stats = align_common_months(supply_df, demand_df)

    # Reset indices to get 0-based AL_ID values.
    supply_aligned = supply_aligned.reset_index(drop=True)
    demand_aligned = demand_aligned.reset_index(drop=True)

    gen_lines = build_generation_inserts(supply_aligned)
    dem_lines = build_demand_inserts(demand_aligned)

    update_db_script(DB_SCRIPT, gen_lines, dem_lines)

    # Write demand_filled.csv alongside the xlsx so AnyLogic can read it directly.
    csv_path = ANYLOGIC_DIR / "demand_filled.csv"
    demand_aligned.to_csv(csv_path, index=False, date_format="%Y-%m-%d")
    print(f"[2d] demand_filled.csv written: {len(demand_aligned)} aligned rows -> {csv_path}")

    # nMonths follows minRows/common-months policy.
    n_months = len(supply_aligned)
    info_path = ANYLOGIC_DIR / "db_update_info.txt"
    dropped_list = ", ".join(stats["dropped_anomaly_month_list"]) if stats["dropped_anomaly_month_list"] else "None"
    info_path.write_text(
        f"SUPPLY raw rows:          {stats['supply_raw_rows']}\n"
        f"DEMAND raw rows:          {stats['demand_raw_rows']}\n"
        f"SUPPLY unique months:     {stats['supply_unique_months']}\n"
        f"DEMAND unique months:     {stats['demand_unique_months']}\n"
        f"COMMON aligned months:    {stats['common_months']}\n"
        f"Dropped anomaly months:   {stats['dropped_anomaly_months']}\n"
        f"Dropped month list:       {dropped_list}\n"
        f"FINAL months written:     {stats['final_months']}\n"
        f"Common month range:       {stats['common_start']} to {stats['common_end']}\n"
        f"GENERATION rows written:  {len(supply_aligned)}\n"
        f"DEMAND rows written:      {len(demand_aligned)}\n"
        f"nMonths to set in AnyLogic Main.nMonths: {n_months}\n",
        encoding="utf-8",
    )
    print(f"[2d] Update info written to {info_path}")
    print(f"[2d] IMPORTANT: Set Main.nMonths = {n_months} in AnyLogic.")


if __name__ == "__main__":
    main()

