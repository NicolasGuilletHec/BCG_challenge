"""Bronze to Silver Pipeline."""

import numpy as np
import pandas as pd

from constants.paths import (
    BARLEY_PATH,
    CLIMATE_PATH,
    SILVER_CLIMATE_PATH,
    SILVER_DIR,
    SILVER_YIELD_PATH,
)
from src.utils.logger import logger


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase, underscores, alphanumeric-only column names."""
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    return df


# ---------------------------------------------------------------------------
# Core cleaning function
# ---------------------------------------------------------------------------

def clean_bronze_data(
    df_yield_raw: pd.DataFrame,
    df_climate_raw: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean both raw datasets and return silver-level tables.

    Yield cleaning:
        1. Recover missing yield via yield = production / area.
        2. Drop departments absent from climate historical data.
        3. Drop years outside the historical climate range (1982-2014).
        4. Drop remaining rows with NaN yield.

    Climate cleaning:
        1. Rename metrics to short names (temp_mean, temp_max, precip).
        2. Aggregate daily values to monthly (mean for temp, sum for precip).
        3. Pivot metrics into separate columns.
        4. Clip negative precipitation to 0.
        5. Keep ALL scenarios (historical + SSP) so future predictions are possible.

    Parameters
    ----------
    df_yield_raw : pd.DataFrame
        Raw barley yield data (semicolon-separated CSV already loaded).
    df_climate_raw : pd.DataFrame
        Raw climate data (parquet already loaded).

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (yield_clean, climate_clean)
    """
    # --- Standardize columns ---
    df_yield = _clean_cols(df_yield_raw).rename(columns={"department": "nom_dep"})
    df_yield["year"] = pd.to_numeric(df_yield["year"], errors="coerce")
    df_yield["yield"] = pd.to_numeric(df_yield["yield"], errors="coerce")
    df_yield["area"] = pd.to_numeric(df_yield["area"], errors="coerce")
    df_yield["production"] = pd.to_numeric(df_yield["production"], errors="coerce")

    df_climate = _clean_cols(df_climate_raw)
    df_climate["time"] = pd.to_datetime(df_climate["time"])

    # --- Reference sets from historical climate ---
    hist_mask = df_climate["scenario"] == "historical"
    climate_depts = set(df_climate.loc[hist_mask, "nom_dep"].unique())
    climate_years = set(df_climate.loc[hist_mask, "year"].unique())

    # ===================== YIELD CLEANING =====================
    n0 = len(df_yield)

    # 1. Recover yield = production / area
    recoverable = (
        df_yield["yield"].isna()
        & df_yield["area"].notna()
        & df_yield["production"].notna()
        & (df_yield["area"] > 0)
    )
    df_yield.loc[recoverable, "yield"] = (
        df_yield.loc[recoverable, "production"] / df_yield.loc[recoverable, "area"]
    )
    logger.info(f"Recovered {recoverable.sum()} yield values from production/area")

    # 2. Keep only departments present in climate
    df_yield = df_yield[df_yield["nom_dep"].str.strip().isin(climate_depts)]
    logger.info(f"After dropping departments not in climate: {len(df_yield)} rows")

    # 3. Keep only years covered by historical climate (1982-2014)
    df_yield = df_yield[df_yield["year"].isin(climate_years)]
    logger.info(f"After dropping years outside historical range: {len(df_yield)} rows")

    # 4. Drop remaining NaN yield
    df_yield = df_yield.dropna(subset=["yield"])
    logger.info(f"After dropping NaN yield: {len(df_yield)} rows")

    # Drop useless index column
    df_yield = df_yield.drop(columns=["unnamed_0"], errors="ignore")

    logger.info(f"Yield: {len(df_yield)} / {n0} rows kept ({100 * len(df_yield) / n0:.1f}%)")

    # ===================== CLIMATE CLEANING =====================
    # Short metric names
    metric_map = {
        "near_surface_air_temperature": "temp_mean",
        "daily_maximum_near_surface_air_temperature": "temp_max",
        "precipitation": "precip",
    }
    df_climate["metric"] = df_climate["metric"].map(metric_map)
    df_climate["month"] = df_climate["time"].dt.month

    # Monthly aggregation: mean for temp, sum for precip
    monthly = (
        df_climate
        .groupby(["scenario", "nom_dep", "code_dep", "year", "month", "metric"])["value"]
        .agg(["mean", "sum"])
        .reset_index()
    )
    monthly["value"] = np.where(
        monthly["metric"].str.startswith("temp"),
        monthly["mean"],
        monthly["sum"],
    )
    monthly = monthly.drop(columns=["mean", "sum"])

    # Pivot metrics into columns
    climate_clean = monthly.pivot_table(
        index=["scenario", "nom_dep", "code_dep", "year", "month"],
        columns="metric",
        values="value",
    ).reset_index()
    climate_clean.columns.name = None

    # Clip negative precipitation
    climate_clean["precip"] = climate_clean["precip"].clip(lower=0)

    logger.info(f"Climate clean shape: {climate_clean.shape}")
    logger.info(f"Scenarios kept: {sorted(climate_clean['scenario'].unique())}")

    return df_yield, climate_clean


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def bronze_to_silver():
    """Pipeline to transform bronze data to silver data."""
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Read bronze data
    logger.info(f"Reading yield data at {BARLEY_PATH}")
    df_yield_raw = pd.read_csv(BARLEY_PATH, sep=";")

    logger.info(f"Reading climate data at {CLIMATE_PATH}")
    df_climate_raw = pd.read_parquet(CLIMATE_PATH)

    # 2. Clean
    logger.info("Cleaning bronze data")
    yield_clean, climate_clean = clean_bronze_data(df_yield_raw, df_climate_raw)

    # 3. Write silver
    logger.info(f"Writing yield silver to {SILVER_YIELD_PATH}")
    yield_clean.to_parquet(SILVER_YIELD_PATH, index=False)

    logger.info(f"Writing climate silver to {SILVER_CLIMATE_PATH}")
    climate_clean.to_parquet(SILVER_CLIMATE_PATH, index=False)

    logger.info("Bronze to Silver pipeline complete")


if __name__ == "__main__":
    logger.info("Starting Bronze to Silver pipeline")
    bronze_to_silver()
    logger.info("Finished Bronze to Silver pipeline")
