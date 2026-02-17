"""Bronze to Silver Pipeline."""

import pandas as pd

from constants.column_names.bronze import (
    BRONZE_CLIMATE_CODE_DEP,
    BRONZE_CLIMATE_METRIC,
    BRONZE_CLIMATE_NOM_DEP,
    BRONZE_CLIMATE_SCENARIO,
    BRONZE_CLIMATE_TIME,
    BRONZE_CLIMATE_VALUE,
    BRONZE_METRIC_PRECIP,
    BRONZE_METRIC_TEMP_MAX,
    BRONZE_METRIC_TEMP_MEAN,
    BRONZE_YIELD_AREA,
    BRONZE_YIELD_DEPARTMENT,
    BRONZE_YIELD_PRODUCTION,
    BRONZE_YIELD_UNNAMED_0,
    SCENARIO_HISTORICAL,
    SCENARIO_SSP2_4_5,
)
from constants.column_names.silver import (
    SILVER_CODE_DEP,
    SILVER_NOM_DEP,
    SILVER_PRECIP,
    SILVER_SCENARIO,
    SILVER_TEMP_MAX,
    SILVER_TEMP_MEAN,
    SILVER_TIME,
    SILVER_YEAR,
    SILVER_YIELD,
)
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
        2. Pivot metrics into separate columns (keep daily granularity).
        3. Clip negative precipitation to 0.
        4. Keep ALL scenarios (historical + SSP) so future predictions are possible.

    Parameters
    ----------
    df_yield_raw : pd.DataFrame
        Raw barley yield data (semicolon-separated CSV already loaded).
    df_climate_raw : pd.DataFrame
        Raw climate data (parquet already loaded).

    Returns:
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (yield_clean, climate_clean)
    """
    # Standardize column names
    df_yield = _clean_cols(df_yield_raw).rename(
        columns={BRONZE_YIELD_DEPARTMENT: SILVER_NOM_DEP}
    )
    df_yield[SILVER_YEAR] = pd.to_numeric(df_yield[SILVER_YEAR], errors="coerce")
    df_yield[SILVER_YIELD] = pd.to_numeric(df_yield[SILVER_YIELD], errors="coerce")
    df_yield[BRONZE_YIELD_AREA] = pd.to_numeric(
        df_yield[BRONZE_YIELD_AREA], errors="coerce"
    )
    df_yield[BRONZE_YIELD_PRODUCTION] = pd.to_numeric(
        df_yield[BRONZE_YIELD_PRODUCTION], errors="coerce"
    )

    df_climate = _clean_cols(df_climate_raw)
    df_climate[SILVER_TIME] = pd.to_datetime(df_climate[BRONZE_CLIMATE_TIME])

    # Reference sets from historical climate
    hist_mask = df_climate[BRONZE_CLIMATE_SCENARIO] == SCENARIO_HISTORICAL
    climate_depts = set(df_climate.loc[hist_mask, BRONZE_CLIMATE_NOM_DEP].unique())
    climate_years = set(df_climate.loc[hist_mask, SILVER_YEAR].unique())

    # ===================== YIELD CLEANING =====================
    n0 = len(df_yield)

    # 1. Recover yield = production / area
    recoverable = (
        df_yield[SILVER_YIELD].isna()
        & df_yield[BRONZE_YIELD_AREA].notna()
        & df_yield[BRONZE_YIELD_PRODUCTION].notna()
        & (df_yield[BRONZE_YIELD_AREA] > 0)
    )
    df_yield.loc[recoverable, SILVER_YIELD] = (
        df_yield.loc[recoverable, BRONZE_YIELD_PRODUCTION]
        / df_yield.loc[recoverable, BRONZE_YIELD_AREA]
    )
    logger.info(f"Recovered {recoverable.sum()} yield values from production/area")

    # 2. Keep only departments present in climate
    df_yield = df_yield[df_yield[SILVER_NOM_DEP].str.strip().isin(climate_depts)]
    logger.info(f"After dropping departments not in climate: {len(df_yield)} rows")

    # 3. Keep only years covered by historical climate (1982-2014)
    df_yield = df_yield[df_yield[SILVER_YEAR].isin(climate_years)]
    logger.info(f"After dropping years outside historical range: {len(df_yield)} rows")

    # 4. Drop remaining NaN yield
    df_yield = df_yield.dropna(subset=[SILVER_YIELD])
    logger.info(f"After dropping NaN yield: {len(df_yield)} rows")

    # Drop useless index column
    df_yield = df_yield.drop(columns=[BRONZE_YIELD_UNNAMED_0], errors="ignore")

    logger.info(
        f"Yield: {len(df_yield)} / {n0} rows kept ({100 * len(df_yield) / n0:.1f}%)"
    )

    # ===================== CLIMATE CLEANING =====================
    # Rename metrics
    metric_map = {
        BRONZE_METRIC_TEMP_MEAN: SILVER_TEMP_MEAN,
        BRONZE_METRIC_TEMP_MAX: SILVER_TEMP_MAX,
        BRONZE_METRIC_PRECIP: SILVER_PRECIP,
    }
    df_climate[BRONZE_CLIMATE_METRIC] = df_climate[BRONZE_CLIMATE_METRIC].map(
        metric_map
    )

    # Pivot metrics into columns
    climate_clean = df_climate.pivot_table(
        index=[
            BRONZE_CLIMATE_SCENARIO,
            BRONZE_CLIMATE_NOM_DEP,
            BRONZE_CLIMATE_CODE_DEP,
            SILVER_YEAR,
            SILVER_TIME,
        ],
        columns=BRONZE_CLIMATE_METRIC,
        values=BRONZE_CLIMATE_VALUE,
    ).reset_index()
    climate_clean.columns.name = None

    # Rename to silver column names
    climate_clean = climate_clean.rename(
        columns={
            BRONZE_CLIMATE_NOM_DEP: SILVER_NOM_DEP,
            BRONZE_CLIMATE_SCENARIO: SILVER_SCENARIO,
            BRONZE_CLIMATE_CODE_DEP: SILVER_CODE_DEP,
        }
    )

    # Clip negative precipitation
    climate_clean[SILVER_PRECIP] = climate_clean[SILVER_PRECIP].clip(lower=0)

    # Log warnings for SSP2_4.5 missing data
    ssp245 = climate_clean[climate_clean[SILVER_SCENARIO] == SCENARIO_SSP2_4_5]
    missing_depts = ssp245[ssp245[SILVER_TEMP_MAX].isna()][SILVER_NOM_DEP].unique()
    if len(missing_depts) > 0:
        logger.warning(
            f"SSP2_4.5 has missing temp_max/precip for {len(missing_depts)} "
            f"departments: {list(missing_depts)}"
        )

    logger.info(f"Climate clean shape: {climate_clean.shape}")
    logger.info(f"Scenarios kept: {sorted(climate_clean[SILVER_SCENARIO].unique())}")

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
