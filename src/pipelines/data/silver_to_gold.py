"""Silver to Gold Pipeline."""

import pandas as pd

from constants.column_names.bronze import SCENARIO_HISTORICAL
from constants.column_names.gold import (
    GOLD_DRY_PERIODS_COUNT,
    GOLD_FREEZE_DAYS_COUNT,
    GOLD_HEAT_DAYS_COUNT,
    GOLD_HEAVY_RAIN_DAYS_COUNT,
    GOLD_MAX_DRY_SPELL_DAYS,
    GOLD_TEMP_MAX_GROWING,
    GOLD_TEMP_MAX_NON_GROWING,
    GOLD_TEMP_MEAN_GROWING,
    GOLD_TEMP_MEAN_NON_GROWING,
    GOLD_TEMP_MIN_GROWING,
    GOLD_TEMP_MIN_NON_GROWING,
    GOLD_TEMP_STD_GROWING,
    GOLD_TEMP_STD_NON_GROWING,
    GOLD_TOTAL_PRECIP_GROWING,
    GOLD_TOTAL_PRECIP_NON_GROWING,
    GOLD_WINTER_PRECIP_TOTAL,
)
from constants.column_names.silver import (
    SILVER_NOM_DEP,
    SILVER_PRECIP,
    SILVER_SCENARIO,
    SILVER_TEMP_MAX,
    SILVER_TEMP_MEAN,
    SILVER_TIME,
    SILVER_YEAR,
)
from constants.constants import (
    DRY_DAY_PRECIP_THRESHOLD_MM,
    FREEZE_THRESHOLD_KELVIN,
    GROWING_SEASON_END_MONTH,
    GROWING_SEASON_START_MONTH,
    HEAT_THRESHOLD_KELVIN,
    HEAVY_RAIN_THRESHOLD_MM,
    MIN_DRY_SPELL_DAYS,
    VALIDATION_THRESHOLD_YEAR,
    WINTER_MONTHS,
    WINTER_START_MONTH,
)
from constants.paths import (
    GOLD_CLIMATE_PATH,
    GOLD_DIR,
    GOLD_SCENARIO_PATH,
    GOLD_TRAINING_PATH,
    GOLD_VALIDATION_PATH,
    SILVER_CLIMATE_PATH,
    SILVER_YIELD_PATH,
)
from src.utils.logger import logger


def dry_periods(
    climate_data: pd.DataFrame,
    precip_col: str = SILVER_PRECIP,
    threshold: int = MIN_DRY_SPELL_DAYS,
) -> pd.DataFrame:
    """Count dry periods and max dry spell length per year.

    Args:
        climate_data: Climate data.
        precip_col: Name of the precipitation column.
        threshold: Minimum consecutive dry days to count as a dry period.

    Returns:
        DataFrame with number of dry periods and max dry spell length
        per department/year.
    """
    df = climate_data[[SILVER_NOM_DEP, SILVER_YEAR, SILVER_TIME, precip_col]].copy()
    df = df.sort_values([SILVER_NOM_DEP, SILVER_TIME])

    # Dry day
    df["is_dry"] = df[precip_col] < DRY_DAY_PRECIP_THRESHOLD_MM

    # Consecutive dry day groups
    df["dry_group"] = (
        df["is_dry"] != df.groupby(SILVER_NOM_DEP)["is_dry"].shift()
    ).cumsum()

    # Count length of each dry period
    dry_runs = (
        df[df["is_dry"]]
        .groupby([SILVER_NOM_DEP, SILVER_YEAR, "dry_group"])
        .size()
        .reset_index(name="run_length")
    )

    # Aggregate per department/year
    result = (
        dry_runs.groupby([SILVER_NOM_DEP, SILVER_YEAR])
        .agg(
            **{
                GOLD_DRY_PERIODS_COUNT: (
                    "run_length",
                    lambda x: (x >= threshold).sum(),
                ),
                GOLD_MAX_DRY_SPELL_DAYS: ("run_length", "max"),
            }
        )
        .reset_index()
    )

    # Years with no dry runs
    all_dept_years = climate_data[[SILVER_NOM_DEP, SILVER_YEAR]].drop_duplicates()
    result = all_dept_years.merge(result, on=[SILVER_NOM_DEP, SILVER_YEAR], how="left")
    result[GOLD_DRY_PERIODS_COUNT] = (
        result[GOLD_DRY_PERIODS_COUNT].fillna(0).astype(int)
    )
    result[GOLD_MAX_DRY_SPELL_DAYS] = (
        result[GOLD_MAX_DRY_SPELL_DAYS].fillna(0).astype(int)
    )

    return result


def extreme_temperatures_and_rain(
    climate_data: pd.DataFrame,
    temp_col: str = SILVER_TEMP_MAX,
    precip_col: str = SILVER_PRECIP,
    freeze_threshold: float = FREEZE_THRESHOLD_KELVIN,
    heat_threshold: float = HEAT_THRESHOLD_KELVIN,
    rain_threshold: float = HEAVY_RAIN_THRESHOLD_MM,
) -> pd.DataFrame:
    """Count extreme temperature and rain days per year.

    Args:
        climate_data: Climate data.
        temp_col: Name of the temperature column.
        precip_col: Name of the precipitation column.
        freeze_threshold: Temperature below which is considered a freeze day (Kelvin).
        heat_threshold: Temperature above which is considered a heat day (Kelvin).
        rain_threshold: Precipitation above which is considered a heavy rain day.

    Returns:
        DataFrame with number of freeze, heat, and heavy rain days per department/year.
    """
    df = climate_data[
        [SILVER_NOM_DEP, SILVER_YEAR, SILVER_TIME, temp_col, precip_col]
    ].copy()
    df = df.sort_values([SILVER_NOM_DEP, SILVER_TIME])

    # Extreme temperature days
    df["is_freeze"] = df[temp_col] < freeze_threshold
    df["is_heat"] = df[temp_col] > heat_threshold
    df["is_heavy_rain"] = df[precip_col] > rain_threshold

    # Extreme days per department/year
    result = (
        df.groupby([SILVER_NOM_DEP, SILVER_YEAR])
        .agg(
            **{
                GOLD_FREEZE_DAYS_COUNT: ("is_freeze", "sum"),
                GOLD_HEAT_DAYS_COUNT: ("is_heat", "sum"),
                GOLD_HEAVY_RAIN_DAYS_COUNT: ("is_heavy_rain", "sum"),
            }
        )
        .reset_index()
    )

    return result


def precipitation_lag(
    climate_data: pd.DataFrame,
    precip_col: str = SILVER_PRECIP,
) -> pd.DataFrame:
    """Compute winter precipitation (Sep-Feb) preceding the growing season.

    This captures soil moisture reserves before the growing season starts in March.

    Args:
        climate_data: Climate data (should be filtered to a single scenario).
        precip_col: Name of the precipitation column.

    Returns:
        DataFrame with winter precipitation total per department/year.
    """
    df = climate_data[[SILVER_NOM_DEP, SILVER_YEAR, SILVER_TIME, precip_col]].copy()
    df["month"] = df[SILVER_TIME].dt.month

    # Winter months: Sep-Dec of previous year + Jan-Feb of current year
    # Assign winter precip to the year of the growing season it precedes
    df["growing_year"] = df.apply(
        lambda row: (
            row[SILVER_YEAR] + 1
            if row["month"] >= WINTER_START_MONTH
            else row[SILVER_YEAR]
        ),
        axis=1,
    )

    # Filter to winter months only (Sep, Oct, Nov, Dec, Jan, Feb)
    winter_df = df[df["month"].isin(WINTER_MONTHS)]

    # Sum winter precipitation per department/year
    result = (
        winter_df.groupby([SILVER_NOM_DEP, "growing_year"])[precip_col]
        .sum()
        .reset_index()
        .rename(
            columns={"growing_year": SILVER_YEAR, precip_col: GOLD_WINTER_PRECIP_TOTAL}
        )
    )

    return result


def seasonal_temperatures_and_rain(
    climate_data: pd.DataFrame,
    temp_col: str = SILVER_TEMP_MEAN,
    precip_col: str = SILVER_PRECIP,
) -> pd.DataFrame:
    """Compute average seasonal temperatures and precipitation per department/year.

    We consider 2 seasons:
        - Growing season: March to July (inclusive)
        - Non-growing season: August to February (inclusive)
    For each department/year, we compute average, min, max and standard deviation
    temperature for both seasons.
    For precipitation, we compute total precipitation for both seasons.

    Args:
        climate_data: Climate data.
        temp_col: Name of the temperature column.
        precip_col: Name of the precipitation column.

    Returns:
        DataFrame with seasonal temperature and precipitation features per
        department/year (one row per department/year, wide format).
    """
    df = climate_data[
        [SILVER_NOM_DEP, SILVER_YEAR, SILVER_TIME, temp_col, precip_col]
    ].copy()
    df = df.sort_values([SILVER_NOM_DEP, SILVER_TIME])

    # Define seasons
    df["month"] = df[SILVER_TIME].dt.month
    df["season"] = df["month"].apply(
        lambda x: (
            "growing"
            if GROWING_SEASON_START_MONTH <= x <= GROWING_SEASON_END_MONTH
            else "non_growing"
        )
    )

    # Seasonal temperature features
    temp_features = (
        df.groupby([SILVER_NOM_DEP, SILVER_YEAR, "season"])[temp_col]
        .agg(["mean", "min", "max", "std"])
        .reset_index()
    )
    temp_features.columns = [
        SILVER_NOM_DEP,
        SILVER_YEAR,
        "season",
        "temp_mean",
        "temp_min",
        "temp_max",
        "temp_std",
    ]

    # Seasonal precipitation features
    precip_features = (
        df.groupby([SILVER_NOM_DEP, SILVER_YEAR, "season"])[precip_col]
        .sum()
        .reset_index()
    )
    precip_features.columns = [SILVER_NOM_DEP, SILVER_YEAR, "season", "total_precip"]

    # Merge temperature and precipitation features
    result = pd.merge(
        temp_features, precip_features, on=[SILVER_NOM_DEP, SILVER_YEAR, "season"]
    )

    # Pivot to wide format (one row per department/year)
    result = result.pivot_table(
        index=[SILVER_NOM_DEP, SILVER_YEAR],
        columns="season",
        values=["temp_mean", "temp_min", "temp_max", "temp_std", "total_precip"],
    ).reset_index()

    # Flatten column names and rename to gold constants
    result.columns = [
        f"{col[0]}_{col[1]}" if col[1] else col[0] for col in result.columns
    ]
    result = result.rename(
        columns={
            "temp_mean_growing": GOLD_TEMP_MEAN_GROWING,
            "temp_mean_non_growing": GOLD_TEMP_MEAN_NON_GROWING,
            "temp_min_growing": GOLD_TEMP_MIN_GROWING,
            "temp_min_non_growing": GOLD_TEMP_MIN_NON_GROWING,
            "temp_max_growing": GOLD_TEMP_MAX_GROWING,
            "temp_max_non_growing": GOLD_TEMP_MAX_NON_GROWING,
            "temp_std_growing": GOLD_TEMP_STD_GROWING,
            "temp_std_non_growing": GOLD_TEMP_STD_NON_GROWING,
            "total_precip_growing": GOLD_TOTAL_PRECIP_GROWING,
            "total_precip_non_growing": GOLD_TOTAL_PRECIP_NON_GROWING,
        }
    )

    return result


def create_gold_datasets(
    df_yield: pd.DataFrame,
    climate_full: pd.DataFrame,
    validation_threshold: int = VALIDATION_THRESHOLD_YEAR,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Create gold datasets by merging yield and climate data, and adding features.

    This function creates:
        - A training dataset.
        - A validation dataset (after the validation_threshold year).
        - One dataset per scenario for prediction.


    Args:
        df_yield: Cleaned yield data.
        climate_full: Full climate data with new features.
        validation_threshold: Year threshold to split training vs validation data.

    Returns:
        Tuple of (training_data, validation_data, scenario_data) DataFrames ready for
        modeling.
    """
    # Separate climate data into historical (for training) and
    # future scenarios (for prediction)
    historical_climate = climate_full[
        climate_full[SILVER_SCENARIO] == SCENARIO_HISTORICAL
    ]
    future_climate = climate_full[climate_full[SILVER_SCENARIO] != SCENARIO_HISTORICAL]

    # Merge yield with historical climate for training/validation datasets
    train_val_data = pd.merge(
        df_yield,
        historical_climate,
        left_on=[SILVER_NOM_DEP, SILVER_YEAR],
        right_on=[SILVER_NOM_DEP, SILVER_YEAR],
        how="inner",
    )

    # Split into training and validation datasets
    training_data = train_val_data[train_val_data[SILVER_YEAR] <= validation_threshold]
    validation_data = train_val_data[train_val_data[SILVER_YEAR] > validation_threshold]

    return training_data, validation_data, future_climate


def silver_to_gold():
    """Pipeline to transform silver data to gold data.

    This function uses the previously defined functions to read silver data,
    process it, and write the gold data.

    To add a step to this pipeline, define the function above and call it here.
    """
    # The structure is always the same:

    # 1. Read data from silver:
    # logger.info(f"Reading XXXX data at {PATH_TO_DATA}")
    # data = pd.read_csv(PATH_TO_DATA)

    # 2. Process data
    # logger.info("Processing XXXX data")
    # processed_data = process_xxxx(data) -> Use function here

    # 3. Write data to gold
    # logger.info(f"Writing processed data to {PATH_TO_GOLD}")
    # processed_data.to_parquet(PATH_TO_GOLD)

    # 1. Read silver data
    logger.info(f"Reading yield silver data at {SILVER_YIELD_PATH}")
    df_yield = pd.read_parquet(SILVER_YIELD_PATH)

    logger.info(f"Reading climate silver data at {SILVER_CLIMATE_PATH}")
    df_climate = pd.read_parquet(SILVER_CLIMATE_PATH)

    # 2. Process data - compute features separately for each scenario
    logger.info("Processing climate data")

    scenario_features = []
    for scenario in df_climate[SILVER_SCENARIO].unique():
        logger.info(f"Computing features for scenario: {scenario}")
        scenario_data = df_climate[df_climate[SILVER_SCENARIO] == scenario]

        # Compute features for this scenario
        dry_periods_df = dry_periods(scenario_data, precip_col=SILVER_PRECIP)
        extreme_events_df = extreme_temperatures_and_rain(
            scenario_data,
            temp_col=SILVER_TEMP_MAX,
            precip_col=SILVER_PRECIP,
        )
        precip_lag_df = precipitation_lag(scenario_data, precip_col=SILVER_PRECIP)
        seasonal_features_df = seasonal_temperatures_and_rain(
            scenario_data, temp_col=SILVER_TEMP_MEAN, precip_col=SILVER_PRECIP
        )

        # Merge all features for this scenario
        scenario_clean = scenario_data[[SILVER_NOM_DEP, SILVER_YEAR]].drop_duplicates()
        scenario_clean[SILVER_SCENARIO] = scenario
        for df in [dry_periods_df, extreme_events_df, seasonal_features_df]:
            scenario_clean = scenario_clean.merge(
                df, on=[SILVER_NOM_DEP, SILVER_YEAR], how="left"
            )

        scenario_clean = scenario_clean.merge(
            precip_lag_df, on=[SILVER_NOM_DEP, SILVER_YEAR], how="left"
        )

        scenario_features.append(scenario_clean)

    # Concatenate all scenarios
    climate_clean = pd.concat(scenario_features, ignore_index=True)

    # Create the three gold datasets (training, validation, scenario)
    # by merging yield and climate data
    training_data, validation_data, scenario_data = create_gold_datasets(
        df_yield=df_yield, climate_full=climate_clean
    )

    # 3. Write gold data
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    logger.info(f"Writing processed climate data to {GOLD_CLIMATE_PATH}")
    climate_clean.to_parquet(GOLD_CLIMATE_PATH, index=False)

    logger.info(f"Writing training data to {GOLD_TRAINING_PATH}")
    training_data.to_parquet(GOLD_TRAINING_PATH, index=False)

    logger.info(f"Writing validation data to {GOLD_VALIDATION_PATH}")
    validation_data.to_parquet(GOLD_VALIDATION_PATH, index=False)

    logger.info(f"Writing scenario data to {GOLD_SCENARIO_PATH}")
    scenario_data.to_parquet(GOLD_SCENARIO_PATH, index=False)


if __name__ == "__main__":
    # Execute the pipeline --> Called using DVC
    logger.info("Starting Silver to Gold pipeline")
    silver_to_gold()
    logger.info("Finished Silver to Gold pipeline")
