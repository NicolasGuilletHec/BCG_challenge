"""Silver to Gold Pipeline."""

import pandas as pd

from src.utils.logger import logger


def dry_periods(
    climate_data: pd.DataFrame,
    precip_col: str = "precipitation",
    threshold: int = 7,
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
    df = climate_data[["nom_dep", "year", "time", precip_col]].copy()
    df = df.sort_values(["nom_dep", "time"])

    # Dry day
    df["is_dry"] = df[precip_col] < 0.1

    # Consecutive dry day groups
    df["dry_group"] = (df["is_dry"] != df.groupby("nom_dep")["is_dry"].shift()).cumsum()

    # Count length of each dry period
    dry_runs = (
        df[df["is_dry"]]
        .groupby(["nom_dep", "year", "dry_group"])
        .size()
        .reset_index(name="run_length")
    )

    # Aggregate per department/year
    result = (
        dry_runs.groupby(["nom_dep", "year"])
        .agg(
            dry_periods_count=("run_length", lambda x: (x >= threshold).sum()),
            max_dry_spell_days=("run_length", "max"),
        )
        .reset_index()
    )

    # Years with no dry runs
    all_dept_years = climate_data[["nom_dep", "year"]].drop_duplicates()
    result = all_dept_years.merge(result, on=["nom_dep", "year"], how="left")
    result["dry_periods_count"] = result["dry_periods_count"].fillna(0).astype(int)
    result["max_dry_spell_days"] = result["max_dry_spell_days"].fillna(0).astype(int)

    return result


def extreme_temperatures_and_rain(
    climate_data: pd.DataFrame,
    temp_col: str = "temperature",
    precip_col: str = "precipitation",
    freeze_threshold: float = 0.0,
    heat_threshold: float = 30.0,
    rain_threshold: float = 20.0,
) -> pd.DataFrame:
    """Count extreme temperature and rain days per year.

    Args:
        climate_data: Climate data.
        temp_col: Name of the temperature column.
        precip_col: Name of the precipitation column.
        freeze_threshold: Temperature below which is considered a freeze day.
        heat_threshold: Temperature above which is considered a heat day.
        rain_threshold: Precipitation above which is considered a heavy rain day.

    Returns:
        DataFrame with number of freeze, heat, and heavy rain days per department/year.
    """
    df = climate_data[["nom_dep", "year", "time", temp_col, precip_col]].copy()
    df = df.sort_values(["nom_dep", "time"])

    # Extreme temperature days
    df["is_freeze"] = df[temp_col] < freeze_threshold
    df["is_heat"] = df[temp_col] > heat_threshold
    df["is_heavy_rain"] = df[precip_col] > rain_threshold

    # Extreme days per department/year
    result = (
        df.groupby(["nom_dep", "year"])
        .agg(
            freeze_days_count=("is_freeze", "sum"),
            heat_days_count=("is_heat", "sum"),
            heavy_rain_days_count=("is_heavy_rain", "sum"),
        )
        .reset_index()
    )

    return result


def precipitation_lag(
    climate_data: pd.DataFrame,
    precip_col: str = "precipitation",
) -> pd.DataFrame:
    """Compute winter precipitation (Sep-Feb) preceding the growing season.

    This captures soil moisture reserves before the growing season starts in March.

    Args:
        climate_data: Climate data.
        precip_col: Name of the precipitation column.

    Returns:
        DataFrame with winter precipitation total per department/year.
    """
    df = climate_data[["nom_dep", "year", "time", precip_col]].copy()
    df["month"] = df["time"].dt.month

    # Winter months: Sep-Dec of previous year + Jan-Feb of current year
    # Assign winter precip to the year of the growing season it precedes
    df["growing_year"] = df.apply(
        lambda row: row["year"] + 1 if row["month"] >= 9 else row["year"], axis=1
    )

    # Filter to winter months only (Sep, Oct, Nov, Dec, Jan, Feb)
    winter_df = df[df["month"].isin([9, 10, 11, 12, 1, 2])]

    result = (
        winter_df.groupby(["nom_dep", "growing_year"])[precip_col]
        .sum()
        .reset_index()
        .rename(columns={"growing_year": "year", precip_col: "winter_precip_total"})
    )

    return result


def seasonal_temperatures_and_rain(
    climate_data: pd.DataFrame,
    temp_col: str = "temperature",
    precip_col: str = "precipitation",
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
    df = climate_data[["nom_dep", "year", "time", temp_col, precip_col]].copy()
    df = df.sort_values(["nom_dep", "time"])

    # Define seasons
    df["month"] = df["time"].dt.month
    df["season"] = df["month"].apply(
        lambda x: "growing" if 3 <= x <= 7 else "non_growing"
    )

    # Seasonal temperature features
    temp_features = (
        df.groupby(["nom_dep", "year", "season"])[temp_col]
        .agg(["mean", "min", "max", "std"])
        .reset_index()
    )
    temp_features.columns = [
        "nom_dep",
        "year",
        "season",
        "temp_mean",
        "temp_min",
        "temp_max",
        "temp_std",
    ]

    # Seasonal precipitation features
    precip_features = (
        df.groupby(["nom_dep", "year", "season"])[precip_col].sum().reset_index()
    )
    precip_features.columns = ["nom_dep", "year", "season", "total_precip"]

    # Merge temperature and precipitation features
    result = pd.merge(temp_features, precip_features, on=["nom_dep", "year", "season"])

    # Pivot to wide format (one row per department/year)
    result = result.pivot_table(
        index=["nom_dep", "year"],
        columns="season",
        values=["temp_mean", "temp_min", "temp_max", "temp_std", "total_precip"],
    ).reset_index()

    # Flatten column names
    result.columns = [
        f"{col[0]}_{col[1]}" if col[1] else col[0] for col in result.columns
    ]

    return result


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


if __name__ == "__main__":
    # Execute the pipeline --> Called using DVC
    logger.info("Starting Silver to Gold pipeline")
    silver_to_gold()
    logger.info("Finished Silver to Gold pipeline")
