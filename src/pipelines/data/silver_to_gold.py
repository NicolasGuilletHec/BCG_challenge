"""Silver to Gold Pipeline."""

from constants.column_names import (
    ACTUAL_TAXI_OUT_SEC,
    AIRCRAFT_MODEL,
    AIRCRAFT_SPAN,
    AIRPORT_ARRIVAL_DEPARTURE,
    AOBT,
    APPARENT_TEMPERATURE,
    ATOT,
    CLOUD_COVER,
    DELAY_SECONDS,
    DEW_POINT,
    DISTANCE,
    DISTANCE_PROXY_M,
    FLIGHT_DATETIME,
    FLIGHT_NUMBER,
    HELICOPTER,
    HOUR,
    ICON,
    LAT_RUNWAY,
    LAT_STAND,
    LNG_RUNWAY,
    LNG_STAND,
    LOG_DISTANCE,
    LOG_DISTANCE_M,
    MONTH,
    MOVEMENT_TYPE,
    PLANES_10MIN,
    PLANES_30MIN,
    PRECIP_INTENSITY,
    PRECIP_PROBABILITY,
    PRECIP_TYPE,
    PRESSURE,
    PRIVATE_FLIGHT,
    Q_DEP_ARR,
    RUNWAY,
    STAND,
    SUMMARY,
    TIME_HOURLY,
    UV_INDEX,
    WEEKDAY,
    WIND_BEARING,
    WIND_GUST,
    YEAR,
)
import numpy as np
import pandas as pd

from constants.paths import (
    GOLD_DIR,
    GOLD_TEST_AIRPORT_DATA_CLEAN_PATH,
    GOLD_TEST_AIRPORT_DATA_PATH,
    GOLD_TRAINING_AIRPORT_DATA_CLEAN_PATH,
    GOLD_TRAINING_AIRPORT_DATA_PATH,
    SILVER_GEOGRAPHIC_DATA_PATH,
    SILVER_TEST_AIRPORT_DATA_PATH,
    SILVER_TRAINING_AIRPORT_DATA_PATH,
    SILVER_WEATHER_DATA_PATH,
)
from src.utils.logger import logger

# Pipeline Steps
# Define your functions here and call them in the silver_to_gold function.


def haversine_distance(lat1, lng1, lat2, lng2):
    """Calculate the Haversine distance between two points in meters."""
    R = 6371000  # Earth's radius in meters

    lat1_rad = np.radians(lat1)
    lat2_rad = np.radians(lat2)
    delta_lat = np.radians(lat2 - lat1)
    delta_lng = np.radians(lng2 - lng1)

    a = (
        np.sin(delta_lat / 2) ** 2
        + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(delta_lng / 2) ** 2
    )
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    return R * c


def create_runway_stand_distances(geographic_df: pd.DataFrame) -> pd.DataFrame:
    """Create a new dataset with runway-stand distances.

    Parameters
    ----------
    geographic_df: pd.DataFrame
        DataFrame containing geographic data.

    Returns:
    -------
    pd.DataFrame
        DataFrame containing runway, stand, and distance between them.
    """
    # Create dataframe with distinct runways and their coordinates
    runways_df = geographic_df[[RUNWAY, LAT_RUNWAY, LNG_RUNWAY]].drop_duplicates()
    runways_df = runways_df.rename(columns={LAT_RUNWAY: "lat", LNG_RUNWAY: "lng"})

    # Create dataframe with distinct stands and their coordinates
    stands_df = geographic_df[[STAND, LAT_STAND, LNG_STAND]].drop_duplicates()
    stands_df = stands_df.rename(columns={LAT_STAND: "lat", LNG_STAND: "lng"})

    # Cycle through all combinations to calculate distances
    distance_records = []
    for _, runway in runways_df.iterrows():
        for _, stand in stands_df.iterrows():
            distance = haversine_distance(
                runway["lat"], runway["lng"], stand["lat"], stand["lng"]
            )
            distance_records.append(
                {
                    RUNWAY: runway[RUNWAY],
                    STAND: stand[STAND],
                    DISTANCE: distance,
                    LOG_DISTANCE: np.log(distance),
                }
            )

    runway_stand_distances_df = pd.DataFrame(distance_records)

    return runway_stand_distances_df


def add_distance_columns(
    airport_data: pd.DataFrame, runway_stand_distances: pd.DataFrame
) -> pd.DataFrame:
    """Add distance and log_distance columns to airport data.

    Parameters
    ----------
    airport_data: pd.DataFrame
        DataFrame containing airport data with 'runway' and 'stand' columns.
    runway_stand_distances: pd.DataFrame
        DataFrame containing runway-stand distances.

    Returns:
    -------
    pd.DataFrame
        Airport data with distance and log_distance columns added.
    """
    return airport_data.merge(
        runway_stand_distances[[RUNWAY, STAND, DISTANCE, LOG_DISTANCE]],
        on=[RUNWAY, STAND],
        how="left",
    )


def clean_weather_data(weather_data: pd.DataFrame) -> pd.DataFrame:
    """Clean weather data by dropping unnecessary columns."""
    columns_to_drop = [
        # FLIGHT_NUMBER - kept for private_flight column creation
        AIRCRAFT_SPAN,
        AIRPORT_ARRIVAL_DEPARTURE,
        MOVEMENT_TYPE,
        DISTANCE_PROXY_M,
        LOG_DISTANCE_M,
        YEAR,
        MONTH,
        WEEKDAY,
        HOUR,
        Q_DEP_ARR,
        TIME_HOURLY,
        APPARENT_TEMPERATURE,
        DEW_POINT,
        WIND_GUST,
    ]
    # Only drop columns that exist in the dataframe
    columns_to_drop = [col for col in columns_to_drop if col in weather_data.columns]
    return weather_data.drop(columns=columns_to_drop)


def merge_airport_weather_data(
    airport_data: pd.DataFrame,
    weather_data: pd.DataFrame,
) -> pd.DataFrame:
    """Merge airport data with weather data.

    Parameters
    ----------
    airport_data: pd.DataFrame
        DataFrame containing airport data.
    weather_data: pd.DataFrame
        DataFrame containing weather data.

    Returns:
    -------
    pd.DataFrame
        Merged DataFrame with airport and weather data.
    """
    merge_columns = [FLIGHT_DATETIME, AIRCRAFT_MODEL, AOBT, ATOT]

    # Ensure merge columns have consistent types (convert to string)
    airport_data = airport_data.copy()
    weather_data = weather_data.copy()
    for col in merge_columns:
        if col in airport_data.columns:
            airport_data[col] = airport_data[col].astype(str)
        if col in weather_data.columns:
            weather_data[col] = weather_data[col].astype(str)

    return airport_data.merge(weather_data, on=merge_columns, how="left")


def add_private_flight_column(df: pd.DataFrame) -> pd.DataFrame:
    """Add private_flight column based on airline code, then drop Flight Number.

    Private flight codes:
    - NJE: NetJets Europe
    - VJT: VistaJet
    - PVT: Private flight designation
    - SIG: Signature Aviation (business aviation)

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame containing Flight Number column.

    Returns:
    -------
    pd.DataFrame
        DataFrame with private_flight column added and Flight Number dropped.
    """
    df = df.copy()

    private_codes = ["NJE", "VJT", "PVT", "SIG"]

    # Extract airline prefix (2-3 letter code at start of Flight Number)
    airline_prefix = df[FLIGHT_NUMBER].str.extract(r"^([A-Z]{2,3})", expand=False)

    # Create private_flight column: 1 if private, 0 otherwise
    df[PRIVATE_FLIGHT] = airline_prefix.isin(private_codes).astype(int)

    # Drop Flight Number column
    df = df.drop(columns=[FLIGHT_NUMBER])

    return df


def add_helicopter_column(df: pd.DataFrame) -> pd.DataFrame:
    """Add helicopter column based on aircraft model.

    Verified helicopter models (from ICAO aircraft database):
    - A109: AgustaWestland A109
    - AW139: Leonardo AW139
    - S-76: Sikorsky S-76
    - 407: Bell 407
    - 214 ST: Bell 214 ST
    - EC 120: Eurocopter EC120
    - EC 135T: Eurocopter EC135
    - EC 155B: Eurocopter EC155
    - SA 365 DAUPHIN 2: Eurocopter Dauphin
    - AEROSPATIALE AS355 ECUREUIL 2 (TWIN SQ.): Twin Squirrel
    - AS 350 ECUREUIL (SQUIRREL): Single Squirrel
    - Helicopter: Generic label

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame containing Aircraft Model column.

    Returns:
    -------
    pd.DataFrame
        DataFrame with helicopter column added.
    """
    df = df.copy()

    helicopter_models = [
        "A109",
        "AW139",
        "S-76",
        "407",
        "214 ST",
        "EC 120",
        "EC 135T",
        "EC 155B",
        "SA 365 DAUPHIN 2",
        "AEROSPATIALE AS355 ECUREUIL 2 (TWIN SQ.)",
        "AS 350 ECUREUIL (SQUIRREL)",
        "Helicopter",
    ]

    # Create helicopter column: 1 if helicopter, 0 otherwise
    df[HELICOPTER] = df[AIRCRAFT_MODEL].isin(helicopter_models).astype(int)

    return df


def cyclical_encode(df: pd.DataFrame, column: str, max_value: int) -> pd.DataFrame:
    """Encode a column as cyclical features using sin/cos transformation.

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame containing the column to encode.
    column: str
        Name of the column to encode.
    max_value: int
        Maximum value for the cycle (e.g., 24 for hours, 12 for months).

    Returns:
    -------
    pd.DataFrame
        DataFrame with new sin/cos columns added.
    """
    df[f"{column}_sin"] = np.sin(2 * np.pi * df[column] / max_value)
    df[f"{column}_cos"] = np.cos(2 * np.pi * df[column] / max_value)
    return df


def handle_nan_values(
    df: pd.DataFrame,
    median_values: dict | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Handle NaN values by dropping high-NaN columns and filling others with median.

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame to process.
    median_values: dict | None
        Pre-computed median values for consistency between train/test.
        If None, medians are computed from the data.

    Returns:
    -------
    tuple[pd.DataFrame, dict]
        Processed DataFrame and median values dict.
    """
    df = df.copy()

    # Columns to drop (>10k NaN in training)
    cols_to_drop = [
        PRECIP_INTENSITY,
        PRECIP_PROBABILITY,
        PRESSURE,
        WIND_BEARING,
        CLOUD_COVER,
        UV_INDEX,
    ]

    # Drop columns that exist
    cols_to_drop = [col for col in cols_to_drop if col in df.columns]
    df = df.drop(columns=cols_to_drop)

    # Fill remaining NaN with median
    if median_values is None:
        median_values = {}

    # Columns that might have NaN and need median filling
    numeric_cols = df.select_dtypes(include=["float64", "int64"]).columns

    for col in numeric_cols:
        if df[col].isna().any():
            if col in median_values:
                # Use pre-computed median
                df[col] = df[col].fillna(median_values[col])
            else:
                # Compute and store median
                median_val = df[col].median()
                median_values[col] = median_val
                df[col] = df[col].fillna(median_val)

    return df, median_values


def compute_delay_and_drop_flight_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Compute delay, plane counts within time windows, then drop Flight Datetime."""
    df = df.copy()

    # Parse once
    flight_dt = pd.to_datetime(
        df[FLIGHT_DATETIME], format="%m/%d/%Y %H:%M", errors="coerce"
    )
    aobt_dt = pd.to_datetime(df[AOBT], format="%m/%d/%Y %H:%M", errors="coerce")
    # Delay in seconds
    df[DELAY_SECONDS] = (aobt_dt - flight_dt).dt.total_seconds()
    # Sort by parsed AOBT (faster than reparsing after sort)
    df = df.assign(_aobt_dt=aobt_dt).sort_values("_aobt_dt").reset_index(drop=True)
    # Convert to int64 seconds since epoch
    aobt_sorted = df["_aobt_dt"]
    valid = aobt_sorted.notna().to_numpy()
    ts = aobt_sorted.values.astype("datetime64[s]").astype(np.int64)
    ts_valid = ts[valid]

    def count_planes_in_window(ts_sorted: np.ndarray, minutes: int) -> np.ndarray:
        w = minutes * 60
        left = np.searchsorted(ts_sorted, ts_sorted - w, side="left")
        right = np.searchsorted(ts_sorted, ts_sorted + w, side="right")
        return right - left - 1  # exclude self

    planes_30 = np.full(len(df), np.nan)
    planes_10 = np.full(len(df), np.nan)

    planes_30[valid] = count_planes_in_window(ts_valid, 30)
    planes_10[valid] = count_planes_in_window(ts_valid, 10)

    df[PLANES_30MIN] = planes_30
    df[PLANES_10MIN] = planes_10

    # Cleanup
    df = df.drop(columns=[FLIGHT_DATETIME, "_aobt_dt"])
    return df


def encode_features(
    df: pd.DataFrame,
    datetime_columns: list[str],
    categorical_columns: list[str],
    category_mappings: dict | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Encode features with cyclical encoding for datetime and categorical encoding.

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame to encode.
    datetime_columns: list[str]
        Columns containing datetime data to cyclically encode.
    categorical_columns: list[str]
        Columns to categorically encode (label encoding).
    category_mappings: dict | None
        Pre-defined category mappings for consistent encoding between train/test.
        If None, categories are learned from the data.

    Returns:
    -------
    tuple[pd.DataFrame, dict]
        Encoded DataFrame and category mappings dict.
    """
    df = df.copy()

    if category_mappings is None:
        category_mappings = {}

    # Cyclical encoding for datetime columns
    for col in datetime_columns:
        if col in df.columns:
            # Parse datetime
            dt = pd.to_datetime(df[col], format="%m/%d/%Y %H:%M", errors="coerce")

            # Create unique column names for this datetime column
            hour_col = f"{col}_hour"
            dow_col = f"{col}_day_of_week"
            month_col = f"{col}_month"
            dom_col = f"{col}_day_of_month"

            # Extract components and encode cyclically
            df[hour_col] = dt.dt.hour
            df = cyclical_encode(df, hour_col, 24)

            df[dow_col] = dt.dt.dayofweek
            df = cyclical_encode(df, dow_col, 7)

            df[month_col] = dt.dt.month
            df = cyclical_encode(df, month_col, 12)

            df[dom_col] = dt.dt.day
            df = cyclical_encode(df, dom_col, 31)

            # Drop intermediate columns and original datetime column
            df = df.drop(columns=[hour_col, dow_col, month_col, dom_col, col])

    # One-hot encoding for categorical columns
    for col in categorical_columns:
        if col in df.columns:
            # Apply one-hot encoding
            dummies = pd.get_dummies(df[col], prefix=col, dtype=int)
            df = pd.concat([df.drop(columns=[col]), dummies], axis=1)

            if col not in category_mappings:
                # Store the dummy column names for consistency
                category_mappings[col] = list(dummies.columns)

    # Ensure consistent columns between train and test
    if category_mappings:
        all_expected_cols = []
        for _, dummy_cols in category_mappings.items():
            all_expected_cols.extend(dummy_cols)

        # Add missing columns (set to 0)
        for dummy_col in all_expected_cols:
            if dummy_col not in df.columns:
                df[dummy_col] = 0

        # Remove extra columns not in training
        extra_cols = [
            c
            for c in df.columns
            if any(
                c.startswith(f"{cat}_") and c not in category_mappings.get(cat, [])
                for cat in categorical_columns
            )
        ]
        if extra_cols:
            df = df.drop(columns=extra_cols)

    # Drop ATOT column
    if ATOT in df.columns:
        df = df.drop(columns=[ATOT])
    if AIRCRAFT_MODEL in df.columns:
        df = df.drop(columns=[AIRCRAFT_MODEL])

    # Convert stand and runway to numerical (e.g., "STAND_62" -> 62, "RUNWAY_3" -> 3)
    if STAND in df.columns:
        df[STAND] = df[STAND].str.extract(r"(\d+)").astype(int)
    if RUNWAY in df.columns:
        df[RUNWAY] = df[RUNWAY].str.extract(r"(\d+)").astype(int)

    # Store column order for consistency
    if "_column_order" not in category_mappings:
        category_mappings["_column_order"] = list(df.columns)
    else:
        # Reorder columns to match training
        expected_cols = category_mappings["_column_order"]
        current_cols = set(df.columns)

        # Add missing columns as 0
        for col in expected_cols:
            if col not in current_cols:
                df[col] = 0

        # Keep only expected columns in the right order
        df = df[expected_cols]

    return df, category_mappings


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
    # processed_data = process_xxxx(data) -> Use your function here

    # 3. Write data to gold
    # logger.info(f"Writing processed data to {PATH_TO_GOLD}")
    # processed_data.to_parquet(PATH_TO_GOLD)

    logger.info(
        f"Reading training airport data from {SILVER_TRAINING_AIRPORT_DATA_PATH}"
    )
    training_airport_data = pd.read_parquet(SILVER_TRAINING_AIRPORT_DATA_PATH)
    logger.info(f"Reading test airport data from {SILVER_TEST_AIRPORT_DATA_PATH}")
    test_airport_data = pd.read_parquet(SILVER_TEST_AIRPORT_DATA_PATH)
    logger.info(f"Reading geographic data from {SILVER_GEOGRAPHIC_DATA_PATH}")
    geographic_data = pd.read_parquet(SILVER_GEOGRAPHIC_DATA_PATH)
    logger.info(f"Reading weather data from {SILVER_WEATHER_DATA_PATH}")
    training_weather_data = pd.read_parquet(SILVER_WEATHER_DATA_PATH)

    logger.info("Creating runway-stand distances")
    runway_stand_distances = create_runway_stand_distances(geographic_data)
    logger.info("Adding distance columns to training airport data")
    training_airport_data = add_distance_columns(
        training_airport_data, runway_stand_distances
    )
    logger.info("Adding distance columns to test airport data")
    test_airport_data = add_distance_columns(test_airport_data, runway_stand_distances)
    logger.info("Cleaning weather data")
    training_weather_data = clean_weather_data(training_weather_data)
    logger.info("Merging airport and weather data")
    training_airport_data = merge_airport_weather_data(
        training_airport_data, training_weather_data
    )
    test_airport_data = merge_airport_weather_data(
        test_airport_data, training_weather_data
    )

    logger.info("Adding private_flight column")
    training_airport_data = add_private_flight_column(training_airport_data)
    test_airport_data = add_private_flight_column(test_airport_data)

    logger.info("Adding helicopter column")
    training_airport_data = add_helicopter_column(training_airport_data)
    test_airport_data = add_helicopter_column(test_airport_data)

    # Filter out rows with taxi time == 0 (data errors) or >= 7200 seconds (2 hours)
    logger.info(f"Filtering out rows with {ACTUAL_TAXI_OUT_SEC} == 0 or >= 7200")
    train_before = len(training_airport_data)
    test_before = len(test_airport_data)
    training_airport_data = training_airport_data[
        (training_airport_data[ACTUAL_TAXI_OUT_SEC] > 0)
        & (training_airport_data[ACTUAL_TAXI_OUT_SEC] < 7200)
    ].reset_index(drop=True)
    test_airport_data = test_airport_data[
        (test_airport_data[ACTUAL_TAXI_OUT_SEC] > 0)
        & (test_airport_data[ACTUAL_TAXI_OUT_SEC] < 7200)
    ].reset_index(drop=True)
    logger.info(f"Filtered training: {train_before} -> {len(training_airport_data)}")
    logger.info(f"Filtered test: {test_before} -> {len(test_airport_data)}")

    logger.info("Computing delay and dropping Flight Datetime")
    training_airport_data = compute_delay_and_drop_flight_datetime(
        training_airport_data
    )
    test_airport_data = compute_delay_and_drop_flight_datetime(test_airport_data)

    logger.info("Encoding features (training - learning categories)")
    training_airport_data, category_mappings = encode_features(
        training_airport_data,
        [AOBT],
        [SUMMARY, ICON, PRECIP_TYPE],
    )
    logger.info("Encoding features (test - using training categories)")
    test_airport_data, _ = encode_features(
        test_airport_data,
        [AOBT],
        [SUMMARY, ICON, PRECIP_TYPE],
        category_mappings=category_mappings,
    )

    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    # Save datasets WITH NaN (for gradient boosting models: XGBoost, LightGBM, CatBoost)
    logger.info(
        f"Writing training data (with NaN) to {GOLD_TRAINING_AIRPORT_DATA_PATH}"
    )
    training_airport_data.to_parquet(GOLD_TRAINING_AIRPORT_DATA_PATH, index=False)
    logger.info(f"Writing test data (with NaN) to {GOLD_TEST_AIRPORT_DATA_PATH}")
    test_airport_data.to_parquet(GOLD_TEST_AIRPORT_DATA_PATH, index=False)

    # Create clean datasets (NaN handled) for sklearn models
    logger.info("Handling NaN values for clean datasets (training - computing medians)")
    training_clean, median_values = handle_nan_values(training_airport_data.copy())
    logger.info(
        "Handling NaN values for clean datasets (test - using training medians)"
    )
    test_clean, _ = handle_nan_values(
        test_airport_data.copy(), median_values=median_values
    )

    # Save clean datasets WITHOUT NaN (for Linear Regression, Random Forest)
    logger.info(
        f"Writing training data (clean) to {GOLD_TRAINING_AIRPORT_DATA_CLEAN_PATH}"
    )
    training_clean.to_parquet(GOLD_TRAINING_AIRPORT_DATA_CLEAN_PATH, index=False)
    logger.info(f"Writing test data (clean) to {GOLD_TEST_AIRPORT_DATA_CLEAN_PATH}")
    test_clean.to_parquet(GOLD_TEST_AIRPORT_DATA_CLEAN_PATH, index=False)


if __name__ == "__main__":
    # Execute the pipeline --> Called using DVC
    logger.info("Starting Silver to Gold pipeline")
    silver_to_gold()
    logger.info("Finished Silver to Gold pipeline")
