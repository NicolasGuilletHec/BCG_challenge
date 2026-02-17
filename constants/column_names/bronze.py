"""Column names for bronze (raw) data."""

# ---------------------------------------------------------------------------
# Yield data (barley_yield_from_1982.csv)
# ---------------------------------------------------------------------------
BRONZE_YIELD_DEPARTMENT = "department"
BRONZE_YIELD_YEAR = "year"
BRONZE_YIELD_YIELD = "yield"
BRONZE_YIELD_AREA = "area"
BRONZE_YIELD_PRODUCTION = "production"
BRONZE_YIELD_UNNAMED_0 = "unnamed_0"  # Artifact from CSV index

# ---------------------------------------------------------------------------
# Climate data (climate_data_from_1982.parquet)
# ---------------------------------------------------------------------------
BRONZE_CLIMATE_SCENARIO = "scenario"
BRONZE_CLIMATE_NOM_DEP = "nom_dep"
BRONZE_CLIMATE_CODE_DEP = "code_dep"
BRONZE_CLIMATE_TIME = "time"
BRONZE_CLIMATE_YEAR = "year"
BRONZE_CLIMATE_METRIC = "metric"
BRONZE_CLIMATE_VALUE = "value"

# Metric values in bronze
BRONZE_METRIC_TEMP_MEAN = "near_surface_air_temperature"
BRONZE_METRIC_TEMP_MAX = "daily_maximum_near_surface_air_temperature"
BRONZE_METRIC_PRECIP = "precipitation"

# Scenario values
SCENARIO_HISTORICAL = "historical"
SCENARIO_SSP1_2_6 = "ssp1_2_6"
SCENARIO_SSP2_4_5 = "ssp2_4_5"
SCENARIO_SSP5_8_5 = "ssp5_8_5"
