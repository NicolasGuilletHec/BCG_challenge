"""Column names for silver (cleaned, pivoted) data."""

# ---------------------------------------------------------------------------
# Common identifiers
# ---------------------------------------------------------------------------
SILVER_NOM_DEP = "nom_dep"
SILVER_YEAR = "year"

# ---------------------------------------------------------------------------
# Yield data
# ---------------------------------------------------------------------------
SILVER_YIELD = "yield"
SILVER_AREA = "area"
SILVER_PRODUCTION = "production"

# ---------------------------------------------------------------------------
# Climate data (pivoted: one column per metric)
# ---------------------------------------------------------------------------
SILVER_SCENARIO = "scenario"
SILVER_CODE_DEP = "code_dep"
SILVER_TIME = "time"

# Metric columns (after pivot)
SILVER_TEMP_MEAN = "temp_mean"
SILVER_TEMP_MAX = "temp_max"
SILVER_PRECIP = "precip"
