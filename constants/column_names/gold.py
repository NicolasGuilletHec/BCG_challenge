"""Column names for gold (feature-engineered) data."""

# ---------------------------------------------------------------------------
# Identifiers
# ---------------------------------------------------------------------------
GOLD_NOM_DEP = "nom_dep"
GOLD_YEAR = "year"

# ---------------------------------------------------------------------------
# Target variable
# ---------------------------------------------------------------------------
GOLD_YIELD = "yield"

# ---------------------------------------------------------------------------
# Dry period features
# ---------------------------------------------------------------------------
GOLD_DRY_PERIODS_COUNT = "dry_periods_count"
GOLD_MAX_DRY_SPELL_DAYS = "max_dry_spell_days"

# ---------------------------------------------------------------------------
# Extreme weather features
# ---------------------------------------------------------------------------
GOLD_FREEZE_DAYS_COUNT = "freeze_days_count"
GOLD_HEAT_DAYS_COUNT = "heat_days_count"
GOLD_HEAVY_RAIN_DAYS_COUNT = "heavy_rain_days_count"

# ---------------------------------------------------------------------------
# Precipitation features
# ---------------------------------------------------------------------------
GOLD_WINTER_PRECIP_TOTAL = "winter_precip_total"

# ---------------------------------------------------------------------------
# Seasonal temperature features
# ---------------------------------------------------------------------------
GOLD_TEMP_MEAN_GROWING = "temp_mean_growing"
GOLD_TEMP_MEAN_NON_GROWING = "temp_mean_non_growing"
GOLD_TEMP_MIN_GROWING = "temp_min_growing"
GOLD_TEMP_MIN_NON_GROWING = "temp_min_non_growing"
GOLD_TEMP_MAX_GROWING = "temp_max_growing"
GOLD_TEMP_MAX_NON_GROWING = "temp_max_non_growing"
GOLD_TEMP_STD_GROWING = "temp_std_growing"
GOLD_TEMP_STD_NON_GROWING = "temp_std_non_growing"

# ---------------------------------------------------------------------------
# Seasonal precipitation features
# ---------------------------------------------------------------------------
GOLD_TOTAL_PRECIP_GROWING = "total_precip_growing"
GOLD_TOTAL_PRECIP_NON_GROWING = "total_precip_non_growing"
