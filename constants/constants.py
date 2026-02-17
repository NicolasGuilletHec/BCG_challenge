"""Numeric constants and thresholds used across the project."""

# ---------------------------------------------------------------------------
# Dry period detection
# ---------------------------------------------------------------------------
DRY_DAY_PRECIP_THRESHOLD_MM = 0.1  # Precipitation below this is considered dry (mm)
MIN_DRY_SPELL_DAYS = 7  # Minimum consecutive dry days to count as a dry period

# ---------------------------------------------------------------------------
# Temperature thresholds (Kelvin)
# ---------------------------------------------------------------------------
FREEZE_THRESHOLD_KELVIN = 273.15  # 0°C - days below this are freeze days
HEAT_THRESHOLD_KELVIN = 303.15  # 30°C - days above this are heat days

# ---------------------------------------------------------------------------
# Precipitation thresholds
# ---------------------------------------------------------------------------
HEAVY_RAIN_THRESHOLD_MM = 20.0  # Precipitation above this is heavy rain (mm)

# ---------------------------------------------------------------------------
# Season definitions (month numbers)
# ---------------------------------------------------------------------------
GROWING_SEASON_START_MONTH = 3  # March
GROWING_SEASON_END_MONTH = 7  # July
WINTER_START_MONTH = 9  # September (start of winter for precipitation lag)
WINTER_MONTHS = [9, 10, 11, 12, 1, 2]  # Sep-Feb for winter precipitation

# ---------------------------------------------------------------------------
# Train/validation split
# ---------------------------------------------------------------------------
VALIDATION_THRESHOLD_YEAR = 2013  # Years <= this go to training, > go to validation
