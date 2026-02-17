# Feature Documentation

This document describes the features engineered in the **silver-to-gold** pipeline stage (`src/pipelines/data/silver_to_gold.py`). All features are computed per department and per year, separately for each climate scenario.

## Target Variable

| Column | Description |
|--------|-------------|
| `yield` | Barley yield in tonnes per hectare (from yield data) |

## Identifiers

| Column | Description |
|--------|-------------|
| `nom_dep` | French department name |
| `year` | Year of observation |
| `scenario` | Climate scenario (`historical`, `ssp1_2_6`, `ssp2_4_5`, `ssp5_8_5`) |

## Seasonal Temperature Features

Temperatures are aggregated over two seasons:

- **Growing season**: March to July (months 3-7)
- **Non-growing season**: August to February (months 8-12, 1-2)

Source column: daily mean near-surface air temperature (Kelvin).

| Column | Description |
|--------|-------------|
| `temp_mean_growing` | Mean daily temperature during the growing season |
| `temp_mean_non_growing` | Mean daily temperature during the non-growing season |
| `temp_min_growing` | Minimum daily temperature during the growing season |
| `temp_min_non_growing` | Minimum daily temperature during the non-growing season |
| `temp_max_growing` | Maximum daily temperature during the growing season |
| `temp_max_non_growing` | Maximum daily temperature during the non-growing season |
| `temp_std_growing` | Standard deviation of daily temperature during the growing season |
| `temp_std_non_growing` | Standard deviation of daily temperature during the non-growing season |

## Seasonal Precipitation Features

Total precipitation summed over the same two seasons defined above.

Source column: daily precipitation (mm).

| Column | Description |
|--------|-------------|
| `total_precip_growing` | Total precipitation during the growing season (mm) |
| `total_precip_non_growing` | Total precipitation during the non-growing season (mm) |

## Winter Precipitation Lag

Captures soil moisture reserves accumulated before the growing season starts in March.

Winter is defined as September-February, and the precipitation is assigned to the year of the growing season it precedes (e.g., Sep 2005-Feb 2006 precipitation is assigned to year 2006).

| Column | Description |
|--------|-------------|
| `winter_precip_total` | Total precipitation during the preceding winter (Sep-Feb), in mm |

## Dry Period Features

A day is considered **dry** if precipitation is below 0.1 mm.

| Column | Description |
|--------|-------------|
| `dry_periods_count` | Number of dry spells lasting at least 7 consecutive days |
| `max_dry_spell_days` | Length (in days) of the longest dry spell in the year |

## Extreme Weather Features

Computed from the daily maximum near-surface air temperature and daily precipitation.

| Column | Threshold | Description |
|--------|-----------|-------------|
| `freeze_days_count` | < 273.15 K (0 °C) | Number of days with max temperature below freezing |
| `heat_days_count` | > 303.15 K (30 °C) | Number of days with max temperature above 30 °C |
| `heavy_rain_days_count` | > 20 mm | Number of days with precipitation exceeding 20 mm |

## Gold Datasets

The pipeline produces four output files:

| File | Description |
|------|-------------|
| `climate_features.parquet` | All climate features for all scenarios |
| `training.parquet` | Historical climate features merged with yield data (years <= 2013) |
| `validation.parquet` | Historical climate features merged with yield data (years > 2013) |
| `scenarios.parquet` | Future climate features for prediction (non-historical scenarios) |
