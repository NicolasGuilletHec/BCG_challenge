"""Path configurations for the project.

This module centralizes all path definitions to ensure consistency across the project.
"""

from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Bronze data paths
BARLEY_PATH = BRONZE_DIR / "barley_yield_from_1982.csv"
CLIMATE_PATH = BRONZE_DIR / "climate_data_from_1982.parquet"

# Silver data paths
SILVER_YIELD_PATH = SILVER_DIR / "yield_clean.parquet"
SILVER_CLIMATE_PATH = SILVER_DIR / "climate_clean.parquet"

# Gold data paths
GOLD_CLIMATE_PATH = GOLD_DIR / "climate_features.parquet"
GOLD_TRAINING_PATH = GOLD_DIR / "training.parquet"
GOLD_VALIDATION_PATH = GOLD_DIR / "validation.parquet"
GOLD_SCENARIO_PATH = GOLD_DIR / "scenarios.parquet"

# Model directory
MODEL_DIR = PROJECT_ROOT / "models"

# Streamlit app paths
APP_DIR = PROJECT_ROOT / "my_app"
APP_DATA_DIR = APP_DIR / "data"
APP_GEO_DIR = APP_DIR / "geo"

APP_HIST_PATH = APP_DATA_DIR / "barley_yield_from_1982.csv"
APP_PRED_126_PATH = APP_DATA_DIR / "scenario_126_predictions.csv"
APP_PRED_245_PATH = APP_DATA_DIR / "scenario_245_predictions.csv"
APP_PRED_585_PATH = APP_DATA_DIR / "scenario_585_predictions.csv"
APP_GEOJSON_PATH = APP_GEO_DIR / "departements.geojson"

# Configurations directory
CONFIG_DIR = PROJECT_ROOT / "config"

# Parameters file path
PARAMS_FILE = PROJECT_ROOT / "params.yaml"
