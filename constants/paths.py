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


# Configurations directory
CONFIG_DIR = PROJECT_ROOT / "config"
