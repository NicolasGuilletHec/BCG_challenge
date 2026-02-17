"""Utilities for loading model inputs."""

import pandas as pd
from sklearn.preprocessing import LabelEncoder
import yaml

from constants.column_names.gold import GOLD_NOM_DEP, GOLD_YIELD
from constants.paths import GOLD_TRAINING_PATH, GOLD_VALIDATION_PATH, PARAMS_FILE

TARGET_COLUMN = GOLD_YIELD


def load_params() -> dict:
    """Load parameters from /params.yaml.

    Returns:
        dict: Parameters dictionary.
    """
    with open(PARAMS_FILE) as f:
        return yaml.safe_load(f)


def load_training_data():
    """Load training and testing data for model training.

    Returns:
        tuple: A tuple containing:
            - X_train (pd.DataFrame): Training feature matrix.
            - y_train (pd.Series): Training target vector.
            - X_test (pd.DataFrame): Testing feature matrix.
            - y_test (pd.Series): Testing target vector.
            - dep_encoder (LabelEncoder): Fitted encoder for nom_dep.
    """
    # Load gold datasets
    train_data = pd.read_parquet(GOLD_TRAINING_PATH)
    test_data = pd.read_parquet(GOLD_VALIDATION_PATH)

    # Fit label encoder on all department names
    all_deps = pd.concat([train_data[GOLD_NOM_DEP], test_data[GOLD_NOM_DEP]])
    dep_encoder = LabelEncoder()
    dep_encoder.fit(all_deps)

    # Encode nom_dep
    train_data[GOLD_NOM_DEP] = dep_encoder.transform(train_data[GOLD_NOM_DEP])
    test_data[GOLD_NOM_DEP] = dep_encoder.transform(test_data[GOLD_NOM_DEP])

    # Split into features and target
    X_train = train_data.drop(columns=[TARGET_COLUMN])
    y_train = train_data[TARGET_COLUMN]

    X_test = test_data.drop(columns=[TARGET_COLUMN])
    y_test = test_data[TARGET_COLUMN]

    return X_train, y_train, X_test, y_test, dep_encoder
