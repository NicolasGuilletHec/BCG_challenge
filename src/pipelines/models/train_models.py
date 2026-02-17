"""Pipeline to train models."""

import pandas as pd
from sklearn.preprocessing import LabelEncoder

from src.pipelines.models.xgboost_model import train_xgboost_model
from src.pipelines.utils.model_inputs_loading import (
    load_params,
    load_training_data,
)
from src.utils.logger import logger


def train_and_predict(
    model_type: str,
    experiment_name: str,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    dep_encoder: LabelEncoder,
):
    """Train a single model, and make predictions.

    Args:
        model_type (str): Type of model to train.
        experiment_name (str): Experiment prefix for MLflow run/model names.
        X_train (pd.DataFrame): Training feature matrix.
        y_train (pd.Series): Training target vector.
        X_test (pd.DataFrame): Testing feature matrix.
        y_test (pd.Series): Testing target vector.
        dep_encoder (LabelEncoder): Fitted encoder for nom_dep.
    """
    run_name = f"{experiment_name}_{model_type}_run"
    model_name = f"{experiment_name}_{model_type}_model"

    if model_type == "xgboost":
        # Train XGBoost model --> Stored and logged in MLflow
        train_xgboost_model(
            run_name=run_name,
            model_name=model_name,
            X_train=X_train,
            y_train=y_train,
            X_test=X_test,
            y_test=y_test,
            dep_encoder=dep_encoder,
        )

    else:
        raise ValueError(f"Unsupported model type: {model_type}")


def train_models(model_types: list[str], experiment_name: str):
    """Train multiple machine learning models for crop yields prediction.

    Args:
        model_types (list[str]): List of model types to train.
        experiment_name (str): Experiment prefix for MLflow run/model names.
    """
    X_train, y_train, X_test, y_test, dep_encoder = load_training_data()
    for model_type in model_types:
        logger.info(f"Training {model_type}...")
        train_and_predict(
            model_type, experiment_name, X_train, y_train, X_test, y_test, dep_encoder
        )


if __name__ == "__main__":
    # Execute the pipeline --> Called using DVC
    # Parameters are loaded from /params.yaml
    params = load_params()
    train_params = params["train_models"]

    train_models(
        model_types=train_params["model_types"],
        experiment_name=train_params["experiment_name"],
    )
