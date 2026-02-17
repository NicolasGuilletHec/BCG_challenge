"""XGBoost Model Implementation.

Defines a training and evaluation pipeline for an XGBoost model.
Logs metrics and model artifacts using MLflow.
"""

from dotenv import load_dotenv
import mlflow
from mlflow.models.signature import infer_signature
import numpy as np
import pandas as pd
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    r2_score,
    root_mean_squared_error,
)
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBRegressor

from constants.constants import RANDOM_STATE
import constants.paths as pth
from src.pipelines.utils.prediction_utils import (
    prediction_pipeline,
)


# Model Training and Logging Function
def train_xgboost_model(
    run_name: str,
    model_name: str,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    dep_encoder: LabelEncoder,
    **kwargs,
):
    """Train and log an XGBoost model using MLflow. Predict on future data.

    Trains an XGBoost model and evaluates it on a held-out test set. Logs the model and
    metrics to MLflow. Logs the predictions datasets with MLflow.

    Args:
        run_name (str): Name of the MLflow run.
        model_name (str): Name to log the model under in MLflow.
        X_train (pd.DataFrame): Training feature matrix.
        y_train (pd.Series): Training target vector.
        X_test (pd.DataFrame): Testing feature matrix.
        y_test (pd.Series): Testing target vector.
        dep_encoder (LabelEncoder): Fitted encoder for nom_dep.
        **kwargs: Additional keyword arguments for XGBRegressor.
    """
    # Load environment variables for MLflow configuration
    load_dotenv()

    with mlflow.start_run(run_name=run_name):
        # ---- Training and evaluating the XGBoost model ---- #
        model = XGBRegressor(
            random_state=RANDOM_STATE,
            device="cuda",  # Use GPU
            verbosity=0,
            **kwargs,
        )
        model.fit(X_train, y_train)

        # Evaluate on held-out test set
        y_pred = model.predict(X_test)
        rmse = root_mean_squared_error(y_test, y_pred)
        mape = mean_absolute_percentage_error(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        std = np.std(y_test - y_pred)

        # Log parameters
        mlflow.log_params(model.get_params())

        # Log metrics
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mape", mape)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("std", std)

        # Log model
        signature = infer_signature(X_train, model.predict(X_train))
        mlflow.xgboost.log_model(
            model,
            name=model_name,
            signature=signature,
            pip_requirements=None,
            conda_env=None,
        )

        # ---- Predicting on future data ---- #
        # Create directories for model artifacts
        model_dir = pth.MODEL_DIR / model_name
        model_dir.mkdir(parents=True, exist_ok=True)
        artifacts_dir = model_dir / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)

        # Retrain on full dataset (train + test)
        full_X = pd.concat([X_train, X_test], axis=0)
        full_y = pd.concat([y_train, y_test], axis=0)
        model.fit(full_X, full_y)

        # Predict on all three scenarii
        # (predictions are saved in artifacts directory)
        prediction_pipeline(model, artifacts_dir, dep_encoder)

        # Log the artifacts directory with MLflow
        # (contains predictions for all scenarii)
        mlflow.log_artifact(str(artifacts_dir), artifact_path="artifacts")

    return None
