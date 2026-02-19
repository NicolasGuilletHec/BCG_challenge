"""Prediction utilities."""

import pandas as pd
from sklearn.preprocessing import LabelEncoder

from constants.column_names.dashboard import DASHBOARD_COLUMNS, DASHBOARD_YIELD
from constants.column_names.gold import GOLD_NOM_DEP, GOLD_SCENARIO, GOLD_YEAR
from constants.constants import SCENARIO_126, SCENARIO_245, SCENARIO_585
import constants.paths as pth


def prepare_prediction_data(
    dep_encoder: LabelEncoder,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Prepare the data for prediction on future scenarios.

    Args:
        dep_encoder (LabelEncoder): Fitted encoder for nom_dep.

    Returns:
        tuple: A tuple containing the prepared data for the three scenarios:
            - scenario_1 (pd.DataFrame): Prepared data for scenario 1.
            - scenario_2 (pd.DataFrame): Prepared data for scenario 2.
            - scenario_3 (pd.DataFrame): Prepared data for scenario 3.
    """
    scenarii = pd.read_parquet(pth.GOLD_SCENARIO_PATH)
    scenario_1 = scenarii[scenarii[GOLD_SCENARIO] == SCENARIO_126].drop(
        columns=[GOLD_SCENARIO]
    )
    scenario_2 = scenarii[scenarii[GOLD_SCENARIO] == SCENARIO_245].drop(
        columns=[GOLD_SCENARIO]
    )
    scenario_3 = scenarii[scenarii[GOLD_SCENARIO] == SCENARIO_585].drop(
        columns=[GOLD_SCENARIO]
    )

    # Encode nom_dep for model input
    for df in [scenario_1, scenario_2, scenario_3]:
        df[GOLD_NOM_DEP] = dep_encoder.transform(df[GOLD_NOM_DEP])

    return scenario_1, scenario_2, scenario_3


def prepare_dashboard_data(
    scenario_data: pd.DataFrame,
    predictions: pd.Series,
    dep_encoder: LabelEncoder,
) -> pd.DataFrame:
    """Prepare the data for dashboard visualization.

    Args:
        scenario_data (pd.DataFrame): The input data for a specific scenario
            (with encoded nom_dep).
        predictions (pd.Series): The predicted yield values corresponding to the input
            data.
        dep_encoder (LabelEncoder): Fitted encoder to decode nom_dep back to names.

    Returns:
        pd.DataFrame: A DataFrame containing the original scenario data along with the
        predictions, ready for dashboard visualization.
    """
    dashboard_data = scenario_data.copy()
    dashboard_data[DASHBOARD_YIELD] = predictions
    dashboard_data[GOLD_NOM_DEP] = dep_encoder.inverse_transform(
        dashboard_data[GOLD_NOM_DEP]
    )
    return dashboard_data[DASHBOARD_COLUMNS]


def prediction_pipeline(
    model: object, artifacts_dir: str, dep_encoder: LabelEncoder
) -> None:
    """Run the prediction pipeline for future scenarios.

    This function takes a trained model and an artifacts directory, prepares the data
    for prediction, makes predictions on the future scenarios, and saves the results
    in the specified artifacts directory.

    Args:
        model (object): The trained machine learning model to use for predictions.
        artifacts_dir (str): The directory where the prediction results will be saved.
        dep_encoder (LabelEncoder): Fitted encoder for nom_dep.
    """
    # Prepare data for prediction
    scenario_126, scenario_245, scenario_585 = prepare_prediction_data(dep_encoder)

    # Make predictions on each scenario (drop year — not a model feature)
    pred_1 = model.predict(scenario_126.drop(columns=[GOLD_YEAR]))
    pred_2 = model.predict(scenario_245.drop(columns=[GOLD_YEAR]))
    pred_3 = model.predict(scenario_585.drop(columns=[GOLD_YEAR]))

    # Prepare dashboard data (decodes nom_dep back to names)
    scenario_126_dashboard_data = prepare_dashboard_data(
        scenario_126, pred_1, dep_encoder
    )
    scenario_245_dashboard_data = prepare_dashboard_data(
        scenario_245, pred_2, dep_encoder
    )
    scenario_585_dashboard_data = prepare_dashboard_data(
        scenario_585, pred_3, dep_encoder
    )

    # Save predictions to CSV files in artifacts directory
    scenario_126_path = artifacts_dir / "scenario_126_predictions.csv"
    scenario_245_path = artifacts_dir / "scenario_245_predictions.csv"
    scenario_585_path = artifacts_dir / "scenario_585_predictions.csv"

    scenario_126_dashboard_data.to_csv(scenario_126_path, index=False)
    scenario_245_dashboard_data.to_csv(scenario_245_path, index=False)
    scenario_585_dashboard_data.to_csv(scenario_585_path, index=False)
    return None
