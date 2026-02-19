# DVC Workflow Guide

Learn how to use DVC (Data Version Control) to manage datasets and pipelines in this project.

## Table of Contents

- [Overview](#overview)
- [Common Commands](#common-commands)
  - [Getting Data](#getting-data)
  - [Working with Data](#working-with-data)
  - [Creating Data Pipelines](#creating-data-pipelines)
- [Adding a Data Processing Function](#adding-a-data-processing-function)
- [Adding a New Model Training Function](#adding-a-new-model-training-function)
- [Launching Training with Specific Parameters](#launching-training-with-specific-parameters)
  - [Option 1: Edit params.yaml Directly](#option-1-edit-paramsyaml-directly)
  - [Option 2: Override Parameters on the Command Line](#option-2-override-parameters-on-the-command-line)
  - [Option 3: Run Experiments in Parallel (Queue)](#option-3-run-experiments-in-parallel-queue)
  - [Comparing Experiments](#comparing-experiments)
  - [Applying an Experiment](#applying-an-experiment)
- [Team Collaboration](#team-collaboration)
- [Best Practices](#best-practices)
- [Common Workflows](#common-workflows)
- [Troubleshooting](#troubleshooting)
- [Quick Reference](#quick-reference)

---

## Overview

**DVC** tracks your data files and pipelines, similar to how Git tracks code. Data is stored in **Backblaze B2** (S3-compatible) cloud storage, while Git only tracks small `.dvc` metadata files.

### Data Organization

```
data/
├── bronze/                              # Raw data (NEVER modify!)
│   ├── barley_yield_from_1982.csv       #   Barley yield by department (semicolon-separated)
│   └── climate_data_from_1982.parquet   #   Climate metrics (temperature, precipitation)
├── silver/                              # Cleaned, validated data (Parquet)
│   ├── yield_clean.parquet              #   Cleaned yield data
│   └── climate_clean.parquet            #   Cleaned & pivoted climate data
└── gold/                                # Feature-engineered, model-ready data
    ├── climate_features.parquet         #   Aggregated climate features
    ├── training.parquet                 #   Training set (historical years)
    ├── validation.parquet               #   Validation set (recent years)
    └── scenarios.parquet                #   Future climate scenario data

src/pipelines/
├── data/                                # Data pipeline scripts (bronze→silver→gold)
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
├── models/                              # Model training & prediction scripts
│   ├── train_models.py                  #   Training orchestration
│   └── xgboost_model.py                #   XGBoost training & prediction
└── utils/                               # Shared utilities
    ├── model_inputs_loading.py          #   Load data & params for training
    └── prediction_utils.py              #   Prediction pipeline utilities

constants/                               # Global constants
├── paths.py                             #   Centralized path definitions
├── constants.py                         #   Numeric thresholds & feature engineering constants
└── column_names/                        #   Dataset column name constants
    ├── bronze.py
    ├── silver.py
    ├── gold.py
    └── dashboard.py

params.yaml                              # All tunable parameters (tracked by DVC)
dvc.yaml                                 # Pipeline stage definitions
```

## Common Commands

### Getting Data

```bash
# Pull latest data from Backblaze B2
dvc pull

# Check what's in sync
dvc status
```

### Working with Data

#### 1. Never Modify Raw Data

Raw data in `data/bronze/` is **immutable**. Always:
- Read from `data/bronze/`
- Write to `data/silver/` or `data/gold/`
- Never modify files in `data/bronze/`

**Tracking rules:**
- `data/bronze/`: Use `dvc add` to track raw data files
- `data/silver/` and `data/gold/`: Tracked automatically via pipeline `outs:` in `dvc.yaml` (don't use `dvc add`)

#### 2. Create New Processed Datasets

Silver and gold files are tracked automatically through the pipeline outputs in `dvc.yaml`. Don't use `dvc add` manually for these files.

```bash
# 1. Add your processing function to the pipeline script
#    (see "Adding a Data Processing Function" section)

# 2. Define the output in dvc.yaml under the appropriate stage:
#    outs:
#      - data/silver/

# 3. Run the pipeline - DVC tracks the output automatically
uv run dvc repro bronze_to_silver

# 4. Commit the lock file
git add dvc.yaml dvc.lock
git commit -m "Add cleaned dataset pipeline"

# 5. Push data and code
dvc push
git push
```

#### 3. Update Existing Datasets

```bash
# Modify your processing script and re-run the pipeline
uv run dvc repro bronze_to_silver

# DVC detects the change automatically via dvc.lock
dvc status

# Commit the updated lock file
git add dvc.lock
git commit -m "Update cleaned dataset: added date parsing"

# Push to Backblaze B2 and Git
dvc push
git push
```

### Creating Data Pipelines

Define reproducible pipelines in `dvc.yaml`:

```yaml
stages:
  bronze_to_silver:
    cmd: uv run python -m src.pipelines.data.bronze_to_silver
    deps:
      - src/pipelines/data/bronze_to_silver.py
      - data/bronze/
    outs:
      - data/silver/

  silver_to_gold:
    cmd: uv run python -m src.pipelines.data.silver_to_gold
    deps:
      - src/pipelines/data/silver_to_gold.py
      - data/silver/
    outs:
      - data/gold/

  train_models:
    cmd: uv run python -m src.pipelines.models.train_models
    deps:
      - src/pipelines/models/train_models.py
      - data/gold/
    outs:
      - models/
```

Run the pipeline:

```bash
# Run entire pipeline
uv run dvc repro

# DVC automatically:
# - Runs stages in correct order
# - Only re-runs what changed
# - Tracks all outputs

# Visualize the pipeline
uv run dvc dag

# Push all results
dvc push
git add dvc.lock
git commit -m "Run data pipeline"
git push
```

## Adding a Data Processing Function

Follow these steps to add a new data processing step to the pipeline.

### Step 1: Write Your Processing Function

Add your function to the appropriate pipeline file:

**For Bronze → Silver transformations:** `src/pipelines/data/bronze_to_silver.py`

```python
# src/pipelines/data/bronze_to_silver.py

import pandas as pd
from constants.paths import BRONZE_DIR, SILVER_DIR
from src.utils.logger import logger


def clean_yield_data(input_path: Path, output_path: Path) -> None:
    """Clean raw yield data and save to silver.

    Args:
        input_path: Path to the input file in bronze directory.
        output_path: Path to the output file in silver directory.
    """
    logger.info(f"Reading data from {input_path}")
    df = pd.read_csv(input_path, sep=";")

    logger.info("Cleaning data: recovering missing values, fixing types...")
    # Recover missing yields from production/area where possible
    mask = df["yield"].isna() & df["production"].notna() & df["area"].notna()
    df.loc[mask, "yield"] = df.loc[mask, "production"] / df.loc[mask, "area"]

    logger.info(f"Writing cleaned data to {output_path}")
    df.to_parquet(output_path, index=False)
```

**For Silver → Gold transformations:** `src/pipelines/data/silver_to_gold.py`

```python
# src/pipelines/data/silver_to_gold.py

import pandas as pd
from constants.paths import SILVER_DIR, GOLD_DIR
from src.utils.logger import logger


def create_climate_features(climate_data: pd.DataFrame) -> pd.DataFrame:
    """Create aggregated climate features for model training.

    Args:
        climate_data: Cleaned climate data from silver layer.

    Returns:
        DataFrame with aggregated climate features per department and year.
    """
    logger.info("Creating climate features...")
    # Feature engineering: dry periods, extreme temps, seasonal aggregates
    # ...
    return features
```

### Step 2: Update dvc.yaml

If your new processing function writes to a new output directory, update `dvc.yaml` accordingly. If it writes within the existing `data/silver/` or `data/gold/` directories, no change is needed since those directories are already tracked as outputs.

### Step 3: Run the Pipeline

```bash
# Run only your stage
uv run dvc repro bronze_to_silver

# Or run the full pipeline
uv run dvc repro

# Visualize the pipeline
uv run dvc dag
```

## Adding a New Model Training Function

Follow these steps to add a new model type to the training pipeline.

### Step 1: Create the Model Training File

Create a new file in `src/pipelines/models/` (e.g., `lightgbm_model.py`):

```python
# src/pipelines/models/lightgbm_model.py

from dotenv import load_dotenv
import mlflow
from mlflow.models.signature import infer_signature
import pandas as pd
from sklearn.metrics import root_mean_squared_error
from lightgbm import LGBMRegressor


def train_lightgbm_model(
    run_name: str,
    model_name: str,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    dep_encoder,
    **kwargs,
):
    """Train and log a LightGBM model using MLflow.

    Args:
        run_name: Name of the MLflow run.
        model_name: Name to log the model under in MLflow.
        X_train: Training feature matrix.
        y_train: Training target vector.
        X_test: Testing feature matrix.
        y_test: Testing target vector.
        dep_encoder: LabelEncoder for department names.
        **kwargs: Additional model hyperparameters.
    """
    load_dotenv()

    with mlflow.start_run(run_name=run_name):
        model = LGBMRegressor(**kwargs)
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        rmse = root_mean_squared_error(y_test, y_pred)

        mlflow.log_params(kwargs)
        mlflow.log_metric("rmse", rmse)

        signature = infer_signature(X_train, model.predict(X_train))
        mlflow.sklearn.log_model(model, name=model_name, signature=signature)
```

### Step 2: Register the Model in train_models.py

Update `src/pipelines/models/train_models.py` to include your new model in the dispatch logic.

### Step 3: Add Hyperparameters to params.yaml

Add the hyperparameter grid for your new model in `params.yaml`:

```yaml
# params.yaml

train_models:
  experiment_name: baseline
  model_types:
    - xgboost
    - lightgbm  # Add your new model here

hyperparameters:
  xgboost: null
  lightgbm:                         # Add your new model's hyperparameter grid
    n_estimators: [50, 100, 200]
    max_depth: [3, 5, 7]
    learning_rate: [0.01, 0.1, 0.2]
```

### Step 4: Run Training

```bash
# Run training with current params.yaml settings
uv run dvc repro train_models

# Or run the full pipeline
uv run dvc repro
```

## Launching Training with Specific Parameters

DVC tracks parameters in `params.yaml` and lets you override them for experiments.

### Option 1: Edit params.yaml Directly

Modify `params.yaml` and run:

```bash
uv run dvc repro train_models
```

### Option 2: Override Parameters on the Command Line

Use `--set-param` to override without editing files:

```bash
# Change model types
uv run dvc exp run --set-param 'train_models.model_types=[xgboost,lightgbm]'

# Change experiment name
uv run dvc exp run --set-param train_models.experiment_name=experiment_v2

# Change multiple parameters at once
uv run dvc exp run \
  --set-param 'train_models.model_types=[xgboost]' \
  --set-param train_models.experiment_name=xgboost_tuned
```

### Option 3: Run Experiments in Parallel (Queue)

Queue multiple experiments with different parameters:

```bash
# Queue experiments
uv run dvc exp run --queue --set-param train_models.experiment_name=exp_v1
uv run dvc exp run --queue --set-param train_models.experiment_name=exp_v2
uv run dvc exp run --queue --set-param train_models.experiment_name=exp_v3

# Run all queued experiments
uv run dvc exp run --run-all
```

### Comparing Experiments

```bash
# View all experiments
uv run dvc exp show

# Compare parameters between experiments
uv run dvc params diff

# Compare specific experiments
uv run dvc exp diff exp-abc123 exp-def456
```

### Applying an Experiment

After finding the best experiment, apply it to your workspace:

```bash
# Apply experiment results to workspace
uv run dvc exp apply exp-abc123

# Commit the changes
git add params.yaml dvc.lock
git commit -m "Apply best experiment"
```

## Team Collaboration

### Pulling Teammate's Data

```bash
# Teammate creates new dataset and pushes
# You pull their changes:

git pull                  # Get .dvc files
dvc pull                 # Get actual data

# Now you have their datasets!
```

### Sharing Your Data

Silver and gold outputs are tracked through the pipeline. After updating your pipeline:

```bash
# Run the pipeline to generate outputs
uv run dvc repro

# Commit the pipeline changes and lock file
git add dvc.yaml dvc.lock
git commit -m "Add new feature set"

# Push data and code
dvc push
git push

# Teammates can now pull your data!
```

### Avoiding Conflicts

**Data files:** DVC handles this automatically. Each version is stored separately in Backblaze B2.

**.dvc files:** Treat like code:
- Pull before making changes: `git pull`
- Communicate with team about major data updates
- Use descriptive commit messages

## Best Practices

### 1. Descriptive Commit Messages

```bash
# Bad
git commit -m "Update data"

# Good
git commit -m "Add climate features: dry spells, extreme temps, seasonal aggregates"
```

### 2. Version Your Outputs

Pipeline outputs are versioned automatically through `dvc.lock`. Each time you run the pipeline, DVC tracks the new version.

```bash
# Iterative improvements: just run the pipeline
uv run dvc repro silver_to_gold
git add dvc.lock
git commit -m "Update gold features: added rolling averages"

# For experiments with different parameters, use DVC experiments
uv run dvc exp run --set-param train_models.experiment_name=experiment_v1
uv run dvc exp run --set-param train_models.experiment_name=experiment_v2
```

### 3. Use Pipelines for Reproducibility

Instead of manual steps, define pipelines in `dvc.yaml`:

```yaml
stages:
  silver_to_gold:
    cmd: uv run python -m src.pipelines.data.silver_to_gold
    deps:
      - src/pipelines/data/silver_to_gold.py
      - data/silver/
    outs:
      - data/gold/
```

Then anyone can reproduce: `uv run dvc repro`

### 4. Document Your Data

Add a `README.md` in each data directory describing the files, their source, and their schema.

## Common Workflows

### Starting Fresh

```bash
# Remove local data
rm -rf data/bronze data/silver data/gold

# Pull everything from Backblaze B2
uv run dvc pull
```

### Checking What Changed

```bash
# See what data changed locally
dvc status

# See what changed in Git
git status

# See pipeline status
dvc status
```

### Reverting to Previous Version

```bash
# Find the commit with the data version you want
git log -- dvc.lock

# Check out that version
git checkout <commit-hash> dvc.lock

# Get the data
uv run dvc checkout
```

## Troubleshooting

**"Unable to locate credentials"**
- Make sure you've configured DVC remote credentials:
  ```bash
  source .env
  dvc remote modify --local b2bucket access_key_id $AWS_ACCESS_KEY_ID
  dvc remote modify --local b2bucket secret_access_key $AWS_SECRET_ACCESS_KEY
  ```

**"File not found in cache"**
- Run `dvc pull` to download from Backblaze B2

**"Conflict in .dvc file"**
- Usually safe to accept both versions
- Then run `dvc checkout` to sync

**"Push takes forever"**
- DVC uploads only new/changed data
- Large files take time on first push
- Subsequent pushes are faster

## Quick Reference

```bash
# Get data
uv run dvc pull                # Download all data from Backblaze B2
uv run dvc status              # Check sync status

# Track data
dvc add <file>                 # Track bronze (raw) data only
dvc push                       # Upload to Backblaze B2
# Note: Silver/gold files are tracked via pipeline outputs in dvc.yaml

# Pipelines
uv run dvc repro               # Run full pipeline
uv run dvc repro <stage>       # Run specific stage
uv run dvc dag                 # Visualize pipeline

# Experiments
uv run dvc exp run             # Run experiment
uv run dvc exp run --set-param train_models.experiment_name=test  # Override params
uv run dvc exp show            # List experiments
uv run dvc params diff         # Compare parameters

# Info
uv run dvc status              # Show data changes
uv run dvc diff                # Compare versions
```
