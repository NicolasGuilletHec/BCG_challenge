# XHEC BCGX Data Science Challenge: Crop Yield Prediction

<!-- Build & CI Status -->
![CI](https://github.com/NicolasGuilletHec/BCG_challenge/actions/workflows/ci.yaml/badge.svg?event=push)

<!-- Code Quality & Tools -->
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)

<!-- Environment & Package Management -->
![Python Version](https://img.shields.io/badge/python-3.13+-blue.svg)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

## Table of Contents

- [XHEC BCGX Data Science Challenge: Crop Yield Prediction](#xhec-bcgx-data-science-challenge-crop-yield-prediction)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Quick Start](#quick-start)
    - [Prerequisites](#prerequisites)
    - [Installation](#installation)
  - [Project Structure](#project-structure)
  - [Commands Reference](#commands-reference)
    - [Data \& Pipeline](#data--pipeline)
    - [Code Quality](#code-quality)
  - [Further Documentation](#further-documentation)
  - [Authors](#authors)

---

## Introduction

This repository contains the codebase for the **XHEC-BCGX Data Science Challenge** focused on predicting **barley crop yield** across French departments based on historical climate data (temperature, precipitation).

The project uses a **medallion architecture** (bronze/silver/gold) for data processing, **DVC** for data versioning and pipeline orchestration, **MLflow** with Databricks for experiment tracking, and a **Streamlit** dashboard for visualizing predictions under different climate scenarios (SSP1-2.6, SSP2-4.5, SSP5-8.5).

---

## Quick Start

### Prerequisites

- Python 3.13+
- [uv](https://github.com/astral-sh/uv) package manager
- Git

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/NicolasGuilletHec/BCG_challenge.git
cd BCG_challenge

# 2. Install dependencies
uv sync

# 3. Set up pre-commit hooks
uv run pre-commit install
```

For full setup instructions (DVC credentials, MLflow, etc.), see [docs/SETUP.md](docs/SETUP.md).

---

## Project Structure

```
ROOT/****
├── constants/                   # Global constants (paths, column names, thresholds)
├── config/                      # Configuration files (logging)
├── data/
│   ├── bronze/                  # Raw data (immutable, DVC-tracked)
│   ├── silver/                  # Cleaned intermediate data
│   └── gold/                    # Feature-engineered, model-ready data
├── models/                      # Trained model artifacts
├── my_app/                      # Streamlit dashboard application
├── notebooks/                   # Jupyter notebooks for EDA & analysis
├── src/
│   ├── pipelines/
│   │   ├── data/                # bronze_to_silver, silver_to_gold
│   │   ├── models/              # Model training (XGBoost)
│   │   └── utils/               # Data loading & prediction utilities
│   └── utils/                   # Logger
├── docs/                        # Detailed documentation
├── dvc.yaml                     # DVC pipeline definition
└── params.yaml                  # Tunable parameters
```

---

## Commands Reference

### Data & Pipeline

```bash

# Run full pipeline
uv run dvc repro

# Run individual stages
uv run dvc repro bronze_to_silver
uv run dvc repro silver_to_gold

# View pipeline DAG
uv run dvc dag

# Check pipeline status
uv run dvc status
```


### Code Quality

```bash
# Lint code
uv run ruff check .

# Format code
uv run ruff format .

# Run pre-commit hooks manually
uv run pre-commit run --all-files
```

---

## Further Documentation

- **[docs/SETUP.md](docs/SETUP.md)** — Complete setup guide for new team members (credentials, DVC, MLflow)
- **[docs/DVC_WORKFLOW.md](docs/DVC_WORKFLOW.md)** — How to use DVC for data versioning, pipelines, and experiments
- **[docs/PROJECT_OWNER_CHECKLIST.md](docs/PROJECT_OWNER_CHECKLIST.md)** — Infrastructure setup checklist for the project owner

---

## Authors

**XHEC Data Science Challenge Team**

- Mehdi DIGUA
- Nicolas GUILLET
- Augustin NATON


---
