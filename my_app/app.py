import json
import re
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Barley – Historical & Predictions", layout="wide")

# -----------------------
# Paths
# -----------------------
DATA_DIR = Path("data")
DATA_DIR_GEO = Path("geo")

HIST_PATH = DATA_DIR / "barley_yield_from_1982.csv"
PRED_585 = DATA_DIR / "scenario_585_predictions.csv"
PRED_245 = DATA_DIR / "scenario_245_predictions.csv"
PRED_126 = DATA_DIR / "scenario_126_predictions.csv"
GEOJSON_PATH = DATA_DIR_GEO / "departements.geojson"

Y0 = 2014  # baseline year for area in prediction mode

# -----------------------
# Name normalization + mapping to GeoJSON
# -----------------------

MANUAL_FIXES = {
    "seine seineoise": "yvelines",
    "seine et oise": "yvelines",
}

def normalize_dep_name(s: str) -> str:
    """Normalize French department names to improve matching with GeoJSON."""
    if s is None:
        return ""
    s = str(s).strip().lower()

    # Normalize apostrophes and underscores
    s = s.replace("’", "'")
    s = s.replace("_", " ")  # ✅ THIS FIX

    # Remove accents
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))

    # Convert hyphens/apostrophes to spaces and squeeze
    s = re.sub(r"[-'’]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # Common abbreviations
    s = s.replace("saint ", "st ")
    s = s.replace("sainte ", "ste ")

    # manual fix
    s = MANUAL_FIXES.get(s, s)

    return s

@st.cache_data
def build_geo_name_map(geo: dict, geo_name_field: str) -> dict:
    """
    Dict: normalized_name -> exact GeoJSON name
    """
    m = {}
    collisions = []
    for feat in geo.get("features", []):
        props = feat.get("properties", {})
        name = props.get(geo_name_field)
        if not isinstance(name, str):
            continue
        key = normalize_dep_name(name)
        if key in m and m[key] != name:
            collisions.append((key, m[key], name))
        m[key] = name

    if collisions:
        st.warning(f"Collisions in GeoJSON name normalization (showing first 5): {collisions[:5]}")
    return m

def align_departments_to_geo(df: pd.DataFrame, geo_name_map: dict) -> pd.DataFrame:
    """
    Adds:
      - dep_norm: normalized department name
      - department_geo: exact name used in GeoJSON (or NaN if not found)
    """
    out = df.copy()
    out["dep_norm"] = out["department"].map(normalize_dep_name)
    out["department_geo"] = out["dep_norm"].map(geo_name_map)
    return out

# -----------------------
# Loaders (cached)
# -----------------------
@st.cache_data
def load_hist(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";", engine="python")
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    required = {"department", "year", "yield", "area", "production"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Historical file missing columns: {missing}")

    df["department"] = df["department"].astype(str).str.strip()
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["yield"] = pd.to_numeric(df["yield"], errors="coerce")
    df["area"] = pd.to_numeric(df["area"], errors="coerce")
    df["production"] = pd.to_numeric(df["production"], errors="coerce")

    df = df.dropna(subset=["year"]).copy()
    df["year"] = df["year"].astype(int)
    return df

@st.cache_data
def load_pred(path: Path, scenario_name: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # normalize columns
    df = df.rename(columns={"nom_dep": "department", "predicted_yield": "yield_pred"})

    required = {"department", "year", "yield_pred"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Prediction file {path.name} missing columns: {missing}")

    df["department"] = df["department"].astype(str).str.strip()
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["yield_pred"] = pd.to_numeric(df["yield_pred"], errors="coerce")

    df = df.dropna(subset=["department", "year", "yield_pred"]).copy()
    df["year"] = df["year"].astype(int)
    df["scenario"] = scenario_name
    return df[["department", "year", "scenario", "yield_pred"]]

@st.cache_data
def load_geojson(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def infer_geo_name_field(geo: dict) -> str:
    props = geo["features"][0].get("properties", {})
    candidates = ["nom", "name", "NOM_DEP", "Nom", "department", "DEP_NOM"]
    for c in candidates:
        if c in props:
            return c
    # fallback: first string-ish field
    for k, v in props.items():
        if isinstance(v, str) and len(v) > 2:
            return k
    raise ValueError("Could not infer department name field in GeoJSON properties.")

# -----------------------
# Viz + computations
# -----------------------
def make_choropleth_map(df, geo, geo_name_field, value_col, title):
    featureidkey = f"properties.{geo_name_field}"
    fig = px.choropleth_mapbox(
        df,
        geojson=geo,
        locations="department_geo",  # IMPORTANT: matched geojson name
        featureidkey=featureidkey,
        color=value_col,
        hover_data={
            "department": True,
            "department_geo": True,
            "year": True,
            value_col: True,
        },
        mapbox_style="carto-positron",
        zoom=4.4,
        center={"lat": 46.8, "lon": 2.5},
        opacity=0.75,
        title=title,
    )
    fig.update_layout(margin={"r": 0, "t": 50, "l": 0, "b": 0})
    return fig

def compute_area_2014(hist: pd.DataFrame, y0: int = Y0) -> pd.DataFrame:
    df = hist[hist["year"] == y0][["department", "area"]].copy()
    df = df.dropna(subset=["area"]).drop_duplicates(subset=["department"])
    df = df.rename(columns={"area": "area_2014"})
    return df

def apply_area_growth(pred: pd.DataFrame, area_2014: pd.DataFrame, growth_rate_pct: float, y0: int = Y0) -> pd.DataFrame:
    df = pred.merge(area_2014, on="department", how="left", validate="m:1")
    g = growth_rate_pct / 100.0
    years_from_y0 = (df["year"] - y0).astype(int)
    df["area_adj"] = df["area_2014"] * np.power((1.0 + g), years_from_y0)
    df["production_pred"] = df["yield_pred"] * df["area_adj"]
    return df

# -----------------------
# Load data
# -----------------------
st.title("🌾 Barley – Historical vs Predictions")

missing_files = [p for p in [HIST_PATH, PRED_585, PRED_245, PRED_126, GEOJSON_PATH] if not p.exists()]
if missing_files:
    st.error("Missing required files:\n" + "\n".join([f"- {p}" for p in missing_files]))
    st.stop()

hist = load_hist(HIST_PATH)
pred_585 = load_pred(PRED_585, "scenario_585")
pred_245 = load_pred(PRED_245, "scenario_245")
pred_126 = load_pred(PRED_126, "scenario_126")
pred_all = pd.concat([pred_585, pred_245, pred_126], ignore_index=True)

geo = load_geojson(GEOJSON_PATH)
geo_name_field = infer_geo_name_field(geo)

# Build mapping and align names (this fixes "missing departments" on the map)
geo_name_map = build_geo_name_map(geo, geo_name_field)
hist = align_departments_to_geo(hist, geo_name_map)
pred_all = align_departments_to_geo(pred_all, geo_name_map)

# Diagnostics: show what doesn't match
missing_hist = sorted(hist.loc[hist["department_geo"].isna(), "department"].unique())
missing_pred = sorted(pred_all.loc[pred_all["department_geo"].isna(), "department"].unique())

if missing_hist:
    st.warning(
        f"⚠️ {len(missing_hist)} départements HISTO ne matchent pas le geojson (ex: {missing_hist[:15]})"
    )
if missing_pred:
    st.warning(
        f"⚠️ {len(missing_pred)} départements PRED ne matchent pas le geojson (ex: {missing_pred[:15]})"
    )

area_2014 = compute_area_2014(hist.rename(columns={"department": "department"}), Y0)

# warn if some depts in predictions missing area_2014
missing_area_deps = sorted(set(pred_all["department"]) - set(area_2014["department"]))
if missing_area_deps:
    st.warning(
        f"{len(missing_area_deps)} departments in predictions have no area in {Y0}. "
        f"Example: {missing_area_deps[:10]}"
    )

# -----------------------
# Layout: 2-sided app
# -----------------------
tab_hist, tab_pred = st.tabs(["📚 Historical", "🔮 Predictions"])

# =======================
# TAB 1 — HISTORICAL
# =======================
with tab_hist:
    st.subheader("Historical values (observed)")

    metric = st.selectbox("Metric", ["yield", "area", "production"], index=2)

    min_y, max_y = int(hist["year"].min()), int(hist["year"].max())
    year = st.slider("Year", min_y, max_y, value=max_y, key="hist_year")

    df_year = hist[hist["year"] == year][["department", "department_geo", "year", metric]].copy()
    df_year = df_year.dropna(subset=[metric, "department_geo"])

    # KPIs
    col1, col2, col3 = st.columns(3)
    sub = hist[hist["year"] == year]

    total_prod = sub["production"].sum(skipna=True)
    total_area = sub["area"].sum(skipna=True)
    yield_w = (sub["yield"] * sub["area"]).sum(skipna=True) / max(total_area, 1e-9)

    col1.metric("France production", f"{total_prod:,.0f}")
    col2.metric("France area", f"{total_area:,.0f}")
    col3.metric("Weighted yield", f"{yield_w:,.2f}")

    left, right = st.columns([2, 1])

    with left:
        fig = make_choropleth_map(
            df_year,
            geo,
            geo_name_field,
            metric,
            title=f"Historical {metric} – {year}",
        )
        st.plotly_chart(fig, use_container_width=True)

    with right:
        dep = st.selectbox("Department (trend)", sorted(hist["department"].unique()))
        dep_ts = hist[hist["department"] == dep].sort_values("year")
        fig2 = px.line(dep_ts, x="year", y=metric, title=f"{dep} – {metric} over time")
        st.plotly_chart(fig2, use_container_width=True)

# =======================
# TAB 2 — PREDICTIONS
# =======================
with tab_pred:
    st.subheader("Predicted values (scenarios)")

    scenario = st.selectbox("Scenario", sorted(pred_all["scenario"].unique()))
    growth_rate = st.slider("Area annual growth rate (%)", -10.0, 10.0, 0.0, 0.1, key="growth")

    df_sc = pred_all[pred_all["scenario"] == scenario].copy()
    df_sc = apply_area_growth(df_sc, area_2014, growth_rate_pct=growth_rate, y0=Y0)

    metric_pred = st.selectbox("Metric", ["yield_pred", "area_adj", "production_pred"], index=2)

    min_py, max_py = int(df_sc["year"].min()), int(df_sc["year"].max())
    year = st.slider("Year", min_py, max_py, value=min_py, key="pred_year")

    df_year = df_sc[df_sc["year"] == year][
        ["department", "department_geo", "year", "scenario", "yield_pred", "area_adj", "production_pred"]
    ].copy()
    df_year = df_year.dropna(subset=[metric_pred, "department_geo"])

    # KPIs
    col1, col2, col3 = st.columns(3)
    prod_total = df_year["production_pred"].sum(skipna=True)
    area_total = df_year["area_adj"].sum(skipna=True)
    yield_w = (df_year["yield_pred"] * df_year["area_adj"]).sum(skipna=True) / max(area_total, 1e-9)

    col1.metric("France production (pred)", f"{prod_total:,.0f}")
    col2.metric("France area (adj)", f"{area_total:,.0f}")
    col3.metric("Weighted yield (pred)", f"{yield_w:,.2f}")

    left, right = st.columns([2, 1])

    with left:
        fig = make_choropleth_map(
            df_year[["department", "department_geo", "year", metric_pred]].copy(),
            geo,
            geo_name_field,
            metric_pred,
            title=f"{scenario} – {metric_pred} – {year} (Y0={Y0}, g={growth_rate:+.1f}%)",
        )
        st.plotly_chart(fig, use_container_width=True)

    with right:
        trend = df_sc.groupby("year", as_index=False).agg(
            production_pred=("production_pred", "sum"),
            area_adj=("area_adj", "sum"),
        )
        fig2 = px.line(trend, x="year", y="production_pred", title="France total production (selected scenario)")
        st.plotly_chart(fig2, use_container_width=True)

# Scenario comparison full width (uses growth_rate from predictions tab)
st.markdown("---")
st.subheader("Scenario comparison (France totals)")

# If user never opened predictions tab, growth_rate may not exist yet; safe default:
growth_rate_safe = st.session_state.get("growth", 0.0)

df_cmp = apply_area_growth(pred_all.copy(), area_2014, growth_rate_pct=growth_rate_safe, y0=Y0)
cmp = df_cmp.groupby(["scenario", "year"], as_index=False)["production_pred"].sum()

fig3 = px.line(
    cmp,
    x="year",
    y="production_pred",
    color="scenario",
    title="France total production – scenario comparison",
)
st.plotly_chart(fig3, use_container_width=True)