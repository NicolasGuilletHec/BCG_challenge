"""BCG-inspired color theme for the Streamlit app."""

# ---------------------------------------------------------------------------
# BCG brand palette
# ---------------------------------------------------------------------------
BCG_GREEN_DARK = "#00694B"
BCG_GREEN = "#00875A"
BCG_GREEN_LIGHT = "#3DCD58"
BCG_TEAL = "#00B2A9"
BCG_LIME = "#A0D468"
BCG_NEAR_BLACK = "#1D1D1B"
BCG_GRAY = "#6D6E71"
BCG_LIGHT_GRAY = "#D1D3D4"
BCG_WHITE = "#FFFFFF"

# ---------------------------------------------------------------------------
# Plotly color scale for choropleth maps (low -> high)
# ---------------------------------------------------------------------------
CHOROPLETH_SCALE = [
    [0.0, BCG_WHITE],
    [0.25, BCG_LIME],
    [0.5, BCG_GREEN_LIGHT],
    [0.75, BCG_GREEN],
    [1.0, BCG_GREEN_DARK],
]

# ---------------------------------------------------------------------------
# Discrete colors for scenario line charts
# ---------------------------------------------------------------------------
SCENARIO_COLORS = {
    "scenario_126": BCG_GREEN_LIGHT,
    "scenario_245": BCG_TEAL,
    "scenario_585": BCG_GREEN_DARK,
}

# Ordered list for px.line color_discrete_sequence
LINE_COLORS = [BCG_GREEN, BCG_TEAL, BCG_GREEN_DARK, BCG_LIME]
