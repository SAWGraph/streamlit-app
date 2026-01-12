# SAWGraph PFAS Explorer - Application Structure

## 📁 Directory Structure

```
streamlit/
├── app.py                          # Main application entry point (single-page app)
├── analysis_registry.py            # Centralized analysis configuration
├── requirements.txt
├── README.md
│
├── analyses/                       # Analysis modules (one per analysis type)
│   ├── __init__.py
│   ├── pfas_upstream.py            # Upstream tracing analysis
│   ├── pfas_downstream.py          # Downstream tracing analysis
│   ├── samples_near_facilities.py  # Samples near facilities analysis
│   ├── regional_overview.py        # Regional contamination overview
│   └── facility_risk.py            # Facility risk assessment (stub)
│
├── components/                     # Reusable UI components
│   ├── __init__.py
│   └── start_page.py               # Landing page with logo and intro
│
├── utils/                          # Shared utilities and query builders
│   ├── __init__.py
│   ├── sparql_helpers.py           # SPARQL connection & query helpers
│   ├── upstream_tracing_queries.py # Upstream SPARQL query builders
│   ├── downstream_tracing_queries.py # Downstream SPARQL query builders
│   ├── nearby_queries.py           # Nearby facilities query builders
│   ├── region_filters.py           # Geographic region filtering
│   ├── substance_filters.py        # PFAS substance filtering
│   ├── material_filters.py         # Sample material type filtering
│   └── ui_components.py            # Shared UI widgets
│
├── data/                           # Static data files
│   ├── pfas_substances.csv         # PFAS substance definitions
│   ├── sample_material_types.csv   # Material type definitions
│   └── us_administrative_regions_fips.csv  # US FIPS codes for regions
│
└── assets/                         # Static assets
    └── Sawgraph-Logo-transparent.png  # SAWGraph project logo
```

## 🚀 How to Run

```bash
streamlit run app.py
```

The app will open in your browser with:
- **Landing page**: SAWGraph logo, project description, getting started guide
- **Sidebar**: Analysis type selector, geographic region filters, analysis-specific parameters

## 🏗️ Architecture

### Single-Page App with Analysis Registry

The app uses a **registry pattern** to manage analyses:

1. **`analysis_registry.py`**: Defines all available analyses with metadata
2. **`app.py`**: Main entry point that:
   - Loads shared data (FIPS codes, substances, material types)
   - Renders the sidebar with analysis selector and region filters
   - Dispatches to the selected analysis module
3. **`analyses/*.py`**: Individual analysis modules with `main(context)` functions

### AnalysisContext

Each analysis receives an `AnalysisContext` object containing:

```python
@dataclass
class AnalysisContext:
    # Shared data (loaded once)
    states_df: pd.DataFrame
    counties_df: pd.DataFrame
    subdivisions_df: pd.DataFrame
    substances_df: pd.DataFrame
    material_types_df: pd.DataFrame

    # Region selection
    selected_state_code: Optional[str]
    selected_state_name: Optional[str]
    selected_county_code: Optional[str]
    selected_county_name: Optional[str]
    region_code: str      # e.g., "23", "23005", or "2301104475"
    region_display: str   # e.g., "Maine" or "Penobscot County, Maine"

    # Configuration
    endpoints: dict       # SPARQL endpoint URLs
    project_dir: str
    analysis_key: str     # "upstream", "downstream", etc.
```

## ➕ Adding New Analyses

1. **Create the analysis module** in `analyses/`:
   ```python
   # analyses/my_new_analysis.py
   from analysis_registry import AnalysisContext

   def main(context: AnalysisContext) -> None:
       import streamlit as st
       st.header("My New Analysis")
       # Your analysis code here
   ```

2. **Register it** in `analysis_registry.py`:
   ```python
   AnalysisSpec(
       key="my_analysis",
       label="My New Analysis",
       title="🔬 My New Analysis",
       description="Description shown in the UI.",
       query=6,  # Unique query number
       enabled=True,
       runner=my_analysis_main,
   )
   ```

3. **Add lazy import** in `build_registry()`:
   ```python
   from analyses.my_new_analysis import main as my_analysis_main
   ```

## 🔧 Available Analyses

| Key | Label | Status |
|-----|-------|--------|
| `upstream` | PFAS Upstream Tracing | ✅ Enabled |
| `downstream` | PFAS Downstream Tracing | ✅ Enabled |
| `near_facilities` | Samples Near Facilities | ✅ Enabled |
| `regional` | Regional Contamination Overview | ✅ Enabled |
| `risk` | Facility Risk Assessment | ⚠️ Disabled (stub) |

## 🔧 Shared Utilities

### `utils/sparql_helpers.py`

```python
from utils.sparql_helpers import get_sparql_wrapper, convertToDataframe

sparql = get_sparql_wrapper('sawgraph')
sparql.setQuery(your_query)
result = sparql.query()
df = convertToDataframe(result)
```

### Available SPARQL Endpoints

- `'sawgraph'`: PFAS contamination observations
- `'spatial'`: Administrative boundaries and spatial relationships
- `'hydrology'`: Water flow networks (NHDPlus V2)
- `'fio'`: Industrial facilities (NAICS data)

### Filter Utilities

- **`region_filters.py`**: Geographic region selection (State → County → Subdivision)
- **`substance_filters.py`**: PFAS substance multi-select with search
- **`material_filters.py`**: Sample material type filtering

### Query Builders

- **`upstream_tracing_queries.py`**: Build upstream tracing SPARQL queries
- **`downstream_tracing_queries.py`**: Build downstream tracing SPARQL queries
- **`nearby_queries.py`**: Build samples-near-facilities queries

## 💡 Tips

1. **Use the context**: All shared data is pre-loaded in `AnalysisContext`
2. **Reuse filter utilities**: Use `region_filters`, `substance_filters`, etc.
3. **Query builders**: Use existing query builder functions for complex SPARQL
4. **Progressive disclosure**: Use `st.expander()` for debug info and advanced options
5. **Clear feedback**: Show progress with `st.spinner()` and status messages

