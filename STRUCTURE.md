# SAWGraph PFAS Explorer - Application Structure

## Directory Structure

```
streamlit/
├── app.py                          # Streamlit entrypoint (modular)
├── analysis_registry.py            # Registry + AnalysisContext for modular analyses
├── requirements.txt
├── README.md
├── SAWGraph_Y3_Demo_NearbyFacilities.ipynb
│
├── analyses/                       # Modular analysis implementations
│   ├── pfas_upstream.py            # PFAS upstream tracing
│   ├── pfas_downstream.py          # PFAS downstream tracing
│   ├── samples_near_facilities.py  # Samples near facilities
│   ├── sockg_sites.py              # SOCKG sites & facilities
│   ├── regional_overview.py        # Stub (coming soon)
│   └── facility_risk.py            # Stub (coming soon)
│
├── components/                     # Reusable UI components
│   └── start_page.py               # Optional landing page component
│
├── utils/                          # Shared utilities and query builders
│   ├── concentration_filters.py    # Concentration ranges + helpers
│   ├── downstream_tracing_queries.py # Downstream SPARQL queries
│   ├── material_filters.py         # Sample material filters
│   ├── nearby_queries.py           # Samples-near-facilities queries
│   ├── region_filters.py           # State/county/subdivision helpers
│   ├── sockg_queries.py            # SOCKG-specific queries
│   ├── sparql_helpers.py           # SPARQL helpers + boundary fetch
│   ├── substance_filters.py        # PFAS substance filters
│   ├── ui_components.py            # Shared UI widgets
│   └── upstream_tracing_queries.py # Upstream SPARQL queries
│
├── data/                           # Static data files
│   ├── pfas_substances.csv         # PFAS substance definitions
│   ├── sample_material_types.csv   # Material type definitions
│   └── us_administrative_regions_fips.csv  # US FIPS codes
│
└── assets/
    └── Sawgraph-Logo-transparent.png
```

## How to Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

The app opens in your browser (usually `http://localhost:8501`).

## Architecture (Current)

- **`app.py` is the entrypoint** and runs analyses via `analysis_registry.py`.
- Analysis results are stored in `st.session_state` and only update in the main panel after an explicit **Execute** click.
- **`analysis_registry.py` + `analyses/`** provide the modular analysis API (`AnalysisContext`) and runners used by `app.py`.

## AnalysisContext (Modular Analyses)

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
    selected_subdivision_code: Optional[str]
    selected_subdivision_name: Optional[str]
    region_code: str      # e.g., "23", "23005", or "2301104475"
    region_display: str   # e.g., "Maine" or "Penobscot County, Maine"

    # Configuration
    endpoints: dict       # SPARQL endpoint URLs
    project_dir: str
    analysis_key: str     # "upstream", "downstream", etc.
    query_number: int
```

## Available Analyses

| Key | Label | Status |
|-----|-------|--------|
| `upstream` | PFAS Upstream Tracing | Enabled |
| `downstream` | PFAS Downstream Tracing | Enabled |
| `near_facilities` | Samples Near Facilities | Enabled |
| `sockg_sites` | SOCKG Sites & Facilities | Enabled |
| `regional` | Regional Contamination Overview | Stub |
| `risk` | Facility Risk Assessment | Stub |

## SPARQL Endpoints (Used)

- `sawgraph`: PFAS contamination observations
- `spatial`: Administrative boundaries and spatial relationships
- `hydrology`: Water flow networks (NHDPlus V2)
- `fio`: Industrial facilities (NAICS data)
- `federation`: Federated endpoint used by consolidated queries

## Notes

- Use `utils/*` query builders to keep SPARQL logic centralized.
- Boundary overlays and results are cached in `st.session_state` after execution to prevent sidebar changes from mutating results until the next run.
