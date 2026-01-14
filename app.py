from __future__ import annotations

import streamlit as st

from analysis_registry import AnalysisContext, build_registry
from components.data_loader import (
    ENDPOINT_URLS,
    PROJECT_DIR,
    load_fips_data,
    load_material_types_data,
    load_substances_data,
    parse_regions,
)
from components.region_selector import render_pfas_region_selector
from components.start_page import render_start_page


def _set_page_config() -> None:
    st.set_page_config(
        page_title="SAWGraph PFAS Explorer",
        page_icon="assets/Sawgraph-Logo-transparent.png",
        layout="wide",
        initial_sidebar_state="expanded",
    )


def main() -> None:
    _set_page_config()

    # Load shared data once (cached in components/data_loader.py)
    fips_df = load_fips_data()
    states_df, counties_df, subdivisions_df = parse_regions(fips_df)
    substances_df = load_substances_data()
    material_types_df = load_material_types_data()
    
    registry = build_registry()
    enabled_specs = [s for s in registry.values() if s.enabled]
    enabled_specs.sort(key=lambda s: s.label)

    st.sidebar.markdown("### 📊 Select Analysis Type")

    if "analysis_selector_modular" not in st.session_state:
        st.session_state.analysis_selector_modular = "-- Home --"

    home_spacer, home_col = st.sidebar.columns([5, 1])
    with home_col:
        if st.button("🏠", help="Return to the homepage", key="home_btn_modular"):
            st.session_state.analysis_selector_modular = "-- Home --"
            st.rerun()

    label_to_key = {s.label: s.key for s in enabled_specs}
    analysis_label = st.sidebar.selectbox(
        "Choose analysis:",
        ["-- Home --"] + [s.label for s in enabled_specs],
        key="analysis_selector_modular",
    )

    st.sidebar.markdown("---")

    # Decide whether to label region selection as required for the selected analysis
    selected_key = label_to_key.get(analysis_label)
    region_required = selected_key in {"upstream", "downstream"}

    region = render_pfas_region_selector(
        states_df=states_df,
        counties_df=counties_df,
        subdivisions_df=subdivisions_df,
        region_required=region_required,
    )

    st.sidebar.markdown("---")
        
    if analysis_label == "-- Home --" or not selected_key:
        render_start_page(PROJECT_DIR)
        return

    spec = registry[selected_key]

    # Build AnalysisContext expected by `analyses/*`
    context = AnalysisContext(
        states_df=states_df,
        counties_df=counties_df,
        subdivisions_df=subdivisions_df,
        substances_df=substances_df,
        material_types_df=material_types_df,
        selected_state_code=region.state_code,
        selected_state_name=region.state_name,
        selected_county_code=region.county_code,
        selected_county_name=region.county_name,
        selected_subdivision_code=region.subdivision_code,
        selected_subdivision_name=region.subdivision_name,
        region_code=region.region_code,
        region_display=region.region_display,
        endpoints=ENDPOINT_URLS,
        project_dir=PROJECT_DIR,
        analysis_key=spec.key,
        query_number=spec.query,
    )

    st.markdown(f"## {spec.title}")
    st.caption(spec.description)

    # Delegate to the modular analysis implementation
    spec.runner(context)


if __name__ == "__main__":
    main()

