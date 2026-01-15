from __future__ import annotations

import streamlit as st

from analysis_registry import AnalysisContext, build_registry
from components.data_loader import (
    ENDPOINT_URLS,
    PROJECT_DIR,
    get_sockg_state_code_set,
    load_fips_data,
    load_material_types_data,
    load_substances_data,
    parse_regions,
)
from components.region_selector import RegionSelection, render_pfas_region_selector
from components.start_page import render_start_page


def _set_page_config() -> None:
    st.set_page_config(
        page_title="SAWGraph PFAS Explorer",
        page_icon="assets/Sawgraph-Logo-transparent.png",
        layout="wide",
        initial_sidebar_state="expanded",
    )


def _render_sockg_state_only_selector(states_df) -> RegionSelection:
    """SOCKG only supports an optional state filter (no county/subdivision selector)."""
    st.sidebar.markdown("### 📍 Geographic Region")
    st.sidebar.markdown("_Optional: select a state to filter SOCKG sites_")

    available_sockg_states = get_sockg_state_code_set()

    state_name_map: dict[str, str] = {}
    available_state_options: list[str] = []
    unavailable_state_options: list[str] = []

    for _, row in states_df.sort_values("state_name").iterrows():
        state_name = row["state_name"]
        state_code = str(row["fipsCode"]).zfill(2)
        if state_code in available_sockg_states:
            display_name = f"✓ {state_name}"
            available_state_options.append(display_name)
        else:
            display_name = f"✗ {state_name}"
            unavailable_state_options.append(display_name)
        state_name_map[display_name] = state_name

    state_options = ["-- All States --"] + available_state_options + unavailable_state_options

    def on_state_change() -> None:
        selected = st.session_state.sockg_state_selector
        if selected.startswith("✗ "):
            rejected_state = selected.replace("✗ ", "")
            st.session_state.sockg_state_rejected_msg = (
                f"❌ {rejected_state} has no SOCKG sites. Please select a state with ✓"
            )
            st.session_state.sockg_state_selector = "-- All States --"

    if "sockg_state_rejected_msg" in st.session_state:
        st.sidebar.error(st.session_state.sockg_state_rejected_msg)
        del st.session_state.sockg_state_rejected_msg

    selected_state_display = st.sidebar.selectbox(
        "Select State (Optional)",
        state_options,
        key="sockg_state_selector",
        on_change=on_state_change,
    )

    region = RegionSelection()
    if selected_state_display != "-- All States --" and not selected_state_display.startswith("✗ "):
        actual_state_name = state_name_map.get(
            selected_state_display, selected_state_display.replace("✓ ", "")
        )
        region.state_name = actual_state_name
        state_row = states_df[states_df["state_name"] == actual_state_name]
        if not state_row.empty:
            region.state_code = str(state_row.iloc[0]["fipsCode"]).zfill(2)

    return region


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

    selected_key = label_to_key.get(analysis_label)
    # Only Upstream requires county selection; Downstream county is optional.
    region_required = selected_key in {"upstream"}

    if selected_key == "sockg_sites":
        region = _render_sockg_state_only_selector(states_df)
    else:
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
    spec.runner(context)


if __name__ == "__main__":
    main()

