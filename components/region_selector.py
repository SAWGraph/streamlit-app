"""
Region Selector Component
Cascading geographic region selection: State → County → Subdivision
Extracted from app.py to keep the main entry point simple.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import streamlit as st
import pandas as pd

from components.data_loader import (
    get_available_state_codes,
    get_available_county_codes,
    get_available_subdivision_codes,
)


@dataclass
class RegionSelection:
    """Container for the selected geographic region."""
    state_code: Optional[str] = None
    state_name: Optional[str] = None
    county_code: Optional[str] = None
    county_name: Optional[str] = None
    subdivision_code: Optional[str] = None
    subdivision_name: Optional[str] = None
    state_has_data: bool = False

    @property
    def region_code(self) -> str:
        """Get the most specific region code available."""
        if self.subdivision_code:
            return self.subdivision_code
        if self.county_code:
            return str(self.county_code).zfill(5)
        if self.state_code:
            return str(self.state_code).zfill(2)
        return ""

    @property
    def region_display(self) -> str:
        """Get a human-readable display string for the selected region."""
        parts = []
        if self.subdivision_name:
            parts.append(self.subdivision_name)
        if self.county_name:
            parts.append(self.county_name)
        if self.state_name:
            parts.append(self.state_name)
        return ", ".join(parts) if parts else "No region selected"


def render_pfas_region_selector(
    states_df: pd.DataFrame,
    counties_df: pd.DataFrame,
    subdivisions_df: pd.DataFrame,
    region_required: bool = False,
) -> RegionSelection:
    """
    Render the full cascading region selector for PFAS analyses.
    State → County → Subdivision with availability markers.

    Args:
        states_df: DataFrame with state data
        counties_df: DataFrame with county data
        subdivisions_df: DataFrame with subdivision data
        region_required: If True, shows "Required" labels for state/county

    Returns:
        RegionSelection with all selected values
    """
    st.sidebar.markdown("### 📍 Geographic Region")
    if region_required:
        st.sidebar.markdown("🆃 **Required**: Select a state and county")
    else:
        st.sidebar.markdown("Optional: select a state and county to limit results")

    selection = RegionSelection()

    # Get states with PFAS data available
    available_state_codes = get_available_state_codes()

    # Build state options with availability markers
    state_name_map = {}  # Map display name (with marker) to actual name
    available_state_options = []
    unavailable_state_options = []
    for _, row in states_df.sort_values("state_name").iterrows():
        state_name = row["state_name"]
        state_code = str(row["fipsCode"]).zfill(2)
        if state_code in available_state_codes:
            display_name = f"✓ {state_name}"
            available_state_options.append(display_name)
        else:
            display_name = f"✗ {state_name}"
            unavailable_state_options.append(display_name)
        state_name_map[display_name] = state_name
    state_options = ["-- Select a State --"] + available_state_options + unavailable_state_options

    # Callback to handle invalid state selection
    def on_state_change():
        selected = st.session_state.state_selector
        if selected.startswith("✗ "):
            rejected_state = selected.replace("✗ ", "")
            st.session_state.state_rejected_msg = f"❌ {rejected_state} has no PFAS data. Please select a state with ✓"
            st.session_state.state_selector = "-- Select a State --"

    # Show rejection message if exists
    if "state_rejected_msg" in st.session_state:
        st.sidebar.error(st.session_state.state_rejected_msg)
        del st.session_state.state_rejected_msg

    # 1. STATE SELECTION
    selected_state_display = st.sidebar.selectbox(
        "1️⃣ Select State",
        state_options,
        key="state_selector",
        on_change=on_state_change,
        help="Select a US state with available sample data (✓ = has data)"
    )

    # Get the selected state's FIPS code
    if selected_state_display != "-- Select a State --" and not selected_state_display.startswith("✗ "):
        actual_state_name = state_name_map.get(selected_state_display, selected_state_display.replace("✓ ", ""))
        selection.state_name = actual_state_name
        state_row = states_df[states_df['state_name'] == actual_state_name]
        if not state_row.empty:
            selection.state_code = str(state_row.iloc[0]['fipsCode']).zfill(2)
            st.session_state.selected_state = {
                'name': selection.state_name,
                'code': selection.state_code
            }
            selection.state_has_data = True

    # 2. COUNTY SELECTION (Optional, filtered by state)
    state_subdivisions = pd.DataFrame()
    state_counties = pd.DataFrame()

    if selection.state_code:
        state_counties = counties_df[
            counties_df['state_code'] == selection.state_code
        ]
        state_subdivisions = subdivisions_df[
            subdivisions_df['state_code'] == selection.state_code
        ]

        if not state_counties.empty:
            available_county_codes = get_available_county_codes(selection.state_code)
            county_options = ["-- Select a County --"]
            county_name_map = {}

            for _, row in state_counties.sort_values('county_name').iterrows():
                county_name = row['county_name']
                county_code = str(row['county_code']).zfill(5)
                if county_code in available_county_codes:
                    display_name = f"✓ {county_name}"
                else:
                    display_name = f"✗ {county_name}"
                county_options.append(display_name)
                county_name_map[display_name] = county_name

            def on_county_change():
                selected = st.session_state.county_selector
                if selected.startswith("✗ "):
                    rejected_county = selected.replace("✗ ", "")
                    st.session_state.county_rejected_msg = (
                        f"❌ {rejected_county} has no PFAS data. Please select a county with ✓"
                    )
                    st.session_state.county_selector = "-- Select a County --"

            if "county_rejected_msg" in st.session_state:
                st.sidebar.error(st.session_state.county_rejected_msg)
                del st.session_state.county_rejected_msg

            county_label = "2️⃣ Select County (Required)" if region_required else "2️⃣ Select County (Optional)"
            selected_county_display = st.sidebar.selectbox(
                county_label,
                county_options,
                key="county_selector",
                on_change=on_county_change,
                help=f"Select a county within {selection.state_name}"
            )

            if selected_county_display != "-- Select a County --" and not selected_county_display.startswith("✗ "):
                selection.county_name = county_name_map.get(
                    selected_county_display,
                    selected_county_display.replace("✓ ", "")
                )
                county_row = state_counties[state_counties['county_name'] == selection.county_name]
                if not county_row.empty:
                    selection.county_code = str(county_row.iloc[0]['county_code']).zfill(5)
                st.session_state.selected_county = selection.county_name
                st.session_state.selected_county_code = selection.county_code
            else:
                st.session_state.selected_county = None
                st.session_state.selected_county_code = None
        else:
            st.sidebar.info(f"ℹ️ No county-level data available for {selection.state_name}.")
    else:
        st.sidebar.info("👆 Please select a state first")

    # 3. SUBDIVISION SELECTION (Optional, filtered by county)
    if selection.state_code and selection.county_code:
        county_subdivisions = state_subdivisions[
            state_subdivisions['county_code'] == selection.county_code
        ]

        if not county_subdivisions.empty:
            available_subdivision_codes = get_available_subdivision_codes(selection.county_code)
            subdivision_name_map = {}
            available_subdivision_options = []
            unavailable_subdivision_options = []

            for _, row in county_subdivisions.sort_values('subdivision_name').iterrows():
                subdivision_name = row['subdivision_name']
                subdivision_code = str(row['fipsCode']).zfill(10)
                if subdivision_code in available_subdivision_codes:
                    display_name = f"✓ {subdivision_name}"
                    available_subdivision_options.append(display_name)
                else:
                    display_name = f"✗ {subdivision_name}"
                    unavailable_subdivision_options.append(display_name)
                subdivision_name_map[display_name] = subdivision_name

            subdivision_options = (
                ["-- All Subdivisions --"]
                + available_subdivision_options
                + unavailable_subdivision_options
            )

            def on_subdivision_change():
                selected = st.session_state.subdivision_selector
                if selected.startswith("✗ "):
                    rejected_subdivision = selected.replace("✗ ", "")
                    st.session_state.subdivision_rejected_msg = (
                        f"❌ {rejected_subdivision} has no PFAS data. Please select a subdivision with ✓"
                    )
                    st.session_state.subdivision_selector = "-- All Subdivisions --"

            if "subdivision_rejected_msg" in st.session_state:
                st.sidebar.error(st.session_state.subdivision_rejected_msg)
                del st.session_state.subdivision_rejected_msg

            selected_subdivision_display = st.sidebar.selectbox(
                "3️⃣ Select Subdivision (Optional)",
                subdivision_options,
                key="subdivision_selector",
                on_change=on_subdivision_change,
                help=f"Select a subdivision within {selection.county_name}"
            )

            if (
                selected_subdivision_display != "-- All Subdivisions --"
                and not selected_subdivision_display.startswith("✗ ")
            ):
                selection.subdivision_name = subdivision_name_map.get(
                    selected_subdivision_display,
                    selected_subdivision_display.replace("✓ ", "")
                )
                subdivision_row = county_subdivisions[
                    county_subdivisions['subdivision_name'] == selection.subdivision_name
                ]
                if not subdivision_row.empty:
                    selection.subdivision_code = str(subdivision_row.iloc[0]['fipsCode']).zfill(10)
                st.session_state.selected_subdivision = {
                    'name': selection.subdivision_name,
                    'code': selection.subdivision_code
                }
            else:
                st.session_state.selected_subdivision = None
        else:
            st.sidebar.info(f"ℹ️ No subdivisions found for {selection.county_name}")
    elif selection.state_code and selection.county_name:
        st.sidebar.info("No subdivisions available for this county")
        st.session_state.selected_subdivision = None

    st.sidebar.markdown("---")
    return selection


def render_sockg_region_selector(states_df: pd.DataFrame) -> RegionSelection:
    """
    Render a state-only selector for SOCKG analysis.
    Uses SOCKG data availability (not PFAS) to mark states.

    Args:
        states_df: DataFrame with state data

    Returns:
        RegionSelection with state selection only
    """
    st.sidebar.markdown("### 📍 Geographic Region")
    st.sidebar.markdown("Optional: select a state to filter results")

    selection = RegionSelection()

    # Get states with SOCKG data (uses the SOCKG-specific query)
    from utils.sockg_queries import get_sockg_state_codes
    sockg_states_df = get_sockg_state_codes()
    available_state_codes = set(sockg_states_df['fips_code'].tolist()) if not sockg_states_df.empty else set()

    # Build state options with SOCKG availability markers
    state_name_map = {}
    available_state_options = []
    unavailable_state_options = []
    for _, row in states_df.sort_values("state_name").iterrows():
        state_name = row["state_name"]
        state_code = str(row["fipsCode"]).zfill(2)
        if state_code in available_state_codes:
            display_name = f"✓ {state_name}"
            available_state_options.append(display_name)
        else:
            display_name = f"✗ {state_name}"
            unavailable_state_options.append(display_name)
        state_name_map[display_name] = state_name
    state_options = ["-- Select a State --"] + available_state_options + unavailable_state_options

    # Callback to handle invalid state selection
    def on_state_change():
        selected = st.session_state.sockg_state_selector
        if selected.startswith("✗ "):
            rejected_state = selected.replace("✗ ", "")
            st.session_state.sockg_state_rejected_msg = f"❌ {rejected_state} has no SOCKG data. Please select a state with ✓"
            st.session_state.sockg_state_selector = "-- Select a State --"

    # Show rejection message if exists
    if "sockg_state_rejected_msg" in st.session_state:
        st.sidebar.error(st.session_state.sockg_state_rejected_msg)
        del st.session_state.sockg_state_rejected_msg

    # STATE SELECTION (no county for SOCKG)
    selected_state_display = st.sidebar.selectbox(
        "🌍 Select State",
        state_options,
        key="sockg_state_selector",
        on_change=on_state_change,
        help="Select a US state with SOCKG data (✓ = has SOCKG sites)"
    )

    # Get the selected state's FIPS code
    if selected_state_display != "-- Select a State --" and not selected_state_display.startswith("✗ "):
        actual_state_name = state_name_map.get(selected_state_display, selected_state_display.replace("✓ ", ""))
        selection.state_name = actual_state_name
        state_row = states_df[states_df['state_name'] == actual_state_name]
        if not state_row.empty:
            selection.state_code = str(state_row.iloc[0]['fipsCode']).zfill(2)
            st.session_state.selected_state = {
                'name': selection.state_name,
                'code': selection.state_code
            }
            selection.state_has_data = True

    st.sidebar.markdown("---")
    return selection


def render_region_selector(
    query_number: int,
    states_df: pd.DataFrame,
    counties_df: pd.DataFrame,
    subdivisions_df: pd.DataFrame,
) -> RegionSelection:
    """
    Main entry point for rendering the appropriate region selector.

    Args:
        query_number: The query type (1, 2, 5, 6, etc.)
        states_df: DataFrame with state data
        counties_df: DataFrame with county data
        subdivisions_df: DataFrame with subdivision data

    Returns:
        RegionSelection with all selected values
    """
    # Initialize session state for region selection
    if 'selected_state' not in st.session_state:
        st.session_state.selected_state = None
    if 'selected_county' not in st.session_state:
        st.session_state.selected_county = None
    if 'selected_subdivision' not in st.session_state:
        st.session_state.selected_subdivision = None

    st.sidebar.header("🔧 Analysis Configuration")

    # SOCKG analysis (query 6) uses state-only selector with SOCKG data markers
    if query_number == 6:
        return render_sockg_region_selector(states_df)

    # All other analyses use the cascading PFAS region selector
    # Query 1 and 5 require region selection; others are optional
    region_required = query_number in (1, 5)
    return render_pfas_region_selector(
        states_df,
        counties_df,
        subdivisions_df,
        region_required=region_required,
    )
