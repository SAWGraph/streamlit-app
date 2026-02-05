"""
Region Filtering and Selection
Unified module for geographic region selection (State → County → Subdivision)
Includes availability queries and UI components.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional, Tuple
import streamlit as st
import pandas as pd
import requests

from core.sparql import ENDPOINT_URLS, parse_sparql_results, execute_sparql_query


# =============================================================================
# CONSTANTS
# =============================================================================

ALASKA_STATE_CODE = "02"


# =============================================================================
# DATA CLASSES
# =============================================================================

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


@dataclass
class RegionConfig:
    """Configuration for region selector visibility and requirements."""
    state: Literal["required", "optional", "hidden"] = "optional"
    county: Literal["required", "optional", "hidden"] = "optional"
    subdivision: Literal["required", "optional", "hidden"] = "optional"
    availability_source: Literal["pfas", "sockg", None] = "pfas"


# =============================================================================
# AVAILABILITY QUERIES
# =============================================================================

def get_available_states() -> pd.DataFrame:
    """
    Get all states that have sample points with PFAS observations.
    Excludes Alaska (FIPS code 02).

    Returns:
        DataFrame with columns: ar1 (state URI), fips_code (2-digit state code)
    """
    query = """
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?ar1 WHERE {
    ?sp rdf:type coso:SamplePoint ;
        kwg-ont:sfWithin|kwg-ont:sfTouches ?ar3 .
    ?ar3 rdf:type kwg-ont:AdministrativeRegion_3 ;
         kwg-ont:administrativePartOf ?ar2 .
    ?ar2 rdf:type kwg-ont:AdministrativeRegion_2 ;
         kwg-ont:administrativePartOf ?ar1 .
    ?ar1 rdf:type kwg-ont:AdministrativeRegion_1 .
    ?observation rdf:type coso:ContaminantObservation ;
                coso:observedAtSamplePoint ?sp .
FILTER(STRSTARTS(STR(?ar1), "http://stko-kwg.geog.ucsb.edu/lod/resource/administrativeRegion.USA.")).
}
"""
    results = execute_sparql_query(ENDPOINT_URLS["federation"], query, timeout=120)
    if not results:
        return pd.DataFrame(columns=['ar1', 'fips_code'])

    df = parse_sparql_results(results)
    if df.empty:
        return df

    df['fips_code'] = df['ar1'].str.extract(r'administrativeRegion\.USA\.(\d+)')
    df['fips_code'] = df['fips_code'].astype(str).str.zfill(2)
    df = df[df['fips_code'] != ALASKA_STATE_CODE].reset_index(drop=True)

    return df[['ar1', 'fips_code']]


def get_available_counties(state_code: str) -> pd.DataFrame:
    """
    Get all counties in a given state that have sample points with observations.

    Args:
        state_code: 2-digit FIPS state code (e.g., "04" for Arizona, "23" for Maine)

    Returns:
        DataFrame with columns: ar2 (county URI), fips_code (5-digit county code)
    """
    state_code_str = str(state_code).zfill(2)
    if state_code_str == ALASKA_STATE_CODE:
        return pd.DataFrame(columns=['ar2', 'fips_code'])
    state_uri = f"<http://stko-kwg.geog.ucsb.edu/lod/resource/administrativeRegion.USA.{state_code_str}>"

    query = f"""
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?ar2 WHERE {{
    ?sp rdf:type coso:SamplePoint ;
        kwg-ont:sfWithin|kwg-ont:sfTouches ?ar3 .
    ?ar3 rdf:type kwg-ont:AdministrativeRegion_3 ;
         kwg-ont:administrativePartOf ?ar2 .
    ?ar2 rdf:type kwg-ont:AdministrativeRegion_2 ;
         kwg-ont:administrativePartOf {state_uri} .
    ?observation rdf:type coso:ContaminantObservation ;
                coso:observedAtSamplePoint ?sp .
FILTER(STRSTARTS(STR(?ar2), "http://stko-kwg.geog.ucsb.edu")).
}}
"""

    results = execute_sparql_query(ENDPOINT_URLS["federation"], query, timeout=120)
    if not results:
        return pd.DataFrame(columns=['ar2', 'fips_code'])

    df = parse_sparql_results(results)
    if df.empty:
        return df

    df['fips_code'] = df['ar2'].str.extract(r'administrativeRegion\.USA\.(\d+)')
    df['fips_code'] = df['fips_code'].astype(str).str.zfill(5)

    return df[['ar2', 'fips_code']]


def get_available_subdivisions(county_code: str) -> pd.DataFrame:
    """
    Get all county subdivisions in a given county that have sample points with observations.

    Args:
        county_code: 5-digit FIPS county code (e.g., "04013" for Maricopa County, AZ)

    Returns:
        DataFrame with columns: ar3 (subdivision URI), fips_code (10-digit subdivision code)
    """
    county_code_str = str(county_code).zfill(5)
    if county_code_str.startswith(ALASKA_STATE_CODE):
        return pd.DataFrame(columns=['ar3', 'fips_code'])
    county_uri = f"<http://stko-kwg.geog.ucsb.edu/lod/resource/administrativeRegion.USA.{county_code_str}>"

    query = f"""
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?ar3 WHERE {{
    ?sp rdf:type coso:SamplePoint ;
        kwg-ont:sfWithin|kwg-ont:sfTouches ?ar3 .
    ?ar3 rdf:type kwg-ont:AdministrativeRegion_3 ;
         kwg-ont:administrativePartOf {county_uri} .
    ?observation rdf:type coso:ContaminantObservation ;
                coso:observedAtSamplePoint ?sp .
    FILTER(STRSTARTS(STR(?ar3), "https://datacommons.org/browser/geoId/")).
}}
"""

    results = execute_sparql_query(ENDPOINT_URLS["federation"], query, timeout=120)
    if not results:
        return pd.DataFrame(columns=['ar3', 'fips_code'])

    df = parse_sparql_results(results)
    if df.empty:
        return df

    df['fips_code'] = df['ar3'].str.extract(r'geoId/(\d+)')
    df['fips_code'] = df['fips_code'].astype(str)

    return df[['ar3', 'fips_code']]


# =============================================================================
# BOUNDARY QUERY
# =============================================================================

def get_region_boundary(region_code: str) -> Optional[pd.DataFrame]:
    """
    Query the boundary geometry for a given administrative region.

    Args:
        region_code: FIPS code as string (2 digits=state, 5=county, >5=subdivision)

    Returns:
        DataFrame with columns: county (region URI), countyWKT (geometry), countyName (label)
        Returns None if query fails or no results
    """
    if len(str(region_code)) > 5:
        region_uri_pattern = f"VALUES ?county {{<https://datacommons.org/browser/geoId/{region_code}>}}"
    else:
        region_uri_pattern = f"VALUES ?county {{kwgr:administrativeRegion.USA.{region_code}}}"

    query = f"""
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT * WHERE {{
    ?county geo:hasGeometry/geo:asWKT ?countyWKT ;
            rdfs:label ?countyName.
    {region_uri_pattern}
}}
"""

    try:
        response = requests.post(
            ENDPOINT_URLS["federation"],
            data={"query": query},
            headers={
                "Accept": "application/sparql-results+json",
                "Content-Type": "application/x-www-form-urlencoded"
            },
            timeout=10
        )

        if response.status_code == 200:
            results = response.json()
            df = parse_sparql_results(results)
            if not df.empty:
                return df
        return None

    except Exception as e:
        print(f"Error querying boundary: {str(e)}")
        return None


# =============================================================================
# CACHED AVAILABILITY FUNCTIONS
# =============================================================================

@st.cache_data(ttl=3600)
def get_available_state_codes() -> set:
    """Get FIPS state codes that have PFAS observations."""
    df = get_available_states()
    if df.empty:
        return set()
    return set(df["fips_code"].astype(str).str.zfill(2).tolist())


@st.cache_data(ttl=3600)
def get_available_county_codes(state_code: str) -> set:
    """Get FIPS county codes with PFAS observations for a given state."""
    df = get_available_counties(state_code)
    if df.empty:
        return set()
    return set(df["fips_code"].astype(str).str.zfill(5).tolist())


@st.cache_data(ttl=3600)
def get_available_subdivision_codes(county_code: str) -> set:
    """Get FIPS subdivision codes with PFAS observations for a given county."""
    df = get_available_subdivisions(county_code)
    if df.empty:
        return set()
    return set(df["fips_code"].astype(str).str.zfill(10).tolist())


# =============================================================================
# UI COMPONENTS
# =============================================================================

def render_region_selector(
    config: RegionConfig,
    states_df: pd.DataFrame,
    counties_df: pd.DataFrame,
    subdivisions_df: pd.DataFrame,
    get_sockg_state_codes_fn=None,
) -> RegionSelection:
    """
    Render a configurable region selector.
    
    Args:
        config: RegionConfig specifying what to show/require
        states_df: DataFrame with state data
        counties_df: DataFrame with county data
        subdivisions_df: DataFrame with subdivision data
        get_sockg_state_codes_fn: Optional function to get SOCKG state codes
    
    Returns:
        RegionSelection with all selected values
    """
    st.sidebar.markdown("### 📍 Geographic Region")
    
    # Determine availability source
    if config.availability_source == "pfas":
        available_state_codes = get_available_state_codes()
    elif config.availability_source == "sockg" and get_sockg_state_codes_fn:
        available_state_codes = get_sockg_state_codes_fn()
    else:
        available_state_codes = set()
    
    # Show requirement message
    if config.state == "required" or config.county == "required":
        required_parts = []
        if config.state == "required":
            required_parts.append("state")
        if config.county == "required":
            required_parts.append("county")
        st.sidebar.markdown(f"🆃 **Required**: Select a {' and '.join(required_parts)}")
    else:
        st.sidebar.markdown("_Optional: select a state to filter results_")
    
    selection = RegionSelection()
    
    # STATE SELECTION
    if config.state != "hidden":
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
        
        # Use "All States" for optional, "Select a State" for required
        default_option = "-- All States --" if config.state == "optional" else "-- Select a State --"
        state_options = [default_option] + available_state_options + unavailable_state_options

        def on_state_change():
            selected = st.session_state.state_selector
            if selected.startswith("✗ "):
                rejected_state = selected.replace("✗ ", "")
                source_name = "SOCKG" if config.availability_source == "sockg" else "PFAS"
                st.session_state.state_rejected_msg = f"❌ {rejected_state} has no {source_name} data. Please select a state with ✓"
                st.session_state.state_selector = default_option

        if "state_rejected_msg" in st.session_state:
            st.sidebar.error(st.session_state.state_rejected_msg)
            del st.session_state.state_rejected_msg

        label = "1️⃣ Select State" if config.county != "hidden" else "🌍 Select State"
        if config.state == "required":
            label += " (Required)"
        else:
            label += " (Optional)"
            
        selected_state_display = st.sidebar.selectbox(
            label,
            state_options,
            key="state_selector",
            on_change=on_state_change,
            help="Select a US state with available data (✓ = has data)"
        )

        if selected_state_display not in ("-- Select a State --", "-- All States --") and not selected_state_display.startswith("✗ "):
            actual_state_name = state_name_map.get(selected_state_display, selected_state_display.replace("✓ ", ""))
            selection.state_name = actual_state_name
            state_row = states_df[states_df['state_name'] == actual_state_name]
            if not state_row.empty:
                selection.state_code = str(state_row.iloc[0]['fipsCode']).zfill(2)
                selection.state_has_data = True

    # COUNTY SELECTION
    if config.county != "hidden" and selection.state_code:
        state_counties = counties_df[counties_df['state_code'] == selection.state_code]
        state_subdivisions = subdivisions_df[subdivisions_df['state_code'] == selection.state_code]

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
                    st.session_state.county_rejected_msg = f"❌ {rejected_county} has no data. Please select a county with ✓"
                    st.session_state.county_selector = "-- Select a County --"

            if "county_rejected_msg" in st.session_state:
                st.sidebar.error(st.session_state.county_rejected_msg)
                del st.session_state.county_rejected_msg

            county_label = "2️⃣ Select County"
            if config.county == "required":
                county_label += " (Required)"
            else:
                county_label += " (Optional)"
                
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
        else:
            st.sidebar.info(f"ℹ️ No county-level data available for {selection.state_name}.")
    elif config.county != "hidden" and not selection.state_code:
        st.sidebar.info("👆 Please select a state first")

    # SUBDIVISION SELECTION
    if config.subdivision != "hidden" and selection.state_code and selection.county_code:
        county_subdivisions = subdivisions_df[
            subdivisions_df['county_code'] == selection.county_code
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
                    st.session_state.subdivision_rejected_msg = f"❌ {rejected_subdivision} has no data. Please select a subdivision with ✓"
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

    st.sidebar.markdown("---")
    return selection


# Legacy function for backward compatibility
def render_pfas_region_selector(
    states_df: pd.DataFrame,
    counties_df: pd.DataFrame,
    subdivisions_df: pd.DataFrame,
    region_required: bool = False,
) -> RegionSelection:
    """Backward-compatible PFAS region selector."""
    config = RegionConfig(
        state="required" if region_required else "optional",
        county="required" if region_required else "optional",
        subdivision="optional",
        availability_source="pfas",
    )
    return render_region_selector(config, states_df, counties_df, subdivisions_df)
