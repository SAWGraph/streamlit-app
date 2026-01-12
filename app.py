





"""
Administrative Region Selector with Cascading Dropdowns
Allows users to select State → County → Subdivision
"""

import streamlit as st
import pandas as pd
import requests
import sys
import os
import folium
import time
import math
from streamlit_folium import st_folium
import geopandas as gpd
from shapely import wkt

# Import query modules
from utils.nearby_queries import NAICS_INDUSTRIES, execute_nearby_analysis, get_pfas_industries
from utils.ui_components import render_hierarchical_naics_selector
from utils.downstream_tracing_queries import (
    execute_downstream_hydrology_query,
    execute_downstream_samples_query,
    execute_downstream_step1_query,
)
from utils.substance_filters import get_available_substances_with_labels
from utils.material_filters import get_available_material_types_with_labels
from utils.concentration_filters import get_max_concentration
from utils.region_filters import (
    get_available_states as fetch_available_states,
    get_available_counties as fetch_available_counties,
    get_available_subdivisions as fetch_available_subdivisions,
    omit_alaska_regions,
)
from utils.sockg_queries import (
    get_sockg_state_codes as fetch_sockg_state_codes,
    get_sockg_locations,
    get_sockg_facilities,
)

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

# SPARQL Endpoints
ENDPOINT_URLS = {
    'sawgraph': "https://frink.apps.renci.org/sawgraph/sparql",
    'spatial': "https://frink.apps.renci.org/spatialkg/sparql",
    'hydrology': "https://frink.apps.renci.org/hydrologykg/sparql",
    'fio': "https://frink.apps.renci.org/fiokg/sparql",
    'federation': "https://frink.apps.renci.org/federation/sparql"
}

# Page configuration
st.set_page_config(
    page_title="SAWGraph PFAS Explorer",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Ensure the app starts on the homepage for a new session
if "analysis_selector" not in st.session_state:
    st.session_state.analysis_selector = "-- Select an Analysis --"

# SIDEBAR: Home button at the top-right
home_spacer, home_col = st.sidebar.columns([5, 1])
with home_col:
    if st.button("🏠", help="Return to the homepage", key="home_btn"):
        st.session_state.analysis_selector = "-- Select an Analysis --"
        st.rerun()

# SIDEBAR: Analysis Selection
st.sidebar.markdown("### 📊 Select Analysis Type")
analysis_type = st.sidebar.selectbox(
    "Choose analysis:",
    [
        "-- Select an Analysis --",
        "PFAS Upstream Tracing",
        "PFAS Downstream Tracing",
        "Samples Near Facilities",
        "SOCKG Sites & Facilities"
    ],
    help="Choose the type of analysis you want to perform",
    key="analysis_selector"
)

st.sidebar.markdown("---")

# Map analysis type to query number
analysis_map = {
    "-- Select an Analysis --": None,
    "PFAS Upstream Tracing": 1,
    "PFAS Downstream Tracing": 5,
    "Samples Near Facilities": 2,
    "SOCKG Sites & Facilities": 6,
}
query_number = analysis_map.get(analysis_type)

# Title based on selection
if query_number is None:
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        logo_path = os.path.join(PROJECT_DIR, "assets", "Sawgraph-Logo-transparent.png")
        if os.path.exists(logo_path):
            st.markdown(
                "<div style='background-color: white; padding: 20px; border-radius: 10px; text-align: center;'>",
                unsafe_allow_html=True
            )
            st.image(logo_path, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(
            """
            <div style='text-align: center; padding: 12px;'>
                <p style='font-size: 1.05em; line-height: 1.6;'>
                    This app is developed as part of the project
                    <strong>"Safe Agricultural Products and Water Graph (SAWGraph):
                    An Open Knowledge Network to Monitor and Trace PFAS and Other Contaminants
                    in the Nation's Food and Water Systems"</strong>.
                </p>
                <p style='font-size: 1em; margin-top: 16px;'>
                    <a href='https://sawgraph.github.io' target='_blank' style='color: #1f77b4; text-decoration: none;'>
                        Learn more at sawgraph.github.io
                    </a>
                </p>
            </div>
            """,
            unsafe_allow_html=True
        )
elif query_number == 1:
    st.title("🌊 PFAS Upstream Tracing")
elif query_number == 5:
    st.title("⬇️ PFAS Downstream Tracing")
elif query_number == 2:
    st.title("🏭 Samples Near Facilities")
elif query_number == 6:
    st.title("🧪 SOCKG Sites & Facilities")
elif query_number == 3:
    st.title("📊 Regional Contamination Overview")
elif query_number == 4:
    st.title("⚠️ Facility Risk Assessment")

# Helper functions for SPARQL processing (moved before if/elif blocks)
def parse_sparql_results(results):
    """Convert SPARQL JSON results to DataFrame"""
    variables = results['head']['vars']
    bindings = results['results']['bindings']

    data = []
    for binding in bindings:
        row = {}
        for var in variables:
            if var in binding:
                row[var] = binding[var]['value']
            else:
                row[var] = None
        data.append(row)

    return pd.DataFrame(data)


def build_substance_options(substances_df: pd.DataFrame) -> tuple[list, dict]:
    """Build substance options from w3id query results."""
    if substances_df.empty:
        return ["-- All Substances --"], {}

    display_map = {}
    for _, row in substances_df.iterrows():
        display_name = row["display_name"]
        uri = row["substance"]
        if display_name not in display_map or uri.endswith("_A"):
            display_map[display_name] = uri

    options = ["-- All Substances --"] + sorted(display_map.keys())
    return options, display_map


def build_material_type_options(material_types_df: pd.DataFrame) -> tuple[list, dict]:
    """Build material type options from w3id query results."""
    if material_types_df.empty:
        return ["-- All Material Types --"], {}

    display_map = {}
    for _, row in material_types_df.iterrows():
        display_name = row["display_name"]
        uri = row["matType"]
        display_map.setdefault(display_name, uri)

    options = ["-- All Material Types --"] + sorted(display_map.keys())
    return options, display_map


@st.cache_data(ttl=3600)
def get_max_concentration_value(
    region_code: str,
    is_subdivision: bool,
    substance_uri: str | None,
    material_uri: str | None,
) -> float | None:
    """Get max concentration for the selected filters (ng/L)."""
    return get_max_concentration(
        region_code=region_code,
        is_subdivision=is_subdivision,
        substance_uri=substance_uri,
        material_uri=material_uri,
    )


@st.cache_data(ttl=3600)
def get_available_state_codes() -> set:
    """Get FIPS state codes that have PFAS observations."""
    df = fetch_available_states()
    if df.empty:
        return set()
    return set(df["fips_code"].astype(str).str.zfill(2).tolist())


@st.cache_data(ttl=3600)
def get_available_county_codes(state_code: str) -> set:
    """Get FIPS county codes with PFAS observations for a given state."""
    df = fetch_available_counties(state_code)
    if df.empty:
        return set()
    return set(df["fips_code"].astype(str).str.zfill(5).tolist())


@st.cache_data(ttl=3600)
def get_available_subdivision_codes(county_code: str) -> set:
    """Get FIPS subdivision codes with PFAS observations for a given county."""
    df = fetch_available_subdivisions(county_code)
    if df.empty:
        return set()
    return set(df["fips_code"].astype(str).str.zfill(10).tolist())


@st.cache_data(ttl=3600)
def get_pfas_industry_options() -> dict:
    """Fetch PFAS-related NAICS industries for the dropdown."""
    df = get_pfas_industries()
    if df.empty:
        return {}

    options = {}
    for _, row in df.iterrows():
        group_code = str(row.get("NAICS", "")).strip()
        industry_code = str(row.get("industryCodeId", "")).strip()
        sector = row.get("industrySector", "")
        group = row.get("industryGroup", "")
        industry_name = row.get("industryName", "")

        if group_code:
            group_label = f"{sector}: {group}" if sector or group else f"NAICS {group_code}"
            options.setdefault(group_code, group_label)

        if industry_code:
            industry_label = industry_name or f"NAICS {industry_code}"
            options.setdefault(industry_code, industry_label)
    return options


@st.cache_data(ttl=3600)
def get_sockg_state_code_set() -> set:
    """Get FIPS state codes that have SOCKG locations."""
    df = fetch_sockg_state_codes()
    if df.empty:
        return set()
    return set(df["fips_code"].astype(str).str.zfill(2).tolist())


@st.cache_data(ttl=3600)
def get_sockg_locations_cached(state_code: str | None) -> pd.DataFrame:
    """Get SOCKG locations (optionally filtered by state)."""
    return get_sockg_locations(state_code)


@st.cache_data(ttl=3600)
def get_sockg_facilities_cached(state_code: str | None) -> pd.DataFrame:
    """Get SOCKG facilities near SOCKG locations (optionally filtered by state)."""
    return get_sockg_facilities(state_code)


# Load the FIPS data
@st.cache_data
def load_fips_data():
    """Load and parse the FIPS codes CSV"""
    csv_path = os.path.join(PROJECT_DIR, "data", "us_administrative_regions_fips.csv")
    df = pd.read_csv(csv_path)
    return df

# Load the substances data
@st.cache_data
def load_substances_data():
    """Load and parse the PFAS substances CSV"""
    csv_path = os.path.join(PROJECT_DIR, "data", "pfas_substances.csv")
    df = pd.read_csv(csv_path)
    return df

# Load the material types data
@st.cache_data
def load_material_types_data():
    """Load and parse the sample material types CSV"""
    csv_path = os.path.join(PROJECT_DIR, "data", "sample_material_types.csv")
    df = pd.read_csv(csv_path)
    return df

# Query region boundary geometry
def get_region_boundary(region_code):
    """
    Query the boundary geometry for a given administrative region.
    Matches the notebook's approach using SPARQL.
    
    Args:
        region_code: FIPS code as string (2 digits=state, 5=county, >5=subdivision)
    
    Returns:
        DataFrame with columns: county (region URI), countyWKT (geometry), countyName (label)
        Returns None if query fails or no results
    """
    print(f"\\n--- Querying boundary for region: {region_code} ---")
    
    # Determine query pattern based on region code length (same logic as notebook)
    if len(str(region_code)) > 5:
        # Subdivision - use DataCommons URI
        region_uri_pattern = f"VALUES ?county {{<https://datacommons.org/browser/geoId/{region_code}>}}"
    else:
        # State or County - use KWG URI
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
    
    sparql_endpoint = ENDPOINT_URLS["federation"]
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    try:
        response = requests.post(
            sparql_endpoint,
            data={"query": query},
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            results = response.json()
            df = parse_sparql_results(results)
            if not df.empty:
                print(f"   > Successfully retrieved boundary for: {df.iloc[0].get('countyName', region_code)}")
                return df
            else:
                print(f"   > No boundary found for region: {region_code}")
                return None
        else:
            print(f"   > Boundary query failed with status {response.status_code}")
            return None
            
    except Exception as e:
        print(f"   > Error querying boundary: {str(e)}")
        return None

# Combined query: samples, upstream flowlines, and facilities

def execute_combined_query(substance_uri, material_uri, min_conc, max_conc, region_code, include_nondetects=False):
    """
    Run a single SPARQL query (inspired by the notebook workflow) that
    retrieves contaminated samples, upstream hydrological cells/flowlines,
    and potential facilities in one pass.
    """
    print("\n--- Running combined upstream tracing query (federation endpoint) ---")
    print(f"Finding samples and facilities in region: {region_code}")
    print(f"Substance URI: {substance_uri}")
    print(f"Material Type URI: {material_uri}")
    print(f"Concentration range: {min_conc} - {max_conc} ng/L")

    def build_values_clause(var_name, uri_value):
        if not uri_value:
            return ""
        return f"VALUES ?{var_name} {{<{uri_value}>}}"

    substance_filter = build_values_clause("substance", substance_uri)
    material_filter = build_values_clause("matType", material_uri)
    min_conc = float(min_conc)
    max_conc = float(max_conc)

    sanitized_region = str(region_code).strip()
    print("this is the sanitized region: ", sanitized_region)
    
    # Validate region code
    if not sanitized_region or sanitized_region == "" or sanitized_region.lower() == "none":
        error_msg = "Invalid region code. Please select a state before executing the query."
        return None, error_msg, {"error": error_msg}
    
    # Region filter logic based on code length
    # Counties are 5 digits, subdivisions are >5 digits (typically 10), states are 2 digits
    if len(sanitized_region) > 5:
        # Subdivision (city/town) level - use DataCommons URI directly
        region_pattern = f"VALUES ?ar3 {{<https://datacommons.org/browser/geoId/{sanitized_region}>}}"
    else:
        # State or County level - use administrative hierarchy
        region_pattern = f"?ar3 rdf:type kwg-ont:AdministrativeRegion_3 ; kwg-ont:administrativePartOf+ kwgr:administrativeRegion.USA.{sanitized_region} ."

    query = f"""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX fio: <http://w3id.org/fio/v1/fio#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX me_egad: <http://sawgraph.spatialai.org/v1/me-egad#>
PREFIX me_egad_data: <http://sawgraph.spatialai.org/v1/me-egad-data#>

SELECT DISTINCT (COUNT(DISTINCT ?subVal) as ?resultCount) (MAX(?result_value) as ?max) 
                ?sp ?spWKT ?upstream_flowlineWKT ?facility ?facWKT ?facilityName 
                ?industryName ?industryCode ?industryGroup ?industryGroupName ?industrySubsector ?industrySubsectorName
WHERE {{
    ?sp rdf:type coso:SamplePoint ;
        geo:hasGeometry/geo:asWKT ?spWKT ;
        spatial:connectedTo ?ar3 ;
        spatial:connectedTo ?s2 .
    
    {region_pattern}
    
    ?s2 rdf:type kwg-ont:S2Cell_Level13 .
    
    ?s2cell rdf:type kwg-ont:S2Cell_Level13 ;
             kwg-ont:sfTouches | owl:sameAs ?s2 .
    
    ?observation rdf:type coso:ContaminantObservation ;
                coso:observedAtSamplePoint ?sp ;
                coso:ofSubstance ?substance ;
                coso:analyzedSample ?sample ;
                coso:hasResult ?result .
    
    ?sample rdfs:label ?sampleLabel ;
            coso:sampleOfMaterialType ?matType .
    
    ?matType rdfs:label ?matTypeLabel .
    
    ?result coso:measurementValue ?result_value ;
            coso:measurementUnit ?unit .
    
    ?unit qudt:symbol ?unit_sym .
    
    VALUES ?unit {{<http://qudt.org/vocab/unit/NanoGM-PER-L>}}
    {substance_filter}
    {material_filter}
    FILTER (?result_value >= {min_conc})
    FILTER (?result_value <= {max_conc})
    
    BIND((CONCAT(str(?result_value) , " ", ?unit_sym)) as ?subVal)
    
    ?downstream_flowline rdf:type hyf:HY_FlowPath ;
                         spatial:connectedTo ?s2cell .
    
    ?upstream_flowline hyf:downstreamFlowPathTC ?downstream_flowline ;
                       geo:hasGeometry/geo:asWKT ?upstream_flowlineWKT .
    
    ?s2cellus spatial:connectedTo ?upstream_flowline ;
              rdf:type kwg-ont:S2Cell_Level13 .
    
    # Find facilities (required, not optional)
    ?s2cellus kwg-ont:sfContains ?facility .
    ?facility fio:ofIndustry ?industryCode, ?industryGroup, ?industrySubsector ;
              geo:hasGeometry/geo:asWKT ?facWKT ;
              rdfs:label ?facilityName .
    
    ?industryCode a naics:NAICS-IndustryCode ;
                  rdfs:label ?industryName ;
                  fio:subcodeOf ?industryGroup .
    
    ?industryGroup a naics:NAICS-IndustryGroup ;
                   rdfs:label ?industryGroupName ;
                   fio:subcodeOf ?industrySubsector .
    
    ?industrySubsector a naics:NAICS-IndustrySubsector ;
                       rdfs:label ?industrySubsectorName ;
                       fio:subcodeOf naics:NAICS-31 .
}} GROUP BY ?sp ?spWKT ?upstream_flowlineWKT ?facility ?facWKT ?facilityName ?industryName ?industryCode ?industryGroup ?industryGroupName ?industrySubsector ?industrySubsectorName
"""


    sparql_endpoint = ENDPOINT_URLS["federation"]
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    debug_info = {
        "endpoint": sparql_endpoint,
        "region_code": sanitized_region,
        "substance_uri": substance_uri,
        "material_uri": material_uri,
        "min_concentration": min_conc,
        "max_concentration": max_conc,
        "query_length": len(query),
        "query": query,
    }

    try:
        start_time = time.time()
        response = requests.post(
            sparql_endpoint,
            data={"query": query},
            headers=headers,
            timeout=300
        )
        elapsed = time.time() - start_time

        debug_info["response_status"] = response.status_code
        debug_info["response_time_sec"] = round(elapsed, 2)

        # Only keep a short snippet of the response text for debugging
        try:
            debug_info["response_text_snippet"] = response.text[:500]
        except Exception:
            debug_info["response_text_snippet"] = "<unavailable>"

        if response.status_code == 200:
            results = response.json()
            df_results = parse_sparql_results(results)
            if df_results.empty:
                print("   > Combined query complete: No results found.")
                debug_info["row_count"] = 0
            else:
                print(f"   > Combined query complete: Retrieved {len(df_results)} rows.")
                debug_info["row_count"] = len(df_results)
            return df_results, None, debug_info
        else:
            return None, f"Error {response.status_code}: {response.text}", debug_info

    except requests.exceptions.RequestException as e:
        debug_info["exception"] = str(e)
        return None, f"Network error: {str(e)}", debug_info
    except Exception as e:
        debug_info["exception"] = str(e)
        return None, f"Error: {str(e)}", debug_info


def split_combined_results(combined_df):
    """Split combined query results into logical tables for the UI."""
    # Updated column names based on the aggregated query output
    sample_columns = [
        "sp",
        "spWKT",
        "resultCount",  # Added from aggregation
        "max",          # Added from aggregation
    ]

    # The query returns upstream_flowlineWKT
    upstream_columns = ["upstream_flowlineWKT"]

    facility_columns = [
        "facility",
        "facWKT",
        "facilityName",
        "industryName",
        "industryCode",
        "industryGroup",
        "industryGroupName",
        "industrySubsector",
        "industrySubsectorName",
    ]

    if combined_df is None or combined_df.empty:
        return (
            pd.DataFrame(columns=sample_columns),
            pd.DataFrame(columns=["s2cell", "upstream_flowlineWKT"]),  # Keep for UI compatibility
            pd.DataFrame(columns=facility_columns),
        )

    # Extract samples data - only columns that exist in the query
    existing_sample_cols = [col for col in sample_columns if col in combined_df.columns]
    samples_df = (
        combined_df[existing_sample_cols]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # Extract upstream flowlines and create compatible structure for UI
    upstream_df = pd.DataFrame()
    if "upstream_flowlineWKT" in combined_df.columns:
        upstream_df = combined_df[["upstream_flowlineWKT"]].drop_duplicates().reset_index(drop=True)
        # Add dummy s2cell column for backward compatibility with the UI
        upstream_df["s2cell"] = None
        upstream_df = upstream_df[["s2cell", "upstream_flowlineWKT"]]
    else:
        upstream_df = pd.DataFrame(columns=["s2cell", "upstream_flowlineWKT"])

    # Extract facilities data
    existing_facility_cols = [col for col in facility_columns if col in combined_df.columns]
    facilities_df = (
        combined_df[existing_facility_cols]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    # Filter out rows that are all NaN (facilities that don't exist)
    if not facilities_df.empty and "facility" in facilities_df.columns:
        facilities_df = facilities_df[facilities_df["facility"].notna()].reset_index(drop=True)

    return samples_df, upstream_df, facilities_df

# Parse the data into hierarchical structure
@st.cache_data
def parse_regions(df):
    """
    Parse FIPS data into hierarchical structure:
    - States (2-digit codes)
    - Counties (5-digit codes, typically 1000-99999)
    - Subdivisions (10+ digit codes)
    """
    # Get states (2-digit FIPS codes)
    # State codes are 1-56, so we filter for codes less than 100
    states = df[df['fipsCode'] < 100].copy()
    # Remove "Geometry of " prefix if present
    states['state_name'] = states['label'].str.replace('Geometry of ', '', regex=False)
    # Remove duplicates - keep first occurrence of each state
    states = states.drop_duplicates(subset=['fipsCode'], keep='first')
    states = states.sort_values('state_name')
    
    # Get counties (5-digit FIPS codes)
    # Counties are typically in the range 1000-99999 (e.g., 01001, 23019)
    counties = df[(df['fipsCode'] >= 100) & (df['fipsCode'] < 100000)].copy()
    
    if not counties.empty:
        # Clean county names - remove "Geometry of " prefix
        counties['county_name'] = counties['label'].str.replace('Geometry of ', '', regex=False)
        # Extract state name (everything after the last comma)
        counties['state_name_county'] = counties['label'].str.split(', ').str[-1]
        # Get state code (first 2 digits of FIPS)
        # IMPORTANT: Must zfill(5) BEFORE slicing to handle leading zeros (e.g., 1001 -> 01001 -> 01)
        counties['state_code'] = counties['fipsCode'].astype(str).str.zfill(5).str[:2]
        # Get county code (5-digit FIPS)
        counties['county_code'] = counties['fipsCode'].astype(str).str.zfill(5)
        # Remove duplicate counties (keep first occurrence based on county_code)
        counties = counties.drop_duplicates(subset=['county_code'], keep='first')
    
    # Get subdivisions (codes longer than county level)
    # Subdivisions are longer (usually 10+ digits)
    subdivisions = df[df['fipsCode'] >= 100000].copy()
    
    # Parse county information from subdivision labels
    # Pattern: "Geometry of [Subdivision], [County], [State]"
    if not subdivisions.empty:
        # Extract subdivision, county, and state from label
        subdivisions['subdivision_name'] = subdivisions['label'].str.replace('Geometry of ', '', regex=False).str.split(', ').str[0]
        subdivisions['county_name'] = subdivisions['label'].str.split(', ').str[-2]
        subdivisions['state_name_sub'] = subdivisions['label'].str.split(', ').str[-1]
        
        # Get state code (first 2 digits of FIPS)
        # IMPORTANT: Must zfill(10) BEFORE slicing to handle leading zeros
        subdivisions['state_code'] = subdivisions['fipsCode'].astype(str).str.zfill(10).str[:2]
        # Get county code (first 5 digits of FIPS)
        subdivisions['county_code'] = subdivisions['fipsCode'].astype(str).str.zfill(10).str[:5]
    states, counties, subdivisions = omit_alaska_regions(states, counties, subdivisions)
    return states, counties, subdivisions

# Load data
try:
    df = load_fips_data()
    states_df, counties_df, subdivisions_df = parse_regions(df)
    
    substances_df = load_substances_data()
    material_types_df = load_material_types_data()
    
    if query_number is not None:
        st.success(
            f"✅ Loaded {len(df)} administrative regions, {len(substances_df)} PFAS substances, "
            f"and {len(material_types_df)} material types"
        )
    
    if query_number is not None:
        # Show data sources
        with st.expander("📊 Data Sources"):
            st.markdown("""
            **Data integrated from multiple knowledge graphs:**
            - 🔬 **PFAS contamination observations** from EPA monitoring programs
            - 🗺️ **Spatial relationships and administrative boundaries** (US Counties & States)
            - 💧 **Hydrological flow networks** (NHDPlus V2)
            - 🏭 **Industrial facility data** with NAICS classification
            """)
        
        # Show data summary
        with st.expander("📈 Data Summary"):
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("PFAS Substances", substances_df['shortName'].nunique())
            with col2:
                st.metric("Material Types", len(material_types_df))
            with col3:
                st.metric("States", len(states_df))
            with col4:
                if not subdivisions_df.empty:
                    unique_counties = subdivisions_df.groupby(['state_code', 'county_name']).size().shape[0]
                    st.metric("Counties", unique_counties)
                else:
                    st.metric("Counties", 0)
            with col5:
                st.metric("Subdivisions", len(subdivisions_df))
    

    # Initialize session state for region selection (always)
    if 'selected_state' not in st.session_state:
        st.session_state.selected_state = None
    if 'selected_county' not in st.session_state:
        st.session_state.selected_county = None
    if 'selected_subdivision' not in st.session_state:
        st.session_state.selected_subdivision = None

    # Initialize variables for region selection
    selected_state_code = None
    selected_state_name = None
    selected_county_name = None
    selected_county_code = None
    selected_subdivision_code = None
    selected_subdivision_name = None
    state_has_data = False

    # ========================================
    # SHARED SIDEBAR: Geographic Region Selection (only when analysis is selected)
    # ========================================
    if query_number is not None:
        st.sidebar.header("🔧 Analysis Configuration")
        if query_number == 6:
            st.sidebar.markdown("### 📍 Geographic Region")
            st.sidebar.markdown("Optional: limit to states with SOCKG sites")

            sockg_state_codes = get_sockg_state_code_set()
            sockg_states_df = states_df[
                states_df["fipsCode"].astype(str).str.zfill(2).isin(sockg_state_codes)
            ]

            if sockg_states_df.empty:
                st.sidebar.warning("No SOCKG states available from the endpoint.")

            state_options = ["-- All States --"] + sockg_states_df.sort_values("state_name")["state_name"].tolist()
            selected_state_display = st.sidebar.selectbox(
                "Select State (Optional)",
                state_options,
                key="sockg_state_selector",
                help="Limit results to a state with SOCKG sites"
            )

            if selected_state_display != "-- All States --":
                selected_state_name = selected_state_display
                state_row = states_df[states_df["state_name"] == selected_state_name]
                if not state_row.empty:
                    selected_state_code = str(state_row.iloc[0]["fipsCode"]).zfill(2)
                st.session_state.selected_state = {
                    "name": selected_state_name,
                    "code": selected_state_code
                }
            else:
                st.session_state.selected_state = None

            st.sidebar.markdown("---")
        else:

            # GEOGRAPHIC REGION SELECTION
            st.sidebar.markdown("### 📍 Geographic Region")
            st.sidebar.markdown("🆃 **Required**: Select a state and county")
    
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
    
            # 1. STATE SELECTION (Mandatory)
            selected_state_display = st.sidebar.selectbox(
                "1️⃣ Select State",
                state_options,
                key="state_selector",
                on_change=on_state_change,
                help="Select a US state with available sample data (✓ = has data)"
            )
    
            # Get the selected state's FIPS code
            if selected_state_display != "-- Select a State --" and not selected_state_display.startswith("✗ "):
                # Extract actual state name from display (remove ✓ prefix)
                actual_state_name = state_name_map.get(selected_state_display, selected_state_display.replace("✓ ", ""))
                selected_state_name = actual_state_name
                state_row = states_df[states_df['state_name'] == actual_state_name]
                if not state_row.empty:
                    selected_state_code = state_row.iloc[0]['fipsCode']
                    st.session_state.selected_state = {
                        'name': selected_state_name,
                        'code': str(selected_state_code).zfill(2)
                    }
                    state_has_data = True
    
            # 2. COUNTY SELECTION (Optional, filtered by state)
            state_subdivisions = pd.DataFrame()
            state_counties = pd.DataFrame()
    
            if selected_state_code:
                state_counties = counties_df[
                    counties_df['state_code'] == str(selected_state_code).zfill(2)
                ]
                state_subdivisions = subdivisions_df[
                    subdivisions_df['state_code'] == str(selected_state_code).zfill(2)
                ]
    
                if not state_counties.empty:
                    available_county_codes = get_available_county_codes(str(selected_state_code).zfill(2))
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
    
                    selected_county_display = st.sidebar.selectbox(
                        "2️⃣ Select County (Required)",
                        county_options,
                        key="county_selector",
                        on_change=on_county_change,
                        help=f"Select a county within {selected_state_name}"
                    )
    
                    if selected_county_display != "-- Select a County --" and not selected_county_display.startswith("✗ "):
                        selected_county_name = county_name_map.get(
                            selected_county_display,
                            selected_county_display.replace("✓ ", "")
                        )
                        county_row = state_counties[state_counties['county_name'] == selected_county_name]
                        if not county_row.empty:
                            selected_county_code = county_row.iloc[0]['county_code']
                        st.session_state.selected_county = selected_county_name
                        st.session_state.selected_county_code = selected_county_code
                    else:
                        st.session_state.selected_county = None
                        st.session_state.selected_county_code = None
                else:
                    st.sidebar.info(f"ℹ️ No county-level data available for {selected_state_name}.")
            else:
                st.sidebar.info("👆 Please select a state first")
    
            # 3. SUBDIVISION SELECTION (Optional, filtered by county)
            if selected_state_code and selected_county_code:
                county_subdivisions = state_subdivisions[
                    state_subdivisions['county_code'] == selected_county_code
                ]
    
                if not county_subdivisions.empty:
                    available_subdivision_codes = get_available_subdivision_codes(
                        str(selected_county_code).zfill(5)
                    )
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
                        help=f"Select a subdivision within {selected_county_name}"
                    )
    
                    if (
                        selected_subdivision_display != "-- All Subdivisions --"
                        and not selected_subdivision_display.startswith("✗ ")
                    ):
                        selected_subdivision_name = subdivision_name_map.get(
                            selected_subdivision_display,
                            selected_subdivision_display.replace("✓ ", "")
                        )
                        subdivision_row = county_subdivisions[
                            county_subdivisions['subdivision_name'] == selected_subdivision_name
                        ]
                        if not subdivision_row.empty:
                            selected_subdivision_code = str(subdivision_row.iloc[0]['fipsCode']).zfill(10)
                        st.session_state.selected_subdivision = {
                            'name': selected_subdivision_name,
                            'code': selected_subdivision_code
                        }
                    else:
                        st.session_state.selected_subdivision = None
                else:
                    st.sidebar.info(f"ℹ️ No subdivisions found for {selected_county_name}")
            elif selected_state_code and selected_county_name:
                st.sidebar.info("No subdivisions available for this county")
                st.session_state.selected_subdivision = None
    
            st.sidebar.markdown("---")
        
    # ========================================
    # Display UI based on query type selection
    # ========================================
    if query_number is None:
        # Home page - no analysis selected, just show the welcome message (already shown above)
        pass
    elif query_number == 1:
        # PFAS UPSTREAM TRACING QUERY
        st.markdown("""
        **What this analysis does:**
        - Finds water samples with PFAS contamination in your selected region
        - Traces upstream through hydrological flow paths  
        - Identifies industrial facilities that may be contamination sources
        
        **3-Step Process:** Find contamination → Trace upstream → Identify potential sources
        """)
        
        # Initialize session state for Query 1 specific params
        if 'selected_substance' not in st.session_state:
            st.session_state.selected_substance = None
        if 'selected_material_type' not in st.session_state:
            st.session_state.selected_material_type = None
        if 'conc_min' not in st.session_state:
            st.session_state.conc_min = 0
        if 'conc_max' not in st.session_state:
            st.session_state.conc_max = 100
        
        # Query 1 specific parameters in sidebar form
        st.sidebar.markdown("### 🧪 Query Parameters")
        
        # Parameters (rerun on change so sliders/inputs stay in sync)
        with st.sidebar.container():
            st.markdown("### 🧪 PFAS Substance")

            region_code = None
            is_subdivision = False
            if selected_subdivision_code:
                region_code = str(selected_subdivision_code)
                is_subdivision = True
            elif selected_county_code:
                region_code = str(selected_county_code).zfill(5)

            substances_view = (
                get_available_substances_with_labels(region_code, is_subdivision)
                if region_code
                else pd.DataFrame()
            )
            substance_options, substance_uri_map = build_substance_options(substances_view)

            selected_substance_display = st.selectbox(
                "Select PFAS Substance (Optional)",
                substance_options,
                help="Select a specific PFAS compound to analyze, or leave as 'All Substances'"
            )
        
            # Get the selected substance's full URI
            selected_substance_uri = None
            selected_substance_name = None
            if selected_substance_display != "-- All Substances --":
                selected_substance_name = selected_substance_display
                selected_substance_uri = substance_uri_map.get(selected_substance_display)
                st.session_state.selected_substance = {
                    'name': selected_substance_name,
                    'uri': selected_substance_uri
                }
            else:
                st.session_state.selected_substance = None
        
            st.markdown("---")
        
            # MATERIAL TYPE SELECTION (Optional)
            st.markdown("### 🧫 Sample Material Type")
        
            material_types_view = (
                get_available_material_types_with_labels(region_code, is_subdivision)
                if region_code
                else pd.DataFrame()
            )
            material_type_options, material_type_uri_map = build_material_type_options(
                material_types_view
            )
        
            selected_material_display = st.selectbox(
                "Select Material Type (Optional)",
                material_type_options,
                help="Select the type of sample material analyzed (e.g., Drinking Water, Groundwater, Soil)"
            )
        
            # Get the selected material type's details
            selected_material_uri = None
            selected_material_short = None
            selected_material_label = None
            selected_material_name = None

            if selected_material_display != "-- All Material Types --":
                selected_material_name = selected_material_display
                selected_material_uri = material_type_uri_map.get(selected_material_display)
                selected_material_short = selected_material_display
                selected_material_label = selected_material_display
                st.session_state.selected_material_type = {
                    'short': selected_material_short,
                    'label': selected_material_label,
                    'uri': selected_material_uri,
                    'name': selected_material_name
                }
            else:
                st.session_state.selected_material_type = None
        
            st.markdown("---")
        
            # DETECTED CONCENTRATION SELECTION (Optional)
            st.markdown("### 📊 Detected Concentration")

            include_nondetects = st.checkbox(
                "Include nondetects",
                value=st.session_state.get("q1_include_nondetects", False),
                key="q1_include_nondetects",
                help="Include observations with zero concentration or nondetect flags"
            )

            if "conc_range" not in st.session_state:
                st.session_state.conc_range = (st.session_state.conc_min, st.session_state.conc_max)
            if "conc_min_input" not in st.session_state:
                st.session_state.conc_min_input = st.session_state.conc_min
            if "conc_max_input" not in st.session_state:
                st.session_state.conc_max_input = st.session_state.conc_max

            def sync_q1_from_inputs():
                min_val = st.session_state.conc_min_input
                max_val = st.session_state.conc_max_input
                st.session_state.conc_min = min_val
                st.session_state.conc_max = max_val
                st.session_state.conc_range = (min_val, max_val)

            def sync_q1_from_slider():
                min_val, max_val = st.session_state.conc_range
                st.session_state.conc_min = min_val
                st.session_state.conc_max = max_val
                st.session_state.conc_min_input = min_val
                st.session_state.conc_max_input = max_val

            max_limit = 60000

            st.session_state.conc_min_input = min(st.session_state.conc_min_input, max_limit)
            st.session_state.conc_max_input = min(st.session_state.conc_max_input, max_limit)
            range_min, range_max = st.session_state.conc_range
            range_min = min(range_min, max_limit)
            range_max = min(range_max, max_limit)
            if range_min > range_max:
                range_max = range_min
            st.session_state.conc_range = (range_min, range_max)
            
            # Simple number inputs for min and max concentration
            col1, col2 = st.columns(2)
            
            with col1:
                min_concentration = st.number_input(
                    "Min (ng/L)",
                    min_value=0,
                    max_value=max_limit,
                    value=st.session_state.conc_min_input,
                    step=1,
                    key="conc_min_input",
                    help="Minimum concentration in nanograms per liter",
                    on_change=sync_q1_from_inputs
                )
                st.session_state.conc_min = min_concentration
            
            with col2:
                max_concentration = st.number_input(
                    "Max (ng/L)",
                    min_value=0,
                    max_value=max_limit,
                    value=st.session_state.conc_max_input,
                    step=1,
                    key="conc_max_input",
                    help="Maximum concentration in nanograms per liter",
                    on_change=sync_q1_from_inputs
                )
                st.session_state.conc_max = max_concentration
            
            # Add slider for visual adjustment
            st.slider(
                "Drag to adjust range",
                min_value=0,
                max_value=max_limit,
                value=st.session_state.conc_range,
                step=1,
                key="conc_range",
                help="Drag the slider or use the number inputs above for precise control",
                on_change=sync_q1_from_slider
            )

            min_concentration, max_concentration = st.session_state.conc_range

            # Validate that min <= max
            if min_concentration > max_concentration:
                st.warning("⚠️ Min concentration cannot be greater than max")
            
            # Show concentration context
            if max_concentration <= 10:
                st.info("🟢 Low range - background levels")
            elif max_concentration <= 70:
                st.info("🟡 Moderate range - measurable contamination")
            else:
                st.warning("🔴 High range - significant concern")
        
            # Execute Query Button
            st.markdown("---")
            county_selected = bool(st.session_state.get("selected_county_code"))
            can_execute = county_selected and state_has_data
            execute_button = st.button(
                "🔍 Execute Query",
                type="primary",
                use_container_width=True,
                disabled=not can_execute,
                help="Select a state with data and a county first" if not can_execute else "Display all selected parameters ready for SPARQL query execution",
                key="execute_q1"
            )
    
        # Display query parameters when Execute button is clicked
        # Logic to handle query execution and result persistence
        if execute_button:
            # Validate required parameters
            if not selected_state_code:
                st.error("❌ **State selection is required!** Please select a state before executing the query.")
            else:
                # Get region code (use subdivision if selected, otherwise county code or state code)
                if selected_subdivision_code:
                    query_region_code = str(selected_subdivision_code)
                elif hasattr(st.session_state, 'selected_county_code') and st.session_state.selected_county_code:
                    # Use the actual county FIPS code if available (5-digit code like "23005")
                    query_region_code = str(st.session_state.selected_county_code)
                else:
                    query_region_code = str(selected_state_code).zfill(2)
                
                # Prepare parameters for display and storage
                params_data = []
                
                # Substance
                substance_val = selected_substance_name if selected_substance_name else "All Substances"
                params_data.append({"Parameter": "PFAS Substance", "Value": substance_val})
                
                # Material Type
                mat_val = selected_material_name if selected_material_name else "All Material Types"
                params_data.append({"Parameter": "Material Type", "Value": mat_val})
                
                # Detected Concentration
                conc_value = f"{min_concentration} - {max_concentration} ng/L"
                if include_nondetects:
                    conc_value += " (including nondetects)"
                params_data.append({"Parameter": "Detected Concentration", "Value": conc_value})
                
                # Geographic Region
                region_display = selected_state_name
                if selected_subdivision_name:
                    region_display = f"{selected_subdivision_name}, {selected_county_name}, {selected_state_name}"
                elif selected_county_name:
                    region_display = f"{selected_county_name}, {selected_state_name}"
                params_data.append({"Parameter": "Geographic Region", "Value": region_display})
                
                params_df = pd.DataFrame(params_data)

                # Run the query
                st.markdown("---")
                st.subheader("🚀 Query Execution")
                
                # Create columns for progress display
                prog_col1, prog_col2, prog_col3 = st.columns(3)
                
                # Initialize placeholders
                samples_df = pd.DataFrame()
                upstream_s2_df = pd.DataFrame()
                facilities_df = pd.DataFrame()
                combined_df = None
                combined_error = None
                debug_info = None
                
                # Step 1: Run combined query
                with prog_col1:
                    with st.spinner("🔄 Step 1: Running upstream tracing query..."):
                        effective_min = 0 if include_nondetects else min_concentration
                        combined_df, combined_error, debug_info = execute_combined_query(
                            substance_uri=selected_substance_uri,
                            material_uri=selected_material_uri,
                            min_conc=effective_min,
                            max_conc=max_concentration,
                            region_code=query_region_code,
                            include_nondetects=include_nondetects
                        )
                    
                    samples_df, upstream_s2_df, facilities_df = split_combined_results(combined_df)
                    
                    # Also query the region boundary for mapping
                    region_boundary_df = get_region_boundary(query_region_code)

                    if combined_error:
                        st.error(f"❌ Step 1 failed: {combined_error}")
                    elif not samples_df.empty:
                        st.success(f"✅ Step 1: Found {len(samples_df)} contaminated samples")
                    else:
                        st.warning(f"⚠️ Step 1: No contaminated samples found")
            
                # Step 2: Trace upstream flow paths
                with prog_col2:
                    if not samples_df.empty:
                        if not upstream_s2_df.empty:
                            st.success(f"✅ Step 2: Traced {len(upstream_s2_df)} upstream paths")
                        else:
                            st.info("ℹ️ Step 2: No upstream sources found")
                    else:
                        st.info("⏭️ Step 2: Skipped (no samples)")
            
                # Step 3: Find facilities
                with prog_col3:
                    if not upstream_s2_df.empty:
                        if not facilities_df.empty:
                            st.success(f"✅ Step 3: Found {len(facilities_df)} facilities")
                        else:
                            st.info("ℹ️ Step 3: No facilities found")
                    else:
                        st.info("⏭️ Step 3: Skipped (no upstream cells)")

                # Store everything in session state
                st.session_state.query_results = {
                    'samples_df': samples_df,
                    'upstream_s2_df': upstream_s2_df,
                    'facilities_df': facilities_df,
                    'combined_error': combined_error,
                    'debug_info': debug_info,
                    'region_boundary_df': region_boundary_df,
                    'params_df': params_df,
                    'query_region_code': query_region_code,
                    'selected_material_name': selected_material_name
                }
                st.session_state.has_results = True

        # Display results if they exist in session state
        if st.session_state.get('has_results', False):
            results = st.session_state.query_results
            
            # Retrieve data from session state
            samples_df = results.get('samples_df')
            upstream_s2_df = results.get('upstream_s2_df')
            facilities_df = results.get('facilities_df')
            combined_error = results.get('combined_error')
            debug_info = results.get('debug_info')
            region_boundary_df = results.get('region_boundary_df')
            params_df = results.get('params_df')
            query_region_code = results.get('query_region_code')
            saved_material_name = results.get('selected_material_name')

            # Display Selected Parameters
            st.markdown("---")
            st.markdown("### 📋 Selected Parameters (from executed query)")
            st.table(params_df)

            st.markdown("---")
            st.markdown("### 🔬 Query Results")

            # Debug information expander
            if debug_info:
                with st.expander("🐞 Debug Info (query & response details)"):
                    debug_copy = dict(debug_info)
                    query_text = debug_copy.pop("query", None)
                    st.json(debug_copy)
                    if query_text:
                        st.code(query_text.strip(), language="sparql")
        
            # Display results for each step
            st.markdown("---")
        
            # Step 1 Results: Contaminated Samples
            if samples_df is not None and not samples_df.empty:
                st.markdown("### 🔬 Step 1: Contaminated Samples")
            
                # Metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Samples", len(samples_df))
                with col2:
                    if 'sp' in samples_df.columns:
                        st.metric("Unique Sample Points", samples_df['sp'].nunique())
                with col3:
                    if 'matType' in samples_df.columns:
                        st.metric("Material Type", saved_material_name or "All")
                with col4:
                    if 'result_value' in samples_df.columns:
                        avg_value = samples_df['result_value'].astype(float).mean()
                        st.metric("Avg Concentration", f"{avg_value:.2f} ng/L")
            
                # Display data
                with st.expander("📊 View Contaminated Samples Data"):
                    st.dataframe(samples_df, use_container_width=True)
                
                    # Download button
                    csv_samples = samples_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Samples CSV",
                        data=csv_samples,
                        file_name=f"contaminated_samples_{query_region_code}.csv",
                        mime="text/csv",
                        key="download_samples"
                    )
        
            # Step 2 Results: Upstream Flow Paths
            if upstream_s2_df is not None and not upstream_s2_df.empty:
                st.markdown("### 🌊 Step 2: Upstream Flow Paths")
            
                # Metrics
                st.metric("Total Upstream Connections", len(upstream_s2_df))
            
                # Note: We don't show the technical data to users
                # The flow paths are visualized on the map instead
        
            # Step 3 Results: Facilities
            if facilities_df is not None and not facilities_df.empty:
                st.markdown("### 🏭 Step 3: Potential Source Facilities")
            
                # Metrics
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Facilities", len(facilities_df))
                with col2:
                    if 'industryName' in facilities_df.columns:
                        st.metric("Industry Types", facilities_df['industryName'].nunique())
            
                # Display data
                with st.expander("📊 View Facilities Data"):
                    # Create a cleaner display version
                    display_df = facilities_df.copy()
                    if 'facilityName' in display_df.columns and 'industryName' in display_df.columns:
                        display_df = display_df[['facilityName', 'industryName', 'facWKT', 'facility']]
                    st.dataframe(display_df, use_container_width=True)
                
                    # Download button
                    csv_facilities = facilities_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Facilities CSV",
                        data=csv_facilities,
                        file_name=f"upstream_facilities_{query_region_code}.csv",
                        mime="text/csv",
                        key="download_facilities"
                    )
                
                # Flat Industry Breakdown + Streamlit-based "Toggle All" control
                selected_facility_groups = None  # Will be used later when building the map
                if 'industryName' in facilities_df.columns:
                    with st.expander("🏭 Industry Breakdown", expanded=False):
                        st.markdown("### Industry Types")
                        
                        # Prepare data for flat table
                        flat_data = facilities_df.copy()
                        
                        # Clean industry name (remove trailing spaces)
                        flat_data['industryName'] = flat_data['industryName'].astype(str).str.strip()
                        
                        # Extract code from URI if available
                        if 'industryCode' in flat_data.columns:
                            # Extract code from URI (e.g., ...#NAICS-325199 -> 325199)
                            flat_data['code_clean'] = flat_data['industryCode'].apply(
                                lambda x: x.split('-')[-1] if isinstance(x, str) and '-' in x else ''
                            )
                            
                            # Prioritize more specific codes (longer length) for the same facility
                            flat_data['code_len'] = flat_data['code_clean'].str.len()
                            flat_data = flat_data.sort_values(['facility', 'code_len'], ascending=[True, False])
                            flat_data = flat_data.drop_duplicates(subset=['facility'], keep='first')
                            
                            flat_data['display_name'] = flat_data.apply(
                                lambda row: f"{row['industryName']} ({row['code_clean']})" if row['code_clean'] else row['industryName'],
                                axis=1
                            )
                        else:
                            flat_data['display_name'] = flat_data['industryName']
                            flat_data = flat_data.drop_duplicates(subset=['facility'], keep='first')
                        
                        # Group by display name
                        industry_summary = flat_data.groupby('display_name').agg(
                            Facilities=('facility', 'nunique')
                        ).reset_index()
                        
                        # Calculate percentage
                        total_facs = flat_data['facility'].nunique()
                        if total_facs > 0:
                            industry_summary['Percentage'] = (
                                industry_summary['Facilities'] / total_facs * 100
                            ).map('{:.1f}%'.format)
                        else:
                            industry_summary['Percentage'] = "0.0%"
                        
                        industry_summary.columns = ['Industry', 'Facilities', 'Percentage']
                        industry_summary = industry_summary.sort_values('Facilities', ascending=False).reset_index(drop=True)
                        
                        # Display as table
                        st.dataframe(industry_summary, use_container_width=True, hide_index=True)

        
            # Create interactive map if we have spatial data
            if (samples_df is not None and not samples_df.empty and 'spWKT' in samples_df.columns) or \
               (facilities_df is not None and not facilities_df.empty and 'facWKT' in facilities_df.columns):
                st.markdown("---")
                st.markdown("### 🗺️ Interactive Map")
            
                # Convert to GeoDataFrames
                samples_gdf = None
                facilities_gdf = None
                flowlines_gdf = None
            
                if samples_df is not None and not samples_df.empty and 'spWKT' in samples_df.columns:
                    # Filter out empty WKT values
                    samples_with_wkt = samples_df[samples_df['spWKT'].notna()].copy()
                    if not samples_with_wkt.empty:
                        try:
                            samples_with_wkt['geometry'] = samples_with_wkt['spWKT'].apply(wkt.loads)
                            samples_gdf = gpd.GeoDataFrame(samples_with_wkt, geometry='geometry')
                            samples_gdf.set_crs(epsg=4326, inplace=True, allow_override=True)
                        except Exception as e:
                            st.warning(f"Could not parse sample geometries: {e}")
            
                if facilities_df is not None and not facilities_df.empty and 'facWKT' in facilities_df.columns:
                    # Filter out empty WKT values
                    facilities_with_wkt = facilities_df[facilities_df['facWKT'].notna()].copy()
                    if not facilities_with_wkt.empty:
                        try:
                            facilities_with_wkt['geometry'] = facilities_with_wkt['facWKT'].apply(wkt.loads)
                            facilities_gdf = gpd.GeoDataFrame(facilities_with_wkt, geometry='geometry')
                            facilities_gdf.set_crs(epsg=4326, inplace=True, allow_override=True)
                        except Exception as e:
                            st.warning(f"Could not parse facility geometries: {e}")
            
                # Process upstream flow lines
                if upstream_s2_df is not None and not upstream_s2_df.empty and 'upstream_flowlineWKT' in upstream_s2_df.columns:
                    # Filter out empty WKT values
                    flowlines_with_wkt = upstream_s2_df[upstream_s2_df['upstream_flowlineWKT'].notna()].copy()
                    if not flowlines_with_wkt.empty:
                        try:
                            flowlines_with_wkt['geometry'] = flowlines_with_wkt['upstream_flowlineWKT'].apply(wkt.loads)
                            flowlines_gdf = gpd.GeoDataFrame(flowlines_with_wkt, geometry='geometry')
                            flowlines_gdf.set_crs(epsg=4326, inplace=True, allow_override=True)
                        except Exception as e:
                            st.warning(f"Could not parse flowline geometries: {e}")
            
                # Create map
                if samples_gdf is not None or facilities_gdf is not None or flowlines_gdf is not None:
                    # Initialize map centered on data
                    if samples_gdf is not None and not samples_gdf.empty:
                        # Center on samples
                        center_lat = samples_gdf.geometry.y.mean()
                        center_lon = samples_gdf.geometry.x.mean()
                        map_obj = folium.Map(location=[center_lat, center_lon], zoom_start=8)
                    elif facilities_gdf is not None and not facilities_gdf.empty:
                        # Center on facilities
                        center_lat = facilities_gdf.geometry.y.mean()
                        center_lon = facilities_gdf.geometry.x.mean()
                        map_obj = folium.Map(location=[center_lat, center_lon], zoom_start=8)
                    else:
                        # Default to US center
                        map_obj = folium.Map(location=[39.8, -98.5], zoom_start=4)
                
                    # Add administrative boundary if available (from SPARQL query)
                    if region_boundary_df is not None and not region_boundary_df.empty:
                        try:
                            # Get boundary info from the query result
                            boundary_wkt = region_boundary_df.iloc[0]['countyWKT']
                            boundary_name = region_boundary_df.iloc[0].get('countyName', 'Region')
                            
                            # Convert WKT to GeoDataFrame
                            from shapely import wkt as shapely_wkt
                            boundary_geom = shapely_wkt.loads(boundary_wkt)
                            boundary_gdf = gpd.GeoDataFrame([{
                                'name': boundary_name,
                                'geometry': boundary_geom
                            }], crs='EPSG:4326')
                            
                            # Determine boundary color based on region type
                            region_code_len = len(str(query_region_code))
                            if region_code_len > 5:
                                # Subdivision - use same gray as county
                                boundary_color = '#666666'
                                region_type = "Subdivision"
                            elif region_code_len == 5:
                                # County - gray
                                boundary_color = '#666666'
                                region_type = "County"
                            else:
                                # State - dark gray
                                boundary_color = '#444444'
                                region_type = "State"
                            
                            # Add boundary to map
                            boundary_gdf.explore(
                                m=map_obj,
                                name=f'<span style="color:{boundary_color};">📍 {boundary_name} Boundary</span>',
                                color=boundary_color,
                                style_kwds=dict(
                                    fillColor='none',
                                    weight=3,
                                    opacity=0.8,
                                    dashArray='5, 5'
                                ),
                                overlay=True,
                                show=True
                            )
                        except Exception as e:
                            # If boundary display fails, just continue without it
                            print(f"Error displaying boundary: {e}")
                            pass
                
                    # Add upstream flow lines (blue lines)
                    if flowlines_gdf is not None and not flowlines_gdf.empty:
                        flowlines_gdf.explore(
                            m=map_obj,
                            name='<span style="color:DodgerBlue;">🌊 Upstream Flowlines</span>',
                            color='DodgerBlue',
                            style_kwds=dict(
                                weight=2,
                                opacity=0.5
                            ),
                            tooltip=False,
                            popup=False
                        )
                
                    # Add contaminated samples (orange markers)
                    if samples_gdf is not None and not samples_gdf.empty:
                        samples_gdf.explore(
                            m=map_obj,
                            name='<span style="color:DarkOrange;">🔬 Contaminated Samples</span>',
                            color='DarkOrange',
                            marker_kwds=dict(radius=8),
                            marker_type='circle_marker',
                            popup=['sp', 'result_value'] if all(col in samples_gdf.columns for col in ['sp', 'result_value']) else True,
                            tooltip=['result_value'] if 'result_value' in samples_gdf.columns else None,
                            style_kwds=dict(
                                fillOpacity=0.7,
                                opacity=0.8
                            )
                        )
                
                    # Add facilities (colored by industry)
                    facility_layer_names = []  # Track facility layer names for JS
                    
                    if facilities_gdf is not None and not facilities_gdf.empty:
                        # Determine grouping column (Subsector > Group > Industry)
                        group_col = 'industryName' # Default
                        
                        if 'industrySubsectorName' in facilities_gdf.columns and facilities_gdf['industrySubsectorName'].notna().any():
                            group_col = 'industrySubsectorName'
                        elif 'industryGroupName' in facilities_gdf.columns and facilities_gdf['industryGroupName'].notna().any():
                            group_col = 'industryGroupName'
                            
                        if group_col in facilities_gdf.columns:
                            # Group by industry and assign colors
                            colors = ['MidnightBlue','MediumBlue','SlateBlue','MediumSlateBlue', 
                                     'DodgerBlue','DeepSkyBlue','SkyBlue','CadetBlue','DarkCyan',
                                     'LightSeaGreen','MediumSeaGreen','PaleVioletRed','Purple',
                                     'Orchid','Fuchsia','MediumVioletRed','HotPink','LightPink']
                        
                            # Count facilities per group and sort by count (descending)
                            group_counts = facilities_gdf.groupby(group_col).size().sort_values(ascending=False)
                            sorted_groups = group_counts.index.tolist()
                            total_facilities = len(facilities_gdf)
                            
                            # Individual industry groups (regular, independent)
                            for idx, group in enumerate(sorted_groups):
                                group_facilities = facilities_gdf[facilities_gdf[group_col] == group]
                                count = len(group_facilities)
                                color = colors[idx % len(colors)]
                                layer_name = f'🏭 {group} ({count})'
                                
                                group_facilities.explore(
                                    m=map_obj,
                                    name=f'<span style="color:{color};">{layer_name}</span>',
                                    color=color,
                                    marker_kwds=dict(radius=6),
                                    popup=['facilityName', 'industryName'] if all(col in group_facilities.columns for col in ['facilityName', 'industryName']) else True,
                                    tooltip=['facilityName', 'industryName'] if 'facilityName' in group_facilities.columns else None,
                                show=True
                            )
                
                    # Add layer control
                    folium.LayerControl(collapsed=False).add_to(map_obj)
                    
                
                    # Display map
                    st_folium(map_obj, width=None, height=600, returned_objects=[])
                
                    # Map legend
                    st.info("""
                    **🗺️ Map Legend:**
                    - 🟠 **Orange circles** = Contaminated sample locations
                    - 🔵 **Blue lines** = Upstream flow paths (hydrological connections)
                    - 🏭 **Colored markers** = Upstream facilities (grouped by industry type, sorted by count)
                    - 📍 **Boundary outline** = Selected region
                    - **Use "✅ ALL FACILITIES" to show/hide all facilities at once.**
                    - **Uncheck "✅ ALL FACILITIES" to hide everything.**
                    - **Check "✅ ALL FACILITIES" to allow individual industries to show.**
                    """)
        
    elif query_number == 5:
        # PFAS DOWNSTREAM TRACING QUERY
        st.markdown("""
        **What this analysis does:**
        - Finds water samples with PFAS contamination in your selected region
        - Traces *downstream* through hydrological flow paths
        - Identifies contaminated sample points downstream using the same PFAS/material/range filters
        
        **3-Step Process:** Find contamination → Trace downstream → Find downstream contamination
        """)
        
        # Initialize session state for Query 5 specific params
        if 'q5_selected_substance' not in st.session_state:
            st.session_state.q5_selected_substance = None
        if 'q5_selected_material_type' not in st.session_state:
            st.session_state.q5_selected_material_type = None
        if 'q5_conc_min' not in st.session_state:
            st.session_state.q5_conc_min = 0
        if 'q5_conc_max' not in st.session_state:
            st.session_state.q5_conc_max = 100
        
        # Query 5 specific parameters in sidebar
        st.sidebar.markdown("### 🧪 Query Parameters")
        
        with st.sidebar.container():
            st.markdown("### 🧪 PFAS Substance")

            region_code = None
            is_subdivision = False
            if selected_subdivision_code:
                region_code = str(selected_subdivision_code)
                is_subdivision = True
            elif selected_county_code:
                region_code = str(selected_county_code).zfill(5)

            substances_view = (
                get_available_substances_with_labels(region_code, is_subdivision)
                if region_code
                else pd.DataFrame()
            )
            substance_options, substance_uri_map = build_substance_options(substances_view)

            selected_substance_display = st.selectbox(
                "Select PFAS Substance (Optional)",
                substance_options,
                key="q5_substance_select",
                help="Select a specific PFAS compound to analyze, or leave as 'All Substances'"
            )
            
            selected_substance_uri = None
            selected_substance_name = None
            if selected_substance_display != "-- All Substances --":
                selected_substance_name = selected_substance_display
                selected_substance_uri = substance_uri_map.get(selected_substance_display)
                st.session_state.q5_selected_substance = {
                    'name': selected_substance_name,
                    'uri': selected_substance_uri
                }
            else:
                st.session_state.q5_selected_substance = None
            
            st.markdown("---")
            
            # MATERIAL TYPE SELECTION (Optional)
            st.markdown("### 🧫 Sample Material Type")
            
            material_types_view = (
                get_available_material_types_with_labels(region_code, is_subdivision)
                if region_code
                else pd.DataFrame()
            )
            material_type_options, material_type_uri_map = build_material_type_options(
                material_types_view
            )
            
            selected_material_display = st.selectbox(
                "Select Material Type (Optional)",
                material_type_options,
                key="q5_material_select",
                help="Select the type of sample material analyzed (e.g., Drinking Water, Groundwater, Soil)"
            )
            
            selected_material_uri = None
            selected_material_short = None
            selected_material_label = None
            selected_material_name = None
            
            if selected_material_display != "-- All Material Types --":
                selected_material_name = selected_material_display
                selected_material_uri = material_type_uri_map.get(selected_material_display)
                selected_material_short = selected_material_display
                selected_material_label = selected_material_display
                st.session_state.q5_selected_material_type = {
                    'short': selected_material_short,
                    'label': selected_material_label,
                    'uri': selected_material_uri,
                    'name': selected_material_name
                }
            else:
                st.session_state.q5_selected_material_type = None
            
            st.markdown("---")
            
            # DETECTED CONCENTRATION SELECTION
            st.markdown("### 📊 Detected Concentration")

            include_nondetects = st.checkbox(
                "Include nondetects",
                value=st.session_state.get("q5_include_nondetects", False),
                key="q5_include_nondetects",
                help="Include observations with zero concentration or nondetect flags"
            )

            if "q5_conc_range" not in st.session_state:
                st.session_state.q5_conc_range = (st.session_state.q5_conc_min, st.session_state.q5_conc_max)
            if "q5_conc_min_input" not in st.session_state:
                st.session_state.q5_conc_min_input = st.session_state.q5_conc_min
            if "q5_conc_max_input" not in st.session_state:
                st.session_state.q5_conc_max_input = st.session_state.q5_conc_max

            def sync_q5_from_inputs():
                min_val = st.session_state.q5_conc_min_input
                max_val = st.session_state.q5_conc_max_input
                st.session_state.q5_conc_min = min_val
                st.session_state.q5_conc_max = max_val
                st.session_state.q5_conc_range = (min_val, max_val)

            def sync_q5_from_slider():
                min_val, max_val = st.session_state.q5_conc_range
                st.session_state.q5_conc_min = min_val
                st.session_state.q5_conc_max = max_val
                st.session_state.q5_conc_min_input = min_val
                st.session_state.q5_conc_max_input = max_val

            max_limit = 60000

            st.session_state.q5_conc_min_input = min(st.session_state.q5_conc_min_input, max_limit)
            st.session_state.q5_conc_max_input = min(st.session_state.q5_conc_max_input, max_limit)
            range_min, range_max = st.session_state.q5_conc_range
            range_min = min(range_min, max_limit)
            range_max = min(range_max, max_limit)
            if range_min > range_max:
                range_max = range_min
            st.session_state.q5_conc_range = (range_min, range_max)

            col1, col2 = st.columns(2)
            
            with col1:
                q5_min_concentration = st.number_input(
                    "Min (ng/L)",
                    min_value=0,
                    max_value=max_limit,
                    value=st.session_state.q5_conc_min_input,
                    step=1,
                    key="q5_conc_min_input",
                    help="Minimum concentration in nanograms per liter",
                    on_change=sync_q5_from_inputs
                )
                st.session_state.q5_conc_min = q5_min_concentration
            
            with col2:
                q5_max_concentration = st.number_input(
                    "Max (ng/L)",
                    min_value=0,
                    max_value=max_limit,
                    value=st.session_state.q5_conc_max_input,
                    step=1,
                    key="q5_conc_max_input",
                    help="Maximum concentration in nanograms per liter",
                    on_change=sync_q5_from_inputs
                )
                st.session_state.q5_conc_max = q5_max_concentration
            
            st.slider(
                "Drag to adjust range",
                min_value=0,
                max_value=max_limit,
                value=st.session_state.q5_conc_range,
                step=1,
                key="q5_conc_range",
                help="Drag the slider or use the number inputs above for precise control",
                on_change=sync_q5_from_slider
            )

            q5_min_concentration, q5_max_concentration = st.session_state.q5_conc_range
            
            if q5_min_concentration > q5_max_concentration:
                st.warning("⚠️ Min concentration cannot be greater than max")
            
            if q5_max_concentration <= 10:
                st.info("🟢 Low range - background levels")
            elif q5_max_concentration <= 70:
                st.info("🟡 Moderate range - measurable contamination")
            else:
                st.warning("🔴 High range - significant concern")
            
            st.markdown("---")
            county_selected = bool(st.session_state.get("selected_county_code"))
            can_execute = county_selected and state_has_data
            execute_q5 = st.button(
                "🔍 Execute Query",
                type="primary",
                use_container_width=True,
                disabled=not can_execute,
                help="Select a state with data and a county first" if not can_execute else "Execute the downstream tracing analysis",
                key="execute_q5"
            )
        
        if execute_q5:
            if not selected_state_code:
                st.error("❌ **State selection is required!** Please select a state before executing the query.")
            else:
                if selected_subdivision_code:
                    query_region_code = str(selected_subdivision_code)
                elif hasattr(st.session_state, 'selected_county_code') and st.session_state.selected_county_code:
                    query_region_code = str(st.session_state.selected_county_code)
                else:
                    query_region_code = str(selected_state_code).zfill(2)
                
                params_data = []
                params_data.append({
                    "Parameter": "PFAS Substance",
                    "Value": selected_substance_name if selected_substance_name else "All Substances"
                })
                params_data.append({
                    "Parameter": "Material Type",
                    "Value": selected_material_name if selected_material_name else "All Material Types"
                })
                conc_value = f"{q5_min_concentration} - {q5_max_concentration} ng/L"
                if include_nondetects:
                    conc_value += " (including nondetects)"
                params_data.append({
                    "Parameter": "Detected Concentration",
                    "Value": conc_value
                })
                
                region_display = selected_state_name
                if selected_subdivision_name:
                    region_display = f"{selected_subdivision_name}, {selected_county_name}, {selected_state_name}"
                elif selected_county_name:
                    region_display = f"{selected_county_name}, {selected_state_name}"
                params_data.append({"Parameter": "Geographic Region", "Value": region_display})
                
                params_df = pd.DataFrame(params_data)
                
                st.markdown("---")
                st.subheader("🚀 Query Execution")
                
                prog_col1, prog_col2, prog_col3 = st.columns(3)
                
                starting_samples_df = pd.DataFrame()
                downstream_s2_df = pd.DataFrame()
                downstream_flowlines_df = pd.DataFrame()
                downstream_samples_df = pd.DataFrame()
                downstream_samples_outside_start_df = pd.DataFrame()
                downstream_overlap_count = 0
                
                step_errors = {}
                debug_info = {}
                
                with prog_col1:
                    with st.spinner("🔄 Step 1: Finding contaminated samples..."):
                        effective_min = 0 if include_nondetects else q5_min_concentration
                        starting_samples_df, step1_error, step1_debug = execute_downstream_step1_query(
                            substance_uri=selected_substance_uri,
                            material_uri=selected_material_uri,
                            min_conc=effective_min,
                            max_conc=q5_max_concentration,
                            region_code=query_region_code,
                            include_nondetects=include_nondetects
                        )
                        debug_info["step1"] = step1_debug
                        if step1_error:
                            step_errors["step1"] = step1_error
                    
                    region_boundary_df = get_region_boundary(query_region_code)
                    
                    if step1_error:
                        st.error(f"❌ Step 1 failed: {step1_error}")
                    elif not starting_samples_df.empty:
                        st.success(f"✅ Step 1: Found {len(starting_samples_df)} contaminated samples")
                    else:
                        st.warning("⚠️ Step 1: No contaminated samples found")
                
                with prog_col2:
                    if not starting_samples_df.empty:
                        with st.spinner("🔄 Step 2: Tracing downstream flow paths..."):
                            downstream_s2_df, downstream_flowlines_df, step2_error, step2_debug = execute_downstream_hydrology_query(
                                starting_samples_df
                            )
                            debug_info["step2"] = step2_debug
                            if step2_error:
                                step_errors["step2"] = step2_error
                        
                        if step2_error:
                            st.error(f"❌ Step 2 failed: {step2_error}")
                        elif not downstream_s2_df.empty:
                            st.success("✅ Step 2: Traced downstream flow paths")
                        else:
                            st.info("ℹ️ Step 2: No downstream flow paths found")
                    else:
                        st.info("⏭️ Step 2: Skipped (no samples)")
                
                with prog_col3:
                    if not downstream_s2_df.empty:
                        with st.spinner("🔄 Step 3: Finding downstream contaminated samples..."):
                            effective_min = 0 if include_nondetects else q5_min_concentration
                            downstream_samples_df, step3_error, step3_debug = execute_downstream_samples_query(
                                downstream_s2_df=downstream_s2_df,
                                substance_uri=selected_substance_uri,
                                material_uri=selected_material_uri,
                                min_conc=effective_min,
                                max_conc=q5_max_concentration
                            )
                            debug_info["step3"] = step3_debug
                            if step3_error:
                                step_errors["step3"] = step3_error

                        downstream_samples_outside_start_df = downstream_samples_df
                        downstream_overlap_count = 0
                        if (
                            downstream_samples_df is not None
                            and not downstream_samples_df.empty
                            and 'sp' in downstream_samples_df.columns
                            and 'sp' in starting_samples_df.columns
                        ):
                            overlap_mask = downstream_samples_df['sp'].isin(starting_samples_df['sp'].dropna().unique())
                            downstream_overlap_count = int(overlap_mask.sum())
                            downstream_samples_outside_start_df = downstream_samples_df[~overlap_mask].reset_index(drop=True)
                        
                        if step3_error:
                            st.error(f"❌ Step 3 failed: {step3_error}")
                        elif not downstream_samples_df.empty:
                            outside_count = len(downstream_samples_outside_start_df) if downstream_samples_outside_start_df is not None else 0
                            st.success(
                                f"✅ Step 3: Found {len(downstream_samples_df)} downstream contaminated samples "
                                f"({outside_count} outside starting region)"
                            )
                        else:
                            st.info("ℹ️ Step 3: No downstream contaminated samples found")
                    else:
                        st.info("⏭️ Step 3: Skipped (no downstream flow paths)")
                
                starting_samples_df = starting_samples_df.drop(columns=["s2cell"], errors="ignore")
                
                st.session_state['q5_results'] = {
                    'starting_samples_df': starting_samples_df,
                    'downstream_flowlines_df': downstream_flowlines_df,
                    'downstream_samples_df': downstream_samples_df,
                    'downstream_samples_outside_start_df': downstream_samples_outside_start_df,
                    'downstream_overlap_count': downstream_overlap_count,
                    'step_errors': step_errors,
                    'debug_info': debug_info,
                    'region_boundary_df': region_boundary_df,
                    'params_df': params_df,
                    'query_region_code': query_region_code,
                    'selected_material_name': selected_material_name
                }
                st.session_state['q5_has_results'] = True
        
        # Display results if available
        if st.session_state.get('q5_has_results', False):
            results = st.session_state.q5_results
            
            starting_samples_df = results.get('starting_samples_df')
            downstream_flowlines_df = results.get('downstream_flowlines_df')
            downstream_samples_df = results.get('downstream_samples_df')
            downstream_samples_outside_start_df = results.get('downstream_samples_outside_start_df')
            downstream_overlap_count = results.get('downstream_overlap_count')
            debug_info = results.get('debug_info')
            region_boundary_df = results.get('region_boundary_df')
            params_df = results.get('params_df')
            query_region_code = results.get('query_region_code')
            saved_material_name = results.get('selected_material_name')
            
            st.markdown("---")
            st.markdown("### 📋 Selected Parameters (from executed query)")
            st.table(params_df)
            
            if debug_info:
                with st.expander("🐞 Debug Info (queries & response details)"):
                    step2_debug = debug_info.get("step2") or {}
                    step2_flowlines_debug = step2_debug.get("downstream_flowlines") or {}
                    st.json(
                        {
                            "step_errors": results.get("step_errors"),
                            "step1": {
                                k: v
                                for k, v in (debug_info.get("step1") or {}).items()
                                if k not in {"query", "response_text_snippet"}
                            },
                            "step2": {k: v for k, v in step2_flowlines_debug.items() if k not in {"query", "response_text_snippet"}},
                            "step3": {
                                k: v
                                for k, v in (debug_info.get("step3") or {}).items()
                                if k not in {"query", "response_text_snippet"}
                            },
                        }
                    )
            
            st.markdown("---")
            st.markdown("### 🔬 Query Results")
            
            # Step 1 Results
            if starting_samples_df is not None and not starting_samples_df.empty:
                st.markdown("### 🔬 Step 1: Contaminated Samples (Start)")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Samples", len(starting_samples_df))
                with col2:
                    if 'sp' in starting_samples_df.columns:
                        st.metric("Unique Sample Points", starting_samples_df['sp'].nunique())
                with col3:
                    st.metric("Material Type", saved_material_name or "All")
                with col4:
                    if 'max' in starting_samples_df.columns:
                        avg_max = pd.to_numeric(starting_samples_df['max'], errors='coerce').mean()
                        if pd.notna(avg_max):
                            st.metric("Avg Max Concentration", f"{avg_max:.2f} ng/L")
                
                with st.expander("📊 View Starting Samples Data"):
                    st.dataframe(starting_samples_df, use_container_width=True)
                    st.download_button(
                        label="📥 Download Starting Samples CSV",
                        data=starting_samples_df.to_csv(index=False),
                        file_name=f"downstream_starting_samples_{query_region_code}.csv",
                        mime="text/csv",
                        key="download_q5_starting_samples"
                    )
            
            # Step 2 Results
            if downstream_flowlines_df is not None and not downstream_flowlines_df.empty:
                st.markdown("### 🌊 Step 2: Downstream Flow Paths")
                st.metric("Flowlines (mapped)", len(downstream_flowlines_df))
            
            # Step 3 Results
            if downstream_samples_df is not None and not downstream_samples_df.empty:
                st.markdown("### ⬇️ Step 3: Contaminated Samples (Downstream)")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Samples", len(downstream_samples_df))
                with col2:
                    if 'sp' in downstream_samples_df.columns:
                        st.metric("Unique Sample Points", downstream_samples_df['sp'].nunique())
                with col3:
                    if 'max' in downstream_samples_df.columns:
                        avg_max = pd.to_numeric(downstream_samples_df['max'], errors='coerce').mean()
                        if pd.notna(avg_max):
                            st.metric("Avg Max Concentration", f"{avg_max:.2f} ng/L")
                with col4:
                    if downstream_samples_outside_start_df is not None:
                        st.metric("Outside Starting Region", len(downstream_samples_outside_start_df))
                
                with st.expander("📊 View Downstream Samples Data"):
                    st.dataframe(downstream_samples_df, use_container_width=True)
                    st.download_button(
                        label="📥 Download Downstream Samples CSV",
                        data=downstream_samples_df.to_csv(index=False),
                        file_name=f"downstream_contaminated_samples_{query_region_code}.csv",
                        mime="text/csv",
                        key="download_q5_downstream_samples"
                    )

                    if downstream_samples_outside_start_df is not None and downstream_samples_outside_start_df.empty:
                        st.caption(
                            "All downstream contaminated samples found are also present in the starting sample set "
                            f"(overlap: {downstream_overlap_count or 0})."
                        )
            
            # Map
            has_starting_wkt = starting_samples_df is not None and not starting_samples_df.empty and 'spWKT' in starting_samples_df.columns
            has_downstream_wkt = downstream_samples_df is not None and not downstream_samples_df.empty and 'spWKT' in downstream_samples_df.columns
            has_flow_wkt = downstream_flowlines_df is not None and not downstream_flowlines_df.empty and 'downstream_flowlineWKT' in downstream_flowlines_df.columns
            
            if has_starting_wkt or has_downstream_wkt or has_flow_wkt:
                st.markdown("---")
                st.markdown("### 🗺️ Interactive Map")
                
                starting_gdf = None
                downstream_gdf = None
                flowlines_gdf = None
                
                if has_starting_wkt:
                    starting_with_wkt = starting_samples_df[starting_samples_df['spWKT'].notna()].copy()
                    if not starting_with_wkt.empty:
                        try:
                            starting_with_wkt['geometry'] = starting_with_wkt['spWKT'].apply(wkt.loads)
                            starting_gdf = gpd.GeoDataFrame(starting_with_wkt, geometry='geometry')
                            starting_gdf.set_crs(epsg=4326, inplace=True, allow_override=True)
                        except Exception as e:
                            st.warning(f"Could not parse starting sample geometries: {e}")
                
                if has_downstream_wkt:
                    downstream_with_wkt = downstream_samples_df[downstream_samples_df['spWKT'].notna()].copy()
                    if not downstream_with_wkt.empty:
                        try:
                            downstream_with_wkt['geometry'] = downstream_with_wkt['spWKT'].apply(wkt.loads)
                            downstream_gdf = gpd.GeoDataFrame(downstream_with_wkt, geometry='geometry')
                            downstream_gdf.set_crs(epsg=4326, inplace=True, allow_override=True)
                        except Exception as e:
                            st.warning(f"Could not parse downstream sample geometries: {e}")
                
                if has_flow_wkt:
                    flowlines_with_wkt = downstream_flowlines_df[downstream_flowlines_df['downstream_flowlineWKT'].notna()].copy()
                    if not flowlines_with_wkt.empty:
                        try:
                            flowlines_with_wkt['geometry'] = flowlines_with_wkt['downstream_flowlineWKT'].apply(wkt.loads)
                            flowlines_gdf = gpd.GeoDataFrame(flowlines_with_wkt, geometry='geometry')
                            flowlines_gdf.set_crs(epsg=4326, inplace=True, allow_override=True)
                        except Exception as e:
                            st.warning(f"Could not parse flowline geometries: {e}")
                
                if starting_gdf is not None and not starting_gdf.empty:
                    center_lat = starting_gdf.geometry.y.mean()
                    center_lon = starting_gdf.geometry.x.mean()
                    map_obj = folium.Map(location=[center_lat, center_lon], zoom_start=8)
                elif downstream_gdf is not None and not downstream_gdf.empty:
                    center_lat = downstream_gdf.geometry.y.mean()
                    center_lon = downstream_gdf.geometry.x.mean()
                    map_obj = folium.Map(location=[center_lat, center_lon], zoom_start=8)
                else:
                    map_obj = folium.Map(location=[39.8, -98.5], zoom_start=4)
                
                # Add administrative boundary if available
                if region_boundary_df is not None and not region_boundary_df.empty:
                    try:
                        boundary_wkt = region_boundary_df.iloc[0]['countyWKT']
                        boundary_name = region_boundary_df.iloc[0].get('countyName', 'Region')
                        
                        from shapely import wkt as shapely_wkt
                        boundary_geom = shapely_wkt.loads(boundary_wkt)
                        boundary_gdf = gpd.GeoDataFrame([{
                            'name': boundary_name,
                            'geometry': boundary_geom
                        }], crs='EPSG:4326')
                        
                        region_code_len = len(str(query_region_code))
                        if region_code_len > 5:
                            boundary_color = '#666666'
                        elif region_code_len == 5:
                            boundary_color = '#666666'
                        else:
                            boundary_color = '#444444'
                        
                        boundary_gdf.explore(
                            m=map_obj,
                            name=f'<span style="color:{boundary_color};">📍 {boundary_name} Boundary</span>',
                            color=boundary_color,
                            style_kwds=dict(
                                fillColor='none',
                                weight=3,
                                opacity=0.8,
                                dashArray='5, 5'
                            ),
                            overlay=True,
                            show=True
                        )
                    except Exception as e:
                        print(f"Error displaying boundary: {e}")
                        pass
                
                if flowlines_gdf is not None and not flowlines_gdf.empty:
                    flowlines_gdf.explore(
                        m=map_obj,
                        name='<span style="color:DodgerBlue;">🌊 Downstream Flowlines</span>',
                        color='DodgerBlue',
                        style_kwds=dict(weight=2, opacity=0.5),
                        tooltip=False,
                        popup=False
                    )
                
                if starting_gdf is not None and not starting_gdf.empty:
                    starting_gdf.explore(
                        m=map_obj,
                        name='<span style="color:DarkOrange;">🔬 Starting Samples</span>',
                        color='DarkOrange',
                        marker_kwds=dict(radius=8),
                        marker_type='circle_marker',
                        popup=['sp', 'max', 'resultCount'] if all(col in starting_gdf.columns for col in ['sp', 'max', 'resultCount']) else True,
                        tooltip=['max'] if 'max' in starting_gdf.columns else None,
                        style_kwds=dict(fillOpacity=0.7, opacity=0.8)
                    )
                
                if downstream_gdf is not None and not downstream_gdf.empty:
                    downstream_gdf.explore(
                        m=map_obj,
                        name='<span style="color:Purple;">⬇️ Downstream Samples</span>',
                        color='Purple',
                        marker_kwds=dict(radius=6),
                        marker_type='circle_marker',
                        popup=['sp', 'max', 'resultCount'] if all(col in downstream_gdf.columns for col in ['sp', 'max', 'resultCount']) else True,
                        tooltip=['max'] if 'max' in downstream_gdf.columns else None,
                        style_kwds=dict(fillOpacity=0.7, opacity=0.8)
                    )
                
                folium.LayerControl(collapsed=False).add_to(map_obj)
                
                st_folium(map_obj, width=None, height=600, returned_objects=[])
                
                st.info("""
                **🗺️ Map Legend:**
                - 🟠 **Orange circles** = Starting contaminated sample locations (in selected region)
                - 🔵 **Blue lines** = Downstream flow paths (hydrological connections)
                - 🟣 **Purple circles** = Downstream contaminated sample locations (found along traced paths)
                - 📍 **Boundary outline** = Selected region
                """)
        else:
            st.info("👈 Select parameters in the sidebar and click 'Execute Query' to run the analysis")
        
    elif query_number == 2:
        # SAMPLES NEAR FACILITIES QUERY
        st.markdown("""
        **What this analysis does:**
        - Find all facilities of a specific industry type in your region
        - Expand search to neighboring areas (S2 cells)
        - Identify contaminated samples near those facilities
        
        **Use case:** Determine if PFAS contamination exists near specific industries (e.g., sewage treatment, landfills, manufacturing)
        """)
        
        # Initialize session state for Query 2 specific params
        if 'q2_conc_min' not in st.session_state:
            st.session_state.q2_conc_min = 0
        if 'q2_conc_max' not in st.session_state:
            st.session_state.q2_conc_max = 100
        
        # --- SIDEBAR PARAMETERS FOR QUERY 2 ---
        # Industry selector using hierarchical tree dropdown (outside form for compatibility)
        st.sidebar.markdown("### 🏭 Industry Type")
        pfas_industries = get_pfas_industry_options()
        if not pfas_industries:
            st.sidebar.info("Using default NAICS list (PFAS industries query returned no results).")
            pfas_industries = NAICS_INDUSTRIES

        default_naics = "221320" if "221320" in pfas_industries else next(iter(pfas_industries), "")
        selected_naics_codes = render_hierarchical_naics_selector(
            naics_dict=pfas_industries,
            key="q2_industry_selector",
            default_value=default_naics,
            multi_select=True,
        )

        if isinstance(selected_naics_codes, str):
            selected_naics_codes = [selected_naics_codes] if selected_naics_codes else []
        selected_naics_codes = [code for code in selected_naics_codes if code]

        def _collapse_naics_selections(codes: list[str]) -> list[str]:
            collapsed = []
            for code in sorted(set(codes), key=len):
                if any(code.startswith(parent) and len(code) > len(parent) for parent in collapsed):
                    continue
                collapsed = [
                    existing
                    for existing in collapsed
                    if not (existing.startswith(code) and len(existing) > len(code))
                ]
                collapsed.append(code)
            return collapsed

        selected_naics_codes = _collapse_naics_selections(selected_naics_codes)

        selected_labels = []
        for code in selected_naics_codes:
            label = pfas_industries.get(code, "Unknown")
            selected_labels.append(f"{code} - {label}" if label else code)

        if not selected_labels:
            selected_industry_display = "No selection"
        elif len(selected_labels) <= 3:
            selected_industry_display = ", ".join(selected_labels)
        else:
            selected_industry_display = f"{len(selected_labels)} industries selected"


        # Other parameters
        with st.sidebar.container():
            
            # Detected Concentration (same as Query 1)
            st.markdown("### 📊 Detected Concentration")

            include_nondetects = st.checkbox(
                "Include nondetects",
                value=st.session_state.get("q2_include_nondetects", False),
                key="q2_include_nondetects",
                help="Include observations with zero concentration or nondetect flags"
            )
            region_code = None
            is_subdivision = False
            if selected_subdivision_code:
                region_code = str(selected_subdivision_code)
                is_subdivision = True
            elif selected_county_code:
                region_code = str(selected_county_code).zfill(5)

            q2_max_limit = 60000

            if "q2_conc_range" not in st.session_state:
                st.session_state.q2_conc_range = (st.session_state.q2_conc_min, st.session_state.q2_conc_max)
            if "q2_conc_min_input" not in st.session_state:
                st.session_state.q2_conc_min_input = st.session_state.q2_conc_min
            if "q2_conc_max_input" not in st.session_state:
                st.session_state.q2_conc_max_input = st.session_state.q2_conc_max

            def sync_q2_from_inputs():
                min_val = st.session_state.q2_conc_min_input
                max_val = st.session_state.q2_conc_max_input
                st.session_state.q2_conc_min = min_val
                st.session_state.q2_conc_max = max_val
                st.session_state.q2_conc_range = (min_val, max_val)

            def sync_q2_from_slider():
                min_val, max_val = st.session_state.q2_conc_range
                st.session_state.q2_conc_min = min_val
                st.session_state.q2_conc_max = max_val
                st.session_state.q2_conc_min_input = min_val
                st.session_state.q2_conc_max_input = max_val

            st.session_state.q2_conc_min_input = min(st.session_state.q2_conc_min_input, q2_max_limit)
            st.session_state.q2_conc_max_input = min(st.session_state.q2_conc_max_input, q2_max_limit)
            range_min, range_max = st.session_state.q2_conc_range
            range_min = min(range_min, q2_max_limit)
            range_max = min(range_max, q2_max_limit)
            if range_min > range_max:
                range_max = range_min
            st.session_state.q2_conc_range = (range_min, range_max)
            
            col1, col2 = st.columns(2)
            with col1:
                q2_min_concentration = st.number_input(
                    "Min (ng/L)",
                    min_value=0,
                    max_value=q2_max_limit,
                    step=1,
                    key="q2_conc_min_input",
                    help="Minimum concentration in nanograms per liter",
                    on_change=sync_q2_from_inputs
                )
                st.session_state.q2_conc_min = q2_min_concentration
            
            with col2:
                q2_max_concentration = st.number_input(
                    "Max (ng/L)",
                    min_value=0,
                    max_value=q2_max_limit,
                    step=1,
                    key="q2_conc_max_input",
                    help="Maximum concentration in nanograms per liter",
                    on_change=sync_q2_from_inputs
                )
                st.session_state.q2_conc_max = q2_max_concentration
            
            # Slider for visual adjustment
            st.slider(
                "Drag to adjust range",
                min_value=0,
                max_value=q2_max_limit,
                value=st.session_state.q2_conc_range,
                step=1,
                key="q2_conc_range",
                help="Drag the slider or use the number inputs above",
                on_change=sync_q2_from_slider
            )

            q2_min_concentration, q2_max_concentration = st.session_state.q2_conc_range
            
            # Validate range
            if q2_min_concentration > q2_max_concentration:
                st.warning("⚠️ Min cannot be greater than max")
            
            # Show concentration context
            if q2_max_concentration <= 10:
                st.info("🟢 Low range - background levels")
            elif q2_max_concentration <= 70:
                st.info("🟡 Moderate range - measurable contamination")
            else:
                st.warning("🔴 High range - significant concern")
            
            st.markdown("---")
            
            # Execute button (same style as Query 1)
            county_selected = bool(st.session_state.get("selected_county_code"))
            industry_selected = bool(selected_naics_codes)
            can_execute = county_selected and state_has_data and industry_selected
            execute_q2 = st.button(
                "🔍 Execute Query",
                type="primary",
                use_container_width=True,
                disabled=not can_execute,
                help=(
                    "Select a state with data, a county, and at least one industry first"
                    if not can_execute
                    else "Execute the nearby facilities analysis"
                ),
                key="execute_q2"
            )
        
        # Determine region code for query
        region_code_q2 = None
        if selected_state_code:
            if selected_subdivision_code:
                region_code_q2 = str(selected_subdivision_code)
            elif selected_county_code:
                region_code_q2 = selected_county_code
            else:
                region_code_q2 = str(selected_state_code).zfill(2)
        
        # Execute the query when form is submitted
        if execute_q2:
            if not selected_state_code:
                st.error("❌ Please select a state in the sidebar first!")
            else:
                with st.spinner(f"Searching for samples near {selected_industry_display}..."):
                    # Execute the consolidated analysis (single query)
                    facilities_df, samples_df = execute_nearby_analysis(
                        naics_code=selected_naics_codes,
                        region_code=region_code_q2,
                        min_concentration=q2_min_concentration,
                        max_concentration=q2_max_concentration,
                        include_nondetects=include_nondetects
                    )
                    
                    # Store results in session state
                    st.session_state['q2_facilities'] = facilities_df
                    st.session_state['q2_samples'] = samples_df
                    st.session_state['q2_industry'] = selected_industry_display
                    st.session_state['q2_industry_codes'] = selected_naics_codes
                    st.session_state['q2_industry_labels'] = {
                        code: pfas_industries.get(code, code) for code in selected_naics_codes
                    }
                    st.session_state['q2_region_code'] = region_code_q2
                    st.session_state['q2_range_used'] = (q2_min_concentration, q2_max_concentration)
                    st.session_state['q2_executed'] = True
        
        # Display Results
        if st.session_state.get('q2_executed', False):
            facilities_df = st.session_state.get('q2_facilities', pd.DataFrame())
            samples_df = st.session_state.get('q2_samples', pd.DataFrame())
            industry_display = st.session_state.get('q2_industry', '')
            
            st.markdown("---")
            st.markdown("### 📊 Results")

            range_used = st.session_state.get('q2_range_used')
            if range_used:
                caption_text = f"Detected concentration filter used: {range_used[0]} - {range_used[1]} ng/L"
                if st.session_state.get("q2_include_nondetects"):
                    caption_text += " (including nondetects)"
                st.caption(caption_text)
            
            # Metrics
            col1, col2 = st.columns(2)
            with col1:
                st.metric("🏭 Facilities Found", len(facilities_df))
            with col2:
                st.metric("🧪 Contaminated Samples", len(samples_df))
            
            # Results tabs
            if not facilities_df.empty or not samples_df.empty:
                tab1, tab2, tab3 = st.tabs(["🗺️ Map", "🏭 Facilities", "🧪 Samples"])
                
                with tab1:
                    # Create map
                    if not facilities_df.empty and 'facWKT' in facilities_df.columns:
                        st.markdown("#### Interactive Map")
                        
                        # Convert to GeoDataFrames
                        try:
                            facilities_gdf = facilities_df.copy()
                            facilities_gdf['geometry'] = facilities_gdf['facWKT'].apply(wkt.loads)
                            facilities_gdf = gpd.GeoDataFrame(facilities_gdf, geometry='geometry', crs='EPSG:4326')
                            
                            # Get center point
                            center_lat = facilities_gdf.geometry.centroid.y.mean()
                            center_lon = facilities_gdf.geometry.centroid.x.mean()
                            
                            # Create map
                            map_obj = folium.Map(location=[center_lat, center_lon], zoom_start=8)
                            
                            # Add region boundary if available
                            query_region_code = st.session_state.get('q2_region_code')
                            if query_region_code:
                                region_boundary_df = get_region_boundary(query_region_code)
                                if region_boundary_df is not None and not region_boundary_df.empty:
                                    boundary_wkt = region_boundary_df.iloc[0]['countyWKT']
                                    boundary_name = region_boundary_df.iloc[0].get('countyName', 'Region')
                                    boundary_gdf = gpd.GeoDataFrame(
                                        index=[0], crs="EPSG:4326", 
                                        geometry=[wkt.loads(boundary_wkt)]
                                    )
                                    
                                    # Determine boundary color based on region type
                                    region_code_len = len(str(query_region_code))
                                    if region_code_len > 5:
                                        boundary_color = '#FF0000'  # Red for subdivision
                                        region_type = "Subdivision"
                                    elif region_code_len == 5:
                                        boundary_color = '#666666'  # Gray for county
                                        region_type = "County"
                                    else:
                                        boundary_color = '#000000'  # Black for state
                                        region_type = "State"
                                    
                                    folium.GeoJson(
                                        boundary_gdf.to_json(),
                                        name=f'<span style="color:{boundary_color};">📍 {region_type}: {boundary_name}</span>',
                                        style_function=lambda x, color=boundary_color: {
                                            'fillColor': '#ffffff00',
                                            'color': color,
                                            'weight': 3,
                                            'fillOpacity': 0.0
                                        }
                                    ).add_to(map_obj)
                            
                            # Add facilities (blue markers)
                            selected_codes = st.session_state.get("q2_industry_codes", [])
                            if "industryCode" in facilities_gdf.columns and selected_codes:
                                facilities_gdf["industry_code_short"] = (
                                    facilities_gdf["industryCode"]
                                    .astype(str)
                                    .str.extract(r"NAICS-(\d+)")
                                    .fillna("")
                                )
                                sorted_codes = sorted(set(selected_codes), key=len, reverse=True)

                                def _match_industry(code_value: str) -> str:
                                    for code in sorted_codes:
                                        if not code:
                                            continue
                                        if len(code) > 4 and code_value == code:
                                            return code
                                        if len(code) <= 4 and code_value.startswith(code):
                                            return code
                                    return "Other"

                                facilities_gdf["industry_selection"] = facilities_gdf[
                                    "industry_code_short"
                                ].apply(_match_industry)

                                palette = [
                                    "#1f77b4",
                                    "#2ca02c",
                                    "#d62728",
                                    "#9467bd",
                                    "#8c564b",
                                    "#e377c2",
                                    "#7f7f7f",
                                    "#17becf",
                                    "#1b6ca8",
                                    "#2f4f4f",
                                ]
                                color_map = {
                                    code: palette[idx % len(palette)]
                                    for idx, code in enumerate(sorted_codes)
                                }
                                color_map["Other"] = "#666666"

                                industry_labels = st.session_state.get("q2_industry_labels", {})

                                for code in sorted_codes + ["Other"]:
                                    subset = facilities_gdf[
                                        facilities_gdf["industry_selection"] == code
                                    ]
                                    if subset.empty:
                                        continue
                                    label = industry_labels.get(code, code)
                                    if code == "Other":
                                        layer_title = f'<span style="color:{color_map[code]};">🏭 Other Facilities ({len(subset)})</span>'
                                    else:
                                        layer_title = (
                                            f'<span style="color:{color_map[code]};">🏭 '
                                            f'{code} - {label} ({len(subset)})</span>'
                                        )
                                    subset.explore(
                                        m=map_obj,
                                        name=layer_title,
                                        color=color_map[code],
                                        marker_kwds=dict(radius=8),
                                        popup=['facilityName', 'industryName']
                                        if 'facilityName' in subset.columns
                                        else True,
                                        show=True,
                                    )
                            else:
                                facility_layer_name = (
                                    f'<span style="color:Blue;">🏭 Facilities ({len(facilities_gdf)})</span>'
                                )
                                facilities_gdf.explore(
                                    m=map_obj,
                                    name=facility_layer_name,
                                    color='Blue',
                                    marker_kwds=dict(radius=8),
                                    popup=['facilityName', 'industryName'] if 'facilityName' in facilities_gdf.columns else True,
                                    show=True
                                )
                            
                            # Add samples if available (orange markers)
                            if not samples_df.empty and 'spWKT' in samples_df.columns:
                                samples_gdf = samples_df.copy()
                                samples_gdf['geometry'] = samples_gdf['spWKT'].apply(wkt.loads)
                                samples_gdf = gpd.GeoDataFrame(samples_gdf, geometry='geometry', crs='EPSG:4326')
                                
                                sample_popup_cols = [
                                    c for c in ["spName", "max", "results", "Materials"] if c in samples_gdf.columns
                                ]
                                samples_gdf.explore(
                                    m=map_obj,
                                    name=f'<span style="color:DarkOrange;">🧪 Contaminated Samples ({len(samples_gdf)})</span>',
                                    color='DarkOrange',
                                    marker_kwds=dict(radius=6),
                                    popup=sample_popup_cols if sample_popup_cols else True,
                                    show=True
                                )
                            
                            # Add layer control
                            folium.LayerControl(collapsed=True).add_to(map_obj)
                            
                            # Display map
                            st_folium(map_obj, width=None, height=600, returned_objects=[])
                            
                            st.caption(f"Selected industries: {industry_display}")

                            st.info("""
                            **🗺️ Map Legend:**
                            - 📍 **Boundary** = Selected region (black=state, gray=county, red=subdivision)
                            - 🏭 **Colored markers** = Facilities grouped by selected industry codes (see layer list)
                            - 🟠 **Orange markers** = Contaminated sample points nearby
                            """)
                            
                        except Exception as e:
                            st.error(f"Error creating map: {e}")
                    else:
                        st.warning("No facility location data available for mapping")
                
                with tab2:
                    if not facilities_df.empty:
                        st.markdown(f"#### 🏭 {industry_display}")
                        
                        # Select display columns
                        display_cols = [c for c in ['facilityName', 'industryName', 'facility'] if c in facilities_df.columns]
                        if display_cols:
                            st.dataframe(facilities_df[display_cols], use_container_width=True)
                        else:
                            st.dataframe(facilities_df, use_container_width=True)
                    else:
                        st.info("No facilities found matching the criteria")
                
                with tab3:
                    if not samples_df.empty:
                        st.markdown("#### 🧪 Contaminated Sample Points")
                        
                        # Select display columns
                        display_cols = [
                            c for c in [
                                'spName',
                                'max',
                                'resultCount',
                                'results',
                                'datedresults',
                                'dates',
                                'Type',
                                'Materials',
                                'sp'
                            ]
                            if c in samples_df.columns
                        ]
                        if display_cols:
                            st.dataframe(samples_df[display_cols], use_container_width=True)
                        else:
                            st.dataframe(samples_df, use_container_width=True)
                        
                        # Summary statistics
                        if 'max' in samples_df.columns or 'maxConcentration' in samples_df.columns:
                            st.markdown("##### 📈 Concentration Statistics")
                            max_col = 'max' if 'max' in samples_df.columns else 'maxConcentration'
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Max (ng/L)", f"{samples_df[max_col].max():.2f}")
                            with col2:
                                st.metric("Mean (ng/L)", f"{samples_df[max_col].mean():.2f}")
                            with col3:
                                st.metric("Median (ng/L)", f"{samples_df[max_col].median():.2f}")
                    else:
                        st.info("No contaminated samples found near the selected facilities")
            else:
                st.warning("No results found. Try a different industry type or region.")
        else:
            st.info("👈 Select parameters in the sidebar and click 'Find Samples Near Facilities' to run the analysis")

    elif query_number == 6:
        # SOCKG SITES & FACILITIES QUERY
        st.markdown("""
        **What this analysis does:**
        - Retrieves SOCKG locations (ARS sites)
        - Finds nearby facilities and flags PFAS-related industries

        **State filter:** Optional (limited to states with SOCKG sites)
        """)

        st.sidebar.markdown("### 🧪 Query Parameters")
        state_filter_label = selected_state_name if selected_state_name else "All SOCKG states"
        st.sidebar.caption(f"State filter: {state_filter_label}")

        execute_sockg = st.sidebar.button(
            "🔍 Execute Query",
            type="primary",
            use_container_width=True,
            key="execute_sockg",
            help="Fetch SOCKG locations and nearby facilities"
        )

        if execute_sockg:
            with st.spinner("Running SOCKG queries..."):
                sites_df = get_sockg_locations_cached(selected_state_code)
                facilities_df = get_sockg_facilities_cached(selected_state_code)

            st.session_state.sockg_results = {
                "sites_df": sites_df,
                "facilities_df": facilities_df,
                "selected_state_name": selected_state_name,
                "selected_state_code": selected_state_code,
            }
            st.session_state.sockg_has_results = True

        if st.session_state.get("sockg_has_results", False):
            results = st.session_state.sockg_results
            sites_df = results.get("sites_df", pd.DataFrame())
            facilities_df = results.get("facilities_df", pd.DataFrame())
            state_name = results.get("selected_state_name")

            st.markdown("---")
            st.markdown("### 📊 Results")
            st.caption(f"State filter used: {state_name if state_name else 'All SOCKG states'}")

            pfas_count = 0
            if not facilities_df.empty and "PFASusing" in facilities_df.columns:
                pfas_count = facilities_df["PFASusing"].astype(str).str.lower().eq("true").sum()

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("📍 SOCKG Locations", len(sites_df))
            with col2:
                st.metric("🏭 Facilities", len(facilities_df))
            with col3:
                st.metric("⚠️ PFAS-Related Facilities", pfas_count)

            if not sites_df.empty or not facilities_df.empty:
                tab1, tab2, tab3 = st.tabs(["🗺️ Map", "📍 Locations", "🏭 Facilities"])

                with tab1:
                    sites_gdf = None
                    facilities_gdf = None

                    if not sites_df.empty and "locationGeometry" in sites_df.columns:
                        sites_with_wkt = sites_df[sites_df["locationGeometry"].notna()].copy()
                        if not sites_with_wkt.empty:
                            sites_with_wkt["geometry"] = sites_with_wkt["locationGeometry"].apply(wkt.loads)
                            sites_gdf = gpd.GeoDataFrame(sites_with_wkt, geometry="geometry")
                            sites_gdf.set_crs(epsg=4326, inplace=True, allow_override=True)

                    if not facilities_df.empty and "facWKT" in facilities_df.columns:
                        fac_with_wkt = facilities_df[facilities_df["facWKT"].notna()].copy()
                        if not fac_with_wkt.empty:
                            fac_with_wkt["PFASusing"] = (
                                fac_with_wkt["PFASusing"].astype(str).str.lower() == "true"
                            )
                            fac_with_wkt["geometry"] = fac_with_wkt["facWKT"].apply(wkt.loads)
                            facilities_gdf = gpd.GeoDataFrame(fac_with_wkt, geometry="geometry")
                            facilities_gdf.set_crs(epsg=4326, inplace=True, allow_override=True)

                    if sites_gdf is not None or facilities_gdf is not None:
                        if sites_gdf is not None and not sites_gdf.empty:
                            centroids = sites_gdf.geometry.centroid
                            center_lat = centroids.y.mean()
                            center_lon = centroids.x.mean()
                            map_obj = folium.Map(location=[center_lat, center_lon], zoom_start=6)
                        elif facilities_gdf is not None and not facilities_gdf.empty:
                            centroids = facilities_gdf.geometry.centroid
                            center_lat = centroids.y.mean()
                            center_lon = centroids.x.mean()
                            map_obj = folium.Map(location=[center_lat, center_lon], zoom_start=6)
                        else:
                            map_obj = folium.Map(location=[39.8, -98.5], zoom_start=4)

                        if sites_gdf is not None and not sites_gdf.empty:
                            sites_points = sites_gdf.copy()
                            sites_points["geometry"] = sites_points.geometry.centroid
                            sites_points.explore(
                                m=map_obj,
                                name='<span style="color:Red;">📍 SOCKG Locations</span>',
                                color="Red",
                                marker_kwds=dict(radius=6),
                                marker_type="circle_marker",
                                popup=["locationId", "locationDescription"]
                                if all(c in sites_points.columns for c in ["locationId", "locationDescription"])
                                else True,
                                show=True,
                            )

                        if facilities_gdf is not None and not facilities_gdf.empty:
                            facilities_points = facilities_gdf.copy()
                            facilities_points["geometry"] = facilities_points.geometry.centroid
                            pfas_facilities = facilities_points[facilities_points["PFASusing"]]
                            other_facilities = facilities_points[~facilities_points["PFASusing"]]

                            if not other_facilities.empty:
                                other_facilities.explore(
                                    m=map_obj,
                                    name='<span style="color:MidnightBlue;">🏭 Other Facilities</span>',
                                    color="MidnightBlue",
                                    marker_kwds=dict(radius=4),
                                    marker_type="circle_marker",
                                    popup=["facilityName", "industrySector"]
                                    if all(c in other_facilities.columns for c in ["facilityName", "industrySector"])
                                    else True,
                                    show=True,
                                )

                            if not pfas_facilities.empty:
                                pfas_facilities.explore(
                                    m=map_obj,
                                    name='<span style="color:DarkRed;">⚠️ PFAS-Related Facilities</span>',
                                    color="DarkRed",
                                    marker_kwds=dict(radius=5),
                                    marker_type="circle_marker",
                                    popup=["facilityName", "industries"]
                                    if all(c in pfas_facilities.columns for c in ["facilityName", "industries"])
                                    else True,
                                    show=True,
                                )

                        selected_state_code = results.get("selected_state_code")
                        if selected_state_code:
                            region_boundary_df = get_region_boundary(selected_state_code)
                            if region_boundary_df is not None and not region_boundary_df.empty:
                                boundary_wkt = region_boundary_df.iloc[0]["countyWKT"]
                                boundary_name = region_boundary_df.iloc[0].get("countyName", "State")
                                boundary_gdf = gpd.GeoDataFrame(
                                    index=[0],
                                    crs="EPSG:4326",
                                    geometry=[wkt.loads(boundary_wkt)]
                                )
                                folium.GeoJson(
                                    boundary_gdf.to_json(),
                                    name=f'<span style="color:#444444;">📍 {boundary_name} Boundary</span>',
                                    style_function=lambda x: {
                                        "fillColor": "#ffffff00",
                                        "color": "#444444",
                                        "weight": 3,
                                        "fillOpacity": 0.0,
                                    },
                                ).add_to(map_obj)

                        folium.LayerControl(collapsed=False).add_to(map_obj)
                        st_folium(map_obj, width=None, height=600, returned_objects=[])

                        st.info("""
                        **🗺️ Map Legend:**
                        - 🔴 **Red circles** = SOCKG locations (ARS sites)
                        - 🔵 **Blue circles** = Other facilities
                        - 🟥 **Dark red circles** = PFAS-related facilities
                        """)
                    else:
                        st.info("No spatial data available to render the map.")

                with tab2:
                    if not sites_df.empty:
                        display_cols = [
                            c for c in ["locationId", "locationDescription", "location"] if c in sites_df.columns
                        ]
                        st.dataframe(sites_df[display_cols] if display_cols else sites_df, use_container_width=True)
                    else:
                        st.info("No SOCKG locations found for the selected state.")

                with tab3:
                    if not facilities_df.empty:
                        display_cols = [
                            c
                            for c in [
                                "facilityName",
                                "industrySector",
                                "industrySubsector",
                                "PFASusing",
                                "industries",
                                "locations",
                            ]
                            if c in facilities_df.columns
                        ]
                        st.dataframe(
                            facilities_df[display_cols] if display_cols else facilities_df,
                            use_container_width=True,
                        )
                    else:
                        st.info("No facilities found for the selected state.")
            else:
                st.warning("No results found. Try again or remove the state filter.")
        else:
            st.info("👈 Select a state (optional) and click 'Execute Query' to run the analysis")

    elif query_number == 3:
        # REGIONAL CONTAMINATION OVERVIEW
        st.subheader("📊 Query 3: Regional Contamination Overview")
        st.markdown("""
        **What this query does:**
        - Provides statistical overview of PFAS contamination in a region
        - Shows distribution of different PFAS compounds
        - Identifies contamination hotspots
        - Compares contamination levels across sub-regions
        
        **Use case:** Get a comprehensive overview of PFAS contamination in an area
        """)
        
        st.info("🚧 This query type is coming soon! Stay tuned for updates.")

    elif query_number == 4:
        # FACILITY RISK ASSESSMENT
        st.subheader("⚠️ Query 4: Facility Risk Assessment")
        st.markdown("""
        **What this query does:**
        - Assess which facilities are at highest risk of causing PFAS contamination
        - Analyze facility types, locations, and proximity to water sources
        - Identify facilities in upstream areas of known contamination
        - Provide risk scores based on multiple factors
        
        **Use case:** Prioritize facilities for investigation or compliance monitoring
        """)
        
        st.info("🚧 This query type is coming soon! Stay tuned for updates.")

except FileNotFoundError as e:
    st.error("❌ Could not find required CSV file!")
    st.info("Please make sure all CSV files are in the data/ directory:")
    st.code("""
    data/
    ├── us_administrative_regions_fips.csv
    ├── pfas_substances.csv
    └── sample_material_types.csv
    """)
    st.info("Run the following scripts to generate them:")
    st.code("""
    python get_fips_codes.py
    python get_substances.py
    python get_material_types.py
    """)
except Exception as e:
    st.error(f"❌ An error occurred: {str(e)}")
    import traceback
    with st.expander("Show error details"):
        st.code(traceback.format_exc())
