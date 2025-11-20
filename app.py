





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
from streamlit_folium import st_folium
import geopandas as gpd
from shapely import wkt

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

# SIDEBAR: Analysis Selection at the top
st.sidebar.markdown("### 📊 Select Analysis Type")
analysis_type = st.sidebar.selectbox(
    "Choose analysis:",
    [
        "PFAS Upstream Tracing",
        "Samples Near Facilities",
        "Regional Contamination Overview",
        "Facility Risk Assessment"
    ],
    help="Choose the type of analysis you want to perform"
)

st.sidebar.markdown("---")

# Map analysis type to query number
analysis_map = {
    "PFAS Upstream Tracing": 1,
    "Samples Near Facilities": 2,
    "Regional Contamination Overview": 3,
    "Facility Risk Assessment": 4
}
query_number = analysis_map[analysis_type]

# Title based on selection
if query_number == 1:
    st.title("🌊 PFAS Upstream Tracing")
elif query_number == 2:
    st.title("🏭 Samples Near Facilities")
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

# Load the FIPS data
@st.cache_data
def load_fips_data():
    """Load and parse the FIPS codes CSV"""
    csv_path = os.path.join(PROJECT_DIR, "us_administrative_regions_fips.csv")
    df = pd.read_csv(csv_path)
    return df

# Load the substances data
@st.cache_data
def load_substances_data():
    """Load and parse the PFAS substances CSV"""
    csv_path = os.path.join(PROJECT_DIR, "pfas_substances.csv")
    df = pd.read_csv(csv_path)
    return df

# Load the material types data
@st.cache_data
def load_material_types_data():
    """Load and parse the sample material types CSV"""
    csv_path = os.path.join(PROJECT_DIR, "sample_material_types.csv")
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

def execute_combined_query(substance_uri, material_uri, min_conc, max_conc, region_code):
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
    
    return states, counties, subdivisions

# Load data
try:
    df = load_fips_data()
    states_df, counties_df, subdivisions_df = parse_regions(df)
    
    substances_df = load_substances_data()
    material_types_df = load_material_types_data()
    
    st.success(f"✅ Loaded {len(df)} administrative regions, {len(substances_df)} PFAS substances, and {len(material_types_df)} material types")
    
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
    
    # Display UI based on query type selection
    if query_number == 1:
        # PFAS UPSTREAM TRACING QUERY
        st.markdown("""
        **What this analysis does:**
        - Finds water samples with PFAS contamination in your selected region
        - Traces upstream through hydrological flow paths  
        - Identifies industrial facilities that may be contamination sources
        
        **3-Step Process:** Find contamination → Trace upstream → Identify potential sources
        """)
        
        # Sidebar parameters
        st.sidebar.header("🔧 Analysis Configuration")
    
        # Initialize session state
        if 'selected_substance' not in st.session_state:
            st.session_state.selected_substance = None
        if 'selected_material_type' not in st.session_state:
            st.session_state.selected_material_type = None
        if 'conc_min' not in st.session_state:
            st.session_state.conc_min = 0
        if 'conc_max' not in st.session_state:
            st.session_state.conc_max = 100
        if 'selected_state' not in st.session_state:
            st.session_state.selected_state = None
        if 'selected_county' not in st.session_state:
            st.session_state.selected_county = None
        if 'selected_subdivision' not in st.session_state:
            st.session_state.selected_subdivision = None
        
        # GEOGRAPHIC REGION SELECTION (Moved to top)
        st.sidebar.markdown("### 📍 Geographic Region")
        st.sidebar.markdown("🆃 **Required**: Select at least a state")
    
        # 1. STATE SELECTION (Mandatory)
        state_options = ["-- Select a State --"] + states_df['state_name'].tolist()
        selected_state_display = st.sidebar.selectbox(
            "1️⃣ Select State",
            state_options,
            help="Select a US state or territory"
        )
    
        # Get the selected state's FIPS code
        selected_state_code = None
        selected_state_name = None
        if selected_state_display != "-- Select a State --":
            selected_state_name = selected_state_display
            state_row = states_df[states_df['state_name'] == selected_state_display]
            if not state_row.empty:
                selected_state_code = state_row.iloc[0]['fipsCode']
                st.session_state.selected_state = {
                    'name': selected_state_name,
                    'code': str(selected_state_code).zfill(2)
                }
    
        # 2. COUNTY SELECTION (Optional, filtered by state)
        selected_county_name = None
        selected_county_code = None
        state_subdivisions = pd.DataFrame()  # Initialize empty DataFrame
        state_counties = pd.DataFrame()  # Initialize empty DataFrame
        
        if selected_state_code:
            # Filter counties by state using the actual counties dataframe
            state_counties = counties_df[
                counties_df['state_code'] == str(selected_state_code).zfill(2)
            ]
            
            # Also filter subdivisions by state for later use
            state_subdivisions = subdivisions_df[
                subdivisions_df['state_code'] == str(selected_state_code).zfill(2)
            ]
        
            if not state_counties.empty:
                # Get county names directly from counties dataframe
                county_options = ["-- All Counties --"] + state_counties['county_name'].sort_values().tolist()
                selected_county_display = st.sidebar.selectbox(
                    "2️⃣ Select County (Optional)",
                    county_options,
                    help=f"Select a county within {selected_state_name}"
                )
            
                if selected_county_display != "-- All Counties --":
                    selected_county_name = selected_county_display
                    # Get county code from counties_df
                    county_row = state_counties[state_counties['county_name'] == selected_county_name]
                    if not county_row.empty:
                        selected_county_code = county_row.iloc[0]['county_code']
                    st.session_state.selected_county = selected_county_name
                    st.session_state.selected_county_code = selected_county_code
                else:
                    st.session_state.selected_county = None
                    st.session_state.selected_county_code = None
            else:
                # No counties found for this state (e.g., Alaska, or incomplete data)
                st.sidebar.info(f"ℹ️ No county-level data available for {selected_state_name}. You can query at the state level.")
        else:
            st.sidebar.info("👆 Please select a state first")
        
        # 3. SUBDIVISION SELECTION (Optional, filtered by county)
        selected_subdivision_code = None
        selected_subdivision_name = None
        if selected_state_code and selected_county_code:
            # Filter subdivisions by county code (more accurate than county name)
            county_subdivisions = state_subdivisions[
                state_subdivisions['county_code'] == selected_county_code
            ]
        
            if not county_subdivisions.empty:
                subdivision_options = ["-- All Subdivisions --"] + \
                    county_subdivisions['subdivision_name'].dropna().sort_values().tolist()
            
                selected_subdivision_display = st.sidebar.selectbox(
                    "3️⃣ Select Subdivision (Optional)",
                    subdivision_options,
                    help=f"Select a subdivision within {selected_county_name}"
                )
            
                if selected_subdivision_display != "-- All Subdivisions --":
                    selected_subdivision_name = selected_subdivision_display
                    subdivision_row = county_subdivisions[
                        county_subdivisions['subdivision_name'] == selected_subdivision_display
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
        elif selected_state_code and selected_county_name: # This block handles the case where a county name is selected but no code is available (e.g., if state_counties was empty)
            st.sidebar.info("No subdivisions available for this county")
            st.session_state.selected_subdivision = None
        
        # Query Parameters Section
        st.sidebar.markdown("---")
        
        # Wrap parameters in a form to prevent immediate reruns
        with st.sidebar.form(key="query_params_form"):
            st.markdown("### 🧪 PFAS Substance")
        
            # Get unique substances and sort them
            unique_substances = sorted(substances_df['shortName'].unique())
            substance_options = ["-- All Substances --"] + list(unique_substances)
        
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
                # Get matching substance URIs - prefer the _A variant if available
                substance_rows = substances_df[substances_df['shortName'] == selected_substance_display]
                if not substance_rows.empty:
                    # Look for the _A variant first (most commonly used in queries)
                    _a_variants = substance_rows[substance_rows['substance'].str.contains('_A', na=False)]
                    if not _a_variants.empty:
                        selected_substance_uri = _a_variants.iloc[0]['substance']
                    else:
                        # Fall back to first available if no _A variant
                        selected_substance_uri = substance_rows.iloc[0]['substance']
                    st.session_state.selected_substance = {
                        'name': selected_substance_name,
                        'uri': selected_substance_uri
                    }
            else:
                st.session_state.selected_substance = None
        
            st.markdown("---")
        
            # MATERIAL TYPE SELECTION (Optional)
            st.markdown("### 🧫 Sample Material Type")
        
            # Create dropdown options with short code and label
            material_type_options = ["-- All Material Types --"]
            material_type_display = {}
        
            for idx, row in material_types_df.iterrows():
                display_name = f"{row['shortName']} - {row['label']}"
                material_type_options.append(display_name)
                material_type_display[display_name] = row
        
            selected_material_display = st.selectbox(
                "Select Material Type (Optional)",
                material_type_options,
                help="Select the type of sample material analyzed (e.g., Drinking Water, Groundwater, Soil)"
            )
        
            # Get the selected material type's details
            selected_material_uri = None
            selected_material_short = None
            selected_material_label = None
            selected_material_name = None  # Add this variable
        
            if selected_material_display != "-- All Material Types --":
                material_info = material_type_display[selected_material_display]
                selected_material_short = material_info['shortName']
                selected_material_label = material_info['label']
                selected_material_uri = material_info['matType']
                selected_material_name = selected_material_display  # Set the display name
                st.session_state.selected_material_type = {
                    'short': selected_material_short,
                    'label': selected_material_label,
                    'uri': selected_material_uri,
                    'name': selected_material_name
                }
            else:
                st.session_state.selected_material_type = None
        
            st.markdown("---")
        
            # CONCENTRATION RANGE SELECTION (Optional)
            st.markdown("### 📊 Concentration Range")
            
            # Simple number inputs for min and max concentration
            col1, col2 = st.columns(2)
            
            with col1:
                min_concentration = st.number_input(
                    "Min (ng/L)",
                    min_value=0,
                    max_value=500,
                    value=st.session_state.conc_min,
                    step=1,
                    key="min_conc_input",
                    help="Minimum concentration in nanograms per liter"
                )
                st.session_state.conc_min = min_concentration
            
            with col2:
                max_concentration = st.number_input(
                    "Max (ng/L)",
                    min_value=0,
                    max_value=500,
                    value=st.session_state.conc_max,
                    step=1,
                    key="max_conc_input",
                    help="Maximum concentration in nanograms per liter"
                )
                st.session_state.conc_max = max_concentration
            
            # Add slider for visual adjustment
            st.slider(
                "Drag to adjust range",
                min_value=0,
                max_value=500,
                value=(st.session_state.conc_min, st.session_state.conc_max),
                step=1,
                key="concentration_slider",
                help="Drag the slider or use the number inputs above for precise control"
            )
            
            # Update session state from slider (will apply on next rerun after submit)
            if st.session_state.concentration_slider != (st.session_state.conc_min, st.session_state.conc_max):
                 st.session_state.conc_min = st.session_state.concentration_slider[0]
                 st.session_state.conc_max = st.session_state.concentration_slider[1]

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
            execute_button = st.form_submit_button(
                "🔍 Execute Query",
                type="primary",
                use_container_width=True,
                help="Display all selected parameters ready for SPARQL query execution"
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
                mat_val = f"{selected_material_short} - {selected_material_label}" if selected_material_short else "All Material Types"
                params_data.append({"Parameter": "Material Type", "Value": mat_val})
                
                # Concentration Range
                params_data.append({"Parameter": "Concentration Range", "Value": f"{min_concentration} - {max_concentration} ng/L"})
                
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
                        combined_df, combined_error, debug_info = execute_combined_query(
                            substance_uri=selected_substance_uri,
                            material_uri=selected_material_uri,
                            min_conc=min_concentration,
                            max_conc=max_concentration,
                            region_code=query_region_code
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
                
                # Flat Industry Breakdown
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
                            # Add length column for sorting
                            flat_data['code_len'] = flat_data['code_clean'].str.len()
                            
                            # Sort by facility and code length (descending) so longest code comes first
                            flat_data = flat_data.sort_values(['facility', 'code_len'], ascending=[True, False])
                            
                            # Deduplicate facilities - keep only the one with the longest code
                            flat_data = flat_data.drop_duplicates(subset=['facility'], keep='first')
                            
                            # Format name as "Name (Code)"
                            flat_data['display_name'] = flat_data.apply(
                                lambda row: f"{row['industryName']} ({row['code_clean']})" if row['code_clean'] else row['industryName'],
                                axis=1
                            )
                        else:
                            flat_data['display_name'] = flat_data['industryName']
                            # Simple deduplication if no codes
                            flat_data = flat_data.drop_duplicates(subset=['facility'], keep='first')
                        
                        # Group by display name
                        industry_summary = flat_data.groupby('display_name').agg(
                            Facilities=('facility', 'nunique')
                        ).reset_index()
                        
                        # Calculate percentage
                        total_facs = flat_data['facility'].nunique() # Use deduplicated count
                        if total_facs > 0:
                            industry_summary['Percentage'] = (industry_summary['Facilities'] / total_facs * 100).map('{:.1f}%'.format)
                        else:
                            industry_summary['Percentage'] = "0.0%"
                        
                        # Rename columns
                        industry_summary.columns = ['Industry', 'Facilities', 'Percentage']
                        
                        # Sort by count
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
                    if facilities_gdf is not None and not facilities_gdf.empty:
                        if 'industryName' in facilities_gdf.columns:
                            # Group by industry and assign colors
                            colors = ['MidnightBlue','MediumBlue','SlateBlue','MediumSlateBlue', 
                                     'DodgerBlue','DeepSkyBlue','SkyBlue','CadetBlue','DarkCyan',
                                     'LightSeaGreen','MediumSeaGreen','PaleVioletRed','Purple',
                                     'Orchid','Fuchsia','MediumVioletRed','HotPink','LightPink']
                        
                            industries = facilities_gdf['industryName'].unique()
                            
                            # First, add "All Facilities" as a single combined layer
                            try:
                                facilities_gdf.explore(
                                    m=map_obj,
                                    name='<span style="color:#4169E1;"><b>🏭 All Facilities (Toggle All)</b></span>',
                                    column='industryName',
                                    cmap=colors[:len(industries)],
                                    marker_kwds=dict(radius=6),
                                    popup=['facilityName', 'industryName'] if all(col in facilities_gdf.columns for col in ['facilityName', 'industryName']) else True,
                                    tooltip=['facilityName'] if 'facilityName' in facilities_gdf.columns else None,
                                    show=True,
                                    legend=False
                                )
                            except:
                                # Fallback: just add all facilities in one color
                                facilities_gdf.explore(
                                    m=map_obj,
                                    name='<span style="color:#4169E1;"><b>🏭 All Facilities</b></span>',
                                    color='RoyalBlue',
                                    marker_kwds=dict(radius=6),
                                    popup=['facilityName', 'industryName'] if all(col in facilities_gdf.columns for col in ['facilityName', 'industryName']) else True,
                                    tooltip=['facilityName'] if 'facilityName' in facilities_gdf.columns else None,
                                    show=True
                                )
                            
                            # Then add individual industry layers (hidden by default)
                            for idx, industry in enumerate(industries):
                                industry_facilities = facilities_gdf[facilities_gdf['industryName'] == industry]
                                color = colors[idx % len(colors)]
                                industry_facilities.explore(
                                    m=map_obj,
                                    name=f'<span style="color:{color};">🏭 {industry}</span>',
                                    color=color,
                                    marker_kwds=dict(radius=6),
                                    popup=['facilityName', 'industryName'] if all(col in industry_facilities.columns for col in ['facilityName', 'industryName']) else True,
                                    tooltip=['facilityName'] if 'facilityName' in industry_facilities.columns else None,
                                    show=False
                                )
                        else:
                            # No industry info, just plot all facilities in one color
                            facilities_gdf.explore(
                                m=map_obj,
                                name='<span style="color:Blue;">🏭 All Facilities</span>',
                                color='Blue',
                                marker_kwds=dict(radius=6),
                                popup=['facilityName'] if 'facilityName' in facilities_gdf.columns else True,
                                show=True
                            )
                
                    # Add OpenStreetMap base layer (explicitly named)
                    folium.TileLayer('openstreetmap', name='OpenStreetMap').add_to(map_obj)
                
                    # Add layer control (collapsed by default to avoid cluttering the map)
                    folium.LayerControl(collapsed=True).add_to(map_obj)
                
                    # Display map
                    st_folium(map_obj, width=None, height=600, returned_objects=[])
                
                    # Map legend
                    st.info("""
                    **🗺️ Map Legend:**
                    - 🟠 **Orange circles** = Contaminated sample locations
                    - 🔵 **Blue lines** = Upstream flow paths (hydrological connections)
                    - 🏭 **Colored markers** = Upstream facilities (grouped by industry type)
                    - 📍 **Boundary outline** = Selected region (red=subdivision, gray=county, if available)
                    - Click markers for details | Use layer control (top right) to toggle layers
                    """)
        
            # Summary of the complete analysis
            st.markdown("---")
            st.markdown("### 📊 Analysis Summary")
        
            summary_text = []
            if samples_df is not None and not samples_df.empty:
                summary_text.append(f"✅ Found **{len(samples_df)}** contaminated samples")
            
                if upstream_s2_df is not None and not upstream_s2_df.empty:
                    summary_text.append(f"✅ Traced **{len(upstream_s2_df)}** upstream flow paths")
                
                    if facilities_df is not None and not facilities_df.empty:
                        summary_text.append(f"✅ Identified **{len(facilities_df)}** potential source facilities")
                        if 'industryName' in facilities_df.columns:
                            top_industry = facilities_df['industryName'].value_counts().iloc[0]
                            summary_text.append(f"🏭 Most common industry type: **{facilities_df['industryName'].value_counts().index[0]}** ({top_industry} facilities)")
                    else:
                        summary_text.append("ℹ️ No facilities found in upstream areas")
                else:
                    summary_text.append("ℹ️ No upstream sources identified")
            else:
                summary_text.append("⚠️ No contaminated samples found with the selected criteria")
                summary_text.append("💡 Try: wider concentration range, different substance, or different region")
        
            for text in summary_text:
                st.write(text)
    elif query_number == 2:
        # SAMPLES NEAR FACILITIES QUERY
        st.subheader("🏭 Query 2: Samples Near Facilities")
        st.markdown("""
        **What this query does:**
        - Find contaminated water samples in a specific region
        - Identify nearby industrial facilities of specific types
        - Analyze spatial proximity between contamination and facilities
        
        **Use case:** Determine if contamination exists near specific industries (e.g., airports, manufacturing plants)
        """)
        
        st.info("🚧 This query type is coming soon! Stay tuned for updates.")
        
        # Placeholder for future implementation
        with st.expander("Preview: What parameters will be available"):
            st.markdown("""
            - **Geographic Region**: State, County, Subdivision
            - **Facility Types**: Airport, Manufacturing, Chemical Plants, etc.
            - **Distance Radius**: How close facilities must be to samples
            - **PFAS Substance**: Optional filter for specific compounds
            - **Concentration Threshold**: Minimum contamination level
            """)

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
    st.info("Please make sure all CSV files are in the project root directory:")
    st.code("""
    - us_administrative_regions_fips.csv
    - pfas_substances.csv
    - sample_material_types.csv
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
