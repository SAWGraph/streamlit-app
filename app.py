





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
    
    # Determine if this is a state-level or county/subdivision-level query
    # State codes are 2 digits, county codes are 5 digits, subdivision codes are 10 digits
    is_state_query = len(sanitized_region) == 2

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

SELECT DISTINCT ?sp ?spWKT ?upstream_flowlineWKT ?facility ?facWKT ?facilityName 
                ?industryName ?industryGroup ?industryGroupName ?industrySubsector ?industrySubsectorName
                ?substance ?sample ?matType ?result_value ?unit
WHERE {{
    ?sp rdf:type coso:SamplePoint ;
        geo:hasGeometry/geo:asWKT ?spWKT ;
        spatial:connectedTo ?ar ;
        spatial:connectedTo ?s2 .
    
    {"?ar rdf:type kwg-ont:AdministrativeRegion_2 ; kwg-ont:sfWithin kwgr:administrativeRegion.USA." + sanitized_region + " ." if is_state_query else "?ar rdf:type kwg-ont:AdministrativeRegion_3 ; kwg-ont:administrativePartOf kwgr:administrativeRegion.USA." + sanitized_region + " ."}
    
    ?s2 rdf:type kwg-ont:S2Cell_Level13 .
    
    ?s2cell rdf:type kwg-ont:S2Cell_Level13 ;
             kwg-ont:sfTouches | owl:sameAs ?s2 .
    
    ?observation rdf:type coso:ContaminantObservation ;
                coso:observedAtSamplePoint ?sp ;
                coso:ofSubstance ?substance ;
                coso:analyzedSample ?sample ;
                coso:hasResult ?result .
    
    ?sample coso:sampleOfMaterialType ?matType .
    
    ?result coso:measurementValue ?result_value ;
            coso:measurementUnit ?unit .
    
    VALUES ?unit {{<http://qudt.org/vocab/unit/NanoGM-PER-L>}}
    {substance_filter}
    {material_filter}
    FILTER (?result_value >= {min_conc})
    FILTER (?result_value <= {max_conc})
    
    ?downstream_flowline rdf:type hyf:HY_FlowPath ;
                         spatial:connectedTo ?s2cell .
    
    ?upstream_flowline hyf:downstreamFlowPathTC ?downstream_flowline ;
                       geo:hasGeometry/geo:asWKT ?upstream_flowlineWKT .
    
    ?s2cellus spatial:connectedTo ?upstream_flowline ;
              rdf:type kwg-ont:S2Cell_Level13 .
    
    OPTIONAL {{
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
                           rdfs:label ?industrySubsectorName .
    }}
}}
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
    # Updated column names based on the optimized query output
    sample_columns = [
        "sp",
        "spWKT",
        "substance",
        "sample",
        "matType",
        "result_value",
        "unit",
    ]

    # The new query returns just upstream_flowlineWKT
    upstream_columns = ["upstream_flowlineWKT"]

    facility_columns = [
        "facility",
        "facWKT",
        "facilityName",
        "industryName",
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

    # Extract samples data - only columns that exist in the new query
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
    - Counties (extracted from labels of subdivisions)
    - Subdivisions (10+ digit codes)
    """
    # Get states (2-digit FIPS codes)
    # Note: FIPS codes are stored as integers, so we need to handle leading zeros
    # State codes are 1-56, so we filter for codes less than 100
    states = df[df['fipsCode'] < 100].copy()
    # Remove "Geometry of " prefix if present
    states['state_name'] = states['label'].str.replace('Geometry of ', '', regex=False)
    # Remove duplicates - keep first occurrence of each state
    states = states.drop_duplicates(subset=['fipsCode'], keep='first')
    states = states.sort_values('state_name')
    
    # Get subdivisions (codes longer than county level)
    # Counties are typically 5 digits or less, subdivisions are longer (usually 10+ digits)
    subdivisions = df[df['fipsCode'] >= 100000].copy()
    
    # Parse county information from subdivision labels
    # Pattern: "Geometry of [Subdivision], [County], [State]"
    if not subdivisions.empty:
        # Extract subdivision, county, and state from label
        subdivisions['subdivision_name'] = subdivisions['label'].str.replace('Geometry of ', '', regex=False).str.split(', ').str[0]
        subdivisions['county_name'] = subdivisions['label'].str.split(', ').str[-2]
        subdivisions['state_name_sub'] = subdivisions['label'].str.split(', ').str[-1]
        
        # Get state code (first 2 digits of FIPS)
        subdivisions['state_code'] = subdivisions['fipsCode'].astype(str).str[:2].str.zfill(2)
    
    return states, subdivisions

# Load data
try:
    df = load_fips_data()
    states_df, subdivisions_df = parse_regions(df)
    
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
        
        if selected_state_code:
            # Filter subdivisions by state
            state_subdivisions = subdivisions_df[
                subdivisions_df['state_code'] == str(selected_state_code).zfill(2)
            ]
        
            if not state_subdivisions.empty:
                # Get unique counties for this state
                counties = state_subdivisions['county_name'].dropna().unique()
                counties_clean = []
                for county in counties:
                    if county:
                        clean_name = county.replace('Geometry of ', '')
                        if clean_name not in counties_clean:
                            counties_clean.append(clean_name)
                counties_clean = sorted(counties_clean)
            
                if len(counties_clean) > 0:
                    county_options = ["-- All Counties --"] + list(counties_clean)
                    selected_county_display = st.sidebar.selectbox(
                        "2️⃣ Select County (Optional)",
                        county_options,
                        help=f"Select a county within {selected_state_name}"
                    )
                
                    if selected_county_display != "-- All Counties --":
                        selected_county_name = selected_county_display
                        county_rows = state_subdivisions[
                            (state_subdivisions['county_name'] == selected_county_name) |
                            (state_subdivisions['county_name'] == f'Geometry of {selected_county_name}')
                        ]
                        if not county_rows.empty:
                            first_fips = str(county_rows.iloc[0]['fipsCode']).zfill(10)
                            selected_county_code = first_fips[:5]
                        st.session_state.selected_county = selected_county_name
                        st.session_state.selected_county_code = selected_county_code
                    else:
                        st.session_state.selected_county = None
                        st.session_state.selected_county_code = None
        else:
            st.sidebar.info("👆 Please select a state first")
        
        # 3. SUBDIVISION SELECTION (Optional, filtered by county)
        selected_subdivision_code = None
        selected_subdivision_name = None
        if selected_state_code and selected_county_name:
            # Filter subdivisions by state and county
            county_subdivisions = state_subdivisions[
                (state_subdivisions['county_name'] == selected_county_name) |
                (state_subdivisions['county_name'] == f'Geometry of {selected_county_name}')
            ]
        
            if not county_subdivisions.empty:
                subdivision_options = ["-- All Subdivisions --"] + \
                    county_subdivisions['subdivision_name'].dropna().tolist()
            
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
                        selected_subdivision_code = subdivision_row.iloc[0]['fipsCode']
                        st.session_state.selected_subdivision = {
                            'name': selected_subdivision_name,
                            'code': str(selected_subdivision_code)
                        }
                else:
                    st.session_state.selected_subdivision = None
            else:
                st.sidebar.info("No subdivisions available for this county")
                st.session_state.selected_subdivision = None
        
        # Query Parameters Section
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 🧪 PFAS Substance")
    
        # Get unique substances and sort them
        unique_substances = sorted(substances_df['shortName'].unique())
        substance_options = ["-- All Substances --"] + list(unique_substances)
    
        selected_substance_display = st.sidebar.selectbox(
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
    
        st.sidebar.markdown("---")
    
        # MATERIAL TYPE SELECTION (Optional)
        st.sidebar.markdown("### 🧫 Sample Material Type")
    
        # Create dropdown options with short code and label
        material_type_options = ["-- All Material Types --"]
        material_type_display = {}
    
        for idx, row in material_types_df.iterrows():
            display_name = f"{row['shortName']} - {row['label']}"
            material_type_options.append(display_name)
            material_type_display[display_name] = row
    
        selected_material_display = st.sidebar.selectbox(
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
    
        st.sidebar.markdown("---")
    
        # CONCENTRATION RANGE SELECTION (Optional)
        st.sidebar.markdown("### 📊 Concentration Range")
    
        # Custom CSS for styled buttons
        st.sidebar.markdown("""
            <style>
            div.stButton > button[kind="secondary"] {
                background-color: transparent;
                border: 2px solid rgba(255, 255, 255, 0.3);
                border-radius: 8px;
                font-size: 20px;
                padding: 8px;
                transition: all 0.3s ease;
            }
            div.stButton > button[kind="secondary"]:hover {
                border-color: rgba(255, 255, 255, 0.6);
                transform: scale(1.05);
            }
            </style>
        """, unsafe_allow_html=True)
    
        # Top row: Plus buttons (green) - with callbacks
        col_top = st.sidebar.columns([1, 6, 1])
        with col_top[0]:
            st.markdown("""
                <style>
                div[data-testid="column"]:nth-of-type(1) div.stButton > button {
                    background: linear-gradient(135deg, #4CAF50, #66BB6A) !important;
                    border: none !important;
                    color: white !important;
                    border-radius: 8px !important;
                    font-size: 18px !important;
                    padding: 6px 10px !important;
                    box-shadow: 0 2px 8px rgba(76, 175, 80, 0.3) !important;
                    transition: all 0.3s ease !important;
                    margin-bottom: 0px !important;
                    width: 100% !important;
                    height: 32px !important;
                }
                div[data-testid="column"]:nth-of-type(1) div.stButton {
                    margin-bottom: -15px !important;
                }
                div[data-testid="column"]:nth-of-type(1) div.stButton > button:hover {
                    transform: scale(1.05) !important;
                    box-shadow: 0 4px 12px rgba(76, 175, 80, 0.5) !important;
                }
                </style>
            """, unsafe_allow_html=True)
            if st.button("➕", key="min_plus", help="Increase min", use_container_width=True):
                if st.session_state.conc_min < st.session_state.conc_max:
                    st.session_state.conc_min += 1
                    st.rerun()
        with col_top[1]:
            st.markdown("<div style='height: 0px;'></div>", unsafe_allow_html=True)
        with col_top[2]:
            st.markdown("""
                <style>
                div[data-testid="column"]:nth-of-type(3) div.stButton > button {
                    background: linear-gradient(135deg, #4CAF50, #66BB6A) !important;
                    border: none !important;
                    color: white !important;
                    border-radius: 8px !important;
                    font-size: 18px !important;
                    padding: 6px 10px !important;
                    box-shadow: 0 2px 8px rgba(76, 175, 80, 0.3) !important;
                    transition: all 0.3s ease !important;
                    margin-bottom: 0px !important;
                    width: 100% !important;
                    height: 32px !important;
                }
                div[data-testid="column"]:nth-of-type(3) div.stButton {
                    margin-bottom: -15px !important;
                }
                div[data-testid="column"]:nth-of-type(3) div.stButton > button:hover {
                    transform: scale(1.05) !important;
                    box-shadow: 0 4px 12px rgba(76, 175, 80, 0.5) !important;
                }
                </style>
            """, unsafe_allow_html=True)
            if st.button("➕", key="max_plus", help="Increase max", use_container_width=True):
                if st.session_state.conc_max < 500:
                    st.session_state.conc_max += 1
                    st.rerun()
    
        # Middle row: Interactive Streamlit slider
        # Use a callback to only update when slider is actually moved by user
        def update_concentration_range():
            st.session_state.conc_min = st.session_state.concentration_slider[0]
            st.session_state.conc_max = st.session_state.concentration_slider[1]
        
        concentration_range = st.sidebar.slider(
            "Drag to adjust range",
            min_value=0,
            max_value=500,
            value=(st.session_state.conc_min, st.session_state.conc_max),
            step=1,
            key="concentration_slider",
            label_visibility="collapsed",
            help="Drag the handles to adjust min/max values, or use +/- buttons for precise control",
            on_change=update_concentration_range
        )
    
        # Get current values directly from session state
        min_concentration = st.session_state.conc_min
        max_concentration = st.session_state.conc_max
    
        # Bottom row: Minus buttons (styled)
        col_bottom = st.sidebar.columns([1, 6, 1])
        with col_bottom[0]:
            st.markdown("""
                <style>
                div[data-testid="column"]:nth-of-type(1) div.stButton > button[kind="secondary"] {
                    background: rgba(255, 255, 255, 0.1) !important;
                    border: 1px solid rgba(255, 255, 255, 0.3) !important;
                    color: white !important;
                    border-radius: 8px !important;
                    font-size: 18px !important;
                    padding: 6px 10px !important;
                    transition: all 0.3s ease !important;
                    margin-top: 0px !important;
                    width: 100% !important;
                    height: 32px !important;
                }
                div[data-testid="column"]:nth-of-type(1) div.stButton {
                    margin-top: -15px !important;
                }
                div[data-testid="column"]:nth-of-type(1) div.stButton > button[kind="secondary"]:hover {
                    background: rgba(255, 255, 255, 0.15) !important;
                    border-color: rgba(255, 255, 255, 0.5) !important;
                    transform: scale(1.05) !important;
                }
                </style>
            """, unsafe_allow_html=True)
            if st.button("➖", key="min_minus", help="Decrease min", use_container_width=True):
                if st.session_state.conc_min > 0:
                    st.session_state.conc_min -= 1
                    st.rerun()
        with col_bottom[1]:
            st.markdown("<div style='height: 0px;'></div>", unsafe_allow_html=True)
        with col_bottom[2]:
            st.markdown("""
                <style>
                div[data-testid="column"]:nth-of-type(3) div.stButton > button[kind="secondary"] {
                    background: rgba(255, 255, 255, 0.1) !important;
                    border: 1px solid rgba(255, 255, 255, 0.3) !important;
                    color: white !important;
                    border-radius: 8px !important;
                    font-size: 18px !important;
                    padding: 6px 10px !important;
                    transition: all 0.3s ease !important;
                    margin-top: 0px !important;
                    width: 100% !important;
                    height: 32px !important;
                }
                div[data-testid="column"]:nth-of-type(3) div.stButton {
                    margin-top: -15px !important;
                }
                div[data-testid="column"]:nth-of-type(3) div.stButton > button[kind="secondary"]:hover {
                    background: rgba(255, 255, 255, 0.15) !important;
                    border-color: rgba(255, 255, 255, 0.5) !important;
                    transform: scale(1.05) !important;
                }
                </style>
            """, unsafe_allow_html=True)
            if st.button("➖", key="max_minus", help="Decrease max", use_container_width=True):
                if st.session_state.conc_max > st.session_state.conc_min:
                    st.session_state.conc_max -= 1
                    st.rerun()
    
        # Show concentration context
        if max_concentration <= 10:
            st.sidebar.info("🟢 Low range - background levels")
        elif max_concentration <= 70:
            st.sidebar.info("🟡 Moderate range - measurable contamination")
        else:
            st.sidebar.warning("🔴 High range - significant concern")
    
        # Execute Query Button
        st.sidebar.markdown("---")
        execute_button = st.sidebar.button(
            "🔍 Execute Query",
            type="primary",
            use_container_width=True,
            help="Display all selected parameters ready for SPARQL query execution"
        )
    
        # Display query parameters when Execute button is clicked
        if execute_button:
            st.markdown("---")
            st.subheader("🚀 Query Execution")
        
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
                
                # ========== SELECTED PARAMETERS SUMMARY (TOP) ==========
                st.markdown("### 📋 Selected Parameters")
                
                # Build parameter table
                params_data = []
                
                # Substance
                if selected_substance_name:
                    params_data.append({
                        "Parameter": "PFAS Substance",
                        "Value": selected_substance_name
                    })
                else:
                    params_data.append({
                        "Parameter": "PFAS Substance",
                        "Value": "All Substances"
                    })
                
                # Material Type
                if selected_material_short:
                    params_data.append({
                        "Parameter": "Material Type",
                        "Value": f"{selected_material_short} - {selected_material_label}"
                    })
                else:
                    params_data.append({
                        "Parameter": "Material Type",
                        "Value": "All Material Types"
                    })
                
                # Concentration Range
                params_data.append({
                    "Parameter": "Concentration Range",
                    "Value": f"{min_concentration} - {max_concentration} ng/L"
                })
                
                # Geographic Region
                region_display = selected_state_name
                if selected_subdivision_name:
                    region_display = f"{selected_subdivision_name}, {selected_county_name}, {selected_state_name}"
                elif selected_county_name:
                    region_display = f"{selected_county_name}, {selected_state_name}"
                
                params_data.append({
                    "Parameter": "Geographic Region",
                    "Value": region_display
                })
                
                # Display as clean table
                params_df = pd.DataFrame(params_data)
                st.table(params_df)
                
                st.markdown("---")
                st.markdown("### 🔬 Query Results")
            
                # Store results from combined query
                samples_df = pd.DataFrame()
                upstream_s2_df = pd.DataFrame()
                facilities_df = pd.DataFrame()
                combined_df = None
                combined_error = None
                debug_info = None
            
                # Create columns for progress display
                prog_col1, prog_col2, prog_col3 = st.columns(3)
            
                # Step 1: Run combined query (samples + upstream tracing + facilities)
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

                    if combined_error:
                        st.error(f"❌ Step 1 failed: {combined_error}")
                    elif not samples_df.empty:
                        st.success(f"✅ Step 1: Found {len(samples_df)} contaminated samples")
                    else:
                        st.warning("⚠️ Step 1: No contaminated samples found")
            
                # Step 2: Trace upstream flow paths (only if step 1 succeeded)
                with prog_col2:
                    if not samples_df.empty:
                        if not upstream_s2_df.empty:
                            st.success(f"✅ Step 2: Traced {len(upstream_s2_df)} upstream paths")
                        else:
                            st.info("ℹ️ Step 2: No upstream sources found")
                    else:
                        st.info("⏭️ Step 2: Skipped (no samples)")
            
                # Step 3: Find facilities (only if step 2 succeeded)
                with prog_col3:
                    if not upstream_s2_df.empty:
                        if not facilities_df.empty:
                            st.success(f"✅ Step 3: Found {len(facilities_df)} facilities")
                        else:
                            st.info("ℹ️ Step 3: No facilities found")
                    else:
                        st.info("⏭️ Step 3: Skipped (no upstream cells)")

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
                            st.metric("Material Type", selected_material_name or "All")
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
                    
                    # Industry breakdown in separate expander
                    if 'industryName' in facilities_df.columns:
                        with st.expander("📈 Industry Type Breakdown"):
                            industry_counts = facilities_df['industryName'].value_counts()
                            
                            # Display as a clean table
                            industry_df = pd.DataFrame({
                                'Industry Type': industry_counts.index,
                                'Facility Count': industry_counts.values.astype(int),  # Convert to int to avoid null display
                                'Percentage': (industry_counts.values / len(facilities_df) * 100).round(1)
                            })
                            
                            # Format percentage column with % symbol
                            industry_df['Percentage'] = industry_df['Percentage'].apply(lambda x: f"{x}%")
                            
                            st.dataframe(industry_df, use_container_width=True, hide_index=True)
                            
                            # Add a simple bar chart visualization
                            st.bar_chart(industry_counts.head(10), height=300)
            
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
                    
                        # Add county boundaries if available
                        if hasattr(st.session_state, 'selected_county_code') and st.session_state.selected_county_code:
                            # Try to get county boundary from US Census TIGER API
                            try:
                                import requests
                                # Extract state and county codes from session state
                                county_code = str(st.session_state.selected_county_code)
                                state_fips = county_code[:2]
                                county_fips = county_code[2:5]
                                county_name = st.session_state.get('selected_county', 'County')
                                
                                # US Census TIGER API for county boundaries
                                tiger_url = f"https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/State_County/MapServer/1/query"
                                params = {
                                    'where': f"STATE='{state_fips}' AND COUNTY='{county_fips}'",
                                    'outFields': '*',
                                    'f': 'geojson',
                                    'geometryPrecision': 6
                                }
                                
                                response = requests.get(tiger_url, params=params, timeout=5)
                                if response.status_code == 200:
                                    county_geojson = response.json()
                                    if county_geojson.get('features'):
                                        # Add county boundary as a GeoJSON layer
                                        county_style = {
                                            'fillColor': 'none',
                                            'color': '#666666',
                                            'weight': 3,
                                            'opacity': 0.8,
                                            'dashArray': '5, 5'
                                        }
                                        folium.GeoJson(
                                            county_geojson,
                                            name=f'<span style="color:#666;">📍 {county_name} Boundary</span>',
                                            style_function=lambda x: county_style,
                                            overlay=True,
                                            control=True
                                        ).add_to(map_obj)
                            except Exception as e:
                                # If TIGER API fails, just continue without county boundaries
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
                        - 📍 **Gray outline** = County boundary (if available)
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
