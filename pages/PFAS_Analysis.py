





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
from streamlit_folium import st_folium
import geopandas as gpd
from shapely import wkt

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
    page_title="PFAS Analysis",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# SIDEBAR: Analysis Type Selection (moved to sidebar for better UX)
st.sidebar.title("🔍 Analysis Selector")
st.sidebar.markdown("---")

analysis_type = st.sidebar.selectbox(
    "Select Analysis Type:",
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
    st.title("🌊 PFAS Upstream Tracing Analysis")
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

def convertS2ListToQueryString(s2_list):
    """Convert S2 cell list to SPARQL VALUES format"""
    s2_list_formatted = []
    for s2 in s2_list:
        # Handle different S2 cell URI formats
        if s2.startswith("http://stko-kwg.geog.ucsb.edu/lod/resource/"):
            # Standard format - use prefix
            s2_list_formatted.append(s2.replace("http://stko-kwg.geog.ucsb.edu/lod/resource/", "kwgr:"))
        elif s2.startswith("https://stko-kwg.geog.ucsb.edu/lod/resource/"):
            # HTTPS variant - use prefix
            s2_list_formatted.append(s2.replace("https://stko-kwg.geog.ucsb.edu/lod/resource/", "kwgr:"))
        elif s2.startswith("kwgr:"):
            # Already has prefix
            s2_list_formatted.append(s2)
        elif s2.startswith("http://") or s2.startswith("https://"):
            # Full URI - wrap in angle brackets
            s2_list_formatted.append(f"<{s2}>")
        else:
            # Assume it's already formatted correctly
            s2_list_formatted.append(s2)
    
    return " ".join(s2_list_formatted)

# Load the FIPS data
@st.cache_data
def load_fips_data():
    """Load and parse the FIPS codes CSV"""
    csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                            "us_administrative_regions_fips.csv")
    df = pd.read_csv(csv_path)
    return df

# Load the substances data
@st.cache_data
def load_substances_data():
    """Load and parse the PFAS substances CSV"""
    csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                            "pfas_substances.csv")
    df = pd.read_csv(csv_path)
    return df

# Load the material types data
@st.cache_data
def load_material_types_data():
    """Load and parse the sample material types CSV"""
    csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                            "sample_material_types.csv")
    df = pd.read_csv(csv_path)
    return df

# STEP 1: Get contaminated samples
def execute_sparql_query(substance_uri, material_uri, min_conc, max_conc, region_code):
    """
    STEP 1: Get contaminated samples matching all criteria.
    Runs on the main 'sawgraph' federated endpoint.
    """
    print(f"--- Running Step 1 (on 'federation' endpoint) ---")
    print(f"Finding samples in region: {region_code}")
    
    # Build VALUES clauses
    substance_filter = f"VALUES ?substance {{<{substance_uri}>}}" if substance_uri else "# No substance filter"
    material_filter = f"VALUES ?matType {{<{material_uri}>}}" if material_uri else "# No material type filter"
    
    query = f"""
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>

SELECT ?observation ?sp ?s2cell ?spWKT ?substance ?sample ?matType ?result_value ?unit ?regionURI
WHERE {{
    ?observation rdf:type coso:ContaminantObservation;
        coso:observedAtSamplePoint ?sp;
        coso:ofSubstance ?substance;
        coso:analyzedSample ?sample;
        coso:hasResult ?result.
    
    ?sample coso:sampleOfMaterialType ?matType.
    
    ?result coso:measurementValue ?result_value;
            coso:measurementUnit ?unit.
    
    # CRITICAL: Unit filter to ensure concentration values are in ng/L
    VALUES ?unit {{<http://qudt.org/vocab/unit/NanoGM-PER-L>}}
    
    # Apply your filters:
    {substance_filter}
    {material_filter}
    FILTER (?result_value >= {min_conc})
    FILTER (?result_value <= {max_conc})
    
    # Get WKT coordinates for mapping
    OPTIONAL {{ ?sp geo:hasGeometry/geo:asWKT ?spWKT . }}
    
    # Region filter - NEW CORRECT PATTERN using CONTAINS:
    ?sp spatial:connectedTo ?s2cell .
    ?s2cell spatial:connectedTo ?regionURI .
    FILTER( CONTAINS( STR(?regionURI), ".USA.{region_code}" ) )
}}
"""
    
    sparql_endpoint = ENDPOINT_URLS["federation"]  # Changed back to federation endpoint
    headers = {"Accept": "application/sparql-results+json"}
    
    try:
        response = requests.get(sparql_endpoint, params={"query": query}, headers=headers, timeout=180) # Increased timeout
        
        if response.status_code == 200:
            results = response.json()
            df_results = parse_sparql_results(results)
            if df_results.empty:
                print("   > Step 1 complete: No results found.")
            else:
                print(f"   > Step 1 complete: Found {len(df_results)} contaminated samples.")
            return df_results, None
        else:
            return None, f"Error {response.status_code}: {response.text}"
            
    except requests.exceptions.RequestException as e:
        return None, f"Network error: {str(e)}"
    except Exception as e:
        return None, f"Error: {str(e)}"

# STEP 2: Find Upstream S2 Cells
def execute_hydrology_query(contaminated_samples_df):
    """
    STEP 2: Get upstream S2 cells from the contaminated list.
    Runs on the 'hydrology' endpoint.
    """
    print(f"\n--- Running Step 2 (on 'hydrology') ---")
    
    s2_list = contaminated_samples_df['s2cell'].unique().tolist()
    if not s2_list:
        print("   > No S2 cells to trace upstream.")
        return pd.DataFrame(), None # Return empty, not error
    
    # Debug: print first few S2 cells to check format
    print(f"   > First few S2 cells from Step 1: {s2_list[:3] if len(s2_list) >= 3 else s2_list}")
    
    # Limit S2 cells to avoid query issues (take most contaminated areas)
    if len(s2_list) > 100:
        print(f"   > Too many S2 cells ({len(s2_list)}), limiting to top 100")
        # Get top 100 S2 cells by sample count
        s2_counts = contaminated_samples_df['s2cell'].value_counts()
        s2_list = s2_counts.head(100).index.tolist()
    
    s2_values_string = convertS2ListToQueryString(s2_list)
    print(f"Tracing upstream from {len(s2_list)} S2 cells...")
    
    # Debug: print formatted VALUES string
    print(f"   > VALUES string preview: {s2_values_string[:200]}..." if len(s2_values_string) > 200 else f"   > VALUES string: {s2_values_string}")

    query = f"""PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT DISTINCT ?s2cell 
WHERE {{
    ?downstream_flowline rdf:type hyf:HY_FlowPath ;
                        spatial:connectedTo ?s2cellds .
    
    ?upstream_flowline hyf:downstreamFlowPathTC ?downstream_flowline .
    
    VALUES ?s2cellds {{ {s2_values_string} }}
    
    ?s2cell spatial:connectedTo ?upstream_flowline ;
            rdf:type kwg-ont:S2Cell_Level13 .
}}"""

    sparql_endpoint = ENDPOINT_URLS["hydrology"]
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    try:
        # Use POST instead of GET to avoid URL length limits
        response = requests.post(sparql_endpoint, data={"query": query}, headers=headers, timeout=120)
        
        if response.status_code == 200:
            results = response.json()
            df_results = parse_sparql_results(results)
            if df_results.empty:
                print("   > Step 2 complete: No upstream hydrological sources found.")
            else:
                 print(f"   > Step 2 complete: Found {len(df_results)} upstream S2 cells.")
            return df_results, None
        else:
            return None, f"Error {response.status_code}: {response.text}"
            
    except requests.exceptions.RequestException as e:
        return None, f"Network error: {str(e)}"
    except Exception as e:
        return None, f"Error: {str(e)}"

# STEP 3: Find Upstream Facilities
def execute_facility_query(upstream_s2_df):
    """
    STEP 3: Get facilities located in the upstream S2 cells.
    Runs on the 'fio' (facility) endpoint.
    """
    print(f"\n--- Running Step 3 (on 'fio') ---")
    
    s2_list = upstream_s2_df['s2cell'].unique().tolist()
    if not s2_list:
        print("   > No upstream S2 cells to check for facilities.")
        return pd.DataFrame(), None # Return empty, not error
    
    # Limit S2 cells to avoid query issues
    if len(s2_list) > 100:
        print(f"   > Too many S2 cells ({len(s2_list)}), limiting to 100 to avoid timeout")
        # Prioritize S2 cells that likely have more facilities (you could enhance this logic)
        s2_list = s2_list[:100]
    
    s2_values_string = convertS2ListToQueryString(s2_list)
    print(f"Finding facilities in {len(s2_list)} upstream S2 cells...")
    print(f"   > Query size: {len(s2_values_string)} characters")
    
    query = f"""PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX fio: <http://w3id.org/fio/v1/fio#>

SELECT DISTINCT ?facility ?facWKT ?facilityName ?industryName
WHERE {{
    ?s2cell kwg-ont:sfContains ?facility .
    VALUES ?s2cell {{ {s2_values_string} }}
    
    ?facility fio:ofIndustry ?industryCode ;
            geo:hasGeometry/geo:asWKT ?facWKT;
            rdfs:label ?facilityName.
    ?industryCode rdfs:label ?industryName .
}}"""
    
    sparql_endpoint = ENDPOINT_URLS["fio"]
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    try:
        # Use POST instead of GET to avoid URL length limits
        # Increase timeout for facility queries
        response = requests.post(sparql_endpoint, data={"query": query}, headers=headers, timeout=300)
        
        if response.status_code == 200:
            results = response.json()
            df_results = parse_sparql_results(results)
            if df_results.empty:
                print("   > Step 3 complete: No facilities found in upstream areas.")
            else:
                 print(f"   > Step 3 complete: Found {len(df_results)} facilities.")
            return df_results, None
        else:
            return None, f"Error {response.status_code}: {response.text}"
            
    except requests.exceptions.RequestException as e:
        return None, f"Network error: {str(e)}"
    except Exception as e:
        return None, f"Error: {str(e)}"

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
    
    # Show data summary
    with st.expander("📊 Data Summary"):
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
        st.sidebar.header("🔧 Query Parameters")
    
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
        
        # 0. SUBSTANCE SELECTION (Optional but recommended)
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
            # Get the first matching substance URI (some substances have multiple URIs)
            substance_row = substances_df[substances_df['shortName'] == selected_substance_display]
            if not substance_row.empty:
                selected_substance_uri = substance_row.iloc[0]['substance']
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
    
        if selected_material_display != "-- All Material Types --":
            material_info = material_type_display[selected_material_display]
            selected_material_short = material_info['shortName']
            selected_material_label = material_info['label']
            selected_material_uri = material_info['matType']
            st.session_state.selected_material_type = {
                'short': selected_material_short,
                'label': selected_material_label,
                'uri': selected_material_uri
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
                    border-radius: 10px !important;
                    font-size: 24px !important;
                    padding: 8px !important;
                    box-shadow: 0 2px 8px rgba(76, 175, 80, 0.3) !important;
                    transition: all 0.3s ease !important;
                    margin-bottom: 0px !important;
                }
                div[data-testid="column"]:nth-of-type(1) div.stButton {
                    margin-bottom: -20px !important;
                }
                div[data-testid="column"]:nth-of-type(1) div.stButton > button:hover {
                    transform: scale(1.1) !important;
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
                    border-radius: 10px !important;
                    font-size: 24px !important;
                    padding: 8px !important;
                    box-shadow: 0 2px 8px rgba(76, 175, 80, 0.3) !important;
                    transition: all 0.3s ease !important;
                    margin-bottom: 0px !important;
                }
                div[data-testid="column"]:nth-of-type(3) div.stButton {
                    margin-bottom: -20px !important;
                }
                div[data-testid="column"]:nth-of-type(3) div.stButton > button:hover {
                    transform: scale(1.1) !important;
                    box-shadow: 0 4px 12px rgba(76, 175, 80, 0.5) !important;
                }
                </style>
            """, unsafe_allow_html=True)
            if st.button("➕", key="max_plus", help="Increase max", use_container_width=True):
                if st.session_state.conc_max < 500:
                    st.session_state.conc_max += 1
                    st.rerun()
    
        # Middle row: Interactive Streamlit slider
        concentration_range = st.sidebar.slider(
            "Drag to adjust range",
            min_value=0,
            max_value=500,
            value=(st.session_state.conc_min, st.session_state.conc_max),
            step=1,
            key="concentration_slider",
            label_visibility="collapsed",
            help="Drag the handles to adjust min/max values, or use +/- buttons for precise control"
        )
    
        # Update session state from slider
        st.session_state.conc_min = concentration_range[0]
        st.session_state.conc_max = concentration_range[1]
    
        # Get current values
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
                    border-radius: 10px !important;
                    font-size: 24px !important;
                    padding: 8px !important;
                    transition: all 0.3s ease !important;
                    margin-top: 0px !important;
                }
                div[data-testid="column"]:nth-of-type(1) div.stButton {
                    margin-top: -20px !important;
                }
                div[data-testid="column"]:nth-of-type(1) div.stButton > button[kind="secondary"]:hover {
                    background: rgba(255, 255, 255, 0.15) !important;
                    border-color: rgba(255, 255, 255, 0.5) !important;
                    transform: scale(1.1) !important;
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
                    border-radius: 10px !important;
                    font-size: 24px !important;
                    padding: 8px !important;
                    transition: all 0.3s ease !important;
                    margin-top: 0px !important;
                }
                div[data-testid="column"]:nth-of-type(3) div.stButton {
                    margin-top: -20px !important;
                }
                div[data-testid="column"]:nth-of-type(3) div.stButton > button[kind="secondary"]:hover {
                    background: rgba(255, 255, 255, 0.15) !important;
                    border-color: rgba(255, 255, 255, 0.5) !important;
                    transform: scale(1.1) !important;
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
    
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 📍 Geographic Region")
    
    
        # 1. STATE SELECTION (Mandatory)
        state_options = ["-- Select a State --"] + states_df['state_name'].tolist()
        selected_state_display = st.sidebar.selectbox(
            "1️⃣ Select State (Required)",
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
        if selected_state_code:
            # Filter subdivisions by state
            state_subdivisions = subdivisions_df[
                subdivisions_df['state_code'] == str(selected_state_code).zfill(2)
            ]
        
            if not state_subdivisions.empty:
                # Get unique counties for this state (filter out "Geometry of" prefixes)
                counties = state_subdivisions['county_name'].dropna().unique()
                # Remove "Geometry of " prefix from county names and deduplicate
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
                        help=f"Select a county within {selected_state_name}, or leave as 'All Counties'"
                    )
                
                    if selected_county_display != "-- All Counties --":
                        selected_county_name = selected_county_display
                        st.session_state.selected_county = selected_county_name
                    else:
                        st.session_state.selected_county = None
                else:
                    st.sidebar.info("No county data available for this state")
                    st.session_state.selected_county = None
            else:
                st.sidebar.info("No subdivision data available for this state")
                st.session_state.selected_county = None
        else:
            st.sidebar.info("👆 Please select a state first")
    
        # 3. SUBDIVISION SELECTION (Optional, filtered by county)
        selected_subdivision_code = None
        selected_subdivision_name = None
        if selected_state_code and selected_county_name:
            # Filter subdivisions by state and county (handle both with and without "Geometry of" prefix)
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
                    help=f"Select a subdivision within {selected_county_name}, or leave as 'All Subdivisions'"
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
                elif selected_county_name:
                    # For county, we still use state code with county filtering
                    # You could enhance this later to get county FIPS codes
                    query_region_code = str(selected_state_code).zfill(2)
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
            
                # Store results from each step
                samples_df = None
                upstream_s2_df = None
                facilities_df = None
            
                # Create columns for progress display
                prog_col1, prog_col2, prog_col3 = st.columns(3)
            
                # Step 1: Get contaminated samples
                with prog_col1:
                    with st.spinner("🔄 Step 1: Finding contaminated samples..."):
                        samples_df, error1 = execute_sparql_query(
                            substance_uri=selected_substance_uri,
                            material_uri=selected_material_uri,
                            min_conc=min_concentration,
                            max_conc=max_concentration,
                            region_code=query_region_code
                        )
                    
                        if error1:
                            st.error(f"❌ Step 1 failed: {error1}")
                        elif samples_df is not None and not samples_df.empty:
                            st.success(f"✅ Step 1: Found {len(samples_df)} contaminated samples")
                        else:
                            st.warning("⚠️ Step 1: No contaminated samples found")
            
                # Step 2: Find upstream S2 cells (only if step 1 succeeded)
                with prog_col2:
                    if samples_df is not None and not samples_df.empty:
                        with st.spinner("🔄 Step 2: Tracing upstream..."):
                            upstream_s2_df, error2 = execute_hydrology_query(samples_df)
                        
                            if error2:
                                st.warning(f"⚠️ Step 2 failed: {error2}")
                            elif upstream_s2_df is not None and not upstream_s2_df.empty:
                                st.success(f"✅ Step 2: Found {len(upstream_s2_df)} upstream S2 cells")
                            else:
                                st.info("ℹ️ Step 2: No upstream sources found")
                    else:
                        st.info("⏭️ Step 2: Skipped (no samples)")
            
                # Step 3: Find facilities (only if step 2 succeeded)
                with prog_col3:
                    if upstream_s2_df is not None and not upstream_s2_df.empty:
                        with st.spinner("🔄 Step 3: Finding facilities... (this may take a minute)"):
                            facilities_df, error3 = execute_facility_query(upstream_s2_df)
                        
                            if error3:
                                st.error(f"⚠️ Step 3 failed: {error3}")
                                if "timeout" in str(error3).lower() or "timed out" in str(error3).lower():
                                    st.info("💡 **Facility Query Timeout:**\n- Too many upstream S2 cells to search\n- Try a smaller region or narrower filters\n- The upstream areas were identified, but facility search timed out")
                            elif facilities_df is not None and not facilities_df.empty:
                                st.success(f"✅ Step 3: Found {len(facilities_df)} facilities")
                            else:
                                st.info("ℹ️ Step 3: No facilities found")
                    else:
                        st.info("⏭️ Step 3: Skipped (no upstream cells)")
            
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
                        if 's2cell' in samples_df.columns:
                            st.metric("Contaminated S2 Cells", samples_df['s2cell'].nunique())
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
            
                # Step 2 Results: Upstream S2 Cells
                if upstream_s2_df is not None and not upstream_s2_df.empty:
                    st.markdown("### 🌊 Step 2: Upstream S2 Cells")
                
                    # Metrics
                    st.metric("Total Upstream S2 Cells", len(upstream_s2_df))
                
                    # Display data
                    with st.expander("📊 View Upstream S2 Cells"):
                        st.dataframe(upstream_s2_df, use_container_width=True)
                    
                        # Download button
                        csv_upstream = upstream_s2_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Upstream S2 Cells CSV",
                            data=csv_upstream,
                            file_name=f"upstream_s2_cells_{query_region_code}.csv",
                            mime="text/csv",
                            key="download_upstream"
                        )
            
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
                
                    # Industry breakdown
                    if 'industryName' in facilities_df.columns:
                        st.markdown("#### 🏢 Industry Breakdown")
                        industry_counts = facilities_df['industryName'].value_counts()
                        for industry, count in industry_counts.items():
                            st.write(f"- **{industry}**: {count} facilities")
            
                # Create interactive map if we have spatial data
                if (samples_df is not None and not samples_df.empty and 'spWKT' in samples_df.columns) or \
                   (facilities_df is not None and not facilities_df.empty and 'facWKT' in facilities_df.columns):
                    st.markdown("---")
                    st.markdown("### 🗺️ Interactive Map")
                
                    # Convert to GeoDataFrames
                    samples_gdf = None
                    facilities_gdf = None
                
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
                
                    # Create map
                    if samples_gdf is not None or facilities_gdf is not None:
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
                    
                        # Add contaminated samples (orange markers)
                        if samples_gdf is not None and not samples_gdf.empty:
                            samples_gdf.explore(
                                m=map_obj,
                                name='<span style="color:DarkOrange;">🔬 Contaminated Samples</span>',
                                color='DarkOrange',
                                marker_kwds=dict(radius=8),
                                marker_type='circle_marker',
                                popup=['sp', 'result_value', 's2cell'] if all(col in samples_gdf.columns for col in ['sp', 'result_value', 's2cell']) else True,
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
                                for idx, industry in enumerate(industries):
                                    industry_facilities = facilities_gdf[facilities_gdf['industryName'] == industry]
                                    color = colors[idx % len(colors)]
                                    industry_facilities.explore(
                                        m=map_obj,
                                        name=f'<span style="color:{color};">🏭 {industry}</span>',
                                        color=color,
                                        marker_kwds=dict(radius=6),
                                        popup=['facilityName', 'industryName'] if all(col in industry_facilities.columns for col in ['facilityName', 'industryName']) else True,
                                        tooltip=['facilityName'] if 'facilityName' in industry_facilities.columns else None
                                    )
                            else:
                                # No industry info, just plot all facilities in one color
                                facilities_gdf.explore(
                                    m=map_obj,
                                    name='<span style="color:Blue;">🏭 Facilities</span>',
                                    color='Blue',
                                    marker_kwds=dict(radius=6),
                                    popup=['facilityName'] if 'facilityName' in facilities_gdf.columns else True
                                )
                    
                        # Add layer control
                        folium.LayerControl(collapsed=False).add_to(map_obj)
                    
                        # Display map
                        st_folium(map_obj, width=None, height=600, returned_objects=[])
                    
                        # Map legend
                        st.info("""
                        **🗺️ Map Legend:**
                        - 🟠 **Orange circles** = Contaminated sample locations
                        - 🔵 **Colored markers** = Upstream facilities (grouped by industry type)
                        - Click markers for details | Use layer control (top right) to toggle layers
                        """)
            
                # Summary of the complete analysis
                st.markdown("---")
                st.markdown("### 📊 Analysis Summary")
            
                summary_text = []
                if samples_df is not None and not samples_df.empty:
                    summary_text.append(f"✅ Found **{len(samples_df)}** contaminated samples")
                
                    if upstream_s2_df is not None and not upstream_s2_df.empty:
                        summary_text.append(f"✅ Traced upstream to **{len(upstream_s2_df)}** S2 cells")
                    
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
