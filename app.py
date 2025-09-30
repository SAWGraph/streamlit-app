"""
SAWGraph PFAS Contamination Analysis Dashboard
Streamlit application for visualizing PFAS contamination data near landfill and DOD sites
"""

import streamlit as st
import folium
from streamlit_folium import st_folium
import geopandas as gpd
import pandas as pd
from shapely import wkt
from SPARQLWrapper import SPARQLWrapper2, JSON, GET, POST, DIGEST
import rdflib
from branca.element import Figure
import time
import streamlit.components.v1 as components
import urllib.error
import urllib.request
import ssl

# Page configuration
st.set_page_config(
    page_title="SAWGraph PFAS Analysis",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main {
        padding-top: 0rem;
    }
    .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
    }
    h1 {
        color: #1e3d59;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        padding-left: 20px;
        padding-right: 20px;
    }
    .leaflet-control {
        z-index: 9999 !important;
    }
    </style>
    """, unsafe_allow_html=True)

# Initialize session state
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'facilities' not in st.session_state:
    st.session_state.facilities = None

# SPARQL endpoint configuration
@st.cache_resource
def setup_sparql_endpoint():
    """Initialize SPARQL endpoint connection"""
    endpoint = 'https://gdb.acg.maine.edu:7201/repositories/PFAS'
    sparql = SPARQLWrapper2(endpoint)
    sparql.setHTTPAuth(DIGEST)
    sparql.setCredentials('sawgraph-endpoint', 'skailab')
    sparql.setMethod(GET)
    sparql.setReturnFormat(JSON)
    # Add timeout
    sparql.setTimeout(30)
    return sparql

def test_endpoint_connection():
    """Test if SPARQL endpoint is accessible"""
    import urllib.request
    import ssl
    
    endpoint = 'https://gdb.acg.maine.edu:7201/repositories/PFAS'
    
    try:
        # Create SSL context that doesn't verify certificates (for testing)
        context = ssl._create_unverified_context()
        
        # Try to connect
        req = urllib.request.Request(endpoint)
        response = urllib.request.urlopen(req, timeout=10, context=context)
        return True, "Connection successful"
    except urllib.error.URLError as e:
        if hasattr(e, 'reason'):
            return False, f"Failed to reach server: {e.reason}"
        elif hasattr(e, 'code'):
            return False, f"Server returned error code: {e.code}"
    except Exception as e:
        return False, f"Connection error: {str(e)}"

def get_demo_data():
    """Return sample data for testing when endpoint is unavailable"""
    # Create sample data that matches the expected structure
    data = {
        'samplePoint': ['sp1', 'sp2', 'sp3'],
        'spWKT': [
            'POINT(-69.1 44.1)',
            'POINT(-69.2 44.2)',
            'POINT(-69.3 44.3)'
        ],
        'sample': ['s1', 's2', 's3'],
        'samples': ['ID-001', 'ID-002', 'ID-003'],
        'resultCount': [5, 3, 7],
        'Max': [125.5, 45.2, 89.3],
        'unit': ['ng/L', 'ng/L', 'ng/L'],
        'results': [
            'PFOS: 125.5 ng/L<br>PFOA: 45.2 ng/L',
            'PFOS: 45.2 ng/L<br>PFBA: 12.3 ng/L',
            'PFOS: 89.3 ng/L<br>PFOA: 67.8 ng/L'
        ]
    }
    return pd.DataFrame(data)

def get_demo_facilities():
    """Return sample facility data for testing"""
    data = {
        'facility': ['fac1', 'fac2'],
        'facWKT': [
            'POINT(-69.15 44.15)',
            'POINT(-69.25 44.25)'
        ],
        'facilityName': ['Demo Landfill Site', 'Demo DOD Facility'],
        'industry': ['ind1', 'ind2'],
        'industryName': ['Solid Waste Landfill', 'National Security']
    }
    return pd.DataFrame(data)

def convertToDataframe(results):
    """Convert SPARQL results to pandas DataFrame"""
    d = []
    for x in results.bindings:
        row = {}
        for k in x:
            v = x[k]
            vv = rdflib.term.Literal(v.value, datatype=v.datatype).toPython()
            row[k] = vv
        d.append(row)
    df = pd.DataFrame(d)
    return df

@st.cache_data(ttl=3600)
def execute_query(query_text, query_name):
    """Execute SPARQL query with caching and error handling"""
    try:
        sparql = setup_sparql_endpoint()
        sparql.setQuery(query_text)
        result = sparql.query()
        df = convertToDataframe(result)
        return df
    except urllib.error.URLError as e:
        st.error(f"❌ Connection Error: Cannot reach SPARQL endpoint")
        st.error(f"Details: {str(e)}")
        st.info("""
        **Troubleshooting Steps:**
        1. Check if you're connected to the university VPN
        2. Verify the endpoint URL is correct
        3. Check if the GraphDB server is running
        4. Try accessing https://gdb.acg.maine.edu:7201 in your browser
        """)
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Query Error: {str(e)}")
        return pd.DataFrame()

def truncate_results(results_str):
    """Truncate long result strings for display"""
    if pd.isna(results_str):
        return results_str
    items = results_str.split('<br>')
    if len(items) > 16:
        return "<br>".join(items[0:20]) + "<br>..."
    return results_str

def build_filtered_query(counties=None, industries=None, sample_types=None, substances=None, min_conc=4, max_conc=1000):
    """
    Build a dynamic SPARQL query with filters
    """
    # Base query structure
    query = '''
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fio: <http://w3id.org/fio/v1/fio#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX me_egad_data: <http://sawgraph.spatialai.org/v1/me-pfas#>

SELECT DISTINCT ?samplePoint ?spWKT ?sample 
    (GROUP_CONCAT(DISTINCT ?sampleId; separator="; ") as ?samples) 
    (COUNT(DISTINCT ?subVal) as ?resultCount) 
    (MAX(?result) as ?Max) ?unit 
    (GROUP_CONCAT(DISTINCT ?subVal; separator=" <br> ") as ?results)
WHERE {
    SERVICE <repository:FIO>{
        ?s2neighbor kwg-ont:sfContains ?facility.
        ?facility fio:ofIndustry ?industry.
'''
    
    # Add industry filter
    if industries and len(industries) > 0:
        industry_values = ' '.join([f'naics:NAICS-{ind}' for ind in industries])
        query += f'        VALUES ?industry {{{industry_values}}}.\n'
    else:
        query += '        VALUES ?industry {naics:NAICS-562212 naics:NAICS-928110}.\n'
    
    query += '''    }
    SERVICE <repository:Spatial>{
        ?s2 kwg-ont:sfTouches|owl:sameAs ?s2neighbor.
        ?s2neighbor rdf:type kwg-ont:S2Cell_Level13.
'''
    
    # Add county filter if specified
    if counties and len(counties) > 0:
        county_values = ' '.join([f'kwgr:administrativeRegion.USA.{c}' for c in counties])
        query += f'''        ?countySub rdf:type kwg-ont:AdministrativeRegion_3;
            kwg-ont:administrativePartOf ?county.
        VALUES ?county {{{county_values}}}.
'''
    
    query += '''    }
    ?samplePoint kwg-ont:sfWithin ?s2;
        rdf:type coso:SamplePoint;
        geo:hasGeometry/geo:asWKT ?spWKT.
'''
    
    # Add county containment if counties specified
    if counties and len(counties) > 0:
        query += '    ?samplePoint kwg-ont:sfWithin ?countySub.\n'
    
    query += '''    ?s2 rdf:type kwg-ont:S2Cell_Level13.
    ?sample coso:fromSamplePoint ?samplePoint;
        dcterms:identifier ?sampleId;
        coso:sampleOfMaterialType ?type.
    ?type rdfs:label ?type_label.
'''
    
    # Add sample type filter
    if sample_types and len(sample_types) > 0:
        type_values = ' '.join([f'me_egad_data:sampleMaterialTypeQualifier.{st}' for st in sample_types])
        query += f'    VALUES ?type {{{type_values}}}.\n'
    
    query += '''    ?observation rdf:type coso:ContaminantObservation;
        coso:observedAtSamplePoint ?samplePoint;
        coso:ofSubstance ?substance;
        coso:hasResult/coso:measurementValue ?result;
        coso:hasResult/coso:measurementUnit/qudt:symbol ?unit.
    ?substance skos:altLabel ?substance_label.
'''
    
    # Add substance filter
    if substances and len(substances) > 0:
        substance_values = ' '.join([f'me_egad_data:parameter.{sub}_A' for sub in substances])
        query += f'    VALUES ?substance {{{substance_values}}}.\n'
    
    # Add concentration filter
    query += f'    FILTER(?result >= {min_conc} && ?result <= {max_conc}).\n'
    
    query += '''    BIND((CONCAT(?substance_label, ": ", str(?result), " ", ?unit)) as ?subVal)
} GROUP BY ?samplePoint ?spWKT ?sample ?unit
'''
    
    return query

# NAICS codes mapping
NAICS_CODES = {
    '562212': 'Solid Waste Landfill',
    '928110': 'National Security',
    '22132': 'Sewage Treatment Facilities',
    '22131': 'Water Supply and Irrigation Systems',
    '3222': 'Converted Paper Product Manufacturing',
    '3261': 'Plastics Product Manufacturing',
    '3133': 'Textile and Fabric Finishing',
    '3251': 'Basic Chemical Manufacturing'
}

# Sample types
SAMPLE_TYPES = {
    'GW': 'Groundwater',
    'SW': 'Surface Water',
    'SL': 'Soil',
    'SD': 'Sediment',
    'V': 'Vegetation',
    'L': 'Leachate',
    'WW': 'Waste Water',
    'SU': 'Sludge',
    'PW': 'Process Water',
    'SR': 'Stormwater Runoff',
    'WH': 'Whole Fish',
    'SF': 'Skinless Fish Fillet',
    'LV': 'Liver'
}

# PFAS substances
PFAS_SUBSTANCES = [
    'PFOS', 'PFOA', 'PFBA', 'PFBEA', 'PFBS', 
    'PFHPA', 'PFHXS', 'PFHXA', 'PFHPS', 'PFNA', 'PFDA'
]

# Maine counties (subset for Knox and Penobscot)
COUNTIES = {
    '23013': 'Knox County',
    '23019': 'Penobscot County'
}

def get_facilities_query():
    """Base query for facilities"""
    return '''
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX fio: <http://w3id.org/fio/v1/fio#>

SELECT DISTINCT ?facility ?facWKT ?facilityName ?industry ?industryName WHERE {
    SERVICE <repository:FIO>{
        ?facility fio:ofIndustry ?industry;
            geo:hasGeometry/geo:asWKT ?facWKT;
            rdfs:label ?facilityName.
        ?industry rdfs:label ?industryName.
        VALUES ?industry {naics:NAICS-562212 naics:NAICS-928110}.
    }
}
'''

@st.cache_data(ttl=3600)
def load_facilities(use_demo=False):
    """Load facilities data with error handling"""
    if use_demo:
        facilities = get_demo_facilities()
    else:
        try:
            facilities = execute_query(get_facilities_query(), "facilities")
            
            # Check if query failed
            if facilities.empty or 'facWKT' not in facilities.columns:
                st.warning("⚠️ Could not load facilities from endpoint. Using demo data.")
                facilities = get_demo_facilities()
        except Exception as e:
            st.warning(f"⚠️ Error loading facilities: {str(e)}. Using demo data.")
            facilities = get_demo_facilities()
    
    # Process geometry
    facilities['facWKT'] = facilities['facWKT'].apply(wkt.loads)
    facilities = gpd.GeoDataFrame(facilities, geometry='facWKT')
    facilities.set_crs(epsg=4326, inplace=True, allow_override=True)
    return facilities

def create_filtered_map():
    """Create map with filtered data"""
    st.subheader("🗺️ Filtered Sample Analysis")
    
    # Sidebar filters
    with st.sidebar:
        st.header("🔍 Query Filters")
        
        # Connection test section
        with st.expander("🔌 Connection Status", expanded=True):
            col1, col2 = st.columns([3, 1])
            with col1:
                if st.button("Test Endpoint Connection", use_container_width=True):
                    with st.spinner("Testing connection..."):
                        success, message = test_endpoint_connection()
                        if success:
                            st.success(f"✅ {message}")
                        else:
                            st.error(f"❌ {message}")
                            st.warning("""
                            **Not connected to university network?**
                            Enable "Use Demo Data" below to test the interface.
                            """)
            
            st.caption("🌐 Endpoint: gdb.acg.maine.edu:7201")
            
            # Option to use demo data - make it prominent
            st.markdown("---")
            use_demo = st.checkbox(
                "🧪 Use Demo Data (for testing without VPN)", 
                value=False,
                help="Enable this when you're not connected to the university network"
            )
            if use_demo:
                st.success("✅ Demo mode enabled - Using sample data")
        
        st.markdown("---")
        
        # County filter
        st.subheader("📍 Geographic Region")
        selected_counties = st.multiselect(
            "Select Counties (optional)",
            options=list(COUNTIES.keys()),
            format_func=lambda x: COUNTIES[x],
            help="Leave empty for all Maine counties"
        )
        
        # Facility type filter
        st.subheader("🏭 Facility Type")
        selected_industries = st.multiselect(
            "Select Facility Types",
            options=list(NAICS_CODES.keys()),
            default=['562212', '928110'],
            format_func=lambda x: f"{x}: {NAICS_CODES[x]}"
        )
        
        # Sample type filter
        st.subheader("🧪 Sample Type")
        selected_sample_types = st.multiselect(
            "Select Sample Material Types",
            options=list(SAMPLE_TYPES.keys()),
            format_func=lambda x: f"{SAMPLE_TYPES[x]} ({x})",
            help="Leave empty for all sample types"
        )
        
        # Substance filter
        st.subheader("⚗️ PFAS Compound")
        selected_substances = st.multiselect(
            "Select PFAS Substances",
            options=PFAS_SUBSTANCES,
            help="Leave empty for all PFAS compounds"
        )
        
        # Concentration filter
        st.subheader("📊 Concentration Range")
        col1, col2 = st.columns(2)
        with col1:
            min_conc = st.number_input("Min (ng/L)", value=4.0, min_value=0.0, step=1.0)
        with col2:
            max_conc = st.number_input("Max (ng/L)", value=1000.0, min_value=0.0, step=10.0)
        
        st.markdown("---")
        run_query = st.button("🚀 Run Query", type="primary", use_container_width=True)
        
        if st.button("🔄 Clear Cache"):
            st.cache_data.clear()
            st.success("Cache cleared!")
    
    # Main content
    if run_query or 'last_query_result' in st.session_state:
        with st.spinner("Executing SPARQL query..."):
            try:
                # Check if using demo data
                if use_demo:
                    st.warning("🧪 Demo Mode: Displaying sample data")
                    samplepoints = get_demo_data()
                else:
                    # Build and execute query
                    query = build_filtered_query(
                        counties=selected_counties if selected_counties else None,
                        industries=selected_industries,
                        sample_types=selected_sample_types if selected_sample_types else None,
                        substances=selected_substances if selected_substances else None,
                        min_conc=min_conc,
                        max_conc=max_conc
                    )
                    
                    # Display query for debugging
                    with st.expander("📝 View Generated SPARQL Query"):
                        st.code(query, language='sparql')
                    
                    # Execute query
                    samplepoints = execute_query(query, f"filtered_query_{time.time()}")
                    
                    # Check if query failed (returned empty dataframe due to connection error)
                    if samplepoints.empty and 'spWKT' not in samplepoints.columns:
                        st.error("Query execution failed. Please check the connection status above.")
                        return
                
                st.session_state.last_query_result = samplepoints
                
                if len(samplepoints) == 0:
                    st.warning("⚠️ No samples found matching the selected criteria. Try adjusting your filters.")
                    return
                
                # Process data
                samplepoints['results'] = samplepoints['results'].apply(truncate_results)
                samplepoints['spWKT'] = samplepoints['spWKT'].apply(wkt.loads)
                samplepoints = gpd.GeoDataFrame(samplepoints, geometry='spWKT')
                samplepoints.set_crs(epsg=4326, inplace=True, allow_override=True)
                
                # Load facilities (with demo mode support)
                facilities = load_facilities(use_demo=use_demo)
                
                # Create map
                map_obj = samplepoints.explore(
                    name='<span style="color:DarkOrange;">Sample Points</span>',
                    color='DarkOrange',
                    style_kwds=dict(style_function=lambda x: {
                        "radius": min(float(x['properties']["Max"]) / 10, 12),
                        "opacity": 0.5,
                        "color": 'DimGray',
                    }),
                    marker_kwds=dict(radius=6),
                    marker_type='circle_marker',
                    popup=["samples", "Max", "unit", "results"],
                    tooltip=["Max", "unit"]
                )
                
                # Add facilities
                colors = ['SaddleBrown', 'MidnightBlue']
                for i, industry in enumerate(facilities.industryName.unique()):
                    facilities[facilities['industryName'] == industry].explore(
                        m=map_obj,
                        name=f'<span style="color:{colors[i]};">{industry}</span>',
                        color=colors[i],
                        marker_kwds=dict(radius=4),
                        popup=["facilityName", "industryName"]
                    )
                
                folium.LayerControl(collapsed=False, position='topright').add_to(map_obj)
                
                # Display map
                map_html = map_obj.get_root().render()
                components.html(map_html, height=600, scrolling=False)
                
                # Display statistics
                st.markdown("### 📊 Query Results")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Sample Points", len(samplepoints))
                with col2:
                    st.metric("Max Concentration", f"{samplepoints['Max'].max():.2f}")
                with col3:
                    st.metric("Avg Concentration", f"{samplepoints['Max'].mean():.2f}")
                with col4:
                    st.metric("Result Count", samplepoints['resultCount'].sum())
                
                # Data table
                with st.expander("📋 View Sample Data Table"):
                    display_df = samplepoints.drop(columns=['spWKT', 'geometry'])
                    st.dataframe(display_df, use_container_width=True)
                
            except Exception as e:
                st.error(f"Error executing query: {str(e)}")
                st.exception(e)
    else:
        st.info("👈 Configure your filters in the sidebar and click '🚀 Run Query' to begin analysis")
        
        # Show example filters
        st.markdown("""
        ### Example Queries
        
        **Basic Query**: Select default facility types (Landfills and DOD sites)
        
        **Groundwater Analysis**: Filter by GW (Groundwater) sample type
        
        **High Concentration**: Set Min: 100, Max: 1000 to find elevated PFAS levels
        
        **Regional Focus**: Select Knox or Penobscot County for localized analysis
        
        **Specific Compound**: Select PFOS or PFOA to track individual contaminants
        """)

# Main App
def main():
    # Header
    st.title("🗺️ SAWGraph PFAS Contamination Analysis")
    st.markdown("""
    Interactive spatial analysis of PFAS contamination near landfill and Department of Defense sites in Maine.
    This dashboard queries live SPARQL endpoints with customizable filters.
    """)
    
    # Display filter legend
    with st.expander("ℹ️ About the Filters"):
        st.markdown("""
        **Administrative Region**: Filter samples by county (Knox, Penobscot)
        
        **Facility Type**: NAICS industry codes for contamination sources:
        - 562212: Solid Waste Landfills
        - 928110: National Security (DOD)
        - 22132: Sewage Treatment
        - 22131: Water Supply Systems
        - 3222: Paper Products
        - 3261: Plastics Manufacturing
        - 3133: Textile Finishing
        - 3251: Chemical Manufacturing
        
        **Sample Type**: Material where PFAS was measured (water, soil, sediment, etc.)
        
        **PFAS Compound**: Specific per- and polyfluoroalkyl substances
        
        **Concentration**: Measurement range in ng/L (parts per trillion)
        """)
    
    # Main map display
    create_filtered_map()
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: gray; font-size: 0.9em;'>
    SAWGraph PFAS Analysis Dashboard | Built with Streamlit & Folium<br>
    Data from SAWGraph Knowledge Graph via SPARQL
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()