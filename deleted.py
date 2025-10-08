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
    /* Ensure map controls (Leaflet) are visible and not clipped */
    .stApp .block-container, .stApp .main, .streamlit-expander, .stContainer {
        overflow: visible !important;
    }
    .leaflet-control {
        z-index: 9999 !important;
    }
    .leaflet-top.leaflet-right {
        right: 10px !important;
    }
    </style>
    """, unsafe_allow_html=True)

# Initialize session state
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'facilities' not in st.session_state:
    st.session_state.facilities = None
if 'current_query' not in st.session_state:
    st.session_state.current_query = None

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
    return sparql

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
    """Execute SPARQL query with caching"""
    sparql = setup_sparql_endpoint()
    sparql.setQuery(query_text)
    result = sparql.query()
    df = convertToDataframe(result)
    return df

def truncate_results(results_str):
    """Truncate long result strings for display"""
    if pd.isna(results_str):
        return results_str
    items = results_str.split('<br>')
    if len(items) > 16:
        return "<br>".join(items[0:20]) + "<br>..."
    return results_str

# Query definitions
QUERIES = {
    "q1_samples_near_facilities": '''
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

select DISTINCT ?samplePoint ?spWKT ?sample (GROUP_CONCAT(DISTINCT ?sampleId; separator="; ") as ?samples) 
    (COUNT(DISTINCT ?subVal) as ?resultCount) (MAX(?result) as ?Max) ?unit 
    (GROUP_CONCAT(DISTINCT ?subVal; separator=" <br> ") as ?results)
where {
    SERVICE <repository:FIO>{
        ?s2neighbor kwg-ont:sfContains ?facility.
        ?facility fio:ofIndustry ?industry.
        VALUES ?industry {naics:NAICS-562212 naics:NAICS-928110}.
    }
    SERVICE <repository:Spatial>{
        ?s2 kwg-ont:sfTouches|owl:sameAs ?s2neighbor.
        ?s2neighbor rdf:type kwg-ont:S2Cell_Level13.
    }
    ?samplePoint kwg-ont:sfWithin ?s2;
        rdf:type coso:SamplePoint;
        geo:hasGeometry/geo:asWKT ?spWKT.
    ?s2 rdf:type kwg-ont:S2Cell_Level13.
    ?sample coso:fromSamplePoint ?samplePoint;
        dcterms:identifier ?sampleId;
        coso:sampleOfMaterialType/rdfs:label ?type.
    ?observation rdf:type coso:ContaminantObservation;
        coso:observedAtSamplePoint ?samplePoint;
        coso:ofSubstance/ skos:altLabel ?substance;
        coso:hasResult/coso:measurementValue ?result;
        coso:hasResult/coso:measurementUnit/qudt:symbol ?unit.
    BIND((CONCAT(?substance, ": ", str(?result) , " ", ?unit) ) as ?subVal)
} GROUP BY ?samplePoint ?spWKT ?sample ?unit
''',
    
    "q2_facilities": '''
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX fio: <http://w3id.org/fio/v1/fio#>

select DISTINCT ?facility ?facWKT ?facilityName ?industry ?industryName where {
    SERVICE <repository:FIO>{
        ?facility fio:ofIndustry ?industry;
            geo:hasGeometry/geo:asWKT ?facWKT;
            rdfs:label ?facilityName.
        ?industry rdfs:label ?industryName.
        VALUES ?industry {naics:NAICS-562212 naics:NAICS-928110}.
    }
}
''',
    
    "q3_counties": '''
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>

SELECT * WHERE {
    SERVICE <repository:Spatial>{
        VALUES ?county {kwgr:administrativeRegion.USA.23013 kwgr:administrativeRegion.USA.23019}
        ?county geo:hasGeometry/geo:asWKT ?countyWKT;
            rdfs:label ?countyName.
    }
}
''',
    
    "q4_samples_knox_penobscot": '''
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

select DISTINCT ?samplePoint ?spWKT ?sample (GROUP_CONCAT(DISTINCT ?sampleId; separator="; ") as ?samples) 
    (COUNT(DISTINCT ?subVal) as ?resultCount) (MAX(?result) as ?Max) ?unit 
    (GROUP_CONCAT(DISTINCT ?subVal; separator=" <br> ") as ?results)
where {
    SERVICE <repository:FIO>{
        ?s2neighbor kwg-ont:sfContains ?facility.
        ?facility fio:ofIndustry ?industry.
        VALUES ?industry {naics:NAICS-562212 naics:NAICS-928110}.
    }
    SERVICE <repository:Spatial>{
        ?s2 kwg-ont:sfTouches|owl:sameAs ?s2neighbor.
        ?s2neighbor rdf:type kwg-ont:S2Cell_Level13.
        ?countySub rdf:type kwg-ont:AdministrativeRegion_3;
            kwg-ont:administrativePartOf ?county.
        VALUES ?county {kwgr:administrativeRegion.USA.23013 kwgr:administrativeRegion.USA.23019}
    }
    ?samplePoint kwg-ont:sfWithin ?s2;
        kwg-ont:sfWithin ?countySub;
        rdf:type coso:SamplePoint;
        geo:hasGeometry/geo:asWKT ?spWKT.
    ?s2 rdf:type kwg-ont:S2Cell_Level13.
    ?sample coso:fromSamplePoint ?samplePoint;
        dcterms:identifier ?sampleId;
        coso:sampleOfMaterialType/rdfs:label ?type.
    ?observation rdf:type coso:ContaminantObservation;
        coso:observedAtSamplePoint ?samplePoint;
        coso:ofSubstance/ skos:altLabel ?substance;
        coso:hasResult/coso:measurementValue ?result;
        coso:hasResult/coso:measurementUnit/qudt:symbol ?unit.
    BIND((CONCAT(?substance, ": ", str(?result) , " ", ?unit) ) as ?subVal)
} GROUP BY ?samplePoint ?spWKT ?sample ?unit
''',
    
    "q5_surface_water_near": '''
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fio: <http://w3id.org/fio/v1/fio#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>
PREFIX nhdplusv2: <http://nhdplusv2.spatialai.org/v1/nhdplusv2#>
PREFIX hyfo: <http://hyfo.spatialai.org/v1/hyfo#>

select DISTINCT ?surfacewater ?surfacewatername ?waterType ?swWKT ?reachCode ?COMID
where {
    SERVICE <repository:FIO>{
        ?s2neighbor kwg-ont:sfContains ?facility.
        ?facility fio:ofIndustry ?industry.
        VALUES ?industry {naics:NAICS-562212 naics:NAICS-928110}.
    }
    SERVICE <repository:Spatial>{
        ?s2 kwg-ont:sfTouches|owl:sameAs ?s2neighbor.
        ?s2neighbor rdf:type kwg-ont:S2Cell_Level13;
            spatial:connectedTo ?countySub.
        ?countySub rdf:type kwg-ont:AdministrativeRegion_3;
            kwg-ont:administrativePartOf ?county.
        VALUES ?county {kwgr:administrativeRegion.USA.23013 kwgr:administrativeRegion.USA.23019}
    }
    SERVICE <repository:Hydrology>{
        ?surfacewater rdf:type ?watertype;
            spatial:connectedTo ?s2neighbor;
            geo:hasGeometry/ geo:asWKT ?swWKT.
        OPTIONAL {?surfacewater rdfs:label ?surfacewatername;
            nhdplusv2:hasFTYPE ?waterType;
            nhdplusv2:hasCOMID ?COMID;
            nhdplusv2:hasReachCode ?reachCode.}
        VALUES ?watertype {hyf:HY_HydroFeature hyfo:WaterFeatureRepresentation}
    }
}
''',
    
    "q6_surface_water_downstream": '''
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fio: <http://w3id.org/fio/v1/fio#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>
PREFIX nhdplusv2: <http://nhdplusv2.spatialai.org/v1/nhdplusv2#>
PREFIX hyfo: <http://hyfo.spatialai.org/v1/hyfo#>

select DISTINCT ?surfacewater ?surfacewatername ?waterType ?swWKT ?reachCode ?COMID
where {
    SERVICE <repository:FIO>{
        ?s2neighbor kwg-ont:sfContains ?facility.
        ?facility fio:ofIndustry ?industry.
        VALUES ?industry {naics:NAICS-562212 naics:NAICS-928110}.
    }
    SERVICE <repository:Spatial>{
        ?s2 kwg-ont:sfTouches|owl:sameAs ?s2neighbor.
        ?s2neighbor rdf:type kwg-ont:S2Cell_Level13;
            spatial:connectedTo ?countySub.
        ?countySub rdf:type kwg-ont:AdministrativeRegion_3;
            kwg-ont:administrativePartOf ?county.
        VALUES ?county {kwgr:administrativeRegion.USA.23013 kwgr:administrativeRegion.USA.23019}
    }
    SERVICE <repository:Hydrology>{
        ?stream rdf:type hyfo:WaterFeatureRepresentation;
            spatial:connectedTo ?s2neighbor;
            hyf:downstreamWaterBody+ ?surfacewater.
        ?surfacewater geo:hasGeometry/ geo:asWKT ?swWKT.
        OPTIONAL {?surfacewater rdfs:label ?surfacewatername;
            nhdplusv2:hasFTYPE ?waterType;
            nhdplusv2:hasCOMID ?COMID;
            nhdplusv2:hasReachCode ?reachCode.}
    }
}
'''
}

def load_base_data():
    """Load facilities data that's used across all queries"""
    if st.session_state.facilities is None:
        with st.spinner("Loading facility data..."):
            facilities = execute_query(QUERIES["q2_facilities"], "facilities")
            facilities['facWKT'] = facilities['facWKT'].apply(wkt.loads)
            facilities = gpd.GeoDataFrame(facilities, geometry='facWKT')
            facilities.set_crs(epsg=4326, inplace=True, allow_override=True)
            st.session_state.facilities = facilities
    return st.session_state.facilities

def create_map_q1():
    """Create map for Q1: Samples near landfill or DOD sites"""
    st.subheader("Q1: What samples in Maine are near landfill or DOD sites?")
    
    col1, col2 = st.columns([10, 1])
    
    with col2:
        st.info("**Data Layers:**")
        st.markdown("""
        - 🟠 **Orange**: Sample points
        - 🟤 **Brown**: Landfill sites  
        - 🔵 **Blue**: DOD facilities
        """)
        
        if st.button("🔄 Refresh Data", key="refresh_q1"):
            st.cache_data.clear()
            st.rerun()
    
    with col1:
        with st.spinner("Loading sample data..."):
            # Load data
            samplepoints = execute_query(QUERIES["q1_samples_near_facilities"], "q1_samples")
            samplepoints['results'] = samplepoints['results'].apply(truncate_results)
            samplepoints['spWKT'] = samplepoints['spWKT'].apply(wkt.loads)
            samplepoints = gpd.GeoDataFrame(samplepoints, geometry='spWKT')
            samplepoints.set_crs(epsg=4326, inplace=True, allow_override=True)
            
            facilities = load_base_data()
            
            # Create map
            map_obj = samplepoints.explore(
                name='<span style="color:DarkOrange;">Samples</span>',
                color='DarkOrange',
                style_kwds=dict(style_function=lambda x: {
                    "radius": float(x['properties']["Max"]) if float(x['properties']["Max"]) < 10 else 12,
                    "opacity": 0.3,
                    "color": 'DimGray',
                }),
                marker_kwds=dict(radius=6),
                marker_type='circle_marker',
                popup=["samples", "Max", "unit", "results"],
            )
            
            # Add facilities
            colors = ['SaddleBrown', 'MidnightBlue']
            for i, industry in enumerate(facilities.industryName.unique()):
                facilities[facilities['industryName'] == industry].explore(
                    m=map_obj,
                    name=f'<span style="color:{colors[i]};">{industry}</span>',
                    color=colors[i],
                    marker_kwds=dict(radius=3),
                    popup=True
                )
            
            folium.LayerControl(collapsed=False, position='topleft').add_to(map_obj)

            # Add a small JS snippet to ensure map padding on the right so controls are visible
            from branca.element import MacroElement, Element
            script = Element(
                """
                <script>
                const map = document.querySelector('.folium-map').querySelector('div');
                if (map && window.L && map._leaflet_map) {
                    try {
                        // set a padding on the map so top-right controls are not clipped by side panels
                        map._leaflet_map.options.zoomControl = true;
                        map._leaflet_map.invalidateSize();
                    } catch (e) {
                        // ignore
                    }
                }
                </script>
                """
            )
            map_obj.get_root().html.add_child(script)

            # Display map
            map_html = map_obj.get_root().render()
            components.html(map_html, height=600, scrolling=False)
    
    # Display statistics
    st.markdown("### Summary Statistics")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Sample Points", len(samplepoints))
    with col2:
        st.metric("Landfill Sites", len(facilities[facilities['industryName'].str.contains('Solid', na=False)]))
    with col3:
        st.metric("DOD Facilities", len(facilities[facilities['industryName'].str.contains('National', na=False)]))

def create_map_q2():
    """Create map for Q2: Filtered to Knox/Penobscot counties"""
    st.subheader("Q2: Samples near facilities - Knox & Penobscot Counties")
    
    col1, col2 = st.columns([3, 1])
    
    with col2:
        st.info("**Data Layers:**")
        st.markdown("""
        - ⬜ **Gray**: County boundaries
        - 🟠 **Orange**: Sample points
        - 🟤 **Brown**: Landfill sites
        - 🔵 **Blue**: DOD facilities
        """)
        
        show_facilities = st.checkbox("Show Facilities", value=False)
    
    with col1:
        with st.spinner("Loading county data..."):
            # Load data
            counties = execute_query(QUERIES["q3_counties"], "counties")
            counties['countyWKT'] = counties['countyWKT'].apply(wkt.loads)
            counties = gpd.GeoDataFrame(counties, geometry='countyWKT')
            counties.set_crs(epsg=4326, inplace=True, allow_override=True)
            
            samplepoints_KxPn = execute_query(QUERIES["q4_samples_knox_penobscot"], "q4_samples")
            samplepoints_KxPn['results'] = samplepoints_KxPn['results'].apply(truncate_results)
            samplepoints_KxPn['spWKT'] = samplepoints_KxPn['spWKT'].apply(wkt.loads)
            samplepoints_KxPn = gpd.GeoDataFrame(samplepoints_KxPn, geometry='spWKT')
            samplepoints_KxPn.set_crs(epsg=4326, inplace=True, allow_override=True)
            
            facilities = load_base_data()
            
            # Create map
            map_obj = counties.explore(name='Counties', style_kwds=dict(color='Gray', fill=0.0))
            
            # Add sample points
            samplepoints_KxPn.explore(
                m=map_obj,
                name='<span style="color:DarkOrange;">Samples</span>',
                color='DarkOrange',
                style_kwds=dict(style_function=lambda x: {
                    "radius": float(x['properties']["Max"]) if float(x['properties']["Max"]) < 10 else 12,
                    "opacity": 0.3,
                    "color": 'DimGray',
                }),
                marker_kwds=dict(radius=6),
                marker_type='circle_marker',
                popup=["samples", "Max", "unit", "results"],
            )
            
            # Add facilities (hidden by default)
            if show_facilities:
                colors = ['SaddleBrown', 'MidnightBlue']
                for i, industry in enumerate(facilities.industryName.unique()):
                    facilities[facilities['industryName'] == industry].explore(
                        m=map_obj,
                        name=f'<span style="color:{colors[i]};">{industry}</span>',
                        color=colors[i],
                        marker_kwds=dict(radius=3),
                        popup=True,
                        hidden=not show_facilities
                    )
            
            folium.LayerControl(collapsed=False).add_to(map_obj)
            
            # Display map
            map_html = map_obj.get_root().render()
            components.html(map_html, height=600, scrolling=False)
    
    # Display statistics
    st.markdown("### County Statistics")
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Sample Points in Counties", len(samplepoints_KxPn))
    with col2:
        st.metric("Counties Shown", len(counties))

def create_map_q3():
    """Create map for Q3: Surface water near facilities"""
    st.subheader("Q3: Surface water bodies near landfill or DOD sites")
    
    col1, col2 = st.columns([3, 1])
    
    with col2:
        st.info("**Data Layers:**")
        st.markdown("""
        - ⬜ **Gray**: County boundaries
        - 💧 **Blue**: Surface water
        - 🟤 **Brown**: Landfill sites
        - 🔵 **Dark Blue**: DOD facilities
        """)
        
        show_facilities = st.checkbox("Show Facilities", value=True, key="show_fac_q3")
    
    with col1:
        with st.spinner("Loading surface water data..."):
            # Load data
            counties = execute_query(QUERIES["q3_counties"], "counties")
            counties['countyWKT'] = counties['countyWKT'].apply(wkt.loads)
            counties = gpd.GeoDataFrame(counties, geometry='countyWKT')
            counties.set_crs(epsg=4326, inplace=True, allow_override=True)
            
            surfacewater = execute_query(QUERIES["q5_surface_water_near"], "q5_water")
            surfacewater['swWKT'] = surfacewater['swWKT'].apply(wkt.loads)
            surfacewater = gpd.GeoDataFrame(surfacewater, geometry='swWKT')
            surfacewater.set_crs(epsg=4326, inplace=True, allow_override=True)
            
            facilities = load_base_data()
            
            # Create map
            map_obj = counties.explore(name='Counties', style_kwds=dict(color='Gray', fill=0.0))
            
            # Add surface water
            surfacewater.explore(
                m=map_obj,
                name='<span style="color:Blue;">Surface Water</span>',
                color='Blue',
                style_kwds=dict(color='Blue', fill=0.8),
            )
            
            # Add facilities
            if show_facilities:
                colors = ['SaddleBrown', 'MidnightBlue']
                for i, industry in enumerate(facilities.industryName.unique()):
                    facilities[facilities['industryName'] == industry].explore(
                        m=map_obj,
                        name=f'<span style="color:{colors[i]};">{industry}</span>',
                        color=colors[i],
                        marker_kwds=dict(radius=3),
                        popup=True,
                        hidden=not show_facilities
                    )
            
            folium.LayerControl(collapsed=False).add_to(map_obj)
            
            # Display map
            map_html = map_obj.get_root().render()
            components.html(map_html, height=600, scrolling=False)
    
    # Display statistics  
    st.markdown("### Water Body Statistics")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Water Bodies Found", len(surfacewater))
    with col2:
        named_water = surfacewater['surfacewatername'].notna().sum()
        st.metric("Named Water Bodies", named_water)
    with col3:
        st.metric("Unique Water Types", surfacewater['waterType'].nunique())

def create_map_q4():
    """Create map for Q4: Downstream water bodies"""
    st.subheader("Q4: Surface water downstream from landfill or DOD sites")
    
    col1, col2 = st.columns([3, 1])
    
    with col2:
        st.info("**Data Layers:**")
        st.markdown("""
        - ⬜ **Gray**: County boundaries
        - 💧 **Blue**: Downstream water
        - 🟤 **Brown**: Landfill sites
        - 🔵 **Dark Blue**: DOD facilities
        """)
        
        show_facilities = st.checkbox("Show Facilities", value=True, key="show_fac_q4")
    
    with col1:
        with st.spinner("Loading downstream water data..."):
            # Load data
            counties = execute_query(QUERIES["q3_counties"], "counties")
            counties['countyWKT'] = counties['countyWKT'].apply(wkt.loads)
            counties = gpd.GeoDataFrame(counties, geometry='countyWKT')
            counties.set_crs(epsg=4326, inplace=True, allow_override=True)
            
            surfacewater2 = execute_query(QUERIES["q6_surface_water_downstream"], "q6_water")
            surfacewater2['swWKT'] = surfacewater2['swWKT'].apply(wkt.loads)
            surfacewater2 = gpd.GeoDataFrame(surfacewater2, geometry='swWKT')
            surfacewater2.set_crs(epsg=4326, inplace=True, allow_override=True)
            
            facilities = load_base_data()
            
            # Create map
            map_obj = counties.explore(name='Counties', style_kwds=dict(color='Gray', fill=0.0))
            
            # Add surface water
            surfacewater2.explore(
                m=map_obj,
                name='<span style="color:Blue;">Surface Water</span>',
                color='Blue',
                style_kwds=dict(color='Blue', fill=0.8),
            )
            
            # Add facilities
            if show_facilities:
                colors = ['SaddleBrown', 'MidnightBlue']
                for i, industry in enumerate(facilities.industryName.unique()):
                    facilities[facilities['industryName'] == industry].explore(
                        m=map_obj,
                        name=f'<span style="color:{colors[i]};">{industry}</span>',
                        color=colors[i],
                        marker_kwds=dict(radius=3),
                        popup=True,
                        hidden=not show_facilities
                    )
            
            folium.LayerControl(collapsed=False).add_to(map_obj)
            
            # Display map
            map_html = map_obj.get_root().render()
            components.html(map_html, height=600, scrolling=False)
    
    # Display statistics
    st.markdown("### Downstream Statistics")
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Downstream Water Bodies", len(surfacewater2))
    with col2:
        st.metric("Total Reach Codes", surfacewater2['reachCode'].nunique())

# Main App
def main():
    # Header
    st.title("🗺️ SAWGraph PFAS Contamination Analysis")
    st.markdown("""
    Interactive spatial analysis of PFAS contamination near landfill and Department of Defense sites in Maine.
    This dashboard queries live SPARQL endpoints to visualize relationships between contamination sources and water bodies.
    """)
    
    # Sidebar
    with st.sidebar:
        st.header("Navigation")
        st.markdown("Select a spatial query to visualize:")
        
        query_option = st.radio(
            "Choose Analysis:",
            ["Q1: Samples Near Facilities (All Maine)",
             "Q2: Samples Near Facilities (Knox/Penobscot)",  
             "Q3: Surface Water Near Facilities",
             "Q4: Downstream Surface Water"],
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        st.markdown("### About")
        st.info("""
        **SAWGraph Project**
        
        Analyzing PFAS contamination patterns using:
        - SPARQL graph databases
        - Geospatial analysis
        - Hydrological modeling
        """)
        
        st.markdown("---")
        st.markdown("### Data Sources")
        st.markdown("""
        - **Facilities**: Landfills & DOD sites
        - **Samples**: PFAS measurement points
        - **Hydrology**: NHDPlus water bodies
        - **Admin**: County boundaries
        """)
    
    # Main content area
    if query_option == "Q1: Samples Near Facilities (All Maine)":
        create_map_q1()
    elif query_option == "Q2: Samples Near Facilities (Knox/Penobscot)":
        create_map_q2()
    elif query_option == "Q3: Surface Water Near Facilities":
        create_map_q3()
    elif query_option == "Q4: Downstream Surface Water":
        create_map_q4()
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: gray; font-size: 0.9em;'>
    SAWGraph PFAS Analysis Dashboard | Built with Streamlit & Folium
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
