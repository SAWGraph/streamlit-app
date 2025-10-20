"""
SAWGraph PFAS Upstream Tracing Analysis - Streamlit App
This app analyzes PFAS contamination data and traces upstream facilities
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

# Page configuration
st.set_page_config(
    page_title="SAWGraph PFAS Analysis",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title and description
st.title("🌊 SAWGraph PFAS Upstream Tracing Analysis")
st.markdown("""
This application analyzes PFAS contamination in water samples and identifies upstream facilities 
that may be potential sources of contamination.
""")

# Helper functions
def convertToDataframe(_results):
    """Convert SPARQL results to pandas DataFrame"""
    d = []
    for x in _results.bindings:
        row = {}
        for k in x:
            v = x[k]
            vv = rdflib.term.Literal(v.value, datatype=v.datatype).toPython()
            row[k] = vv
        d.append(row)
    df = pd.DataFrame(d)
    return df

def convertS2ListToQueryString(s2list):
    """Convert S2 cell list to query string format"""
    s2list_short = [s2cell.replace("http://stko-kwg.geog.ucsb.edu/lod/resource/","kwgr:") for s2cell in s2list]
    s2_values_string = " ".join(s2list_short)
    return s2_values_string

# Sidebar for parameters
st.sidebar.header("🔧 Analysis Parameters")

# Add a test query button
if st.sidebar.button("🧪 Test Connection & Show Available Data"):
    with st.spinner("Testing SPARQL endpoints..."):
        try:
            # Test query to see what substances and material types have data
            test_query = '''
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX me_egad: <http://sawgraph.spatialai.org/v1/me-egad#>

SELECT DISTINCT ?substance ?substanceLabel ?matType ?matTypeLabel (COUNT(?obs) as ?count) WHERE {
    ?obs rdf:type coso:ContaminantObservation;
         coso:ofSubstance ?substance;
         coso:analyzedSample ?sample;
         coso:hasResult ?result.
    ?sample coso:sampleOfMaterialType ?matType.
    ?substance rdfs:label ?substanceLabel.
    ?matType rdfs:label ?matTypeLabel.
    ?result coso:measurementValue ?value.
    FILTER(?value > 0)
} GROUP BY ?substance ?substanceLabel ?matType ?matTypeLabel
ORDER BY DESC(?count)
LIMIT 20
'''
            sparql_test = SPARQLWrapper2("https://frink.apps.renci.org/sawgraph/sparql")
            sparql_test.setHTTPAuth(DIGEST)
            sparql_test.setMethod(POST)
            sparql_test.setReturnFormat(JSON)
            sparql_test.setQuery(test_query)
            
            test_result = sparql_test.query()
            test_df = convertToDataframe(test_result)
            
            st.success("✅ Connection successful!")
            st.write("**Available data combinations (top 20):**")
            st.dataframe(test_df)
            
        except Exception as e:
            st.error(f"Connection test failed: {str(e)}")

st.sidebar.markdown("---")

# Substance selection
substances = ["PFOS", "PFOA", "PFBA", "PFBEA", "PFBS", "PFHPA", "PFHXS", "PFHXA", "PFHPS", "PFNA", "PFDA"]
substance = st.sidebar.selectbox(
    "Select PFAS Substance",
    substances,
    index=substances.index("PFHPA")
)
substanceCode = "me_egad:parameter." + substance + "_A"

# Material type selection
material_types = {
    "DW (Drinking Water)": "DW",
    "GW (Groundwater)": "GW",
    "WW (Waste Water)": "WW",
    "SW (Surface Water)": "SW",
    "PW (Pore Water)": "PW",
    "L (Leachate)": "L",
    "SR (Storm Water Runoff)": "SR",
    "SL (Soil)": "SL"
}
materialType = st.sidebar.selectbox(
    "Select Sample Material Type",
    list(material_types.keys()),
    index=1
)
matTypeCode = "me_egad_data:sampleMaterialType." + material_types[materialType]

# Administrative region selection
regions = {
    "23 (Maine)": "23",
    "23019 (Penobscot County, Maine)": "23019", 
    "23011 (Kennebec County, Maine)": "23011",
    "23005 (Cumberland County, Maine)": "23005",
    "33 (New Hampshire)": "33",
    "17 (Illinois)": "17"
}
admin_region = st.sidebar.selectbox(
    "Select Administrative Region",
    list(regions.keys()),
    index=3  # Default to Cumberland County
)
regionCode = regions[admin_region]

# Concentration range
st.sidebar.subheader("Concentration Range (ng/L)")
col1, col2 = st.sidebar.columns(2)
with col1:
    minValue = st.number_input("Min", value=20, min_value=0, step=5)
with col2:
    maxValue = st.number_input("Max", value=30, min_value=0, step=5)

# Run analysis button
if st.sidebar.button("🔍 Run Analysis", type="primary"):
    with st.spinner("Executing SPARQL queries and processing data..."):
        
        # Progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Debug information
        with st.expander("🐛 Debug Information (click to expand)"):
            st.write("**Selected Parameters:**")
            st.write(f"- Substance: {substance} → Code: {substanceCode}")
            st.write(f"- Material Type: {materialType} → Code: {matTypeCode}")
            st.write(f"- Region: {admin_region} → Code: {regionCode}")
            st.write(f"- Concentration Range: {minValue} - {maxValue} ng/L")
        
        try:
            # Query 1: Get sample points
            status_text.text("Querying sample points...")
            progress_bar.progress(10)
            
            q1 = f'''
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX me_egad: <http://sawgraph.spatialai.org/v1/me-egad#>
PREFIX me_egad_data: <http://sawgraph.spatialai.org/v1/me-egad-data#>

SELECT (COUNT(DISTINCT ?subVal) as ?resultCount) (MAX(?result_value) as ?max) ?sp ?spWKT  WHERE {{
    ?sp rdf:type coso:SamplePoint ;
        geo:hasGeometry/geo:asWKT ?spWKT.
    ?observation rdf:type coso:ContaminantObservation;
        coso:observedAtSamplePoint ?sp;
        coso:ofSubstance ?substance ;
        coso:analyzedSample ?sample ;
        coso:hasResult ?result .
    ?sample rdfs:label ?sampleLabel;
        coso:sampleOfMaterialType ?matType.
    ?matType rdfs:label ?matTypeLabel.
    ?result coso:measurementValue ?result_value;
        coso:measurementUnit ?unit .
    VALUES ?unit {{<http://qudt.org/vocab/unit/NanoGM-PER-L>}}.
    VALUES ?substance {{{substanceCode}}}
    VALUES ?matType {{{matTypeCode}}}
    FILTER (?result_value >= {minValue}).
    FILTER (?result_value <= {maxValue}).
    ?unit qudt:symbol ?unit_sym.
    BIND((CONCAT(str(?result_value) , " ", ?unit_sym)) as ?subVal)
}} GROUP BY ?sp ?spWKT
'''
            
            sparqlGET = SPARQLWrapper2("https://frink.apps.renci.org/sawgraph/sparql")
            sparqlGET.setHTTPAuth(DIGEST)
            sparqlGET.setMethod(POST)
            sparqlGET.setReturnFormat(JSON)
            
            sparqlGET.setQuery(q1)
            samplepoint_result = sparqlGET.query()
            samplepoints = convertToDataframe(samplepoint_result)
            
            # Debug output
            with st.expander("Query 1 Results"):
                st.write(f"Sample points found: {len(samplepoints)}")
                if not samplepoints.empty:
                    st.write("Columns:", list(samplepoints.columns))
                    st.write("First few rows:", samplepoints.head())
            
            if samplepoints.empty:
                st.warning("No sample points found with the specified criteria.")
                st.info("Try adjusting:")
                st.info("• The concentration range (current: " + str(minValue) + "-" + str(maxValue) + " ng/L)")
                st.info("• The PFAS substance (current: " + substance + ")")
                st.info("• The material type (current: " + materialType + ")")
                
                # Show what the query was looking for
                with st.expander("🔍 What was searched"):
                    st.code(f"""
Searching for:
- Substance: {substanceCode}
- Material Type: {matTypeCode}
- Concentration: {minValue} < value < {maxValue} ng/L
- Unit: NanoGM-PER-L
                    """)
                st.stop()
            
            # Query 2: Get S2 cells
            status_text.text("Querying S2 cells...")
            progress_bar.progress(25)
            
            q2 = f'''
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX me_egad: <http://sawgraph.spatialai.org/v1/me-egad#>
PREFIX me_egad_data: <http://sawgraph.spatialai.org/v1/me-egad-data#>

SELECT DISTINCT ?s2cell WHERE {{
    ?sp rdf:type coso:SamplePoint;
        spatial:connectedTo ?s2cell .
    ?s2cell rdf:type kwg-ont:S2Cell_Level13 .
    ?observation rdf:type coso:ContaminantObservation;
        coso:observedAtSamplePoint ?sp;
        coso:ofSubstance ?substance ;
        coso:analyzedSample ?sample ;
        coso:hasResult ?result .
    ?sample coso:sampleOfMaterialType ?matType.
    ?matType rdfs:label ?matTypeLabel.
    ?result coso:measurementValue ?result_value;
        coso:measurementUnit ?unit .
    VALUES ?unit {{<http://qudt.org/vocab/unit/NanoGM-PER-L>}}.
    VALUES ?substance {{{substanceCode}}}
    VALUES ?matType {{{matTypeCode}}}
    FILTER (?result_value >= {minValue}).
    FILTER (?result_value <= {maxValue}).
}} GROUP BY ?s2cell
'''
            
            sparqlGET.setQuery(q2)
            s2_result = sparqlGET.query()
            s2cells = convertToDataframe(s2_result)
            
            if s2cells.empty or 's2cell' not in s2cells.columns:
                st.warning("No S2 cells found matching the criteria. This could mean:")
                st.info("• No samples found with the specified PFAS substance and concentration range")
                st.info("• The material type may not have samples in this range")
                st.info("• Try adjusting the concentration range or selecting different parameters")
                st.stop()
            
            s2_values_string = convertS2ListToQueryString(s2cells['s2cell'].tolist())
            
            # Query 3: Filter S2 cells by admin region
            status_text.text("Filtering by administrative region...")
            progress_bar.progress(40)
            
            q3 = f'''
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>

SELECT ?s2cell WHERE {{
    ?s2neighbor spatial:connectedTo kwgr:administrativeRegion.USA.{regionCode} .
    VALUES ?s2neighbor {{{s2_values_string}}}
    ?s2neighbor kwg-ont:sfTouches | owl:sameAs ?s2cell.
}}'''
            
            # Query counties
            q_counties = f'''
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>

SELECT * WHERE {{
    VALUES ?county {{kwgr:administrativeRegion.USA.{regionCode}}}
    ?county geo:hasGeometry/geo:asWKT ?countyWKT;
            rdfs:label ?countyName.
}}'''
            
            sparqlGET_spatial = SPARQLWrapper2("https://frink.apps.renci.org/spatialkg/sparql")
            sparqlGET_spatial.setHTTPAuth(DIGEST)
            sparqlGET_spatial.setMethod(POST)
            sparqlGET_spatial.setReturnFormat(JSON)
            
            sparqlGET_spatial.setQuery(q3)
            s2_filtered_result = sparqlGET_spatial.query()
            s2_filtered = convertToDataframe(s2_filtered_result)
            
            sparqlGET_spatial.setQuery(q_counties)
            counties_result = sparqlGET_spatial.query()
            counties = convertToDataframe(counties_result)
            
            if len(s2_filtered) == 0:
                st.warning("No S2 cells found in the selected region.")
                st.stop()
            
            s2_filtered_values_string = convertS2ListToQueryString(s2_filtered['s2cell'].tolist())
            
            # Query 4: Get upstream S2 cells from hydrology
            status_text.text("Analyzing upstream hydrology...")
            progress_bar.progress(60)
            
            q4 = f'''
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>
PREFIX nhdplusv2: <http://nhdplusv2.spatialai.org/v1/nhdplusv2#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?s2cell WHERE {{
    ?downstream_flowline rdf:type hyf:HY_FlowPath ;
                        spatial:connectedTo ?s2cellds .
    ?upstream_flowline hyf:downstreamFlowPathTC ?downstream_flowline .
    VALUES ?s2cellds {{{s2_filtered_values_string}}}
    ?s2cell spatial:connectedTo ?upstream_flowline ;
            rdf:type kwg-ont:S2Cell_Level13 .
}}'''
            
            sparqlGET_hydro = SPARQLWrapper2("https://frink.apps.renci.org/hydrologykg/sparql")
            sparqlGET_hydro.setHTTPAuth(DIGEST)
            sparqlGET_hydro.setMethod(POST)
            sparqlGET_hydro.setReturnFormat(JSON)
            
            sparqlGET_hydro.setQuery(q4)
            hydrology_result = sparqlGET_hydro.query()
            hydrology = convertToDataframe(hydrology_result)
            
            if hydrology.empty or 's2cell' not in hydrology.columns:
                st.warning("No upstream S2 cells found in the hydrology network.")
                st.info("Using the original S2 cells for facility search...")
                s2_upstream_values_string = s2_filtered_values_string
            else:
                s2_upstream_values_string = convertS2ListToQueryString(hydrology['s2cell'].tolist())
            
            # Query 5: Get facilities
            status_text.text("Identifying upstream facilities...")
            progress_bar.progress(80)
            
            q5 = f'''
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fio: <http://w3id.org/fio/v1/fio#>

SELECT DISTINCT ?facility ?facWKT ?facilityName ?industry ?industryName ?industryGroup 
                ?industryGroupName ?industrySubsector ?industrySubsectorName WHERE {{
    ?s2cell kwg-ont:sfContains ?facility.
    VALUES ?s2cell {{{s2_upstream_values_string}}}
    ?facility fio:ofIndustry ?industryCode, ?industryGroup, ?industrySubsector ;
            geo:hasGeometry/geo:asWKT ?facWKT;
            rdfs:label ?facilityName.
    ?industryCode a naics:NAICS-IndustryCode;
        rdfs:label ?industryName ;
        fio:subcodeOf ?industryGroup .
    ?industryGroup a naics:NAICS-IndustryGroup;
        rdfs:label ?industryGroupName ;
        fio:subcodeOf ?industrySubsector .
    ?industrySubsector a naics:NAICS-IndustrySubsector;
        rdfs:label ?industrySubsectorName;
        fio:subcodeOf naics:NAICS-31 .
}}'''
            
            sparqlGET_fio = SPARQLWrapper2("https://frink.apps.renci.org/fiokg/sparql")
            sparqlGET_fio.setHTTPAuth(DIGEST)
            sparqlGET_fio.setMethod(POST)
            sparqlGET_fio.setReturnFormat(JSON)
            
            sparqlGET_fio.setQuery(q5)
            facility_result = sparqlGET_fio.query()
            facilities = convertToDataframe(facility_result)
            
            # Process spatial data
            status_text.text("Processing spatial data...")
            progress_bar.progress(90)
            
            # Convert to GeoDataFrames
            if not samplepoints.empty and 'spWKT' in samplepoints.columns:
                samplepoints['spWKT'] = samplepoints['spWKT'].apply(wkt.loads)
                samplepoints = gpd.GeoDataFrame(samplepoints, geometry='spWKT')
                samplepoints.set_crs(epsg=4326, inplace=True, allow_override=True)
            
            if not facilities.empty and 'facWKT' in facilities.columns:
                facilities['facWKT'] = facilities['facWKT'].apply(wkt.loads)
                facilities = gpd.GeoDataFrame(facilities, geometry='facWKT')
                facilities.set_crs(epsg=4326, inplace=True, allow_override=True)
            
            if not counties.empty and 'countyWKT' in counties.columns:
                counties['countyWKT'] = counties['countyWKT'].apply(wkt.loads)
                counties = gpd.GeoDataFrame(counties, geometry='countyWKT')
                counties.set_crs(epsg=4326, inplace=True, allow_override=True)
            
            # Create map
            status_text.text("Creating interactive map...")
            progress_bar.progress(95)
            
            # Initialize map with county boundaries
            if not counties.empty and 'countyWKT' in counties.columns:
                map_obj = counties.explore(name='Counties', style_kwds=dict(color='Gray', fill=0.0))
            else:
                map_obj = folium.Map(location=[45.0, -69.0], zoom_start=7)
            
            # Add sample points
            if not samplepoints.empty and 'spWKT' in samplepoints.columns:
                samplepoints.explore(
                    m=map_obj,
                    name='<span style="color:DarkOrange;">Samples</span>',
                    color='DarkOrange',
                    style_kwds=dict(style_function=lambda x: {
                        "radius": float(x['properties']["max"])/20 if float(x['properties']["max"]) < 320 else 16,
                        "opacity": 0.3,
                        "color": 'DimGray',
                    }),
                    marker_kwds=dict(radius=6),
                    marker_type='circle_marker',
                    popup=["sp", "max", "resultCount"],
                )
            
            # Add facilities by industry type
            if not facilities.empty and 'industrySubsectorName' in facilities.columns:
                colors = ['MidnightBlue','MediumBlue','SlateBlue','MediumSlateBlue', 
                         'DodgerBlue','DeepSkyBlue','SkyBlue','CadetBlue','DarkCyan',
                         'LightSeaGreen','MediumSageGreen','PaleVioletRed','Purple',
                         'Orchid','Fuchsia','MediumVioletRed','HotPink','LightPink']
                
                c = 0
                for industry in list(facilities.industrySubsectorName.unique()):
                    facilities[facilities['industrySubsectorName'] == industry].explore(
                        m=map_obj,
                        name=f'<span style="color:{colors[c % len(colors)]};">{industry}</span>',
                        color=colors[c % len(colors)],
                        marker_kwds=dict(radius=6),
                        popup=True
                    )
                    c += 1
            
            folium.LayerControl(collapsed=True).add_to(map_obj)
            
            progress_bar.progress(100)
            status_text.text("Analysis complete!")
            
            # Display results
            st.success("✅ Analysis completed successfully!")
            
            # Display statistics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Sample Points Found", len(samplepoints) if not samplepoints.empty else 0)
            with col2:
                st.metric("Upstream Facilities", len(facilities) if not facilities.empty else 0)
            with col3:
                if not facilities.empty and 'industrySubsectorName' in facilities.columns:
                    st.metric("Industry Types", len(facilities['industrySubsectorName'].unique()))
                else:
                    st.metric("Industry Types", 0)
            
            # Display map
            st.subheader("📍 Interactive Map")
            st_folium(map_obj, width=None, height=600, returned_objects=[])
            
            # Display data tables
            with st.expander("📊 Sample Points Data"):
                if not samplepoints.empty:
                    display_cols = [col for col in samplepoints.columns if col != 'spWKT']
                    st.dataframe(samplepoints[display_cols])
                else:
                    st.info("No sample points found matching the criteria.")
            
            with st.expander("🏭 Facilities Data"):
                if not facilities.empty:
                    display_cols = [col for col in facilities.columns if col != 'facWKT']
                    st.dataframe(facilities[display_cols])
                else:
                    st.info("No facilities found in the upstream areas.")
            
            # Export options
            st.subheader("💾 Export Options")
            col1, col2 = st.columns(2)
            with col1:
                if not samplepoints.empty:
                    csv_samples = samplepoints.drop(columns=['spWKT']).to_csv(index=False)
                    st.download_button(
                        label="📥 Download Sample Points (CSV)",
                        data=csv_samples,
                        file_name=f"sample_points_{substance}_{regionCode}.csv",
                        mime="text/csv"
                    )
            with col2:
                if not facilities.empty:
                    csv_facilities = facilities.drop(columns=['facWKT']).to_csv(index=False)
                    st.download_button(
                        label="📥 Download Facilities (CSV)",
                        data=csv_facilities,
                        file_name=f"facilities_{substance}_{regionCode}.csv",
                        mime="text/csv"
                    )
            
        except Exception as e:
            st.error(f"An error occurred during analysis: {str(e)}")
            st.info("Please check your parameters and try again.")

# Information section
with st.expander("ℹ️ About this Application"):
    st.markdown("""
    ### Overview
    This application performs upstream tracing analysis to identify potential sources of PFAS contamination 
    in water samples. It combines multiple knowledge graphs:
    
    - **PFAS Graph**: Contains contamination observation data
    - **Spatial Graph**: Provides administrative boundaries and spatial relationships
    - **Hydrology Graph**: Traces upstream flow paths
    - **Facility Graph**: Identifies industrial facilities
    
    ### How it Works
    1. **Sample Selection**: Identifies water samples with PFAS concentrations in the specified range
    2. **Spatial Filtering**: Filters results to the selected administrative region
    3. **Upstream Tracing**: Uses hydrological data to find upstream areas
    4. **Facility Identification**: Locates industrial facilities in upstream areas
    
    ### Data Sources
    - SPARQL endpoints from FRINK (Framework for Representing INtegrated Knowledge)
    - NHDPlus V2 hydrology data
    - NAICS industry classification
    """)

# Footer