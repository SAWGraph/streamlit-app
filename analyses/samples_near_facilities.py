"""
Samples Near Facilities Analysis (Query 2)
Find contaminated samples near facilities of a specific industry type
"""
import streamlit as st
import pandas as pd
import folium
import math
from streamlit_folium import st_folium
import geopandas as gpd
from shapely import wkt

from analysis_registry import AnalysisContext
from utils.nearby_queries import NAICS_INDUSTRIES, execute_nearby_analysis
from utils.ui_components import render_hierarchical_naics_selector
from utils.sparql_helpers import get_region_boundary


def main(context: AnalysisContext) -> None:
    """Main function for Samples Near Facilities analysis"""
    # Check for old session state keys and show migration notice
    old_keys = ['q2_conc_min', 'q2_conc_max', 'q2_executed', 'q2_facilities', 'q2_samples']
    if any(key in st.session_state for key in old_keys):
        if 'migration_notice_shown' not in st.session_state:
            st.warning("Session state has been reset. Please reconfigure your analysis parameters.")
            st.session_state.migration_notice_shown = True
    
    st.markdown("""
    **What this analysis does:**
    - Find all facilities of a specific industry type in your region
    - Expand search to neighboring areas (S2 cells)
    - Identify contaminated samples near those facilities
    
    **Use case:** Determine if PFAS contamination exists near specific industries (e.g., sewage treatment, landfills, manufacturing)
    """)
    
    # Initialize session state for analysis-specific params
    analysis_key = context.analysis_key
    conc_min_key = f"{analysis_key}_conc_min"
    conc_max_key = f"{analysis_key}_conc_max"
    executed_key = f"{analysis_key}_executed"
    facilities_key = f"{analysis_key}_facilities"
    samples_key = f"{analysis_key}_samples"
    industry_key = f"{analysis_key}_industry"
    region_code_key = f"{analysis_key}_region_code"
    
    if conc_min_key not in st.session_state:
        st.session_state[conc_min_key] = 0
    if conc_max_key not in st.session_state:
        st.session_state[conc_max_key] = 100
    
    # --- SIDEBAR PARAMETERS ---
    # Industry selector using hierarchical tree dropdown (outside form for compatibility)
    st.sidebar.markdown("### 🏭 Industry Type")
    selected_naics_code = render_hierarchical_naics_selector(
        naics_dict=NAICS_INDUSTRIES,
        key=f"{analysis_key}_industry_selector",
        default_value="221320"  # Default to Sewage Treatment Facilities
    )

    # Get display name for selected code
    selected_industry_display = f"{selected_naics_code} - {NAICS_INDUSTRIES.get(selected_naics_code, 'Unknown')}" if selected_naics_code else "No selection"

    # Other parameters in a form
    with st.sidebar.form(key=f"{analysis_key}_params_form"):
        
        # DETECTED CONCENTRATION
        st.markdown("### 📊 Detected Concentration")

        # Include nondetects option
        include_nondetects_key = f"{analysis_key}_include_nondetects"
        if include_nondetects_key not in st.session_state:
            st.session_state[include_nondetects_key] = False

        include_nondetects = st.checkbox(
            "Include nondetects",
            value=st.session_state[include_nondetects_key],
            key=f"{analysis_key}_nondetects_checkbox",
            help="Include observations with zero concentration or nondetect flags"
        )
        st.session_state[include_nondetects_key] = include_nondetects

        max_limit = 60000

        st.session_state[conc_min_key] = min(st.session_state[conc_min_key], max_limit)
        st.session_state[conc_max_key] = min(st.session_state[conc_max_key], max_limit)
        if st.session_state[conc_min_key] > st.session_state[conc_max_key]:
            st.session_state[conc_max_key] = st.session_state[conc_min_key]

        # Show current min/max boxes above the slider
        min_col, max_col = st.columns(2)
        min_col.number_input(
            "Min (ng/L)",
            value=st.session_state[conc_min_key],
            min_value=0,
            format="%d",
            disabled=True,
            key=f"{analysis_key}_concentration_min_display",
            help="Minimum value reflected from the slider"
        )
        max_col.number_input(
            "Max (ng/L)",
            value=st.session_state[conc_max_key],
            min_value=0,
            format="%d",
            disabled=True,
            key=f"{analysis_key}_concentration_max_display",
            help="Maximum value reflected from the slider"
        )

        # Range slider for concentration selection
        # Using slider as primary control to avoid sync issues with form
        slider_value = st.slider(
            "Select concentration range (ng/L)",
            min_value=0,
            max_value=max_limit,
            value=(st.session_state[conc_min_key], st.session_state[conc_max_key]),
            step=1,
            key=f"{analysis_key}_concentration_slider",
            help="Drag to select min and max concentration in nanograms per liter"
        )

        # Extract slider values
        min_concentration, max_concentration = slider_value

        # Update session state
        st.session_state[conc_min_key] = min_concentration
        st.session_state[conc_max_key] = max_concentration

        # Display selected range clearly
        st.markdown(f"**Selected range:** {min_concentration} - {max_concentration} ng/L")

        # Show concentration context
        if max_concentration <= 10:
            st.info("🟢 Low range - background levels")
        elif max_concentration <= 70:
            st.info("🟡 Moderate range - measurable contamination")
        else:
            st.warning("🔴 High range - significant concern")
        
        st.markdown("---")

        # Execute button
        county_selected = context.selected_county_code is not None
        industry_selected = selected_naics_code is not None and selected_naics_code != ""
        can_execute = county_selected and industry_selected

        execute_button = st.form_submit_button(
            "🔍 Execute Query",
            type="primary",
            use_container_width=True,
            disabled=not can_execute,
            help="Select a county and industry type first" if not can_execute else "Execute the nearby facilities analysis"
        )

    # Execute the query when form is submitted
    if execute_button:
        if not context.selected_state_code:
            st.error("❌ Please select a state in the sidebar first!")
        elif not selected_naics_code:
            st.error("❌ Please select an industry type first!")
        else:
            with st.spinner(f"Searching for samples near {selected_industry_display}..."):
                # Execute the consolidated analysis (single query)
                facilities_df, samples_df = execute_nearby_analysis(
                    naics_code=selected_naics_code,
                    region_code=context.region_code,
                    min_concentration=min_concentration,
                    max_concentration=max_concentration,
                    include_nondetects=include_nondetects
                )
                
                # Store results in session state
                st.session_state[facilities_key] = facilities_df
                st.session_state[samples_key] = samples_df
                st.session_state[industry_key] = selected_industry_display
                st.session_state[region_code_key] = context.region_code
                st.session_state[executed_key] = True
    
    # Display Results
    if st.session_state.get(executed_key, False):
        facilities_df = st.session_state.get(facilities_key, pd.DataFrame())
        samples_df = st.session_state.get(samples_key, pd.DataFrame())
        industry_display = st.session_state.get(industry_key, '')
        
        st.markdown("---")
        st.markdown("### 📊 Results")
        
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
                        query_region_code = st.session_state.get(region_code_key)
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
                        facilities_gdf.explore(
                            m=map_obj,
                            name=f'<span style="color:Blue;">🏭 {industry_display} ({len(facilities_gdf)})</span>',
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
                            
                            samples_gdf.explore(
                                m=map_obj,
                                name=f'<span style="color:DarkOrange;">🧪 Contaminated Samples ({len(samples_gdf)})</span>',
                                color='DarkOrange',
                                marker_kwds=dict(radius=6),
                                popup=['substances', 'materials', 'maxConcentration'] if all(c in samples_gdf.columns for c in ['substances', 'materials']) else True,
                                show=True
                            )
                        
                        # Add layer control
                        folium.LayerControl(collapsed=False).add_to(map_obj)
                        
                        # Display map
                        st_folium(map_obj, width=None, height=600, returned_objects=[])
                        
                        st.info("""
                        **🗺️ Map Legend:**
                        - 📍 **Boundary** = Selected region (black=state, gray=county, red=subdivision)
                        - 🔵 **Blue markers** = Facilities of selected industry type
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
                    display_cols = [c for c in ['maxConcentration', 'substances', 'materials', 'resultCount', 'sp'] if c in samples_df.columns]
                    if display_cols:
                        st.dataframe(samples_df[display_cols], use_container_width=True)
                    else:
                        st.dataframe(samples_df, use_container_width=True)
                    
                    # Summary statistics
                    if 'maxConcentration' in samples_df.columns:
                        st.markdown("##### 📈 Concentration Statistics")
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Max (ng/L)", f"{samples_df['maxConcentration'].max():.2f}")
                        with col2:
                            st.metric("Mean (ng/L)", f"{samples_df['maxConcentration'].mean():.2f}")
                        with col3:
                            st.metric("Median (ng/L)", f"{samples_df['maxConcentration'].median():.2f}")
                else:
                    st.info("No contaminated samples found near the selected facilities")
        else:
            st.warning("No results found. Try a different industry type or region.")
    else:
        st.info("👈 Select parameters in the sidebar and click 'Execute Query' to run the analysis")
