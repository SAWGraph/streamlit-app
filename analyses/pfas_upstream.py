"""
PFAS Upstream Tracing Analysis (Query 1)
Trace contamination upstream to identify potential sources
"""
import streamlit as st
import pandas as pd
import folium
import math
from streamlit_folium import st_folium
import geopandas as gpd
from shapely import wkt

from analysis_registry import AnalysisContext
from utils.upstream_tracing_queries import (
    execute_combined_query,
    split_combined_results,
)
from utils.substance_filters import get_available_substances_with_labels
from utils.material_filters import get_available_material_types_with_labels
from utils.sparql_helpers import get_region_boundary


def main(context: AnalysisContext) -> None:
    """Main function for PFAS Upstream Tracing analysis"""
    # Check for old session state keys and show migration notice
    old_keys = ['conc_min', 'conc_max', 'has_results', 'query_results', 'selected_substance', 'selected_material_type']
    if any(key in st.session_state for key in old_keys):
        if 'migration_notice_shown' not in st.session_state:
            st.warning("Session state has been reset. Please reconfigure your analysis parameters.")
            st.session_state.migration_notice_shown = True
    
    st.markdown("""
    **What this analysis does:**
    - Finds water samples with PFAS contamination in your selected region
    - Traces upstream through hydrological flow paths  
    - Identifies industrial facilities that may be contamination sources
    
    **3-Step Process:** Find contamination → Trace upstream → Identify potential sources
    """)
    
    # Initialize session state for analysis-specific params
    analysis_key = context.analysis_key
    conc_min_key = f"{analysis_key}_conc_min"
    conc_max_key = f"{analysis_key}_conc_max"
    has_results_key = f"{analysis_key}_has_results"
    results_key = f"{analysis_key}_results"
    selected_substance_key = f"{analysis_key}_selected_substance"
    selected_material_type_key = f"{analysis_key}_selected_material_type"
    
    if selected_substance_key not in st.session_state:
        st.session_state[selected_substance_key] = None
    if selected_material_type_key not in st.session_state:
        st.session_state[selected_material_type_key] = None
    if conc_min_key not in st.session_state:
        st.session_state[conc_min_key] = 0
    if conc_max_key not in st.session_state:
        st.session_state[conc_max_key] = 60000
    
    # Query parameters in sidebar form
    st.sidebar.markdown("### 🧪 Query Parameters")
    
    # Get available substances for the selected region (cached)
    @st.cache_data(ttl=3600)
    def get_available_substances_view(region_code: str, is_subdivision: bool, _version: int = 6):
        """Get substances available in the selected region with display names."""
        return get_available_substances_with_labels(region_code, is_subdivision)

    # Determine if region is a subdivision
    is_subdivision = len(context.region_code) > 5 if context.region_code else False

    substances_view = (
        get_available_substances_view(context.region_code, is_subdivision, _version=6)
        if context.region_code
        else pd.DataFrame()
    )
    
    # Wrap parameters in a form to prevent immediate reruns
    with st.sidebar.form(key=f"{analysis_key}_params_form"):
        st.markdown("### 🧪 PFAS Substance")

        substance_map = {}
        if not substances_view.empty:
            for _, row in substances_view.iterrows():
                name = row["display_name"]
                uri = row["substance"]
                if name not in substance_map or uri.endswith("_A"):
                    substance_map[name] = uri

        substance_options = ["-- All Substances --"] + sorted(substance_map.keys())

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
            selected_substance_uri = substance_map.get(selected_substance_display)
            if selected_substance_uri:
                st.session_state[selected_substance_key] = {
                    'name': selected_substance_name,
                    'uri': selected_substance_uri
                }
        else:
            st.session_state[selected_substance_key] = None
    
        st.markdown("---")
    
        # MATERIAL TYPE SELECTION (Optional)
        st.markdown("### 🧫 Sample Material Type")
        
        # Get available material types for the selected region (cached)
        @st.cache_data(ttl=3600)
        def get_available_material_types_view(region_code: str, is_subdivision: bool, _version: int = 4):
            """Get material types available in the selected region with display names."""
            return get_available_material_types_with_labels(region_code, is_subdivision)

        material_types_view = (
            get_available_material_types_view(context.region_code, is_subdivision, _version=4)
            if context.region_code
            else pd.DataFrame()
        )
    
        material_type_options = ["-- All Material Types --"]
        material_type_display = {}

        if not material_types_view.empty:
            for _, row in material_types_view.iterrows():
                display_name = row["display_name"]
                material_type_options.append(display_name)
                material_type_display[display_name] = row["matType"]
    
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
            selected_material_uri = material_type_display.get(selected_material_display)
            selected_material_short = selected_material_display
            selected_material_label = selected_material_display
            st.session_state[selected_material_type_key] = {
                'short': selected_material_short,
                'label': selected_material_label,
                'uri': selected_material_uri,
                'name': selected_material_name
            }
        else:
            st.session_state[selected_material_type_key] = None
    
        st.markdown("---")
    
        # DETECTED CONCENTRATION SELECTION
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

        # Display current min/max values above the slider
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
        # Max set to 60000 to cover highest observed concentrations (SUM_OF_5_PFAS can reach 53000+)
        slider_value = st.slider(
            "Select concentration range (ng/L)",
            min_value=0,
            max_value=max_limit,
            value=(st.session_state[conc_min_key], st.session_state[conc_max_key]),
            step=10,
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
    
        # Execute Query Button
        st.markdown("---")
        county_selected = context.selected_county_code is not None
        execute_button = st.form_submit_button(
            "🔍 Execute Query",
            type="primary",
            use_container_width=True,
            disabled=not county_selected,
            help="Select a county first" if not county_selected else "Execute the upstream tracing analysis"
        )

    # Display query parameters when Execute button is clicked
    # Logic to handle query execution and result persistence
    if execute_button:
        # Validate required parameters
        if not context.selected_state_code:
            st.error("❌ **State selection is required!** Please select a state before executing the query.")
        else:
            # Prepare parameters for display and storage
            params_data = []
            
            # Substance
            substance_val = selected_substance_name if selected_substance_name else "All Substances"
            params_data.append({"Parameter": "PFAS Substance", "Value": substance_val})
            
            # Material Type
            mat_val = selected_material_name if selected_material_name else "All Material Types"
            params_data.append({"Parameter": "Material Type", "Value": mat_val})
            
            # Detected Concentration
            conc_range_text = f"{min_concentration} - {max_concentration} ng/L"
            if include_nondetects:
                conc_range_text += " (including nondetects)"
            params_data.append({"Parameter": "Detected Concentration", "Value": conc_range_text})
            
            # Geographic Region
            params_data.append({"Parameter": "Geographic Region", "Value": context.region_display})
            
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
                        region_code=context.region_code,
                        include_nondetects=include_nondetects
                    )
                
                samples_df, upstream_s2_df, facilities_df = split_combined_results(combined_df)
                
                # Also query the region boundary for mapping
                region_boundary_df = get_region_boundary(context.region_code)

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
            st.session_state[results_key] = {
                'samples_df': samples_df,
                'upstream_s2_df': upstream_s2_df,
                'facilities_df': facilities_df,
                'combined_error': combined_error,
                'debug_info': debug_info,
                'region_boundary_df': region_boundary_df,
                'params_df': params_df,
                'query_region_code': context.region_code,
                'selected_material_name': selected_material_name
            }
            st.session_state[has_results_key] = True

    # Display results if they exist in session state
    if st.session_state.get(has_results_key, False):
        results = st.session_state[results_key]
        
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
                elif 'max' in samples_df.columns:
                    avg_value = pd.to_numeric(samples_df['max'], errors='coerce').mean()
                    if pd.notna(avg_value):
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
                    key=f"download_{analysis_key}_samples"
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
                    key=f"download_{analysis_key}_facilities"
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
                        popup=['sp', 'result_value'] if all(col in samples_gdf.columns for col in ['sp', 'result_value']) else (['sp', 'max'] if all(col in samples_gdf.columns for col in ['sp', 'max']) else True),
                        tooltip=['result_value'] if 'result_value' in samples_gdf.columns else (['max'] if 'max' in samples_gdf.columns else None),
                        style_kwds=dict(
                            fillOpacity=0.7,
                            opacity=0.8
                        )
                    )
            
                # Add facilities (colored by industry)
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
