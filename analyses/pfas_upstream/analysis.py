"""
PFAS Upstream Tracing Analysis (Query 1)
Trace contamination upstream to identify potential sources
"""
from __future__ import annotations

import streamlit as st
import pandas as pd
import folium
import math
from streamlit_folium import st_folium
import geopandas as gpd
from shapely import wkt

from analysis_registry import AnalysisContext
from analyses.pfas_upstream.queries import (
    execute_sparql_query,
    execute_hydrology_query,
    execute_facility_query,
)
from filters.substance import get_available_substances_with_labels
from filters.material import get_available_material_types_with_labels
from filters.region import get_region_boundary, add_region_boundary_layers
from filters.concentration import render_concentration_filter, apply_concentration_filter


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
    has_results_key = f"{analysis_key}_has_results"
    results_key = f"{analysis_key}_results"
    selected_substance_key = f"{analysis_key}_selected_substance"
    selected_material_type_key = f"{analysis_key}_selected_material_type"
    
    if selected_substance_key not in st.session_state:
        st.session_state[selected_substance_key] = None
    if selected_material_type_key not in st.session_state:
        st.session_state[selected_material_type_key] = None
    
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
    
    # Sidebar parameters (outside a form so concentration inputs can sync with slider)
    st.sidebar.markdown("### 🧪 PFAS Substance")

    substance_map = {}
    if not substances_view.empty:
        for _, row in substances_view.iterrows():
            name = row["display_name"]
            uri = row["substance"]
            if name not in substance_map or uri.endswith("_A"):
                substance_map[name] = uri

    substance_options = ["-- All Substances --"] + sorted(substance_map.keys())

    selected_substance_display = st.sidebar.selectbox(
        "Select PFAS Substance (Optional)",
        substance_options,
        help="Select a specific PFAS compound to analyze, or leave as 'All Substances'",
    )

    # Get the selected substance's full URI
    selected_substance_uri = None
    selected_substance_name = None
    if selected_substance_display != "-- All Substances --":
        selected_substance_name = selected_substance_display
        selected_substance_uri = substance_map.get(selected_substance_display)
        if selected_substance_uri:
            st.session_state[selected_substance_key] = {
                "name": selected_substance_name,
                "uri": selected_substance_uri,
            }
    else:
        st.session_state[selected_substance_key] = None

    st.sidebar.markdown("---")

    # MATERIAL TYPE SELECTION (Optional)
    st.sidebar.markdown("### 🧫 Sample Material Type")

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

    selected_material_display = st.sidebar.selectbox(
        "Select Material Type (Optional)",
        material_type_options,
        help="Select the type of sample material analyzed (e.g., Drinking Water, Groundwater, Soil)",
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
            "short": selected_material_short,
            "label": selected_material_label,
            "uri": selected_material_uri,
            "name": selected_material_name,
        }
    else:
        st.session_state[selected_material_type_key] = None

    st.sidebar.markdown("---")

    # Concentration filter (includes nondetects checkbox)
    conc_filter = render_concentration_filter(analysis_key, default_max=500)
    min_concentration = conc_filter.min_concentration
    max_concentration = conc_filter.max_concentration
    county_selected = context.selected_county_code is not None
    execute_button = st.sidebar.button(
        "🔍 Execute Query",
        type="primary",
        use_container_width=True,
        disabled=not county_selected,
        help="Select a county first" if not county_selected else "Execute the upstream tracing analysis",
    )

    # Display query parameters when Execute button is clicked
    if execute_button:
        # Apply pending concentration filter values
        min_concentration, max_concentration, include_nondetects = apply_concentration_filter(analysis_key)
        
        if not context.selected_state_code:
            st.error("❌ **State selection is required!** Please select a state before executing the query.")
        else:
            params_data = []
            
            substance_val = selected_substance_name if selected_substance_name else "All Substances"
            params_data.append({"Parameter": "PFAS Substance", "Value": substance_val})
            
            mat_val = selected_material_name if selected_material_name else "All Material Types"
            params_data.append({"Parameter": "Material Type", "Value": mat_val})
            
            conc_range_text = f"{min_concentration} - {max_concentration} ng/L"
            if include_nondetects:
                conc_range_text += " (including nondetects)"
            params_data.append({"Parameter": "Detected Concentration", "Value": conc_range_text})
            
            params_data.append({"Parameter": "Geographic Region", "Value": context.region_display})
            
            params_df = pd.DataFrame(params_data)

            st.markdown("---")
            st.subheader("🚀 Query Execution")
            
            prog_col1, prog_col2, prog_col3 = st.columns(3)
            
            samples_df = pd.DataFrame()
            upstream_s2_df = pd.DataFrame()
            upstream_flowlines_df = pd.DataFrame()
            facilities_df = pd.DataFrame()
            combined_error = None
            debug_info = None

            step1_error = None
            step2_error = None
            step3_error = None

            with prog_col1:
                with st.spinner("🔄 Step 1: Finding contaminated samples..."):
                    effective_min = 0 if include_nondetects else min_concentration
                    samples_df, step1_error = execute_sparql_query(
                        selected_substance_uri,
                        selected_material_uri,
                        effective_min,
                        max_concentration,
                        context.region_code,
                        include_nondetects=include_nondetects,
                    )

                if samples_df is None:
                    samples_df = pd.DataFrame()

                state_boundary_df = (
                    get_region_boundary(context.selected_state_code) if context.selected_state_code else None
                )
                county_boundary_df = (
                    get_region_boundary(context.selected_county_code) if context.selected_county_code else None
                )
                region_boundary_df = (
                    county_boundary_df
                    if (county_boundary_df is not None and not county_boundary_df.empty)
                    else state_boundary_df
                )

                if step1_error:
                    st.error(f"❌ Step 1 failed: {step1_error}")
                elif not samples_df.empty:
                    st.success(f"✅ Step 1: Found {len(samples_df)} contaminated samples")
                else:
                    st.warning("⚠️ Step 1: No contaminated samples found")
        
            with prog_col2:
                if step1_error:
                    st.info("⏭️ Step 2: Skipped (step 1 error)")
                elif not samples_df.empty:
                    with st.spinner("🔄 Step 2: Tracing upstream flow paths..."):
                        upstream_s2_df, upstream_flowlines_df, step2_error = execute_hydrology_query(samples_df)
                        if upstream_s2_df is None:
                            upstream_s2_df = pd.DataFrame()
                        if upstream_flowlines_df is None:
                            upstream_flowlines_df = pd.DataFrame()

                    if step2_error:
                        st.error(f"❌ Step 2 failed: {step2_error}")
                    elif not upstream_s2_df.empty:
                        st.success(f"✅ Step 2: Traced {len(upstream_s2_df)} upstream paths")
                    else:
                        st.info("ℹ️ Step 2: No upstream sources found")
                else:
                    st.info("⏭️ Step 2: Skipped (no samples)")
        
            with prog_col3:
                if step2_error:
                    st.info("⏭️ Step 3: Skipped (step 2 error)")
                elif not upstream_s2_df.empty:
                    with st.spinner("🔄 Step 3: Finding upstream facilities..."):
                        facilities_df, step3_error = execute_facility_query(upstream_s2_df)
                        if facilities_df is None:
                            facilities_df = pd.DataFrame()

                    if step3_error:
                        st.error(f"❌ Step 3 failed: {step3_error}")
                    elif not facilities_df.empty:
                        st.success(f"✅ Step 3: Found {len(facilities_df)} facilities")
                    else:
                        st.info("ℹ️ Step 3: No facilities found")
                else:
                    st.info("⏭️ Step 3: Skipped (no upstream cells)")

            combined_error = step1_error or step2_error or step3_error
            debug_info = {
                "step1_error": step1_error,
                "step2_error": step2_error,
                "step3_error": step3_error,
            }
            if not combined_error:
                debug_info = None

            st.session_state[results_key] = {
                'samples_df': samples_df,
                'upstream_s2_df': upstream_s2_df,
                'upstream_flowlines_df': upstream_flowlines_df,
                'facilities_df': facilities_df,
                'combined_error': combined_error,
                'debug_info': debug_info,
                'region_boundary_df': region_boundary_df,
                'state_boundary_df': state_boundary_df,
                'county_boundary_df': county_boundary_df,
                'params_df': params_df,
                'query_region_code': context.region_code,
                'selected_material_name': selected_material_name
            }
            st.session_state[has_results_key] = True

    # Display results if they exist in session state
    if st.session_state.get(has_results_key, False):
        results = st.session_state[results_key]
        
        samples_df = results.get('samples_df')
        upstream_s2_df = results.get('upstream_s2_df')
        upstream_flowlines_df = results.get('upstream_flowlines_df')
        facilities_df = results.get('facilities_df')
        combined_error = results.get('combined_error')
        debug_info = results.get('debug_info')
        region_boundary_df = results.get('region_boundary_df')
        state_boundary_df = results.get('state_boundary_df')
        county_boundary_df = results.get('county_boundary_df')
        params_df = results.get('params_df')
        query_region_code = results.get('query_region_code')
        saved_material_name = results.get('selected_material_name')

        st.markdown("---")
        st.markdown("### 📋 Selected Parameters (from executed query)")
        st.table(params_df)

        st.markdown("---")
        st.markdown("### 🔬 Query Results")

        if debug_info:
            with st.expander("🐞 Debug Info (query & response details)"):
                debug_copy = dict(debug_info)
                query_text = debug_copy.pop("query", None)
                st.json(debug_copy)
                if query_text:
                    st.code(query_text.strip(), language="sparql")
    
        st.markdown("---")
    
        if samples_df is not None and not samples_df.empty:
            st.markdown("### 🔬 Step 1: Contaminated Samples")
        
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
                    avg_value = pd.to_numeric(samples_df['result_value'], errors='coerce').mean()
                    if pd.notna(avg_value):
                        st.metric("Avg Concentration", f"{avg_value:.2f} ng/L")
                elif 'max' in samples_df.columns:
                    avg_value = pd.to_numeric(samples_df['max'], errors='coerce').mean()
                    if pd.notna(avg_value):
                        st.metric("Avg Concentration", f"{avg_value:.2f} ng/L")
        
            with st.expander("📊 View Contaminated Samples Data"):
                st.dataframe(samples_df, use_container_width=True)
            
                csv_samples = samples_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Samples CSV",
                    data=csv_samples,
                    file_name=f"contaminated_samples_{query_region_code}.csv",
                    mime="text/csv",
                    key=f"download_{analysis_key}_samples"
                )
    
        if upstream_s2_df is not None and not upstream_s2_df.empty:
            st.markdown("### 🌊 Step 2: Upstream Flow Paths")
            st.metric("Total Upstream Connections", len(upstream_s2_df))
    
        if facilities_df is not None and not facilities_df.empty:
            st.markdown("### 🏭 Step 3: Potential Source Facilities")
        
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Facilities", len(facilities_df))
            with col2:
                if 'industryName' in facilities_df.columns:
                    st.metric("Industry Types", facilities_df['industryName'].nunique())
        
            with st.expander("📊 View Facilities Data"):
                display_df = facilities_df.copy()
                if 'facilityName' in display_df.columns and 'industryName' in display_df.columns:
                    display_df = display_df[['facilityName', 'industryName', 'facWKT', 'facility']]
                st.dataframe(display_df, use_container_width=True)
            
                csv_facilities = facilities_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Facilities CSV",
                    data=csv_facilities,
                    file_name=f"upstream_facilities_{query_region_code}.csv",
                    mime="text/csv",
                    key=f"download_{analysis_key}_facilities"
                )
            
            if 'industryName' in facilities_df.columns:
                with st.expander("🏭 Industry Breakdown", expanded=False):
                    st.markdown("### Industry Types")
                    
                    flat_data = facilities_df.copy()
                    flat_data['industryName'] = flat_data['industryName'].astype(str).str.strip()
                    
                    if 'industryCode' in flat_data.columns:
                        flat_data['code_clean'] = flat_data['industryCode'].apply(
                            lambda x: x.split('-')[-1] if isinstance(x, str) and '-' in x else ''
                        )
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
                    
                    industry_summary = flat_data.groupby('display_name').agg(
                        Facilities=('facility', 'nunique')
                    ).reset_index()
                    
                    total_facs = flat_data['facility'].nunique()
                    if total_facs > 0:
                        industry_summary['Percentage'] = (
                            industry_summary['Facilities'] / total_facs * 100
                        ).map('{:.1f}%'.format)
                    else:
                        industry_summary['Percentage'] = "0.0%"
                    
                    industry_summary.columns = ['Industry', 'Facilities', 'Percentage']
                    industry_summary = industry_summary.sort_values('Facilities', ascending=False).reset_index(drop=True)
                    
                    st.dataframe(industry_summary, use_container_width=True, hide_index=True)

    
        if (samples_df is not None and not samples_df.empty and 'spWKT' in samples_df.columns) or \
           (facilities_df is not None and not facilities_df.empty and 'facWKT' in facilities_df.columns):
            st.markdown("---")
            st.markdown("### 🗺️ Interactive Map")
        
            samples_gdf = None
            facilities_gdf = None
            flowlines_gdf = None
        
            if samples_df is not None and not samples_df.empty and 'spWKT' in samples_df.columns:
                samples_with_wkt = samples_df[samples_df['spWKT'].notna()].copy()
                if not samples_with_wkt.empty:
                    try:
                        samples_with_wkt['geometry'] = samples_with_wkt['spWKT'].apply(wkt.loads)
                        samples_gdf = gpd.GeoDataFrame(samples_with_wkt, geometry='geometry')
                        samples_gdf.set_crs(epsg=4326, inplace=True, allow_override=True)
                    except Exception as e:
                        st.warning(f"Could not parse sample geometries: {e}")
        
            if facilities_df is not None and not facilities_df.empty and 'facWKT' in facilities_df.columns:
                facilities_with_wkt = facilities_df[facilities_df['facWKT'].notna()].copy()
                if not facilities_with_wkt.empty:
                    try:
                        facilities_with_wkt['geometry'] = facilities_with_wkt['facWKT'].apply(wkt.loads)
                        facilities_gdf = gpd.GeoDataFrame(facilities_with_wkt, geometry='geometry')
                        facilities_gdf.set_crs(epsg=4326, inplace=True, allow_override=True)
                    except Exception as e:
                        st.warning(f"Could not parse facility geometries: {e}")
        
            flowlines_source = None
            if upstream_flowlines_df is not None and not upstream_flowlines_df.empty and 'upstream_flowlineWKT' in upstream_flowlines_df.columns:
                flowlines_source = upstream_flowlines_df
            elif upstream_s2_df is not None and not upstream_s2_df.empty and 'upstream_flowlineWKT' in upstream_s2_df.columns:
                flowlines_source = upstream_s2_df

            if flowlines_source is not None:
                flowlines_with_wkt = flowlines_source[flowlines_source['upstream_flowlineWKT'].notna()].copy()
                if not flowlines_with_wkt.empty:
                    try:
                        flowlines_with_wkt['geometry'] = flowlines_with_wkt['upstream_flowlineWKT'].apply(wkt.loads)
                        flowlines_gdf = gpd.GeoDataFrame(flowlines_with_wkt, geometry='geometry')
                        flowlines_gdf.set_crs(epsg=4326, inplace=True, allow_override=True)
                    except Exception as e:
                        st.warning(f"Could not parse flowline geometries: {e}")
        
            if samples_gdf is not None or facilities_gdf is not None or flowlines_gdf is not None:
                if samples_gdf is not None and not samples_gdf.empty:
                    # Use centroid to handle any geometry type (Point, Polygon, etc.)
                    centroids = samples_gdf.geometry.centroid
                    center_lat = centroids.y.mean()
                    center_lon = centroids.x.mean()
                    map_obj = folium.Map(location=[center_lat, center_lon], zoom_start=8)
                elif facilities_gdf is not None and not facilities_gdf.empty:
                    centroids = facilities_gdf.geometry.centroid
                    center_lat = centroids.y.mean()
                    center_lon = centroids.x.mean()
                    map_obj = folium.Map(location=[center_lat, center_lon], zoom_start=8)
                else:
                    map_obj = folium.Map(location=[39.8, -98.5], zoom_start=4)

                try:
                    popup_css = """
<style>
.leaflet-popup-content { min-width: 420px !important; max-width: 900px !important; width: auto !important; }
.leaflet-popup-content table { width: 100% !important; table-layout: auto; }
.leaflet-popup-content td, .leaflet-popup-content th {
  overflow-wrap: anywhere;
  white-space: normal !important;
}
.leaflet-popup-content a {
  overflow-wrap: anywhere;
  white-space: normal !important;
}
</style>
"""
                    map_obj.get_root().header.add_child(folium.Element(popup_css))
                except Exception:
                    pass
            
                add_region_boundary_layers(
                    map_obj,
                    state_boundary_df=state_boundary_df,
                    county_boundary_df=county_boundary_df,
                    region_boundary_df=region_boundary_df,
                    region_code=context.region_code,
                )
            
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
            
                if samples_gdf is not None and not samples_gdf.empty:
                    sample_fields = [
                        c
                        for c in ["sp", "result_value", "substance", "matType", "regionURI"]
                        if c in samples_gdf.columns
                    ]
                    if not sample_fields:
                        sample_fields = (
                            ['sp', 'result_value'] if all(col in samples_gdf.columns for col in ['sp', 'result_value'])
                            else (['sp', 'max'] if all(col in samples_gdf.columns for col in ['sp', 'max']) else [])
                        )
                    samples_gdf.explore(
                        m=map_obj,
                        name='<span style="color:DarkOrange;">🔬 Contaminated Samples</span>',
                        color='DarkOrange',
                        marker_kwds=dict(radius=8),
                        marker_type='circle_marker',
                        popup=sample_fields if sample_fields else True,
                        tooltip=sample_fields if sample_fields else None,
                        style_kwds=dict(
                            fillOpacity=0.7,
                            opacity=0.8
                        )
                    )
            
                if facilities_gdf is not None and not facilities_gdf.empty:
                    group_col = 'industryName'
                    
                    if 'industrySubsectorName' in facilities_gdf.columns and facilities_gdf['industrySubsectorName'].notna().any():
                        group_col = 'industrySubsectorName'
                    elif 'industryGroupName' in facilities_gdf.columns and facilities_gdf['industryGroupName'].notna().any():
                        group_col = 'industryGroupName'
                        
                    if group_col in facilities_gdf.columns:
                        colors = ['MidnightBlue','MediumBlue','SlateBlue','MediumSlateBlue', 
                                 'DodgerBlue','DeepSkyBlue','SkyBlue','CadetBlue','DarkCyan',
                                 'LightSeaGreen','MediumSeaGreen','PaleVioletRed','Purple',
                                 'Orchid','Fuchsia','MediumVioletRed','HotPink','LightPink']
                    
                        group_counts = facilities_gdf.groupby(group_col).size().sort_values(ascending=False)
                        sorted_groups = group_counts.index.tolist()
                        total_facilities = len(facilities_gdf)
                        
                        for idx, group in enumerate(sorted_groups):
                            group_facilities = facilities_gdf[facilities_gdf[group_col] == group]
                            count = len(group_facilities)
                            color = colors[idx % len(colors)]
                            layer_name = f'🏭 {group} ({count})'
                            
                            facility_fields = [
                                c
                                for c in [
                                    "facilityName",
                                    "industryName",
                                    "industryCode",
                                    "facWKT",
                                    "facility",
                                ]
                                if c in group_facilities.columns
                            ]
                            group_facilities.explore(
                                m=map_obj,
                                name=f'<span style="color:{color};">{layer_name}</span>',
                                color=color,
                                marker_kwds=dict(radius=6),
                                popup=facility_fields if facility_fields else True,
                                tooltip=facility_fields if facility_fields else None,
                            show=True
                        )
            
                folium.LayerControl(collapsed=True).add_to(map_obj)
                
            
                with st.spinner("Loading map..."):
                    st_folium(map_obj, width=None, height=600, returned_objects=[])
            
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
