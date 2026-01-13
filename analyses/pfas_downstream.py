"""
PFAS Downstream Tracing Analysis (Query 5)
Trace contamination downstream through hydrological flow paths
"""
import streamlit as st
import pandas as pd
import folium
import math
from streamlit_folium import st_folium
import geopandas as gpd
from shapely import wkt

from analysis_registry import AnalysisContext
from utils.downstream_tracing_queries import (
    execute_downstream_hydrology_query,
    execute_downstream_samples_query,
    execute_downstream_step1_query,
)
from utils.substance_filters import get_available_substances_with_labels
from utils.material_filters import get_available_material_types_with_labels


def main(context: AnalysisContext) -> None:
    """Main function for PFAS Downstream Tracing analysis"""
    # Check for old session state keys and show migration notice
    old_keys = ['q5_conc_min', 'q5_conc_max', 'q5_has_results', 'q5_results', 'q5_selected_substance', 'q5_selected_material_type']
    if any(key in st.session_state for key in old_keys):
        if 'migration_notice_shown' not in st.session_state:
            st.warning("Session state has been reset. Please reconfigure your analysis parameters.")
            st.session_state.migration_notice_shown = True
    
    st.markdown("""
    **What this analysis does:**
    - Finds water samples with PFAS contamination in your selected region
    - Traces *downstream* through hydrological flow paths
    - Identifies contaminated sample points downstream using the same PFAS/material/range filters
    
    **3-Step Process:** Find contamination → Trace downstream → Find downstream contamination
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
        st.session_state[conc_max_key] = 100
    
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
            key=f"{analysis_key}_substance_select",
            help="Select a specific PFAS compound to analyze, or leave as 'All Substances'"
        )
        
        selected_substance_uri = None
        selected_substance_name = None
        if selected_substance_display != "-- All Substances --":
            selected_substance_name = selected_substance_display
            selected_substance_uri = substance_map.get(selected_substance_display)
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
            key=f"{analysis_key}_material_select",
            help="Select the type of sample material analyzed (e.g., Drinking Water, Groundwater, Soil)"
        )
        
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

        max_limit = 500

        st.session_state[conc_min_key] = min(st.session_state[conc_min_key], max_limit)
        st.session_state[conc_max_key] = min(st.session_state[conc_max_key], max_limit)
        if st.session_state[conc_min_key] > st.session_state[conc_max_key]:
            st.session_state[conc_max_key] = st.session_state[conc_min_key]

        # Display current min/max boxes above the slider
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
        county_selected = context.selected_county_code is not None
        execute_button = st.form_submit_button(
            "🔍 Execute Query",
            type="primary",
            use_container_width=True,
            disabled=not county_selected,
            help="Select a county first" if not county_selected else "Execute the downstream tracing analysis"
        )
    
    if execute_button:
        if not context.selected_state_code:
            st.error("❌ **State selection is required!** Please select a state before executing the query.")
        else:
            params_data = []
            params_data.append({
                "Parameter": "PFAS Substance",
                "Value": selected_substance_name if selected_substance_name else "All Substances"
            })
            params_data.append({
                "Parameter": "Material Type",
                "Value": selected_material_name if selected_material_name else "All Material Types"
            })
            conc_range_text = f"{min_concentration} - {max_concentration} ng/L"
            if include_nondetects:
                conc_range_text += " (including nondetects)"
            params_data.append({
                "Parameter": "Detected Concentration",
                "Value": conc_range_text
            })
            params_data.append({"Parameter": "Geographic Region", "Value": context.region_display})
            
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
                    starting_samples_df, step1_error, step1_debug = execute_downstream_step1_query(
                        substance_uri=selected_substance_uri,
                        material_uri=selected_material_uri,
                        min_conc=min_concentration,
                        max_conc=max_concentration,
                        region_code=context.region_code,
                        include_nondetects=include_nondetects
                    )
                    debug_info["step1"] = step1_debug
                    if step1_error:
                        step_errors["step1"] = step1_error
                
                # Import get_region_boundary from app - will be refactored later
                from app import get_region_boundary
                region_boundary_df = get_region_boundary(context.region_code)
                
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
                        downstream_samples_df, step3_error, step3_debug = execute_downstream_samples_query(
                            downstream_s2_df=downstream_s2_df,
                            substance_uri=selected_substance_uri,
                            material_uri=selected_material_uri,
                            min_conc=min_concentration,
                            max_conc=max_concentration,
                            include_nondetects=include_nondetects
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
            
            st.session_state[results_key] = {
                'starting_samples_df': starting_samples_df,
                'downstream_flowlines_df': downstream_flowlines_df,
                'downstream_samples_df': downstream_samples_df,
                'downstream_samples_outside_start_df': downstream_samples_outside_start_df,
                'downstream_overlap_count': downstream_overlap_count,
                'step_errors': step_errors,
                'debug_info': debug_info,
                'region_boundary_df': region_boundary_df,
                'params_df': params_df,
                'query_region_code': context.region_code,
                'selected_material_name': selected_material_name
            }
            st.session_state[has_results_key] = True
    
    # Display results if available
    if st.session_state.get(has_results_key, False):
        results = st.session_state[results_key]
        
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
                    key=f"download_{analysis_key}_starting_samples"
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
                    key=f"download_{analysis_key}_downstream_samples"
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
