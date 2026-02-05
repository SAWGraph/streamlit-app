"""
PFAS Downstream Tracing Analysis (Query 5)
Trace contamination downstream from facilities of specific industry types
"""
from __future__ import annotations

import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import geopandas as gpd
from shapely import wkt
from branca.element import Figure

from analysis_registry import AnalysisContext
from analyses.pfas_downstream.queries import (
    execute_downstream_facilities_query,
    execute_downstream_streams_query,
    execute_downstream_samples_query,
)
from core.data_loader import load_naics_dict
from filters.industry import render_hierarchical_naics_selector
from filters.region import get_region_boundary
from filters.concentration import render_concentration_filter, apply_concentration_filter


def main(context: AnalysisContext) -> None:
    """Main function for PFAS Downstream Tracing analysis"""
    # Check for old session state keys and show migration notice
    old_keys = ['q5_conc_min', 'q5_conc_max', 'q5_has_results', 'q5_results']
    if any(key in st.session_state for key in old_keys):
        if 'migration_notice_shown' not in st.session_state:
            st.warning("Session state has been reset. Please reconfigure your analysis parameters.")
            st.session_state.migration_notice_shown = True
    
    st.markdown("""
    **What this analysis does:**
    - Finds facilities of a specific industry type in your selected region
    - Traces *downstream* through hydrological flow paths from those facilities
    - Identifies contaminated sample points downstream
    
    **Use case:** Determine if PFAS contamination flows downstream from specific industries (e.g., waste treatment, landfills, manufacturing)
    """)
    
    # Initialize session state for analysis-specific params
    analysis_key = context.analysis_key
    has_results_key = f"{analysis_key}_has_results"
    results_key = f"{analysis_key}_results"
    
    # --- SIDEBAR PARAMETERS ---
    # Industry selector using hierarchical tree dropdown (outside form for compatibility)
    naics_dict = load_naics_dict()
    st.sidebar.markdown("### 🏭 Industry Type")
    st.sidebar.markdown("_Required: select an industry to trace downstream_")
    selected_naics_code = render_hierarchical_naics_selector(
        naics_dict=naics_dict,
        key=f"{analysis_key}_industry_selector",
        default_value=None,  # No default - user must select
        allow_empty=True,  # Allow empty so we can show validation message
    )

    # Get display name for selected code
    selected_industry_display = (
        f"{selected_naics_code} - {naics_dict.get(selected_naics_code, 'Unknown')}"
        if selected_naics_code
        else "Not Selected"
    )

    # NOTE: Downstream tracing runs from ALL facilities that match the selected industry
    # in the selected region (matches the notebook's "industry-driven" flow).

    # Concentration filter (includes nondetects checkbox)
    conc_filter = render_concentration_filter(analysis_key)
    min_concentration = conc_filter.min_concentration
    max_concentration = conc_filter.max_concentration

    # Execute button - industry required; state and county optional (per RegionConfig)
    has_industry = bool(selected_naics_code)
    can_execute = has_industry

    missing = []
    if not has_industry:
        missing.append("industry")

    help_text = f"Select {', '.join(missing)} first" if missing else "Execute the downstream tracing analysis"

    execute_button = st.sidebar.button(
            "🔍 Execute Query",
            type="primary",
            use_container_width=True,
        disabled=not can_execute,
        help=help_text,
        )
    
    # Execute the query when form is submitted
    if execute_button:
        # Apply pending concentration filter values
        min_concentration, max_concentration, include_nondetects = apply_concentration_filter(analysis_key)
        
        # Validate required fields (industry required; state/county optional)
        missing_fields = []
        if not selected_naics_code:
            missing_fields.append("industry type")

        if missing_fields:
            st.error(f"❌ **Missing required selections!** Please select: {', '.join(missing_fields)}")
        else:
            # Always fetch region boundary for mapping (state/county boundary)
            # Fetch both state + county boundaries (when available) so we can draw the
            # correct border(s) regardless of whether the user selected only a state
            # or both state+county. County is drawn on top of state.
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

            # Run the full notebook-style pipeline driven by industry:
            step_errors = {}
            debug_info = {}
            params_data = [
                {"Parameter": "Industry Type", "Value": selected_industry_display},
                {"Parameter": "Geographic Region", "Value": context.region_display or "All Regions"},
                {"Parameter": "Detected Concentration", "Value": f"{min_concentration} - {max_concentration} ng/L"},
                {"Parameter": "Include nondetects", "Value": "Yes" if include_nondetects else "No"},
            ]
            params_df = pd.DataFrame(params_data)
            
            st.markdown("---")
            st.subheader("🚀 Query Execution")
            
            prog_col1, prog_col2, prog_col3 = st.columns(3)
            
            facilities_df = pd.DataFrame()
            streams_df = pd.DataFrame()
            samples_df = pd.DataFrame()
            
            with prog_col1:
                with st.spinner("🔄 Step 1: Finding facilities..."):
                    facilities_df, step1_error, step1_debug = execute_downstream_facilities_query(
                        naics_code=selected_naics_code,
                        region_code=context.region_code,
                    )
                    debug_info["step1"] = step1_debug
                    if step1_error:
                        step_errors["step1"] = step1_error
                
                if step1_error:
                    st.error(f"❌ Step 1 failed: {step1_error}")
                elif not facilities_df.empty:
                    st.success(f"✅ Step 1: Found {len(facilities_df)} facilities")
                else:
                    st.warning("⚠️ Step 1: No facilities found")

            with prog_col2:
                with st.spinner("🔄 Step 2: Tracing downstream streams..."):
                    streams_df, step2_error, step2_debug = execute_downstream_streams_query(
                        naics_code=selected_naics_code,
                        region_code=context.region_code,
                    )
                debug_info["step2"] = step2_debug
                if step2_error:
                    step_errors["step2"] = step2_error

                if step2_error:
                    st.error(f"❌ Step 2 failed: {step2_error}")
                elif not streams_df.empty:
                    stream_names = (
                        streams_df["streamName"].dropna().unique()
                        if "streamName" in streams_df.columns
                        else []
                    )
                    st.success(
                        f"✅ Step 2: Found {len(streams_df)} flowlines ({len(stream_names)} named streams)"
                    )
                else:
                    st.info("ℹ️ Step 2: No downstream flow paths found")

            with prog_col3:
                with st.spinner("🔄 Step 3: Finding downstream samples..."):
                    samples_df, step3_error, step3_debug = execute_downstream_samples_query(
                        naics_code=selected_naics_code,
                        region_code=context.region_code,
                        min_conc=min_concentration,
                        max_conc=max_concentration,
                        include_nondetects=include_nondetects,
                    )
                debug_info["step3"] = step3_debug
                if step3_error:
                    step_errors["step3"] = step3_error

                if step3_error:
                    st.error(f"❌ Step 3 failed: {step3_error}")
                elif not samples_df.empty:
                    st.success(f"✅ Step 3: Found {len(samples_df)} downstream samples")
                else:
                    st.info("ℹ️ Step 3: No downstream samples found")

            st.session_state[results_key] = {
                "facilities_df": facilities_df,
                "streams_df": streams_df,
                "samples_df": samples_df,
                "step_errors": step_errors,
                "debug_info": debug_info,
                "region_boundary_df": region_boundary_df,
                "state_boundary_df": state_boundary_df,
                "county_boundary_df": county_boundary_df,
                "params_df": params_df,
                "query_region_code": context.region_code,
                "selected_industry": selected_industry_display,
            }
            st.session_state[has_results_key] = True
    
    # Display results if available
    if st.session_state.get(has_results_key, False):
        results = st.session_state[results_key]
        
        facilities_df = results.get('facilities_df')
        streams_df = results.get('streams_df')
        samples_df = results.get('samples_df')
        debug_info = results.get('debug_info')
        region_boundary_df = results.get('region_boundary_df')
        state_boundary_df = results.get("state_boundary_df")
        county_boundary_df = results.get("county_boundary_df")
        params_df = results.get('params_df')
        query_region_code = results.get('query_region_code')
        selected_industry = results.get('selected_industry')
        
        st.markdown("---")
        st.markdown("### 📋 Selected Parameters (from executed query)")
        st.table(params_df)
        
        if debug_info:
            with st.expander("🐞 Debug Info (queries & response details)"):
                st.json(
                    {
                        "step_errors": results.get("step_errors"),
                        "step1": {
                            k: v
                            for k, v in (debug_info.get("step1") or {}).items()
                            if k not in {"query", "response_text_snippet"}
                        },
                        "step2": {
                            k: v
                            for k, v in (debug_info.get("step2") or {}).items()
                            if k not in {"query", "response_text_snippet"}
                        },
                        "step3": {
                            k: v
                            for k, v in (debug_info.get("step3") or {}).items()
                            if k not in {"query", "response_text_snippet"}
                        },
                    }
                )
        
        st.markdown("---")
        st.markdown("### 🔬 Query Results")
        
        # Summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("🏭 Facilities", len(facilities_df) if facilities_df is not None else 0)
        with col2:
            st.metric("🌊 Flowlines", len(streams_df) if streams_df is not None else 0)
        with col3:
            st.metric("🧪 Downstream Samples", len(samples_df) if samples_df is not None else 0)
        
        # Map section
        has_facility_wkt = facilities_df is not None and not facilities_df.empty and 'facWKT' in facilities_df.columns
        has_stream_wkt = streams_df is not None and not streams_df.empty and 'dsflWKT' in streams_df.columns
        has_sample_wkt = samples_df is not None and not samples_df.empty and 'spWKT' in samples_df.columns
        
        if has_facility_wkt or has_stream_wkt or has_sample_wkt:
            st.markdown("---")
            st.markdown("### 🗺️ Interactive Map")
            
            facilities_gdf = None
            streams_gdf = None
            samples_gdf = None
            
            # Process facilities
            if has_facility_wkt:
                facilities_with_wkt = facilities_df[facilities_df['facWKT'].notna()].copy()
                if not facilities_with_wkt.empty:
                    try:
                        facilities_with_wkt['geometry'] = facilities_with_wkt['facWKT'].apply(wkt.loads)
                        facilities_gdf = gpd.GeoDataFrame(facilities_with_wkt, geometry='geometry')
                        facilities_gdf.set_crs(epsg=4326, inplace=True, allow_override=True)
                        # Format facility links for popup
                        if 'facility' in facilities_gdf.columns:
                            facilities_gdf['facility_link'] = facilities_gdf['facility'].apply(
                                lambda x: f'<a href="https://frs-public.epa.gov/ords/frs_public2/fii_query_detail.disp_program_facility?p_registry_id={x.split(".")[-1]}" target="_blank">{x}</a>'
                                if x else x
                            )
                    except Exception as e:
                        st.warning(f"Could not parse facility geometries: {e}")
            
            # Process streams
            if has_stream_wkt:
                streams_with_wkt = streams_df[streams_df['dsflWKT'].notna()].copy()
                if not streams_with_wkt.empty:
                    try:
                        streams_with_wkt['geometry'] = streams_with_wkt['dsflWKT'].apply(wkt.loads)
                        streams_gdf = gpd.GeoDataFrame(streams_with_wkt, geometry='geometry')
                        streams_gdf.set_crs(epsg=4326, inplace=True, allow_override=True)
                    except Exception as e:
                        st.warning(f"Could not parse stream geometries: {e}")
            
            # Process samples
            if has_sample_wkt:
                samples_with_wkt = samples_df[samples_df['spWKT'].notna()].copy()
                if not samples_with_wkt.empty:
                    try:
                        samples_with_wkt['geometry'] = samples_with_wkt['spWKT'].apply(wkt.loads)
                        samples_gdf = gpd.GeoDataFrame(samples_with_wkt, geometry='geometry')
                        samples_gdf.set_crs(epsg=4326, inplace=True, allow_override=True)
                        # Format sample point links for popup
                        if 'samplePoint' in samples_gdf.columns:
                            def _short_uri_label(uri: str) -> str:
                                u = str(uri)
                                if "#" in u:
                                    return u.split("#")[-1]
                                return u.rstrip("/").split("/")[-1]

                            samples_gdf['samplePoint_link'] = samples_gdf['samplePoint'].apply(
                                lambda x: f'<a href="{x}" target="_blank">{_short_uri_label(x)}</a>' if x else x
                            )
                        # Clean up unit encoding
                        if 'unit' in samples_gdf.columns:
                            samples_gdf['unit'] = samples_gdf['unit'].str.replace('Î¼', 'μ')
                        if 'results' in samples_gdf.columns:
                            samples_gdf['results'] = samples_gdf['results'].str.replace('Î¼', 'μ')
                    except Exception as e:
                        st.warning(f"Could not parse sample geometries: {e}")
            
            # Determine map center
            if samples_gdf is not None and not samples_gdf.empty:
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
            
            # Ensure popups wrap long content instead of overflowing outside the card.
            try:
                popup_css = """
<style>
.leaflet-popup { max-width: 920px !important; }
.leaflet-popup-content-wrapper { max-width: 920px !important; }
.leaflet-popup-content { min-width: 420px !important; max-width: 900px !important; width: auto !important; }
.leaflet-popup-content table { width: 100% !important; table-layout: auto; }
.leaflet-popup-content td, .leaflet-popup-content th {
  overflow-wrap: anywhere;
  white-space: normal !important;
}
/* Ensure long URLs wrap instead of overflowing */
.leaflet-popup-content a {
  display: inline-block;
  max-width: 100%;
  overflow-wrap: anywhere;
  white-space: normal !important;
}
</style>
"""
                map_obj.get_root().header.add_child(folium.Element(popup_css))
            except Exception:
                pass
            
            # Add state + county boundaries (state first, then county on top)
            boundary_layers = []
            if state_boundary_df is not None and not state_boundary_df.empty:
                boundary_layers.append(("State", state_boundary_df, "#000000"))
            if county_boundary_df is not None and not county_boundary_df.empty:
                boundary_layers.append(("County", county_boundary_df, "#666666"))

            for region_type, bdf, color in boundary_layers:
                try:
                    boundary_wkt_val = bdf.iloc[0]["countyWKT"]
                    boundary_name = bdf.iloc[0].get("countyName", region_type)
                    boundary_gdf = gpd.GeoDataFrame(
                        index=[0],
                        crs="EPSG:4326",
                        geometry=[wkt.loads(boundary_wkt_val)],
                    )
                    folium.GeoJson(
                        boundary_gdf.to_json(),
                        name=f'<span style="color:{color};">📍 {region_type}: {boundary_name}</span>',
                        style_function=lambda _x, c=color: {
                            "fillColor": "#ffffff00",
                            "color": c,
                            "weight": 3,
                            "fillOpacity": 0.0,
                        },
                    ).add_to(map_obj)
                except Exception as e:
                    st.warning(f"Could not display {region_type.lower()} boundary: {e}")
            
            # Add samples layer (notebook-style: scaled radius + popup=True)
            if samples_gdf is not None and not samples_gdf.empty:
                # Notebook-style marker scaling:
                # - non-detect -> small marker w/ black outline
                # - detected < 40 -> small marker
                # - detected < 160 -> scale value/8
                # - detected >= 160 -> cap at 25
                def _sample_style(feature):
                    props = (feature or {}).get("properties", {}) or {}
                    max_val = props.get("Max")

                    # Determine non-detect
                    is_nondetect = False
                    if max_val in ["non-detect", "http://w3id.org/coso/v1/contaminoso#non-detect"]:
                        is_nondetect = True
                    else:
                        try:
                            is_nondetect = float(max_val) == 0
                        except Exception:
                            is_nondetect = False

                    # Radius scaling
                    # Clamp radius to keep markers readable (prevents huge circles).
                    radius = 4
                    if not is_nondetect:
                        try:
                            v = float(max_val)
                            if v < 40:
                                radius = 4
                            elif v < 160:
                                # gentler scaling than notebook to avoid oversized markers
                                radius = v / 16
                            else:
                                radius = 12
                        except Exception:
                            radius = 4

                    # final clamp
                    try:
                        radius = float(radius)
                    except Exception:
                        radius = 4
                    radius = max(3, min(12, radius))

                    return {
                        "radius": radius,
                        "opacity": 0.3,
                        "color": "Black" if is_nondetect else "DimGray",
                    }

                samples_gdf.explore(
                    m=map_obj,
                    name=f'<span style="color:DarkOrange;">Samples</span>',
                    color="DarkOrange",
                    marker_kwds=dict(radius=6),
                    marker_type="circle_marker",
                    popup=True,
                    popup_kwds={"max_height": 500, "max_width": 650},
                    style_kwds=dict(style_function=_sample_style),
                )
            
            # Add streams layer (notebook-style popup fields)
            if streams_gdf is not None and not streams_gdf.empty:
                if "downstream_flowline" in streams_gdf.columns:
                    streams_gdf["downstream_flowline"] = streams_gdf["downstream_flowline"].apply(
                        lambda x: f'<a href="{x}" target="_blank">{x}</a>' if x else x
                    )
                stream_popup = [c for c in ["streamName", "fl_type", "downstream_flowline"] if c in streams_gdf.columns]
                streams_gdf.explore(
                    m=map_obj,
                    name=f'<span style="color:LightSkyBlue;">Streams</span>',
                    color="LightSkyBlue",
                    popup=stream_popup if stream_popup else True,
                    popup_kwds={"max_width": 350},
                )
            
            # Add facilities layer (notebook-style: separate layer per industryName w/ color list)
            if facilities_gdf is not None and not facilities_gdf.empty:
                # Short, clickable FRS link (prevents huge URIs from blowing up popups)
                if "facility" in facilities_gdf.columns:
                    def _frs_id_from_uri(u: str) -> str:
                        try:
                            return str(u).split(".")[-1]
                        except Exception:
                            return str(u)

                    facilities_gdf["facility_frs_id"] = facilities_gdf["facility"].apply(_frs_id_from_uri)
                    facilities_gdf["facility_link"] = facilities_gdf["facility_frs_id"].apply(
                        lambda rid: (
                            f'<a href="https://frs-public.epa.gov/ords/frs_public2/fii_query_detail.disp_program_facility?p_registry_id={rid}" target="_blank">FRS {rid}</a>'
                            if rid
                            else rid
                        )
                    )

                # Curated fields: use same set for hover + click
                facility_popup_fields = [
                    c
                    for c in ["facility_link", "facilityName", "industryName", "industryCode"]
                    if c in facilities_gdf.columns
                ]
                colors = [
                    "Purple", "PaleVioletRed", "Orchid", "Fuchsia", "MediumVioletRed", "HotPink", "LightPink",
                    "red", "lightred", "pink", "orange",
                    "MidnightBlue", "MediumBlue", "SlateBlue", "MediumSlateBlue", "DodgerBlue", "DeepSkyBlue",
                    "SkyBlue", "CadetBlue", "DarkCyan", "LightSeaGreen",
                    "lightblue", "gray", "blue", "darkred", "lightgreen", "green", "darkblue", "darkpurple",
                    "cadetblue", "lightgray", "darkgreen",
                ]

                if "industryName" in facilities_gdf.columns and facilities_gdf["industryName"].notna().any():
                    unique_industries = list(sorted(facilities_gdf["industryName"].dropna().unique()))
                    for i, industry in enumerate(unique_industries):
                        c = colors[i % len(colors)]
                        facilities_gdf[facilities_gdf["industryName"] == industry].explore(
                            m=map_obj,
                            name=f'<span style="color:{c};">{industry}</span>',
                            color=c,
                            marker_kwds=dict(radius=3),
                            tooltip=facility_popup_fields if facility_popup_fields else True,
                            popup=facility_popup_fields if facility_popup_fields else True,
                            popup_kwds={"max_width": 900, "parse_html": True},
                        )
                else:
                    facilities_gdf.explore(
                    m=map_obj,
                        name=f'<span style="color:Purple;">Facilities</span>',
                        color="Purple",
                        marker_kwds=dict(radius=3),
                        tooltip=facility_popup_fields if facility_popup_fields else True,
                        popup=facility_popup_fields if facility_popup_fields else True,
                        popup_kwds={"max_width": 900, "parse_html": True},
                    )
            
            # Hide legend by default (consistent with other analyses in the app)
            folium.LayerControl(collapsed=True).add_to(map_obj)
            
            # Wrap in Figure for consistent sizing
            fig = Figure(width='100%', height=900)
            fig.add_child(map_obj)
            
            st_folium(map_obj, width=None, height=700, returned_objects=[])
            
            st.info("""
            **🗺️ Map Legend:**
            - 📍 **Boundary outline** = Selected region (black=state, gray=county, red=subdivision)
            - 🟠 **Orange circles** = Contaminated sample locations downstream of facilities
            - 🔵 **Light blue lines** = Downstream flow paths (streams/rivers)
            - 🟣 **Purple/pink markers** = Facilities of selected industry type (colored by specific industry)
            
            *Use the layer control on the right to toggle layers on/off*
            """)
            
            # Display stream names if available
            if streams_gdf is not None and not streams_gdf.empty and 'streamName' in streams_gdf.columns:
                stream_names = streams_gdf['streamName'].dropna().unique()
                if len(stream_names) > 0:
                    with st.expander(f"🌊 Stream Names ({len(stream_names)} unique streams)"):
                        st.write(", ".join(sorted(stream_names)))
        
        # Data tabs
        st.markdown("---")
        st.markdown("### 📊 Data Tables")
        
        tab1, tab2, tab3 = st.tabs(["🏭 Facilities", "🌊 Streams", "🧪 Samples"])
        
        with tab1:
            if facilities_df is not None and not facilities_df.empty:
                st.markdown(f"#### 🏭 {selected_industry}")
                
                # Select display columns
                display_cols = [c for c in ['facilityName', 'industryName', 'industryCode', 'facility'] if c in facilities_df.columns]
                if display_cols:
                    st.dataframe(facilities_df[display_cols], use_container_width=True)
                else:
                    st.dataframe(facilities_df, use_container_width=True)
                
                st.download_button(
                    label="📥 Download Facilities CSV",
                    data=facilities_df.to_csv(index=False),
                    file_name=f"downstream_facilities_{query_region_code or 'all'}.csv",
                    mime="text/csv",
                    key=f"download_{analysis_key}_facilities"
                )
            else:
                st.info("No facilities found")
        
        with tab2:
            if streams_df is not None and not streams_df.empty:
                st.markdown("#### 🌊 Downstream Flowlines")
                
                # Select display columns
                display_cols = [c for c in ['streamName', 'fl_type', 'downstream_flowline'] if c in streams_df.columns]
                if display_cols:
                    st.dataframe(streams_df[display_cols], use_container_width=True)
                else:
                    st.dataframe(streams_df, use_container_width=True)
                
                st.download_button(
                    label="📥 Download Streams CSV",
                    data=streams_df.to_csv(index=False),
                    file_name=f"downstream_streams_{query_region_code or 'all'}.csv",
                    mime="text/csv",
                    key=f"download_{analysis_key}_streams"
                )
            else:
                st.info("No downstream flowlines found")
        
        with tab3:
            if samples_df is not None and not samples_df.empty:
                st.markdown("#### 🧪 Contaminated Samples Downstream")
                
                # Select display columns
                display_cols = [c for c in ['Max', 'resultCount', 'unit', 'results', 'samplePoint', 'sample'] if c in samples_df.columns]
                if display_cols:
                    st.dataframe(samples_df[display_cols], use_container_width=True)
                else:
                    st.dataframe(samples_df, use_container_width=True)
                
                # Summary statistics
                if 'Max' in samples_df.columns:
                    st.markdown("##### 📈 Concentration Statistics")
                    try:
                        max_vals = pd.to_numeric(samples_df['Max'], errors='coerce')
                        if max_vals.notna().any():
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Max (ng/L)", f"{max_vals.max():.2f}")
                            with col2:
                                st.metric("Mean (ng/L)", f"{max_vals.mean():.2f}")
                            with col3:
                                st.metric("Median (ng/L)", f"{max_vals.median():.2f}")
                    except Exception:
                        pass
                
                st.download_button(
                    label="📥 Download Samples CSV",
                    data=samples_df.to_csv(index=False),
                    file_name=f"downstream_samples_{query_region_code or 'all'}.csv",
                    mime="text/csv",
                    key=f"download_{analysis_key}_samples"
                )
            else:
                st.info("No contaminated samples found downstream")
    else:
        st.info("👈 Select a state, county, and industry type in the sidebar, then click 'Execute Query' to run the analysis")
