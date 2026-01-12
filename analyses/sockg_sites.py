"""
SOCKG Sites & Facilities Analysis
Optional state filter; shows SOCKG locations and nearby facilities.
"""
from __future__ import annotations

import streamlit as st
import pandas as pd
import folium
import geopandas as gpd
from shapely import wkt
from streamlit_folium import st_folium

from analysis_registry import AnalysisContext
from utils.sockg_queries import get_sockg_state_codes, get_sockg_locations, get_sockg_facilities


def _build_sockg_state_options(states_df: pd.DataFrame) -> list[str]:
    sockg_codes = get_sockg_state_codes()
    if sockg_codes.empty:
        return ["-- All States --"]

    allowed = set(sockg_codes["fips_code"].astype(str).str.zfill(2).tolist())
    subset = states_df[
        states_df["fipsCode"].astype(str).str.zfill(2).isin(allowed)
    ].sort_values("state_name")
    return ["-- All States --"] + subset["state_name"].tolist()


def main(context: AnalysisContext) -> None:
    """Render the SOCKG analysis UI."""
    st.markdown("""
    **What this analysis does:**
    - Retrieves SOCKG locations (ARS sites)
    - Finds nearby facilities and flags PFAS-related industries

    **State filter:** Optional (limited to states with SOCKG sites)
    """)

    st.sidebar.markdown("### 📍 SOCKG State (Optional)")
    state_options = _build_sockg_state_options(context.states_df)
    state_choice = st.sidebar.selectbox(
        "Select State (Optional)",
        state_options,
        key=f"{context.analysis_key}_sockg_state",
        help="Limit results to a state with SOCKG sites",
    )

    selected_state_code = None
    selected_state_name = None
    if state_choice != "-- All States --":
        selected_state_name = state_choice
        state_row = context.states_df[context.states_df["state_name"] == state_choice]
        if not state_row.empty:
            selected_state_code = str(state_row.iloc[0]["fipsCode"]).zfill(2)

    execute = st.sidebar.button(
        "🔍 Execute Query",
        type="primary",
        use_container_width=True,
        key=f"{context.analysis_key}_execute",
    )

    if execute:
        region_boundary_df = None
        with st.spinner("Running SOCKG queries..."):
            sites_df = get_sockg_locations(selected_state_code)
            facilities_df = get_sockg_facilities(selected_state_code)
            if selected_state_code:
                from utils.sparql_helpers import get_region_boundary
                region_boundary_df = get_region_boundary(selected_state_code)

        st.session_state[f"{context.analysis_key}_results"] = {
            "sites_df": sites_df,
            "facilities_df": facilities_df,
            "selected_state_name": selected_state_name,
            "selected_state_code": selected_state_code,
            "region_boundary_df": region_boundary_df,
        }
        st.session_state[f"{context.analysis_key}_has_results"] = True

    if not st.session_state.get(f"{context.analysis_key}_has_results", False):
        st.info("👈 Select a state (optional) and click 'Execute Query' to run the analysis")
        return

    results = st.session_state[f"{context.analysis_key}_results"]
    sites_df = results.get("sites_df", pd.DataFrame())
    facilities_df = results.get("facilities_df", pd.DataFrame())
    state_label = results.get("selected_state_name") or "All SOCKG states"
    region_boundary_df = results.get("region_boundary_df")

    st.markdown("---")
    st.markdown("### 📊 Results")
    st.caption(f"State filter used: {state_label}")

    pfas_count = 0
    if not facilities_df.empty and "PFASusing" in facilities_df.columns:
        pfas_count = facilities_df["PFASusing"].astype(str).str.lower().eq("true").sum()

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📍 SOCKG Locations", len(sites_df))
    with col2:
        st.metric("🏭 Facilities", len(facilities_df))
    with col3:
        st.metric("⚠️ PFAS-Related Facilities", pfas_count)

    if sites_df.empty and facilities_df.empty:
        st.warning("No results found. Try again or remove the state filter.")
        return

    tab1, tab2, tab3 = st.tabs(["🗺️ Map", "📍 Locations", "🏭 Facilities"])

    with tab1:
        sites_gdf = None
        facilities_gdf = None

        if not sites_df.empty and "locationGeometry" in sites_df.columns:
            sites_with_wkt = sites_df[sites_df["locationGeometry"].notna()].copy()
            if not sites_with_wkt.empty:
                sites_with_wkt["geometry"] = sites_with_wkt["locationGeometry"].apply(wkt.loads)
                sites_gdf = gpd.GeoDataFrame(sites_with_wkt, geometry="geometry")
                sites_gdf.set_crs(epsg=4326, inplace=True, allow_override=True)

        if not facilities_df.empty and "facWKT" in facilities_df.columns:
            fac_with_wkt = facilities_df[facilities_df["facWKT"].notna()].copy()
            if not fac_with_wkt.empty:
                fac_with_wkt["PFASusing"] = (
                    fac_with_wkt["PFASusing"].astype(str).str.lower() == "true"
                )
                fac_with_wkt["geometry"] = fac_with_wkt["facWKT"].apply(wkt.loads)
                facilities_gdf = gpd.GeoDataFrame(fac_with_wkt, geometry="geometry")
                facilities_gdf.set_crs(epsg=4326, inplace=True, allow_override=True)

        if sites_gdf is None and facilities_gdf is None:
            st.info("No spatial data available to render the map.")
            return

        if sites_gdf is not None and not sites_gdf.empty:
            centroids = sites_gdf.geometry.centroid
            center_lat = centroids.y.mean()
            center_lon = centroids.x.mean()
            map_obj = folium.Map(location=[center_lat, center_lon], zoom_start=6)
        elif facilities_gdf is not None and not facilities_gdf.empty:
            centroids = facilities_gdf.geometry.centroid
            center_lat = centroids.y.mean()
            center_lon = centroids.x.mean()
            map_obj = folium.Map(location=[center_lat, center_lon], zoom_start=6)
        else:
            map_obj = folium.Map(location=[39.8, -98.5], zoom_start=4)

        if sites_gdf is not None and not sites_gdf.empty:
            sites_points = sites_gdf.copy()
            sites_points["geometry"] = sites_points.geometry.centroid
            sites_points.explore(
                m=map_obj,
                name='<span style="color:Red;">📍 SOCKG Locations</span>',
                color="Red",
                marker_kwds=dict(radius=6),
                marker_type="circle_marker",
                popup=["locationId", "locationDescription"]
                if all(c in sites_points.columns for c in ["locationId", "locationDescription"])
                else True,
                show=True,
            )

        if facilities_gdf is not None and not facilities_gdf.empty:
            facilities_points = facilities_gdf.copy()
            facilities_points["geometry"] = facilities_points.geometry.centroid
            pfas_facilities = facilities_points[facilities_points["PFASusing"]]
            other_facilities = facilities_points[~facilities_points["PFASusing"]]

            if not other_facilities.empty:
                other_facilities.explore(
                    m=map_obj,
                    name='<span style="color:MidnightBlue;">🏭 Other Facilities</span>',
                    color="MidnightBlue",
                    marker_kwds=dict(radius=4),
                    marker_type="circle_marker",
                    popup=["facilityName", "industrySector"]
                    if all(c in other_facilities.columns for c in ["facilityName", "industrySector"])
                    else True,
                    show=True,
                )

            if not pfas_facilities.empty:
                pfas_facilities.explore(
                    m=map_obj,
                    name='<span style="color:DarkRed;">⚠️ PFAS-Related Facilities</span>',
                    color="DarkRed",
                    marker_kwds=dict(radius=5),
                    marker_type="circle_marker",
                    popup=["facilityName", "industries"]
                    if all(c in pfas_facilities.columns for c in ["facilityName", "industries"])
                    else True,
                    show=True,
                )

        if region_boundary_df is not None and not region_boundary_df.empty:
            boundary_wkt = region_boundary_df.iloc[0]["countyWKT"]
            boundary_name = region_boundary_df.iloc[0].get("countyName", "State")
            boundary_gdf = gpd.GeoDataFrame(
                index=[0],
                crs="EPSG:4326",
                geometry=[wkt.loads(boundary_wkt)]
            )
            folium.GeoJson(
                boundary_gdf.to_json(),
                name=f'<span style="color:#444444;">📍 {boundary_name} Boundary</span>',
                style_function=lambda x: {
                    "fillColor": "#ffffff00",
                    "color": "#444444",
                    "weight": 3,
                    "fillOpacity": 0.0,
                },
            ).add_to(map_obj)

        folium.LayerControl(collapsed=False).add_to(map_obj)
        st_folium(map_obj, width=None, height=600, returned_objects=[])

    with tab2:
        if sites_df.empty:
            st.info("No SOCKG locations found for the selected state.")
        else:
            display_cols = [
                c for c in ["locationId", "locationDescription", "location"] if c in sites_df.columns
            ]
            st.dataframe(sites_df[display_cols] if display_cols else sites_df, use_container_width=True)

    with tab3:
        if facilities_df.empty:
            st.info("No facilities found for the selected state.")
        else:
            display_cols = [
                c
                for c in [
                    "facilityName",
                    "industrySector",
                    "industrySubsector",
                    "PFASusing",
                    "industries",
                    "locations",
                ]
                if c in facilities_df.columns
            ]
            st.dataframe(facilities_df[display_cols] if display_cols else facilities_df, use_container_width=True)
