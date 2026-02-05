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
from analyses.sockg_sites.queries import get_sockg_locations, get_sockg_facilities
from filters.region import get_region_boundary


def main(context: AnalysisContext) -> None:
    """Render the SOCKG analysis UI."""
    st.markdown("""
    **What this analysis does:**
    - Retrieves SOCKG locations (ARS sites)
    - Finds nearby facilities and flags PFAS-related industries

    **State filter:** Optional (use the region selector in the sidebar)
    """)

    # Use just the state code for SOCKG (only state filtering needed)
    state_code = context.selected_state_code
    state_name = context.selected_state_name
    state_display = state_name if state_name else "All states"

    # Show current filter status in sidebar
    st.sidebar.markdown("### 🧪 Query Parameters")
    st.sidebar.caption(f"State filter: {state_display}")

    execute = st.sidebar.button(
        "🔍 Execute Query",
        type="primary",
        use_container_width=True,
        key=f"{context.analysis_key}_execute",
    )

    if execute:
        region_boundary_df = None
        with st.spinner("Running SOCKG queries..."):
            sites_df = get_sockg_locations(state_code)
            facilities_df = get_sockg_facilities(state_code)
            if state_code:
                region_boundary_df = get_region_boundary(state_code)

        st.session_state[f"{context.analysis_key}_results"] = {
            "sites_df": sites_df,
            "facilities_df": facilities_df,
            "state_display": state_display,
            "state_code": state_code,
            "region_boundary_df": region_boundary_df,
        }
        st.session_state[f"{context.analysis_key}_has_results"] = True

    if not st.session_state.get(f"{context.analysis_key}_has_results", False):
        st.info("👈 Click 'Execute Query' to run the analysis. State filter is optional.")
        return

    results = st.session_state[f"{context.analysis_key}_results"]
    sites_df = results.get("sites_df", pd.DataFrame())
    facilities_df = results.get("facilities_df", pd.DataFrame())
    state_label = results.get("state_display") or "All states"
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

        # Ensure popups/tooltips wrap long URLs instead of overflowing outside the card.
        try:
            popup_css = """
<style>
.leaflet-popup-content { min-width: 420px !important; max-width: 900px !important; width: auto !important; }
.leaflet-popup-content table { width: 100% !important; table-layout: auto; }
.leaflet-popup-content td, .leaflet-popup-content th {
  overflow-wrap: anywhere;
  white-space: normal !important;
}
.leaflet-popup-content a, .leaflet-tooltip a {
  overflow-wrap: anywhere;
  white-space: normal !important;
}
</style>
"""
            map_obj.get_root().header.add_child(folium.Element(popup_css))
        except Exception:
            pass

        if sites_gdf is not None and not sites_gdf.empty:
            sites_points = sites_gdf.copy()
            sites_points["geometry"] = sites_points.geometry.centroid
            site_fields = [c for c in ["locationId", "locationDescription", "location"] if c in sites_points.columns]
            sites_points.explore(
                m=map_obj,
                name='<span style="color:Red;">📍 SOCKG Locations</span>',
                color="Red",
                marker_kwds=dict(radius=6),
                marker_type="circle_marker",
                tooltip=site_fields if site_fields else True,
                tooltip_kwds=dict(
                    aliases=site_fields if site_fields else None,
                    localize=True,
                    labels=True,
                    sticky=False,
                    style=(
                        "background-color: white; border-radius: 3px; "
                        "box-shadow: 3px 3px 5px grey; padding: 10px; "
                        "font-family: sans-serif; font-size: 14px; max-width: 450px; "
                        "overflow-wrap: break-word;"
                    ),
                ),
                popup=site_fields if site_fields else True,
                popup_kwds=dict(
                    aliases=site_fields if site_fields else None,
                    localize=True,
                    labels=True,
                    style=(
                        "background-color: white; border-radius: 3px; "
                        "box-shadow: 3px 3px 5px grey; padding: 10px; "
                        "font-family: sans-serif; font-size: 14px; max-width: 450px; "
                        "overflow-wrap: break-word;"
                    ),
                ),
                show=True,
            )

        if facilities_gdf is not None and not facilities_gdf.empty:
            facilities_points = facilities_gdf.copy()
            facilities_points["geometry"] = facilities_points.geometry.centroid
            pfas_facilities = facilities_points[facilities_points["PFASusing"]]
            other_facilities = facilities_points[~facilities_points["PFASusing"]]

            if not other_facilities.empty:
                other_fields = [
                    c
                    for c in ["facilityName", "industrySector", "industrySubsector", "industries", "locations"]
                    if c in other_facilities.columns
                ]
                other_facilities.explore(
                    m=map_obj,
                    name='<span style="color:MidnightBlue;">🏭 Other Facilities</span>',
                    color="MidnightBlue",
                    marker_kwds=dict(radius=4),
                    marker_type="circle_marker",
                    tooltip=other_fields if other_fields else True,
                    tooltip_kwds=dict(
                        aliases=other_fields if other_fields else None,
                        localize=True,
                        labels=True,
                        sticky=False,
                        style=(
                            "background-color: white; border-radius: 3px; "
                            "box-shadow: 3px 3px 5px grey; padding: 10px; "
                            "font-family: sans-serif; font-size: 14px; max-width: 650px; "
                            "overflow-wrap: break-word;"
                        ),
                    ),
                    popup=other_fields if other_fields else True,
                    popup_kwds=dict(
                        aliases=other_fields if other_fields else None,
                        localize=True,
                        labels=True,
                        style=(
                            "background-color: white; border-radius: 3px; "
                            "box-shadow: 3px 3px 5px grey; padding: 10px; "
                            "font-family: sans-serif; font-size: 14px; max-width: 650px; "
                            "overflow-wrap: break-word;"
                        ),
                    ),
                    show=True,
                )

            if not pfas_facilities.empty:
                pfas_fields = [
                    c
                    for c in ["facilityName", "industrySector", "industrySubsector", "PFASusing", "industries", "locations"]
                    if c in pfas_facilities.columns
                ]
                pfas_facilities.explore(
                    m=map_obj,
                    name='<span style="color:DarkRed;">⚠️ PFAS-Related Facilities</span>',
                    color="DarkRed",
                    marker_kwds=dict(radius=5),
                    marker_type="circle_marker",
                    tooltip=pfas_fields if pfas_fields else True,
                    tooltip_kwds=dict(
                        aliases=pfas_fields if pfas_fields else None,
                        localize=True,
                        labels=True,
                        sticky=False,
                        style=(
                            "background-color: white; border-radius: 3px; "
                            "box-shadow: 3px 3px 5px grey; padding: 10px; "
                            "font-family: sans-serif; font-size: 14px; max-width: 650px; "
                            "overflow-wrap: break-word;"
                        ),
                    ),
                    popup=pfas_fields if pfas_fields else True,
                    popup_kwds=dict(
                        aliases=pfas_fields if pfas_fields else None,
                        localize=True,
                        labels=True,
                        style=(
                            "background-color: white; border-radius: 3px; "
                            "box-shadow: 3px 3px 5px grey; padding: 10px; "
                            "font-family: sans-serif; font-size: 14px; max-width: 650px; "
                            "overflow-wrap: break-word;"
                        ),
                    ),
                    show=True,
                )

        if region_boundary_df is not None and not region_boundary_df.empty:
            # `get_region_boundary` returns columns named countyWKT/countyName even for states.
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

        folium.LayerControl(collapsed=True).add_to(map_obj)
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
