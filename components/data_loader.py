"""
Data Loader - Centralized data loading and caching functions
Extracted from app.py to keep the main entry point simple.
"""
from __future__ import annotations

import os
import streamlit as st
import pandas as pd

from utils.region_filters import (
    get_available_states as fetch_available_states,
    get_available_counties as fetch_available_counties,
    get_available_subdivisions as fetch_available_subdivisions,
    omit_alaska_regions,
)
from utils.nearby_queries import get_pfas_industries
from utils.sockg_queries import (
    get_sockg_state_codes as fetch_sockg_state_codes,
    get_sockg_locations,
    get_sockg_facilities,
)

# SPARQL Endpoints
ENDPOINT_URLS = {
    'sawgraph': "https://frink.apps.renci.org/sawgraph/sparql",
    'spatial': "https://frink.apps.renci.org/spatialkg/sparql",
    'hydrology': "https://frink.apps.renci.org/hydrologykg/sparql",
    'fio': "https://frink.apps.renci.org/fiokg/sparql",
    'federation': "https://frink.apps.renci.org/federation/sparql"
}

# Project directory (parent of components/)
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def parse_sparql_results(results: dict) -> pd.DataFrame:
    """Convert SPARQL JSON results to DataFrame"""
    variables = results['head']['vars']
    bindings = results['results']['bindings']

    data = []
    for binding in bindings:
        row = {}
        for var in variables:
            if var in binding:
                row[var] = binding[var]['value']
            else:
                row[var] = None
        data.append(row)

    return pd.DataFrame(data)


def build_substance_options(substances_df: pd.DataFrame) -> tuple[list, dict]:
    """Build substance options from w3id query results."""
    if substances_df.empty:
        return ["-- All Substances --"], {}

    display_map = {}
    for _, row in substances_df.iterrows():
        display_name = row["display_name"]
        uri = row["substance"]
        if display_name not in display_map or uri.endswith("_A"):
            display_map[display_name] = uri

    options = ["-- All Substances --"] + sorted(display_map.keys())
    return options, display_map


def build_material_type_options(material_types_df: pd.DataFrame) -> tuple[list, dict]:
    """Build material type options from w3id query results."""
    if material_types_df.empty:
        return ["-- All Material Types --"], {}

    display_map = {}
    for _, row in material_types_df.iterrows():
        display_name = row["display_name"]
        uri = row["matType"]
        display_map.setdefault(display_name, uri)

    options = ["-- All Material Types --"] + sorted(display_map.keys())
    return options, display_map


# -----------------------------------------------------------------------------
# Cached availability queries (for region selectors)
# -----------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def get_available_state_codes() -> set:
    """Get FIPS state codes that have PFAS observations."""
    df = fetch_available_states()
    if df.empty:
        return set()
    return set(df["fips_code"].astype(str).str.zfill(2).tolist())


@st.cache_data(ttl=3600)
def get_available_county_codes(state_code: str) -> set:
    """Get FIPS county codes with PFAS observations for a given state."""
    df = fetch_available_counties(state_code)
    if df.empty:
        return set()
    return set(df["fips_code"].astype(str).str.zfill(5).tolist())


@st.cache_data(ttl=3600)
def get_available_subdivision_codes(county_code: str) -> set:
    """Get FIPS subdivision codes with PFAS observations for a given county."""
    df = fetch_available_subdivisions(county_code)
    if df.empty:
        return set()
    return set(df["fips_code"].astype(str).str.zfill(10).tolist())


@st.cache_data(ttl=3600)
def get_pfas_industry_options() -> dict:
    """Fetch PFAS-related NAICS industries for the dropdown."""
    df = get_pfas_industries()
    if df.empty:
        return {}

    options = {}
    for _, row in df.iterrows():
        group_code = str(row.get("NAICS", "")).strip()
        industry_code = str(row.get("industryCodeId", "")).strip()
        sector = row.get("industrySector", "")
        group = row.get("industryGroup", "")
        industry_name = row.get("industryName", "")

        if group_code:
            group_label = f"{sector}: {group}" if sector or group else f"NAICS {group_code}"
            options.setdefault(group_code, group_label)

        if industry_code:
            industry_label = industry_name or f"NAICS {industry_code}"
            options.setdefault(industry_code, industry_label)
    return options


@st.cache_data(ttl=3600)
def get_sockg_state_code_set() -> set:
    """Get FIPS state codes that have SOCKG locations."""
    df = fetch_sockg_state_codes()
    if df.empty:
        return set()
    return set(df["fips_code"].astype(str).str.zfill(2).tolist())


@st.cache_data(ttl=3600)
def get_sockg_locations_cached(state_code: str | None) -> pd.DataFrame:
    """Get SOCKG locations (optionally filtered by state)."""
    return get_sockg_locations(state_code)


@st.cache_data(ttl=3600)
def get_sockg_facilities_cached(state_code: str | None) -> pd.DataFrame:
    """Get SOCKG facilities near SOCKG locations (optionally filtered by state)."""
    return get_sockg_facilities(state_code)


# -----------------------------------------------------------------------------
# Static data loaders
# -----------------------------------------------------------------------------

@st.cache_data
def load_fips_data() -> pd.DataFrame:
    """Load and parse the FIPS codes CSV"""
    csv_path = os.path.join(PROJECT_DIR, "data", "us_administrative_regions_fips.csv")
    df = pd.read_csv(csv_path)
    return df


@st.cache_data
def load_substances_data() -> pd.DataFrame:
    """Load and parse the PFAS substances CSV"""
    csv_path = os.path.join(PROJECT_DIR, "data", "pfas_substances.csv")
    df = pd.read_csv(csv_path)
    return df


@st.cache_data
def load_material_types_data() -> pd.DataFrame:
    """Load and parse the sample material types CSV"""
    csv_path = os.path.join(PROJECT_DIR, "data", "sample_material_types.csv")
    df = pd.read_csv(csv_path)
    return df


@st.cache_data
def parse_regions(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Parse FIPS data into hierarchical structure:
    - States (2-digit codes)
    - Counties (5-digit codes, typically 1000-99999)
    - Subdivisions (10+ digit codes)
    """
    # Get states (2-digit FIPS codes)
    # State codes are 1-56, so we filter for codes less than 100
    states = df[df['fipsCode'] < 100].copy()
    # Remove "Geometry of " prefix if present
    states['state_name'] = states['label'].str.replace('Geometry of ', '', regex=False)
    # Remove duplicates - keep first occurrence of each state
    states = states.drop_duplicates(subset=['fipsCode'], keep='first')
    states = states.sort_values('state_name')

    # Get counties (5-digit FIPS codes)
    # Counties are typically in the range 1000-99999 (e.g., 01001, 23019)
    counties = df[(df['fipsCode'] >= 100) & (df['fipsCode'] < 100000)].copy()

    if not counties.empty:
        # Clean county names - remove "Geometry of " prefix
        counties['county_name'] = counties['label'].str.replace('Geometry of ', '', regex=False)
        # Extract state name (everything after the last comma)
        counties['state_name_county'] = counties['label'].str.split(', ').str[-1]
        # Get state code (first 2 digits of FIPS)
        # IMPORTANT: Must zfill(5) BEFORE slicing to handle leading zeros (e.g., 1001 -> 01001 -> 01)
        counties['state_code'] = counties['fipsCode'].astype(str).str.zfill(5).str[:2]
        # Get county code (5-digit FIPS)
        counties['county_code'] = counties['fipsCode'].astype(str).str.zfill(5)
        # Remove duplicate counties (keep first occurrence based on county_code)
        counties = counties.drop_duplicates(subset=['county_code'], keep='first')

    # Get subdivisions (codes longer than county level)
    # Subdivisions are longer (usually 10+ digits)
    subdivisions = df[df['fipsCode'] >= 100000].copy()

    # Parse county information from subdivision labels
    # Pattern: "Geometry of [Subdivision], [County], [State]"
    if not subdivisions.empty:
        # Extract subdivision, county, and state from label
        subdivisions['subdivision_name'] = subdivisions['label'].str.replace('Geometry of ', '', regex=False).str.split(', ').str[0]
        subdivisions['county_name'] = subdivisions['label'].str.split(', ').str[-2]
        subdivisions['state_name_sub'] = subdivisions['label'].str.split(', ').str[-1]

        # Get state code (first 2 digits of FIPS)
        # IMPORTANT: Must zfill(10) BEFORE slicing to handle leading zeros
        subdivisions['state_code'] = subdivisions['fipsCode'].astype(str).str.zfill(10).str[:2]
        # Get county code (first 5 digits of FIPS)
        subdivisions['county_code'] = subdivisions['fipsCode'].astype(str).str.zfill(10).str[:5]

    states, counties, subdivisions = omit_alaska_regions(states, counties, subdivisions)
    return states, counties, subdivisions


def load_all_data() -> dict:
    """
    Load all required static data and return as a dictionary.
    This is the main entry point for data loading.
    """
    fips_df = load_fips_data()
    states_df, counties_df, subdivisions_df = parse_regions(fips_df)
    substances_df = load_substances_data()
    material_types_df = load_material_types_data()

    return {
        "fips_df": fips_df,
        "states_df": states_df,
        "counties_df": counties_df,
        "subdivisions_df": subdivisions_df,
        "substances_df": substances_df,
        "material_types_df": material_types_df,
    }
