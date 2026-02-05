"""
Data Loader - Centralized data loading and caching functions
Handles static CSV data and cached availability queries.
"""
from __future__ import annotations

import os
import streamlit as st
import pandas as pd

from core.sparql import ENDPOINT_URLS


# Project directory (parent of core/)
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# =============================================================================
# STATIC DATA LOADERS
# =============================================================================

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
def load_naics_dict() -> dict[str, str]:
    """Load NAICS 2022 code → title from data/naics_2022.csv."""
    csv_path = os.path.join(PROJECT_DIR, "data", "naics_2022.csv")
    df = pd.read_csv(csv_path, skiprows=1)
    code_col = "2022 NAICS Code"
    title_col = "2022 NAICS Title"
    df = df.dropna(subset=[code_col])
    df[code_col] = df[code_col].astype(str).str.strip()
    df[title_col] = df[title_col].astype(str).str.replace(r"T\s*$", "", regex=True).str.strip()
    out = dict(zip(df[code_col], df[title_col]))
    # CSV uses "31-33" for Manufacturing; expand so 311, 312... nest under 31, etc.
    if "31-33" in out:
        title = out.pop("31-33")
        for sector in ("31", "32", "33"):
            out[sector] = title
    return out


def omit_alaska_regions(
    states_df: pd.DataFrame,
    counties_df: pd.DataFrame,
    subdivisions_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Remove Alaska rows from region DataFrames using FIPS/state_code."""
    ALASKA_STATE_CODE = "02"
    
    if not states_df.empty and "fipsCode" in states_df.columns:
        states_df = states_df[states_df["fipsCode"] != int(ALASKA_STATE_CODE)].copy()

    if not counties_df.empty:
        if "state_code" in counties_df.columns:
            counties_df = counties_df[counties_df["state_code"] != ALASKA_STATE_CODE].copy()
        elif "fipsCode" in counties_df.columns:
            counties_df = counties_df[
                ~counties_df["fipsCode"].astype(str).str.zfill(5).str.startswith(ALASKA_STATE_CODE)
            ].copy()

    if not subdivisions_df.empty:
        if "state_code" in subdivisions_df.columns:
            subdivisions_df = subdivisions_df[subdivisions_df["state_code"] != ALASKA_STATE_CODE].copy()
        elif "fipsCode" in subdivisions_df.columns:
            subdivisions_df = subdivisions_df[
                ~subdivisions_df["fipsCode"].astype(str).str.zfill(10).str.startswith(ALASKA_STATE_CODE)
            ].copy()

    return states_df, counties_df, subdivisions_df


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


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

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
