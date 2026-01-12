"""
Region Filtering Utilities
SPARQL queries to filter states, counties, and subdivisions based on available sample points
"""
import pandas as pd
import requests
from typing import List, Tuple, Optional

# SPARQL Endpoint
FEDERATION_ENDPOINT = "https://frink.apps.renci.org/federation/sparql"
ALASKA_STATE_CODE = "02"


def omit_alaska_regions(
    states_df: pd.DataFrame,
    counties_df: pd.DataFrame,
    subdivisions_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Remove Alaska rows from region DataFrames using FIPS/state_code."""
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


def parse_sparql_results(results: dict) -> pd.DataFrame:
    """Convert SPARQL JSON results to DataFrame"""
    if not results or 'results' not in results:
        return pd.DataFrame()
    
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


def execute_sparql_query(query: str, timeout: int = 60) -> Optional[dict]:
    """Execute a SPARQL query and return JSON results"""
    headers = {
        'Accept': 'application/sparql-results+json',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    try:
        response = requests.post(
            FEDERATION_ENDPOINT,
            data={'query': query},
            headers=headers,
            timeout=timeout
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"SPARQL query error: {e}")
        return None


def get_available_states() -> pd.DataFrame:
    """
    Get all states that have sample points with observations.
    Filters to URIs starting with http://stko-kwg.geog.ucsb.edu
    Excludes Alaska (FIPS code 02).

    Returns:
        DataFrame with columns: ar1 (state URI), fips_code (2-digit state code)
    """
    query = """
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?ar1 WHERE {
    ?sp rdf:type coso:SamplePoint ;
        kwg-ont:sfWithin|kwg-ont:sfTouches ?ar3 .
    ?ar3 rdf:type kwg-ont:AdministrativeRegion_3 ;
         kwg-ont:administrativePartOf ?ar2 .
    ?ar2 rdf:type kwg-ont:AdministrativeRegion_2 ;
         kwg-ont:administrativePartOf ?ar1 .
    ?ar1 rdf:type kwg-ont:AdministrativeRegion_1 .
    ?observation rdf:type coso:ContaminantObservation ;
                coso:observedAtSamplePoint ?sp .
FILTER(STRSTARTS(STR(?ar1), "http://stko-kwg.geog.ucsb.edu/lod/resource/administrativeRegion.USA.")).
}
"""

    results = execute_sparql_query(query, timeout=120)
    if not results:
        print("get_available_states: No results from SPARQL query")
        return pd.DataFrame(columns=['ar1', 'fips_code'])

    df = parse_sparql_results(results)
    if df.empty:
        print("get_available_states: Empty dataframe after parsing")
        return df

    # Extract FIPS code from URI (e.g., ...administrativeRegion.USA.04 -> 04)
    df['fips_code'] = df['ar1'].str.extract(r'administrativeRegion\.USA\.(\d+)')
    df['fips_code'] = df['fips_code'].astype(str).str.zfill(2)

    # Exclude Alaska (FIPS code 02)
    df = df[df['fips_code'] != ALASKA_STATE_CODE].reset_index(drop=True)

    print(f"get_available_states: Found {len(df)} states with data")
    return df[['ar1', 'fips_code']]


def get_available_counties(state_code: str) -> pd.DataFrame:
    """
    Get all counties in a given state that have sample points with observations.
    Filters to URIs starting with http://stko-kwg.geog.ucsb.edu

    Args:
        state_code: 2-digit FIPS state code (e.g., "04" for Arizona, "23" for Maine)

    Returns:
        DataFrame with columns: ar2 (county URI), fips_code (5-digit county code)
    """
    # Use state code as-is (2-digit format with leading zero if needed)
    state_code_str = str(state_code).zfill(2)
    if state_code_str == ALASKA_STATE_CODE:
        print("get_available_counties: Skipping Alaska (FIPS code 02)")
        return pd.DataFrame(columns=['ar2', 'fips_code'])
    state_uri = f"<http://stko-kwg.geog.ucsb.edu/lod/resource/administrativeRegion.USA.{state_code_str}>"

    print(f"get_available_counties: Querying for state_code={state_code}, URI={state_uri}")

    query = f"""
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?ar2 WHERE {{
    ?sp rdf:type coso:SamplePoint ;
        kwg-ont:sfWithin|kwg-ont:sfTouches ?ar3 .
    ?ar3 rdf:type kwg-ont:AdministrativeRegion_3 ;
         kwg-ont:administrativePartOf ?ar2 .
    ?ar2 rdf:type kwg-ont:AdministrativeRegion_2 ;
         kwg-ont:administrativePartOf {state_uri} .
    ?observation rdf:type coso:ContaminantObservation ;
                coso:observedAtSamplePoint ?sp .
FILTER(STRSTARTS(STR(?ar2), "http://stko-kwg.geog.ucsb.edu")).
}}
"""

    results = execute_sparql_query(query, timeout=120)
    if not results:
        print(f"get_available_counties: No results from SPARQL for state {state_code}")
        return pd.DataFrame(columns=['ar2', 'fips_code'])

    df = parse_sparql_results(results)
    if df.empty:
        print(f"get_available_counties: Empty dataframe for state {state_code}")
        return df

    # Extract FIPS code from URI (e.g., ...administrativeRegion.USA.04013 -> 04013)
    df['fips_code'] = df['ar2'].str.extract(r'administrativeRegion\.USA\.(\d+)')
    df['fips_code'] = df['fips_code'].astype(str).str.zfill(5)

    print(f"get_available_counties: Found {len(df)} counties for state {state_code}")
    return df[['ar2', 'fips_code']]


def get_available_subdivisions(county_code: str) -> pd.DataFrame:
    """
    Get all county subdivisions in a given county that have sample points with observations.
    Note: Subdivisions use https://datacommons.org/browser/geoId/... format

    Args:
        county_code: 5-digit FIPS county code (e.g., "04013" for Maricopa County, AZ)

    Returns:
        DataFrame with columns: ar3 (subdivision URI), fips_code (10-digit subdivision code)
    """
    county_code_str = str(county_code).zfill(5)
    if county_code_str.startswith(ALASKA_STATE_CODE):
        print("get_available_subdivisions: Skipping Alaska counties (FIPS code 02)")
        return pd.DataFrame(columns=['ar3', 'fips_code'])
    county_uri = f"<http://stko-kwg.geog.ucsb.edu/lod/resource/administrativeRegion.USA.{county_code_str}>"

    print(f"get_available_subdivisions: Querying for county_code={county_code}, URI={county_uri}")

    query = f"""
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?ar3 WHERE {{
    ?sp rdf:type coso:SamplePoint ;
        kwg-ont:sfWithin|kwg-ont:sfTouches ?ar3 .
    ?ar3 rdf:type kwg-ont:AdministrativeRegion_3 ;
         kwg-ont:administrativePartOf {county_uri} .
    ?observation rdf:type coso:ContaminantObservation ;
                coso:observedAtSamplePoint ?sp .
    FILTER(STRSTARTS(STR(?ar3), "https://datacommons.org/browser/geoId/")).
}}
"""

    results = execute_sparql_query(query, timeout=120)
    if not results:
        print(f"get_available_subdivisions: No results from SPARQL for county {county_code}")
        return pd.DataFrame(columns=['ar3', 'fips_code'])

    df = parse_sparql_results(results)
    if df.empty:
        print(f"get_available_subdivisions: Empty dataframe for county {county_code}")
        return df

    # Extract FIPS code from DataCommons URI (e.g., ...geoId/2301104475 -> 2301104475)
    df['fips_code'] = df['ar3'].str.extract(r'geoId/(\d+)')
    df['fips_code'] = df['fips_code'].astype(str)

    print(f"get_available_subdivisions: Found {len(df)} subdivisions for county {county_code}")
    return df[['ar3', 'fips_code']]
