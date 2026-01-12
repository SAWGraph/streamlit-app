"""
Substance Filtering Utilities
SPARQL queries to filter substances based on available observations in a region
"""
import pandas as pd
import requests
from typing import List, Optional

# SPARQL Endpoint
FEDERATION_ENDPOINT = "https://frink.apps.renci.org/federation/sparql"


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


def _fallback_substance_name(substance_uri: str) -> str:
    return substance_uri.rstrip("/").rsplit("/", 1)[-1]


def get_available_substances_with_labels(
    region_code: str,
    is_subdivision: bool = False,
) -> pd.DataFrame:
    """
    Get all substances that have observations in the given region.
    Only includes substances with URIs starting with http://w3id.org/.
    Returns a DataFrame with substance URI and display name.

    Args:
        region_code: FIPS code for the region (county or subdivision)
        is_subdivision: True if region_code is a subdivision (uses DataCommons URI format)

    Returns:
        DataFrame with columns: substance, display_name
    """
    if is_subdivision:
        # Subdivision uses DataCommons URI format with sfWithin/sfTouches
        query = f"""
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?substance ?label WHERE {{
    ?sp rdf:type coso:SamplePoint ;
        kwg-ont:sfWithin|kwg-ont:sfTouches <https://datacommons.org/browser/geoId/{region_code}> .
    ?observation rdf:type coso:ContaminantObservation ;
                coso:observedAtSamplePoint ?sp ;
                coso:ofSubstance ?substance .
    OPTIONAL {{ ?substance rdfs:label ?label . }}
    FILTER(STRSTARTS(STR(?substance), "http://w3id.org/")).
}}
"""
    else:
        # County uses administrativePartOf with sfWithin/sfTouches
        query = f"""
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?substance ?label WHERE {{
    ?sp rdf:type coso:SamplePoint ;
        kwg-ont:sfWithin|kwg-ont:sfTouches ?ar3 .
    ?ar3 rdf:type kwg-ont:AdministrativeRegion_3 ;
         kwg-ont:administrativePartOf <http://stko-kwg.geog.ucsb.edu/lod/resource/administrativeRegion.USA.{region_code}> .
    ?observation rdf:type coso:ContaminantObservation ;
                coso:observedAtSamplePoint ?sp ;
                coso:ofSubstance ?substance .
    OPTIONAL {{ ?substance rdfs:label ?label . }}
    FILTER(STRSTARTS(STR(?substance), "http://w3id.org/")).
}}
"""

    results = execute_sparql_query(query)
    if not results:
        return pd.DataFrame(columns=["substance", "display_name"])

    df = parse_sparql_results(results)
    if df.empty:
        return pd.DataFrame(columns=["substance", "display_name"])

    df = df.dropna(subset=["substance"]).copy()
    df["has_label"] = df["label"].notna()
    df = df.sort_values("has_label", ascending=False)
    df = df.drop_duplicates(subset=["substance"], keep="first")
    df["display_name"] = df["label"]
    df["display_name"] = df["display_name"].where(
        df["display_name"].notna(),
        df["substance"].apply(_fallback_substance_name),
    )
    return df[["substance", "display_name"]].reset_index(drop=True)


def get_available_substances(region_code: str, is_subdivision: bool = False) -> List[str]:
    """
    Get all substances that have observations in the given region.
    Only includes substances with URIs starting with http://w3id.org/

    Args:
        region_code: FIPS code for the region (county or subdivision)
        is_subdivision: True if region_code is a subdivision (uses DataCommons URI format)

    Returns:
        List of substance URIs
    """
    df = get_available_substances_with_labels(region_code, is_subdivision)
    if df.empty:
        return []
    return df["substance"].tolist()
