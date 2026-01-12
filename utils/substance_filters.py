"""
Substance Filtering Utilities
SPARQL queries to filter substances based on available observations in a region
"""
import pandas as pd
import requests
from typing import List, Optional
from functools import lru_cache

# SPARQL Endpoint
FEDERATION_ENDPOINT = "https://frink.apps.renci.org/federation/sparql"
COMPTox_DSS_TOX_ENDPOINT = (
    "https://comptox.epa.gov/dashboard-api/ccdapp2/chemical-detail/search/by-dsstoxsid"
)
DSSTOX_URI_PREFIX = "http://w3id.org/DSSTox/v1/DTXSID"


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


def _extract_dsstox_id(substance_uri: str) -> Optional[str]:
    if substance_uri.startswith(DSSTOX_URI_PREFIX):
        return _fallback_substance_name(substance_uri)
    return None


@lru_cache(maxsize=2048)
def _fetch_comptox_label(dsstox_id: str) -> Optional[str]:
    try:
        response = requests.get(
            COMPTox_DSS_TOX_ENDPOINT,
            params={"id": dsstox_id},
            timeout=10
        )
        if response.status_code != 200:
            return None
        data = response.json()
    except Exception:
        return None

    label = data.get("label") or data.get("preferredName")
    if not isinstance(label, str) or not label.strip():
        return None
    return label.strip()


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
    df["dsstox_id"] = df["substance"].apply(_extract_dsstox_id)

    def _resolve_display_name(row: pd.Series) -> str:
        if pd.notna(row["display_name"]):
            return row["display_name"]
        dsstox_id = row.get("dsstox_id")
        if isinstance(dsstox_id, str) and dsstox_id:
            comptox_label = _fetch_comptox_label(dsstox_id)
            if comptox_label:
                return comptox_label
        return _fallback_substance_name(row["substance"])

    df["display_name"] = df.apply(_resolve_display_name, axis=1)
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
