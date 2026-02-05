"""
Core SPARQL Utilities
Unified module for all SPARQL connection and query operations.
This is the single source of truth for SPARQL utilities.
"""
from __future__ import annotations

from typing import Optional
import pandas as pd
import rdflib
import requests
from SPARQLWrapper import SPARQLWrapper2, JSON, POST, DIGEST


# =============================================================================
# SPARQL ENDPOINT URLS - Single source of truth
# =============================================================================

ENDPOINT_URLS = {
    'sawgraph': "https://frink.apps.renci.org/sawgraph/sparql",
    'spatial': "https://frink.apps.renci.org/spatialkg/sparql",
    'hydrology': "https://frink.apps.renci.org/hydrologykg/sparql",
    'fio': "https://frink.apps.renci.org/fiokg/sparql",
    'federation': "https://frink.apps.renci.org/federation/sparql"
}

# Alias for backward compatibility
ENDPOINTS = ENDPOINT_URLS


# =============================================================================
# SPARQL WRAPPER FUNCTIONS
# =============================================================================

def get_sparql_wrapper(endpoint_name: str) -> SPARQLWrapper2:
    """
    Create and configure a SPARQLWrapper instance for the specified endpoint.
    
    Args:
        endpoint_name: Key from ENDPOINT_URLS dict ('sawgraph', 'spatial', 'hydrology', 'fio', 'federation')
    
    Returns:
        Configured SPARQLWrapper2 instance
    
    Raises:
        ValueError: If endpoint_name is not recognized
    """
    if endpoint_name not in ENDPOINT_URLS:
        raise ValueError(f"Unknown endpoint: {endpoint_name}. Choose from {list(ENDPOINT_URLS.keys())}")
    
    sparql = SPARQLWrapper2(ENDPOINT_URLS[endpoint_name])
    sparql.setHTTPAuth(DIGEST)
    sparql.setMethod(POST)
    sparql.setReturnFormat(JSON)
    return sparql


# =============================================================================
# RESULT PARSING FUNCTIONS
# =============================================================================

def parse_sparql_results(results: dict) -> pd.DataFrame:
    """
    Convert SPARQL JSON results to pandas DataFrame.
    
    This is THE canonical function for parsing SPARQL results.
    All other modules should import this function from here.
    
    Args:
        results: SPARQL JSON response with 'head' and 'results' keys
    
    Returns:
        pandas DataFrame with one row per binding
    """
    if not results or 'results' not in results or 'head' not in results:
        return pd.DataFrame()
    
    variables = results['head']['vars']
    bindings = results['results']['bindings']
    
    if not bindings:
        return pd.DataFrame(columns=variables)
    
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


def convertToDataframe(_results) -> pd.DataFrame:
    """
    Convert SPARQLWrapper2 results to pandas DataFrame.
    
    This function handles the SPARQLWrapper2 result format (with .bindings attribute).
    For JSON results from requests, use parse_sparql_results() instead.
    
    Args:
        _results: SPARQLWrapper2 query results object
    
    Returns:
        pandas DataFrame
    """
    d = []
    for x in _results.bindings:
        row = {}
        for k in x:
            v = x[k]
            vv = rdflib.term.Literal(v.value, datatype=v.datatype).toPython()
            row[k] = vv
        d.append(row)
    df = pd.DataFrame(d)
    return df


# =============================================================================
# QUERY EXECUTION FUNCTIONS
# =============================================================================

def execute_sparql_query(
    endpoint: str,
    query: str,
    method: str = 'POST',
    timeout: int = 180
) -> Optional[dict]:
    """
    Execute a SPARQL query and return JSON results.
    
    This is THE canonical function for executing SPARQL queries via HTTP.
    
    Args:
        endpoint: Full URL of the SPARQL endpoint, or key from ENDPOINT_URLS
        query: SPARQL query string
        method: HTTP method ('POST' or 'GET')
        timeout: Request timeout in seconds
    
    Returns:
        JSON response dict, or None if query failed
    """
    # Allow passing endpoint name instead of full URL
    if endpoint in ENDPOINT_URLS:
        endpoint = ENDPOINT_URLS[endpoint]
    
    headers = {
        'Accept': 'application/sparql-results+json',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    try:
        if method.upper() == 'POST':
            response = requests.post(endpoint, data={'query': query}, headers=headers, timeout=timeout)
        else:
            response = requests.get(endpoint, params={'query': query}, headers=headers, timeout=timeout)
        
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"SPARQL query error: {e}")
        return None


def test_connection(endpoint_name: str = 'sawgraph') -> tuple[bool, str, Optional[pd.DataFrame]]:
    """
    Test connection to a SPARQL endpoint.
    
    Args:
        endpoint_name: Endpoint to test
    
    Returns:
        tuple: (success: bool, message: str, data: DataFrame or None)
    """
    try:
        test_query = '''
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT DISTINCT ?substance ?substanceLabel (COUNT(?obs) as ?count) WHERE {
    ?obs rdf:type coso:ContaminantObservation;
         coso:ofSubstance ?substance;
         coso:hasResult ?result.
    ?substance rdfs:label ?substanceLabel.
    ?result coso:measurementValue ?value.
    FILTER(?value > 0)
} GROUP BY ?substance ?substanceLabel
ORDER BY DESC(?count)
LIMIT 10
'''
        sparql = get_sparql_wrapper(endpoint_name)
        sparql.setQuery(test_query)
        result = sparql.query()
        df = convertToDataframe(result)
        return True, f"Connected to {endpoint_name} successfully!", df
    except Exception as e:
        return False, f"Connection failed: {str(e)}", None
