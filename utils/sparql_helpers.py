"""
Shared SPARQL connection and query utilities
"""
import pandas as pd
import rdflib
from SPARQLWrapper import SPARQLWrapper2, JSON, POST, DIGEST


# SPARQL Endpoints
ENDPOINTS = {
    'sawgraph': "https://frink.apps.renci.org/sawgraph/sparql",
    'spatial': "https://frink.apps.renci.org/spatialkg/sparql",
    'hydrology': "https://frink.apps.renci.org/hydrologykg/sparql",
    'fio': "https://frink.apps.renci.org/fiokg/sparql"
}


def get_sparql_wrapper(endpoint_name):
    """
    Create and configure a SPARQLWrapper instance for the specified endpoint
    
    Args:
        endpoint_name: Key from ENDPOINTS dict ('sawgraph', 'spatial', 'hydrology', 'fio')
    
    Returns:
        Configured SPARQLWrapper2 instance
    """
    if endpoint_name not in ENDPOINTS:
        raise ValueError(f"Unknown endpoint: {endpoint_name}. Choose from {list(ENDPOINTS.keys())}")
    
    sparql = SPARQLWrapper2(ENDPOINTS[endpoint_name])
    sparql.setHTTPAuth(DIGEST)
    sparql.setMethod(POST)
    sparql.setReturnFormat(JSON)
    return sparql


def convertToDataframe(_results):
    """
    Convert SPARQL results to pandas DataFrame
    
    Args:
        _results: SPARQL query results
    
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


def convertS2ListToQueryString(s2list):
    """
    Convert S2 cell list to query string format for SPARQL VALUES clause
    
    Args:
        s2list: List of S2 cell URIs
    
    Returns:
        String formatted for SPARQL VALUES clause
    """
    s2list_short = [s2cell.replace("http://stko-kwg.geog.ucsb.edu/lod/resource/","kwgr:") for s2cell in s2list]
    s2_values_string = " ".join(s2list_short)
    return s2_values_string


def test_connection(endpoint_name='sawgraph'):
    """
    Test connection to a SPARQL endpoint
    
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
        return True, f"✅ Connected to {endpoint_name} successfully!", df
    except Exception as e:
        return False, f"❌ Connection failed: {str(e)}", None

