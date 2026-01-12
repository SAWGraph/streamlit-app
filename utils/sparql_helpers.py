"""
Shared SPARQL connection and query utilities
"""
import pandas as pd
import rdflib
import requests
from SPARQLWrapper import SPARQLWrapper2, JSON, POST, DIGEST


# SPARQL Endpoints
ENDPOINTS = {
    'sawgraph': "https://frink.apps.renci.org/sawgraph/sparql",
    'spatial': "https://frink.apps.renci.org/spatialkg/sparql",
    'hydrology': "https://frink.apps.renci.org/hydrologykg/sparql",
    'fio': "https://frink.apps.renci.org/fiokg/sparql",
    'federation': "https://frink.apps.renci.org/federation/sparql"
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


def parse_sparql_results(results):
    """
    Convert SPARQL JSON results to DataFrame

    Args:
        results: SPARQL JSON response with 'head' and 'results' keys

    Returns:
        pandas DataFrame
    """
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


def get_region_boundary(region_code):
    """
    Query the boundary geometry for a given administrative region.

    Args:
        region_code: FIPS code as string (2 digits=state, 5=county, >5=subdivision)

    Returns:
        DataFrame with columns: county (region URI), countyWKT (geometry), countyName (label)
        Returns None if query fails or no results
    """
    # Determine query pattern based on region code length
    if len(str(region_code)) > 5:
        # Subdivision - use DataCommons URI
        region_uri_pattern = f"VALUES ?county {{<https://datacommons.org/browser/geoId/{region_code}>}}"
    else:
        # State or County - use KWG URI
        region_uri_pattern = f"VALUES ?county {{kwgr:administrativeRegion.USA.{region_code}}}"

    query = f"""
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT * WHERE {{
    ?county geo:hasGeometry/geo:asWKT ?countyWKT ;
            rdfs:label ?countyName.
    {region_uri_pattern}
}}
"""

    sparql_endpoint = ENDPOINTS["federation"]
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    try:
        response = requests.post(
            sparql_endpoint,
            data={"query": query},
            headers=headers,
            timeout=10
        )

        if response.status_code == 200:
            results = response.json()
            df = parse_sparql_results(results)
            if not df.empty:
                return df
            else:
                return None
        else:
            return None

    except Exception as e:
        print(f"Error querying boundary: {str(e)}")
        return None

