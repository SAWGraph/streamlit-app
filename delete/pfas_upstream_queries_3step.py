"""
PFAS Upstream Tracing Query Functions
======================================

This module contains the 3-step pipeline for tracing PFAS contamination upstream
to identify potential industrial sources.

OVERVIEW:
---------
The upstream tracing analysis works in 3 sequential steps, each querying a different
SPARQL endpoint:

    Step 1 (Federation Endpoint): Find contaminated samples in a region
        ↓ (returns S2 grid cells with contamination)
    
    Step 2 (Hydrology Endpoint): Trace upstream through water flow networks
        ↓ (returns S2 grid cells hydrologically upstream)
    
    Step 3 (FIO Endpoint): Find facilities in upstream areas
        ↓ (returns industrial facilities that may be contamination sources)
"""
from __future__ import annotations

import time
from typing import Any, Dict
import pandas as pd
import requests

from core.sparql import ENDPOINT_URLS, parse_sparql_results, convert_s2_list_to_query_string


# =============================================================================
# STEP 1: FIND CONTAMINATED SAMPLES
# =============================================================================

def execute_sparql_query(
    substance_uri,
    material_uri,
    min_conc,
    max_conc,
    region_code,
    include_nondetects: bool = False,
):
    """
    STEP 1: Find contaminated water samples matching all user-specified criteria.
    
    ENDPOINT: Federation (aggregates data from multiple knowledge graphs)
    
    Returns:
        tuple: (df_results, error, debug_info)
    """
    print(f"--- Running Step 1 (on 'federation' endpoint) ---")
    print(f"Finding samples in region: {region_code}")
    
    substance_filter = f"VALUES ?substance {{<{substance_uri}>}}" if substance_uri else "# No substance filter"
    material_filter = f"VALUES ?matType {{<{material_uri}>}}" if material_uri else "# No material type filter"
    
    if len(region_code) > 5:
        region_pattern = f"VALUES ?regionURI {{<https://datacommons.org/browser/geoId/{region_code}>}}"
    else:
        region_pattern = f"""
    ?regionURI rdf:type kwg-ont:AdministrativeRegion_3 ;
               kwg-ont:administrativePartOf+ kwgr:administrativeRegion.USA.{region_code} ."""

    concentration_filter = (
        f"FILTER( ?isNonDetect || (BOUND(?numericValue) && ?numericValue >= {min_conc} && ?numericValue <= {max_conc}) )"
        if include_nondetects
        else "\n".join(
            [
                "FILTER(!?isNonDetect)",
                "FILTER(BOUND(?numericValue))",
                "FILTER(?numericValue > 0)",
                f"FILTER (?numericValue >= {min_conc})",
                f"FILTER (?numericValue <= {max_conc})",
            ]
        )
    )

    query = f"""
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>

SELECT ?observation ?sp ?s2cell ?spWKT ?substance ?sample ?matType ?result_value ?unit ?regionURI
WHERE {{
    ?observation rdf:type coso:ContaminantObservation;
        coso:observedAtSamplePoint ?sp;
        coso:ofSubstance ?substance;
        coso:analyzedSample ?sample;
        coso:hasResult ?result.
    
    ?sample coso:sampleOfMaterialType ?matType.
    
    ?result coso:measurementValue ?result_value;
            coso:measurementUnit ?unit.

    OPTIONAL {{ ?result qudt:quantityValue/qudt:numericValue ?numericResult }}
    OPTIONAL {{ ?result qudt:enumeratedValue ?enumDetected }}
    BIND(
      (BOUND(?enumDetected) || LCASE(STR(?result_value)) = "non-detect" || STR(?result_value) = STR(coso:non-detect))
      as ?isNonDetect
    )
    BIND(
      IF(
        ?isNonDetect,
        0,
        COALESCE(xsd:decimal(?numericResult), xsd:decimal(?result_value))
      ) as ?numericValue
    )
    
    VALUES ?unit {{<http://qudt.org/vocab/unit/NanoGM-PER-L>}}
    
    {substance_filter}
    {material_filter}
    {concentration_filter}
    
    OPTIONAL {{ ?sp geo:hasGeometry/geo:asWKT ?spWKT . }}
    
    ?sp spatial:connectedTo ?regionURI .
    {region_pattern}

    ?sp spatial:connectedTo ?s2cell .
}}
"""
    
    sparql_endpoint = ENDPOINT_URLS["federation"]
    headers = {"Accept": "application/sparql-results+json"}
    debug_info: Dict[str, Any] = {
        "endpoint": sparql_endpoint,
        "query": query,
    }

    try:
        response = requests.get(sparql_endpoint, params={"query": query}, headers=headers, timeout=None)
        debug_info["response_status"] = response.status_code
        
        if response.status_code == 200:
            results = response.json()
            df_results = parse_sparql_results(results)
            
            if df_results.empty:
                print("   > Step 1 complete: No results found.")
            else:
                print(f"   > Step 1 complete: Found {len(df_results)} contaminated samples.")
            debug_info["row_count"] = len(df_results)
            
            return df_results, None, debug_info
        else:
            return None, f"Error {response.status_code}: {response.text}", debug_info
            
    except requests.exceptions.RequestException as e:
        debug_info["exception"] = str(e)
        return None, f"Network error: {str(e)}", debug_info
    except Exception as e:
        debug_info["exception"] = str(e)
        return None, f"Error: {str(e)}", debug_info


# =============================================================================
# STEP 2: TRACE UPSTREAM THROUGH HYDROLOGICAL NETWORKS
# =============================================================================

def execute_hydrology_query(contaminated_samples_df, max_start_s2_cells: int = 100, max_flowlines: int = 1000):
    """
    STEP 2: Trace upstream to find all S2 cells that flow toward contaminated areas.
    
    ENDPOINT: Hydrology (water flow network knowledge graph)
    
    Returns:
        tuple: (upstream_s2_df, flowlines_df, error, debug_info)
    """
    print(f"\n--- Running Step 2 (on 'hydrology') ---")
    sparql_endpoint = ENDPOINT_URLS["hydrology"]

    s2_list = contaminated_samples_df['s2cell'].unique().tolist()

    if not s2_list:
        print("   > No S2 cells to trace upstream.")
        return pd.DataFrame(), pd.DataFrame(), None, {
            "endpoint": sparql_endpoint,
            "error": "Skipped: no S2 cells to trace upstream",
        }
    
    print(f"   > First few S2 cells from Step 1: {s2_list[:3] if len(s2_list) >= 3 else s2_list}")
    
    if len(s2_list) > max_start_s2_cells:
        print(f"   > Too many S2 cells ({len(s2_list)}), limiting to top {max_start_s2_cells}")
        s2_counts = contaminated_samples_df['s2cell'].value_counts()
        s2_list = s2_counts.head(max_start_s2_cells).index.tolist()
        print(f"   > Top S2 cell has {s2_counts.iloc[0]} contaminated samples")
    
    s2_values_string = convert_s2_list_to_query_string(s2_list)
    print(f"   > Tracing upstream from {len(s2_list)} S2 cells...")
    
    cells_query = f"""PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT DISTINCT ?s2cell
WHERE {{
    ?downstream_flowline rdf:type hyf:HY_FlowPath ;
                        spatial:connectedTo ?s2cellds .
    
    ?upstream_flowline hyf:downstreamFlowPathTC ?downstream_flowline .
    
    VALUES ?s2cellds {{ {s2_values_string} }}
    
    ?s2cell spatial:connectedTo ?upstream_flowline ;
            rdf:type kwg-ont:S2Cell_Level13 .
}}"""

    flowlines_query = f"""PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT DISTINCT ?upstream_flowlineWKT
WHERE {{
    ?downstream_flowline rdf:type hyf:HY_FlowPath ;
                        spatial:connectedTo ?s2cellds .

    ?upstream_flowline hyf:downstreamFlowPathTC ?downstream_flowline ;
                       geo:hasGeometry/geo:asWKT ?upstream_flowlineWKT .

    VALUES ?s2cellds {{ {s2_values_string} }}
}}
LIMIT {int(max_flowlines)}
"""

    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    debug_info: Dict[str, Any] = {
        "endpoint": sparql_endpoint,
        "queries": [
            {"label": "Step 2A: Upstream Cells", "query": cells_query},
            {"label": "Step 2B: Upstream Flowlines", "query": flowlines_query},
        ],
    }

    try:
        print(f"   > Sending query to hydrology endpoint...")
        response = requests.post(sparql_endpoint, data={"query": cells_query}, headers=headers, timeout=None)
        debug_info["queries"][0]["response_status"] = response.status_code
        
        if response.status_code == 200:
            results = response.json()
            df_results = parse_sparql_results(results)
            
            if df_results.empty:
                print("   > Step 2 complete: No upstream hydrological sources found.")
            else:
                print(f"   > Step 2 complete: Found {len(df_results)} upstream S2 cells.")
            df_results = df_results.drop_duplicates().reset_index(drop=True)
            debug_info["queries"][0]["row_count"] = len(df_results)

            flowlines_df = pd.DataFrame()
            try:
                flowlines_response = requests.post(
                    sparql_endpoint,
                    data={"query": flowlines_query},
                    headers=headers,
                    timeout=None
                )
                debug_info["queries"][1]["response_status"] = flowlines_response.status_code
                if flowlines_response.status_code == 200:
                    flowlines_results = flowlines_response.json()
                    flowlines_df = parse_sparql_results(flowlines_results)
                    if not flowlines_df.empty:
                        flowlines_df = flowlines_df.drop_duplicates().reset_index(drop=True)
                    debug_info["queries"][1]["row_count"] = len(flowlines_df)
                else:
                    debug_info["queries"][1]["error"] = (
                        f"Error {flowlines_response.status_code}: {flowlines_response.text}"
                    )
            except requests.exceptions.RequestException as e:
                print(f"   > Flowlines query error: {str(e)}")
                debug_info["queries"][1]["exception"] = str(e)

            return df_results, flowlines_df, None, debug_info
        else:
            return (
                pd.DataFrame(),
                pd.DataFrame(),
                f"Error {response.status_code}: {response.text}",
                debug_info,
            )
            
    except requests.exceptions.RequestException as e:
        debug_info["exception"] = str(e)
        return pd.DataFrame(), pd.DataFrame(), f"Network error: {str(e)}", debug_info
    except Exception as e:
        debug_info["exception"] = str(e)
        return pd.DataFrame(), pd.DataFrame(), f"Error: {str(e)}", debug_info


# =============================================================================
# STEP 3: FIND FACILITIES IN UPSTREAM AREAS
# =============================================================================

def execute_facility_query(upstream_s2_df):
    """
    STEP 3: Identify industrial facilities located in upstream S2 cells.
    
    ENDPOINT: FIO (Facility and Industry Ontology knowledge graph)
    
    Returns:
        tuple: (df_results, error, debug_info)
    """
    print(f"\n--- Running Step 3 (on 'fio') ---")
    sparql_endpoint = ENDPOINT_URLS["fio"]

    s2_list = upstream_s2_df['s2cell'].unique().tolist()

    if not s2_list:
        print("   > No upstream S2 cells to check for facilities.")
        return pd.DataFrame(), None, {
            "endpoint": sparql_endpoint,
            "error": "Skipped: no upstream S2 cells",
        }

    if len(s2_list) > 100:
        print(f"   > Too many S2 cells ({len(s2_list)}), limiting to 100")
        s2_list = s2_list[:100]
    
    s2_values_string = convert_s2_list_to_query_string(s2_list)
    print(f"   > Finding facilities in {len(s2_list)} upstream S2 cells...")
    
    query = f"""PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX fio: <http://w3id.org/fio/v1/fio#>

SELECT DISTINCT ?facility ?facWKT ?facilityName ?industryCode ?industryName
WHERE {{
    ?s2cell kwg-ont:sfContains ?facility .

    VALUES ?s2cell {{ {s2_values_string} }}

    ?facility fio:ofIndustry ?industryCode ;
            geo:hasGeometry/geo:asWKT ?facWKT;
            rdfs:label ?facilityName.

    ?industryCode rdfs:label ?industryName .
}}"""
    
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    debug_info: Dict[str, Any] = {
        "endpoint": sparql_endpoint,
        "query": query,
    }

    try:
        print(f"   > Sending query to facility endpoint...")
        response = requests.post(sparql_endpoint, data={"query": query}, headers=headers, timeout=None)
        debug_info["response_status"] = response.status_code
        
        if response.status_code == 200:
            results = response.json()
            df_results = parse_sparql_results(results)
            
            if df_results.empty:
                print("   > Step 3 complete: No facilities found in upstream areas.")
            else:
                print(f"   > Step 3 complete: Found {len(df_results)} facilities.")
            debug_info["row_count"] = len(df_results)
            
            return df_results, None, debug_info
        else:
            return None, f"Error {response.status_code}: {response.text}", debug_info
            
    except requests.exceptions.RequestException as e:
        debug_info["exception"] = str(e)
        return None, f"Network error: {str(e)}", debug_info
    except Exception as e:
        debug_info["exception"] = str(e)
        return None, f"Error: {str(e)}", debug_info


# =============================================================================
# COMBINED QUERY (SINGLE-PASS UPSTREAM TRACING) - NOT USED
# =============================================================================
# The code below is commented out because it is never called. The app uses the
# 3-step pipeline (execute_sparql_query -> execute_hydrology_query ->
# execute_facility_query) only. This was an alternative single-pass approach
# that fetches only contaminated samples (no upstream/facilities); it was never
# wired into analysis.py. Kept for reference in case a combined path is needed later.
# =============================================================================
#
# def execute_combined_query(substance_uri, material_uri, min_conc, max_conc, region_code, include_nondetects=False):
#     """
#     Run a single SPARQL query that retrieves contaminated samples.
#
#     Returns:
#         tuple: (DataFrame, error_message, debug_info)
#     """
#     print("\n--- Running combined upstream tracing query (federation endpoint) ---")
#     print(f"Finding samples and facilities in region: {region_code}")
#
#     def build_values_clause(var_name, uri_value):
#         if not uri_value:
#             return ""
#         return f"VALUES ?{var_name} {{<{uri_value}>}}"
#
#     substance_filter = build_values_clause("substance", substance_uri)
#     material_filter = build_values_clause("matType", material_uri)
#     min_conc = float(min_conc)
#     max_conc = float(max_conc)
#
#     sanitized_region = str(region_code).strip()
#
#     if not sanitized_region or sanitized_region == "" or sanitized_region.lower() == "none":
#         error_msg = "Invalid region code. Please select a state before executing the query."
#         return None, error_msg, {"error": error_msg}
#
#     if len(sanitized_region) > 5:
#         region_pattern = f"VALUES ?region {{<https://datacommons.org/browser/geoId/{sanitized_region}>}}"
#     else:
#         region_pattern = f"?region kwg-ont:administrativePartOf+ <http://stko-kwg.geog.ucsb.edu/lod/resource/administrativeRegion.USA.{sanitized_region}> ."
#
#     concentration_filter = (
#         f"FILTER( ?isNonDetect || (BOUND(?numericValue) && ?numericValue >= {min_conc} && ?numericValue <= {max_conc}) )"
#         if include_nondetects
#         else "\n".join(
#             [
#                 "FILTER(!?isNonDetect)",
#                 "FILTER(BOUND(?numericValue))",
#                 "FILTER(?numericValue > 0)",
#                 f"FILTER (?numericValue >= {min_conc})",
#                 f"FILTER (?numericValue <= {max_conc})",
#             ]
#         )
#     )
#
#     query = f"""
# PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
# PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
# PREFIX geo: <http://www.opengis.net/ont/geosparql#>
# PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
# PREFIX qudt: <http://qudt.org/schema/qudt/>
# PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
# PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
# PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
# PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
#
# SELECT DISTINCT ?sp ?spWKT ?substance ?result_value ?matType
# WHERE {{
#     ?observation rdf:type coso:ContaminantObservation ;
#                 coso:observedAtSamplePoint ?sp ;
#                 coso:ofSubstance ?substance ;
#                 coso:analyzedSample ?sample ;
#                 coso:hasResult ?result .
#
#     ?sample coso:sampleOfMaterialType ?matType .
#     ?result coso:measurementValue ?result_value ;
#             coso:measurementUnit ?unit .
#     OPTIONAL {{ ?result qudt:quantityValue/qudt:numericValue ?numericResult }}
#     OPTIONAL {{ ?result qudt:enumeratedValue ?enumDetected }}
#     BIND(
#       (BOUND(?enumDetected) || LCASE(STR(?result_value)) = "non-detect" || STR(?result_value) = STR(coso:non-detect))
#       as ?isNonDetect
#     )
#     BIND(
#       IF(
#         ?isNonDetect,
#         0,
#         COALESCE(xsd:decimal(?numericResult), xsd:decimal(?result_value))
#       ) as ?numericValue
#     )
#
#     VALUES ?unit {{<http://qudt.org/vocab/unit/NanoGM-PER-L>}}
#
#     ?sp spatial:connectedTo ?region .
#     {region_pattern}
#
#     {substance_filter}
#     {material_filter}
#     {concentration_filter}
#
#     OPTIONAL {{ ?sp geo:hasGeometry/geo:asWKT ?spWKT . }}
# }}
# LIMIT 1000
# """
#
#     sparql_endpoint = ENDPOINT_URLS["federation"]
#     headers = {
#         "Accept": "application/sparql-results+json",
#         "Content-Type": "application/x-www-form-urlencoded"
#     }
#
#     debug_info = {
#         "endpoint": sparql_endpoint,
#         "region_code": sanitized_region,
#         "query_length": len(query),
#     }
#
#     try:
#         start_time = time.time()
#         response = requests.post(
#             sparql_endpoint,
#             data={"query": query},
#             headers=headers,
#             timeout=None
#         )
#         elapsed = time.time() - start_time
#
#         debug_info["response_status"] = response.status_code
#         debug_info["response_time_sec"] = round(elapsed, 2)
#
#         if response.status_code == 200:
#             results = response.json()
#             df_results = parse_sparql_results(results)
#             debug_info["row_count"] = len(df_results)
#             return df_results, None, debug_info
#         else:
#             return None, f"Error {response.status_code}: {response.text}", debug_info
#
#     except requests.exceptions.RequestException as e:
#         debug_info["exception"] = str(e)
#         return None, f"Network error: {str(e)}", debug_info
#     except Exception as e:
#         debug_info["exception"] = str(e)
#         return None, f"Error: {str(e)}", debug_info
#
#
# def split_combined_results(combined_df):
#     """Split combined query results into logical tables for the UI. Not used: only called with execute_combined_query."""
#     if combined_df is None or combined_df.empty:
#         return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
#
#     samples_df = combined_df.copy()
#     upstream_df = pd.DataFrame()
#     facilities_df = pd.DataFrame()
#
#     return samples_df, upstream_df, facilities_df
