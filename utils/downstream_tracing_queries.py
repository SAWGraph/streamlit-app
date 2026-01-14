"""
PFAS Downstream Tracing Query Functions
=======================================

Implements a 3-step pipeline matching the notebook approach:

    Step 1: Find facilities by NAICS industry type in a region (federation)
    Step 2: Find downstream flowlines/streams from facility locations (federation)
    Step 3: Find contaminated samples in downstream S2 cells (federation)

This module traces contamination DOWNSTREAM from facilities of specific industry types.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests


ENDPOINT_URLS = {
    "sawgraph": "https://frink.apps.renci.org/sawgraph/sparql",
    "spatial": "https://frink.apps.renci.org/spatialkg/sparql",
    "hydrology": "https://frink.apps.renci.org/hydrologykg/sparql",
    "fio": "https://frink.apps.renci.org/fiokg/sparql",
    "federation": "https://frink.apps.renci.org/federation/sparql",
}


def parse_sparql_results(results: Optional[dict]) -> pd.DataFrame:
    if not results or "head" not in results or "results" not in results:
        return pd.DataFrame()

    variables = results.get("head", {}).get("vars", [])
    bindings = results.get("results", {}).get("bindings", [])
    if not bindings:
        return pd.DataFrame(columns=variables)

    rows = []
    for binding in bindings:
        row = {}
        for var in variables:
            row[var] = binding.get(var, {}).get("value")
        rows.append(row)
    return pd.DataFrame(rows)


def _post_sparql(endpoint: str, query: str, timeout: int) -> Tuple[Optional[dict], Optional[str], Dict[str, Any]]:
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    debug_info: Dict[str, Any] = {
        "endpoint": endpoint,
        "query_length": len(query),
        "query": query,
        "timeout_sec": timeout,
    }

    try:
        response = requests.post(endpoint, data={"query": query}, headers=headers, timeout=timeout)
        debug_info["response_status"] = response.status_code
        debug_info["response_text_snippet"] = response.text[:500]

        if response.status_code != 200:
            return None, f"Error {response.status_code}: {response.text}", debug_info

        return response.json(), None, debug_info
    except requests.exceptions.RequestException as e:
        debug_info["exception"] = str(e)
        return None, f"Network error: {str(e)}", debug_info
    except Exception as e:
        debug_info["exception"] = str(e)
        return None, f"Error: {str(e)}", debug_info


def _build_industry_filter(naics_code: Optional[str]) -> str:
    """
    Build SPARQL VALUES clause for NAICS industry filtering.
    
    - If code is 6 digits (specific industry), filter by industryCode
    - If code is 4 digits (industry group), filter by industryGroup
    """
    if not naics_code:
        return ""
    
    code = str(naics_code).strip()
    if len(code) > 4:
        # Specific industry code (e.g., 221320)
        return f"VALUES ?industryCode {{naics:NAICS-{code}}}."
    else:
        # Industry group (e.g., 5622)
        return f"VALUES ?industryGroup {{naics:NAICS-{code}}}."


def _build_region_filter(region_code: Optional[str]) -> str:
    """
    Build SPARQL region filter for county/state.
    """
    if not region_code:
        return ""
    
    code = str(region_code).strip()
    if len(code) <= 2:
        # State code (e.g., "18" for Indiana)
        return f"""?county rdf:type kwg-ont:AdministrativeRegion_2 ;
                   kwg-ont:administrativePartOf kwgr:administrativeRegion.USA.{code} ."""
    elif len(code) == 5:
        # County FIPS code (e.g., "23019")
        state_code = code[:2]
        return f"""?county rdf:type kwg-ont:AdministrativeRegion_2 ;
                   kwg-ont:administrativePartOf kwgr:administrativeRegion.USA.{state_code} ."""
    else:
        # More specific region - not supported in this query style
        return ""


def _build_facility_values(facility_uris: Optional[List[str]]) -> str:
    """
    Build SPARQL VALUES clause for filtering to specific facility URIs.

    Expects full HTTP(S) URIs (e.g., "http://w3id.org/fio/v1/epa-frs-data#d.FRS-Facility.110000000000").
    """
    if not facility_uris:
        return ""

    cleaned: List[str] = []
    for uri in facility_uris:
        if not uri:
            continue
        u = str(uri).strip()
        if not u:
            continue
        # Ensure it's wrapped as an IRI, not treated as a prefixed name / literal
        if u.startswith("<") and u.endswith(">"):
            cleaned.append(u)
        elif u.startswith("http://") or u.startswith("https://"):
            cleaned.append(f"<{u}>")
        else:
            # Skip anything that doesn't look like an IRI to avoid malformed SPARQL
            continue

    if not cleaned:
        return ""

    return f"VALUES ?facility {{ {' '.join(cleaned)} }}."


def execute_downstream_facilities_query(
    naics_code: Optional[str],
    region_code: Optional[str],
    timeout: int = 180,
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """
    Step 1: Find facilities by NAICS industry type in a region.
    
    Based on notebook q2 query.
    
    Returns:
        DataFrame with columns: facility, facWKT, facilityName, industryCode, industryName
    """
    industry_filter = _build_industry_filter(naics_code)
    region_filter = _build_region_filter(region_code)
    
    if not industry_filter:
        return pd.DataFrame(), "Industry type is required for downstream tracing", {"error": "No industry selected"}
    
    query = f"""
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fio: <http://w3id.org/fio/v1/fio#>

SELECT DISTINCT ?facility ?facWKT ?facilityName ?industryCode ?industryName WHERE {{
    ?facility fio:ofIndustry ?industryGroup;
        fio:ofIndustry ?industryCode ;
        spatial:connectedTo ?county ;
        geo:hasGeometry/geo:asWKT ?facWKT;
        rdfs:label ?facilityName.
    {region_filter}
    ?industryCode a naics:NAICS-IndustryCode;
        fio:subcodeOf ?industryGroup ;
        rdfs:label ?industryName.
    {industry_filter}
}}
"""

    results_json, error, debug_info = _post_sparql(ENDPOINT_URLS["federation"], query, timeout=timeout)
    if error or not results_json:
        return pd.DataFrame(), error, debug_info

    df = parse_sparql_results(results_json)
    return df, None, debug_info


def execute_downstream_streams_query(
    naics_code: Optional[str],
    region_code: Optional[str],
    facility_uris: Optional[List[str]] = None,
    timeout: int = 180,
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """
    Step 2: Find downstream flowlines/streams from facilities of the specified industry.
    
    Based on notebook q1a query.
    
    Returns:
        DataFrame with columns: downstream_flowline, dsflWKT, fl_type, streamName
    """
    # Backward-compat safety: older call sites may still pass positional args
    # (naics_code, region_code, timeout) which would land `facility_uris` as an int.
    if facility_uris is not None and not isinstance(facility_uris, list):
        facility_uris = None

    facility_values = _build_facility_values(facility_uris)
    industry_filter = _build_industry_filter(naics_code)
    region_filter = _build_region_filter(region_code)

    # If a facility is provided, we trace from that facility directly (not from the whole industry set).
    if facility_values:
        industry_filter = ""
        region_filter = ""
    elif not industry_filter:
        return pd.DataFrame(), "Industry type is required", {"error": "No industry selected"}
    
    query = f"""
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fio: <http://w3id.org/fio/v1/fio#>
PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>
PREFIX nhdplusv2: <http://nhdplusv2.spatialai.org/v1/nhdplusv2#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

SELECT DISTINCT ?downstream_flowline ?dsflWKT ?fl_type ?streamName
WHERE {{
    {{SELECT ?s2neighbor WHERE {{
        ?s2neighbor kwg-ont:sfContains ?facility.
        {facility_values}
        ?facility fio:ofIndustry ?industryGroup;
            fio:ofIndustry ?industryCode;
            spatial:connectedTo ?county.
        {region_filter}
        ?industryCode a naics:NAICS-IndustryCode;
            fio:subcodeOf ?industryGroup ;
            rdfs:label ?industryName.
        {industry_filter}
    }}}}
    
    ?s2 kwg-ont:sfTouches|owl:sameAs ?s2neighbor.
    ?s2neighbor rdf:type kwg-ont:S2Cell_Level13;
              spatial:connectedTo ?upstream_flowline.

    ?upstream_flowline rdf:type hyf:HY_FlowPath ;
              hyf:downstreamFlowPathTC ?downstream_flowline .
    ?downstream_flowline geo:hasGeometry/geo:asWKT ?dsflWKT;
              nhdplusv2:hasFTYPE ?fl_type.
    OPTIONAL {{?downstream_flowline rdfs:label ?streamName}}
}}
"""

    results_json, error, debug_info = _post_sparql(ENDPOINT_URLS["federation"], query, timeout=timeout)
    if error or not results_json:
        return pd.DataFrame(), error, debug_info

    df = parse_sparql_results(results_json)
    return df, None, debug_info


def execute_downstream_samples_query(
    naics_code: Optional[str],
    region_code: Optional[str],
    facility_uris: Optional[List[str]] = None,
    min_conc: float = 0.0,
    max_conc: float = 500.0,
    include_nondetects: bool = False,
    timeout: int = 300,
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """
    Step 3: Find contaminated samples downstream of facilities.
    
    Based on notebook q1 query with concentration filtering.
    
    Args:
        naics_code: NAICS industry code to filter facilities
        region_code: Region code to filter by location
        min_conc: Minimum concentration in ng/L
        max_conc: Maximum concentration in ng/L
        timeout: Query timeout in seconds
    
    Returns:
        DataFrame with columns: samplePoint, spWKT, sample, samples, resultCount, Max, unit, results
    """
    # Backward-compat safety: some older call sites may still pass positional args
    # (naics_code, region_code, min_conc, max_conc, include_nondetects, timeout)
    # which would land `facility_uris` as a number/bool. Normalize that here.
    if facility_uris is not None and not isinstance(facility_uris, list):
        facility_uris = None

    facility_values = _build_facility_values(facility_uris)
    industry_filter = _build_industry_filter(naics_code)
    region_filter = _build_region_filter(region_code)

    # If a facility is provided, we trace from that facility directly (not from the whole industry set).
    if facility_values:
        industry_filter = ""
        region_filter = ""
    elif not industry_filter:
        return pd.DataFrame(), "Industry type is required", {"error": "No industry selected"}
    
    # Build concentration filter.
    # Desired behavior:
    # - include_nondetects=False: only keep detected numeric results within [min,max]
    # - include_nondetects=True: keep (detected numeric within [min,max]) OR (non-detect flagged)
    concentration_filter = (
        f"FILTER( ?isNonDetect || (BOUND(?numericValue) && ?numericValue >= {float(min_conc)} && ?numericValue <= {float(max_conc)}) )"
        if include_nondetects
        else "\n".join(
            [
                "FILTER(!?isNonDetect)",
                "FILTER(BOUND(?numericValue))",
                "FILTER(?numericValue > 0)",
                f"FILTER (?numericValue >= {float(min_conc)})",
                f"FILTER (?numericValue <= {float(max_conc)})",
            ]
        )
    )
    
    query = f"""
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fio: <http://w3id.org/fio/v1/fio#>
PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?samplePoint ?spWKT ?sample 
    (GROUP_CONCAT(DISTINCT ?sampleId; separator="; ") as ?samples) 
    (COUNT(DISTINCT ?subVal) as ?resultCount) 
    (MAX(?numericValue) as ?Max) 
    ?unit 
    (GROUP_CONCAT(DISTINCT ?subVal; separator=" <br> ") as ?results)
WHERE {{
    {{ SELECT DISTINCT ?s2cell WHERE {{
        ?s2neighbor kwg-ont:sfContains ?facility.
        {facility_values}
        ?facility fio:ofIndustry ?industryGroup;
            fio:ofIndustry ?industryCode;
            spatial:connectedTo ?county.
        {region_filter}
        ?industryCode a naics:NAICS-IndustryCode;
            fio:subcodeOf ?industryGroup ;
            rdfs:label ?industryName.
        {industry_filter}
        
        ?s2 kwg-ont:sfTouches|owl:sameAs ?s2neighbor.
        ?s2neighbor rdf:type kwg-ont:S2Cell_Level13;
              spatial:connectedTo ?upstream_flowline.

        ?upstream_flowline rdf:type hyf:HY_FlowPath ;
              hyf:downstreamFlowPathTC ?downstream_flowline .
        ?s2cell spatial:connectedTo ?downstream_flowline ;
              rdf:type kwg-ont:S2Cell_Level13 .
    }}}}

    ?samplePoint kwg-ont:sfWithin ?s2cell;
        rdf:type coso:SamplePoint;
        geo:hasGeometry/geo:asWKT ?spWKT.
    ?s2cell rdf:type kwg-ont:S2Cell_Level13.
    ?sample coso:fromSamplePoint ?samplePoint;
        dcterms:identifier ?sampleId;
        coso:sampleOfMaterialType/rdfs:label ?type.
    ?observation rdf:type coso:ContaminantObservation;
        coso:observedAtSamplePoint ?samplePoint;
        coso:ofDSSToxSubstance/skos:altLabel ?substance;
        coso:hasResult ?res .
    ?res coso:measurementValue ?result_value;
        coso:measurementUnit/qudt:symbol ?unit.
    OPTIONAL {{ ?res qudt:quantityValue/qudt:numericValue ?numericResult }}
    OPTIONAL {{ ?res qudt:enumeratedValue ?enumDetected }}
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
    {concentration_filter}
    BIND((CONCAT(?substance, ": ", str(?result_value) , " ", ?unit) ) as ?subVal)

}} GROUP BY ?samplePoint ?spWKT ?sample ?unit
"""

    results_json, error, debug_info = _post_sparql(ENDPOINT_URLS["federation"], query, timeout=timeout)
    if error or not results_json:
        return pd.DataFrame(), error, debug_info

    df = parse_sparql_results(results_json)
    return df, None, debug_info


# ============================================================================
# LEGACY FUNCTIONS - Keep for backward compatibility but mark as deprecated
# ============================================================================

def convertS2ListToQueryString(s2_list: list[str]) -> str:
    """Convert S2 cell URIs to SPARQL VALUES string format."""
    s2_list_formatted = []
    for s2 in s2_list:
        if s2.startswith("http://stko-kwg.geog.ucsb.edu/lod/resource/"):
            s2_list_formatted.append(
                s2.replace("http://stko-kwg.geog.ucsb.edu/lod/resource/", "kwgr:")
            )
        elif s2.startswith("https://stko-kwg.geog.ucsb.edu/lod/resource/"):
            s2_list_formatted.append(
                s2.replace("https://stko-kwg.geog.ucsb.edu/lod/resource/", "kwgr:")
            )
        elif s2.startswith("kwgr:"):
            s2_list_formatted.append(s2)
        elif s2.startswith("http://") or s2.startswith("https://"):
            s2_list_formatted.append(f"<{s2}>")
        else:
            s2_list_formatted.append(s2)
    return " ".join(s2_list_formatted)


def execute_downstream_step1_query(
    substance_uri: Optional[str],
    material_uri: Optional[str],
    min_conc: float,
    max_conc: float,
    region_code: str,
    include_nondetects: bool = False,
    timeout: int = 180,
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """
    DEPRECATED: Legacy Step 1 - Find contaminated sample points in a region.
    
    This is kept for backward compatibility but the new approach starts from facilities.
    """
    sanitized_region = str(region_code).strip()
    if not sanitized_region or sanitized_region.lower() == "none":
        error_msg = "Invalid region code. Please select a state before executing the query."
        return pd.DataFrame(), error_msg, {"error": error_msg}

    substance_filter = f"VALUES ?substance {{<{substance_uri}>}}" if substance_uri else ""
    material_filter = f"VALUES ?matType {{<{material_uri}>}}" if material_uri else ""

    if len(sanitized_region) > 5:
        region_pattern = f"VALUES ?ar3 {{<https://datacommons.org/browser/geoId/{sanitized_region}>}}"
    else:
        region_pattern = (
            f"?ar3 rdf:type kwg-ont:AdministrativeRegion_3 ; "
            f"kwg-ont:administrativePartOf+ kwgr:administrativeRegion.USA.{sanitized_region} ."
        )

    query = f"""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>

SELECT DISTINCT (COUNT(DISTINCT ?subVal) as ?resultCount) (MAX(?result_value) as ?max)
                ?sp ?spWKT ?s2
WHERE {{
    ?sp rdf:type coso:SamplePoint ;
        geo:hasGeometry/geo:asWKT ?spWKT ;
        spatial:connectedTo ?ar3 ;
        spatial:connectedTo ?s2 .

    {region_pattern}
    ?s2 rdf:type kwg-ont:S2Cell_Level13 .

    ?observation rdf:type coso:ContaminantObservation ;
        coso:observedAtSamplePoint ?sp ;
        coso:ofSubstance ?substance ;
        coso:analyzedSample ?sample ;
        coso:hasResult ?result .

    ?sample coso:sampleOfMaterialType ?matType .
    ?result coso:measurementValue ?result_value ;
            coso:measurementUnit ?unit .
    ?unit qudt:symbol ?unit_sym .

    VALUES ?unit {{<http://qudt.org/vocab/unit/NanoGM-PER-L>}}
    {substance_filter}
    {material_filter}
    FILTER (?result_value >= {float(min_conc)})
    FILTER (?result_value <= {float(max_conc)})

    BIND((CONCAT(str(?result_value) , " ", ?unit_sym)) as ?subVal)
}}
GROUP BY ?sp ?spWKT ?s2
"""

    results_json, error, debug_info = _post_sparql(ENDPOINT_URLS["federation"], query, timeout=timeout)
    if error or not results_json:
        return pd.DataFrame(), error, debug_info

    df = parse_sparql_results(results_json)
    if not df.empty and "s2" in df.columns:
        df = df.rename(columns={"s2": "s2cell"})
    return df, None, debug_info


def execute_downstream_hydrology_query(
    contaminated_samples_df: pd.DataFrame,
    max_start_s2_cells: int = 100,
    max_flowlines: int = 1000,
    timeout: int = 180,
) -> Tuple[pd.DataFrame, pd.DataFrame, Optional[str], Dict[str, Any]]:
    """
    DEPRECATED: Legacy Step 2 - Trace downstream from contaminated samples.
    
    The new approach traces from facilities instead.
    """
    if contaminated_samples_df is None or contaminated_samples_df.empty or "s2cell" not in contaminated_samples_df.columns:
        return pd.DataFrame(), pd.DataFrame(), None, {"info": "No starting S2 cells provided."}

    start_s2_list = contaminated_samples_df["s2cell"].dropna().unique().tolist()
    if not start_s2_list:
        return pd.DataFrame(), pd.DataFrame(), None, {"info": "No starting S2 cells provided."}

    if len(start_s2_list) > max_start_s2_cells:
        start_s2_list = start_s2_list[:max_start_s2_cells]

    s2_values_string = convertS2ListToQueryString(start_s2_list)

    downstream_cells_query = f"""
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

SELECT DISTINCT ?s2cell
WHERE {{
    VALUES ?s2start {{ {s2_values_string} }}

    ?s2start rdf:type kwg-ont:S2Cell_Level13 .

    ?s2neighbor rdf:type kwg-ont:S2Cell_Level13 ;
                kwg-ont:sfTouches | owl:sameAs ?s2start .

    ?start_flowline rdf:type hyf:HY_FlowPath ;
                    spatial:connectedTo ?s2neighbor .

    ?start_flowline hyf:downstreamFlowPathTC ?downstream_flowline .

    ?s2cell spatial:connectedTo ?downstream_flowline ;
            rdf:type kwg-ont:S2Cell_Level13 .
}}
"""

    results_json, error, debug_cells = _post_sparql(
        ENDPOINT_URLS["federation"], downstream_cells_query, timeout=timeout
    )
    if error or not results_json:
        return pd.DataFrame(), pd.DataFrame(), error, debug_cells

    downstream_s2_df = parse_sparql_results(results_json)

    downstream_flowlines_query = f"""
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

SELECT DISTINCT ?downstream_flowlineWKT
WHERE {{
    VALUES ?s2start {{ {s2_values_string} }}
    ?s2start rdf:type kwg-ont:S2Cell_Level13 .

    ?s2neighbor rdf:type kwg-ont:S2Cell_Level13 ;
                kwg-ont:sfTouches | owl:sameAs ?s2start .

    ?start_flowline rdf:type hyf:HY_FlowPath ;
                    spatial:connectedTo ?s2neighbor .

    ?start_flowline hyf:downstreamFlowPathTC ?downstream_flowline .
    ?downstream_flowline geo:hasGeometry/geo:asWKT ?downstream_flowlineWKT .
}}
LIMIT {int(max_flowlines)}
"""

    flowlines_json, flowlines_error, debug_flowlines = _post_sparql(
        ENDPOINT_URLS["federation"], downstream_flowlines_query, timeout=timeout
    )

    debug_info = {"downstream_cells": debug_cells, "downstream_flowlines": debug_flowlines}
    if flowlines_error or not flowlines_json:
        return downstream_s2_df, pd.DataFrame(), flowlines_error, debug_info

    downstream_flowlines_df = parse_sparql_results(flowlines_json)
    return downstream_s2_df, downstream_flowlines_df, None, debug_info
