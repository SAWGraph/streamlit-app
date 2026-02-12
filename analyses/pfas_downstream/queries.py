"""
PFAS Downstream Tracing Query Functions
Implements a 3-step pipeline:
    Step 1: Find facilities by NAICS industry type in a region 
    Step 2: Find downstream flowlines/streams from facilities
    Step 3: Find samplepoints in downstream S2 cells
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import requests

from core.sparql import ENDPOINT_URLS, parse_sparql_results
from core.naics_utils import normalize_naics_codes, build_simple_naics_values


def _post_sparql(endpoint: str, query: str, timeout: Optional[int] = None) -> Tuple[Optional[dict], Optional[str], Dict[str, Any]]:
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    debug_info: Dict[str, Any] = {
        "endpoint": endpoint,
        "query_length": len(query),
        "query": query,
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
    Build a NAICS VALUES clause for downstream queries.

    Behavior:
      - If no code is provided, return empty string.
      - If code length > 4, constrain ?industryCode
      - Otherwise constrain ?industryGroup

    This mirrors the original downstream implementation but uses the
    shared core.naics helpers for consistency.
    """
    codes = normalize_naics_codes(naics_code)
    if not codes:
        return ""
    return build_simple_naics_values(codes[0])


def _build_region_filter(region_code: Optional[str], county_var: str = "?county") -> str:
    if not region_code:
        return ""
    code = str(region_code).strip()
    if len(code) <= 5:
        return f"""{county_var} rdf:type kwg-ont:AdministrativeRegion_2 ;
                   kwg-ont:administrativePartOf kwgr:administrativeRegion.USA.{code} ."""
    return ""


def _state_code_from_region(region_code: Optional[str]) -> Optional[str]:
    if not region_code:
        return None
    code = str(region_code).strip()
    if not code:
        return None
    if len(code) == 5:
        return code[:2]
    if len(code) <= 2:
        return code
    return None


def _build_ar3_region_filter(region_code: Optional[str], ar3_var: str = "?ar3") -> str:
    if not region_code:
        return ""
    code = str(region_code).strip()
    if not code:
        return ""
    if len(code) > 5:
        return f"VALUES {ar3_var} {{ <https://datacommons.org/browser/geoId/{code}> }} ."
    return (
        f"{ar3_var} rdf:type kwg-ont:AdministrativeRegion_3 ; "
        f"kwg-ont:administrativePartOf+ kwgr:administrativeRegion.USA.{code} ."
    )


def _build_facility_values(facility_uris: Optional[List[str]]) -> str:
    if not facility_uris:
        return ""
    cleaned: List[str] = []
    for uri in facility_uris:
        if not uri:
            continue
        u = str(uri).strip()
        if not u:
            continue
        if u.startswith("<") and u.endswith(">"):
            cleaned.append(u)
        elif u.startswith("http://") or u.startswith("https://"):
            cleaned.append(f"<{u}>")
    if not cleaned:
        return ""
    return f"VALUES ?facility {{ {' '.join(cleaned)} }}."


def execute_downstream_facilities_query(
    naics_code: Optional[str],
    region_code: Optional[str],
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """Step 1: Find facilities by NAICS industry type in a region."""
    industry_filter = _build_industry_filter(naics_code)
    facilities_region_code = _state_code_from_region(region_code)
    region_filter = _build_region_filter(facilities_region_code, county_var="?facCounty")
    
    if not industry_filter:
        return pd.DataFrame(), "Industry type is required for downstream tracing", {"error": "No industry selected"}
    
    query = f"""
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fio: <http://w3id.org/fio/v1/fio#>

SELECT DISTINCT ?facility ?facWKT ?facilityName ?industryCode ?industryName WHERE {{
    ?facility fio:ofIndustry ?industryGroup;
        fio:ofIndustry ?industryCode ;
        spatial:connectedTo ?facCounty ;
        geo:hasGeometry/geo:asWKT ?facWKT;
        rdfs:label ?facilityName.
    {region_filter}
    ?industryCode a naics:NAICS-IndustryCode;
        fio:subcodeOf ?industryGroup ;
        rdfs:label ?industryName.
    {industry_filter}
}}
"""
    results_json, error, debug_info = _post_sparql(ENDPOINT_URLS["federation"], query)
    if error or not results_json:
        return pd.DataFrame(), error, debug_info
    df = parse_sparql_results(results_json)
    return df, None, debug_info


def execute_downstream_streams_query(
    naics_code: Optional[str],
    region_code: Optional[str],
    facility_uris: Optional[List[str]] = None,
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """Step 2: Find downstream flowlines/streams from facilities."""
    if facility_uris is not None and not isinstance(facility_uris, list):
        facility_uris = None

    facility_values = _build_facility_values(facility_uris)
    industry_filter = _build_industry_filter(naics_code)
    facilities_region_code = _state_code_from_region(region_code)
    region_filter = _build_region_filter(facilities_region_code, county_var="?facCounty")

    if facility_values:
        industry_filter = ""
        region_filter = ""
    elif not industry_filter:
        return pd.DataFrame(), "Industry type is required", {"error": "No industry selected"}
    
    query = f"""
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
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
            spatial:connectedTo ?facCounty.
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
    results_json, error, debug_info = _post_sparql(ENDPOINT_URLS["federation"], query)
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
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """Step 3: Find contaminated samples downstream of facilities."""
    if facility_uris is not None and not isinstance(facility_uris, list):
        facility_uris = None

    facility_values = _build_facility_values(facility_uris)
    industry_filter = _build_industry_filter(naics_code)
    sample_region_filter = _build_ar3_region_filter(region_code, ar3_var="?ar3")
    facilities_region_code = _state_code_from_region(region_code)
    facility_region_filter = _build_region_filter(facilities_region_code, county_var="?facCounty")

    if facility_values:
        industry_filter = ""
        region_filter = ""
    elif not industry_filter:
        return pd.DataFrame(), "Industry type is required", {"error": "No industry selected"}
    
    concentration_filter = (
        f"FILTER( ?isNonDetect || (BOUND(?numericValue) && ?numericValue >= {float(min_conc)} && ?numericValue <= {float(max_conc)}) )"
        if include_nondetects
        else "\n".join([
            "FILTER(!?isNonDetect)",
            "FILTER(BOUND(?numericValue))",
            "FILTER(?numericValue > 0)",
            f"FILTER (?numericValue >= {float(min_conc)})",
            f"FILTER (?numericValue <= {float(max_conc)})",
        ])
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
            spatial:connectedTo ?facCounty.
        {facility_region_filter}
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
        geo:hasGeometry/geo:asWKT ?spWKT;
        spatial:connectedTo ?ar3.
    {sample_region_filter}
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
    results_json, error, debug_info = _post_sparql(ENDPOINT_URLS["federation"], query)
    if error or not results_json:
        return pd.DataFrame(), error, debug_info
    df = parse_sparql_results(results_json)
    return df, None, debug_info


