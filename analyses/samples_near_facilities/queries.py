"""
Nearby Samples Queries - Consolidated Version
Finds contaminated samples near specific facility types (by NAICS code)
using a single federated SPARQL query.

Similar to upstream tracing, this uses the federation endpoint to combine
data from multiple knowledge graphs in one query.
"""
from __future__ import annotations

import pandas as pd
import requests
from typing import Optional, Tuple

from core.sparql import ENDPOINT_URLS, parse_sparql_results

# Alias for backward compatibility
ENDPOINTS = ENDPOINT_URLS

def _normalize_naics_codes(naics_code: str | list[str]) -> list[str]:
    if isinstance(naics_code, (list, tuple, set)):
        codes = [str(code).strip() for code in naics_code if str(code).strip()]
    else:
        codes = [str(naics_code).strip()] if naics_code else []
    return sorted(set(codes))


def _build_industry_filter(naics_codes: list[str]) -> str:
    if not naics_codes:
        return ""

    industry_codes = [code for code in naics_codes if len(code) > 4]
    industry_groups = [code for code in naics_codes if len(code) <= 4]

    def _values(values: list[str]) -> str:
        return ", ".join(f"naics:NAICS-{value}" for value in values)

    if industry_codes and industry_groups:
        return (
            f"FILTER(?industryCode IN ({_values(industry_codes)}) || "
            f"?industryGroup IN ({_values(industry_groups)}))."
        )
    if industry_codes:
        return f"FILTER(?industryCode IN ({_values(industry_codes)}))."
    return f"FILTER(?industryGroup IN ({_values(industry_groups)}))."


# parse_sparql_results is imported from core.sparql


def get_pfas_industries() -> pd.DataFrame:
    """
    Fetch PFAS-related industries from the KG.

    Returns:
        DataFrame with columns: industryCodeId, industryName, NAICS, industryGroup, industrySector
    """
    query = """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX fio: <http://w3id.org/fio/v1/fio#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX fio-pfas:  <http://w3id.org/fio/v1/pfas#>

SELECT DISTINCT ?industryCodeId ?industryName ?NAICS ?industryGroup ?industrySector
WHERE {
  ?pfasList fio:hasMember ?industryCode;
            rdfs:subClassOf fio-pfas:IndustryCollectionByPFASContaminationConcern.
  ?industryCode rdfs:label ?industryName;
                fio:subcodeOf ?industryG.
  OPTIONAL { ?industryCode dcterms:identifier ?industryCodeIdRaw. }
  BIND(COALESCE(?industryCodeIdRaw, STRAFTER(STR(?industryCode), "NAICS-")) AS ?industryCodeId)
  ?industryG rdf:type naics:NAICS-IndustryGroup;
             rdfs:label ?industryGroup;
             dcterms:identifier ?NAICS.
  ?industryG fio:subcodeOf ?industryS.
  ?industryS rdf:type naics:NAICS-IndustrySector;
             rdfs:label ?industrySector.
} ORDER BY ?industrySector
"""
    results = execute_sparql_query(ENDPOINTS["federation"], query, timeout=120)
    df = parse_sparql_results(results)
    if df.empty:
        return pd.DataFrame(
            columns=["industryCodeId", "industryName", "NAICS", "industryGroup", "industrySector"]
        )
    return df.dropna(subset=["NAICS"]).reset_index(drop=True)


def _normalize_samples_df(samples_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize sample columns to a common shape for UI display."""
    if samples_df.empty:
        return samples_df

    if "max" not in samples_df.columns and "maxConcentration" in samples_df.columns:
        samples_df = samples_df.rename(columns={"maxConcentration": "max"})
    if "Materials" not in samples_df.columns and "materials" in samples_df.columns:
        samples_df = samples_df.rename(columns={"materials": "Materials"})
    if "results" not in samples_df.columns and "substances" in samples_df.columns:
        samples_df["results"] = samples_df["substances"]
    if "datedresults" not in samples_df.columns:
        samples_df["datedresults"] = ""
    if "dates" not in samples_df.columns:
        samples_df["dates"] = ""
    if "Type" not in samples_df.columns:
        samples_df["Type"] = ""

    for col in ("max", "resultCount"):
        if col in samples_df.columns:
            samples_df[col] = pd.to_numeric(samples_df[col], errors="coerce")

    return samples_df


def execute_sparql_query(endpoint: str, query: str, method: str = 'POST', timeout: int = 180) -> Optional[dict]:
    """Execute a SPARQL query and return JSON results"""
    headers = {
        'Accept': 'application/sparql-results+json',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    try:
        if method == 'POST':
            response = requests.post(endpoint, data={'query': query}, headers=headers, timeout=timeout)
        else:
            response = requests.get(endpoint, params={'query': query}, headers=headers, timeout=timeout)
        
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"SPARQL query error: {e}")
        return None


def _build_industry_values_clause(naics_codes: list[str]) -> tuple[str, str]:
    """
    Build industry filtering clauses (notebook style), including correct NAICS level handling.

    Returns:
      (values_clause, hierarchy_clause)
    - values_clause: VALUES ... restricting the appropriate NAICS node.
    - hierarchy_clause: additional fio:subcodeOf joins needed for sector/subsector filtering.
    """
    if not naics_codes:
        return "", ""

    code = str(naics_codes[0]).strip()
    if not code:
        return "", ""

    # NAICS levels:
    # - 2 digits: Sector
    # - 3 digits: Subsector
    # - 4 digits: Industry Group
    # - 5-6 digits: Industry Code
    if len(code) > 4:
        return f"VALUES ?industryCode {{naics:NAICS-{code}}}.", ""
    if len(code) == 4:
        return f"VALUES ?industryGroup {{naics:NAICS-{code}}}.", ""
    if len(code) == 3:
        return (
            f"VALUES ?industrySubsector {{naics:NAICS-{code}}}.",
            "?industryGroup fio:subcodeOf ?industrySubsector .",
        )
    if len(code) == 2:
        return (
            f"VALUES ?industrySector {{naics:NAICS-{code}}}.",
            "\n".join(
                [
                    "?industryGroup fio:subcodeOf ?industrySubsector .",
                    "?industrySubsector fio:subcodeOf ?industrySector .",
                ]
            ),
        )

    # Fallback: treat as group
    return f"VALUES ?industryGroup {{naics:NAICS-{code}}}.", ""


def execute_nearby_analysis(
    naics_code: str | list[str],
    region_code: Optional[str],
    min_concentration: float = 0.0,
    max_concentration: float = 500.0,
    include_nondetects: bool = False
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Execute the complete "Samples Near Facilities" analysis using separate queries
    matching the notebook approach (SAWGraph_Y3_Demo_NearbyFacilities.ipynb).

    Query 1: Get all facilities of the specified industry type
    Query 2: Get samples near those facilities using S2 cell neighbor subquery

    Args:
        naics_code: NAICS industry code(s) to search for
        region_code: FIPS region code (state or county) - optional
        min_concentration: Minimum contamination threshold (ng/L)
        max_concentration: Maximum contamination threshold (ng/L)
        include_nondetects: If True, include samples with zero concentration

    Returns:
        Tuple of (facilities_df, samples_df)
    """
    naics_codes = _normalize_naics_codes(naics_code)
    industry_label = ", ".join(naics_codes) if naics_codes else "ALL industries"
    region_label = str(region_code).strip() if region_code else "ALL regions"

    print(f"\n{'='*60}")
    print(f"NEARBY ANALYSIS: {industry_label} in region {region_label}")
    print(f"Concentration range: {min_concentration}-{max_concentration} ng/L")
    print(f"Include nondetects: {include_nondetects}")
    print(f"{'='*60}\n")
    
    # Build industry filter using VALUES clause (notebook style)
    industry_values, industry_hierarchy = _build_industry_values_clause(naics_codes)
    
    # Build region filter (optional).
    # IMPORTANT: filter on the facility-connected county (not S2 cells), so state + county behave correctly.
    # - state (2 digits): keep counties within the selected state
    # - county (5 digits): restrict to that county
    sanitized_region = str(region_code).strip() if region_code else ""
    region_filter = ""
    if sanitized_region:
        if len(sanitized_region) == 2:
            region_filter = f"""
    ?county rdf:type kwg-ont:AdministrativeRegion_2 ;
            kwg-ont:administrativePartOf kwgr:administrativeRegion.USA.{sanitized_region} .
"""
        elif len(sanitized_region) == 5:
            region_filter = f"""
    VALUES ?county {{ kwgr:administrativeRegion.USA.{sanitized_region} }} .
"""
        else:
            # Subdivision / other codes not currently supported for this analysis
            region_filter = ""
    
    # =========================================================================
    # QUERY 1: Get facilities (matches notebook q2)
    # =========================================================================
    facilities_query = f"""
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
              fio:ofIndustry ?industryCode;
              spatial:connectedTo ?county;
              geo:hasGeometry/geo:asWKT ?facWKT;
              rdfs:label ?facilityName.
    {region_filter}
    ?industryCode a naics:NAICS-IndustryCode;
                  fio:subcodeOf ?industryGroup;
                  rdfs:label ?industryName.
    {industry_hierarchy}
    {industry_values}
}}
"""
    
    print("--- Query 1: Fetching facilities ---")
    facilities_result = execute_sparql_query(ENDPOINTS['federation'], facilities_query, timeout=300)
    facilities_df = parse_sparql_results(facilities_result)
    
    if not facilities_df.empty:
        print(f"   > Found {len(facilities_df)} facilities")
    else:
        print("   > No facilities found")
    
    # =========================================================================
    # QUERY 2: Get samples near facilities (matches notebook q5)
    # Uses subquery for S2 neighbors exactly as in notebook
    # =========================================================================
    
    # Build concentration filter.
    # NOTE: Nondetect handling is expensive in the federated query; when include_nondetects=False
    # we omit the non-detect machinery entirely for performance.
    if include_nondetects:
        concentration_filter = (
            f"FILTER( ?isNonDetect || (BOUND(?numericValue) && ?numericValue >= {min_concentration} && ?numericValue <= {max_concentration}) )"
        )
        nondetect_fragment = """
    OPTIONAL { ?result qudt:enumeratedValue ?enumDetected }
    # Non-detect detection: enumeratedValue OR explicit "non-detect" value (string/URI)
    BIND(
      (BOUND(?enumDetected) || LCASE(STR(?result_value)) = "non-detect" || STR(?result_value) = STR(coso:non-detect))
      as ?isNonDetect
    )
    # Numeric value for detected results; for non-detects force numericValue=0.
    BIND(
      IF(
        ?isNonDetect,
        0,
        COALESCE(xsd:decimal(?numericResult), xsd:decimal(?result_value))
      ) as ?numericValue
    )
"""
    else:
        concentration_filter = "\n".join(
            [
                "FILTER(BOUND(?numericValue))",
                "FILTER(?numericValue > 0)",
                f"FILTER (?numericValue >= {min_concentration} && ?numericValue <= {max_concentration})",
            ]
        )
        nondetect_fragment = """
    # Detected-only fast path: numericValue derived from numericResult/result_value, no non-detect handling
    BIND(COALESCE(xsd:decimal(?numericResult), xsd:decimal(?result_value)) as ?numericValue)
"""
    
    samples_query = f"""
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX fio: <http://w3id.org/fio/v1/fio#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX qudt: <http://qudt.org/schema/qudt/>

SELECT DISTINCT (COUNT(DISTINCT ?observation) as ?resultCount) (MAX(?numericValue) as ?max) (GROUP_CONCAT(DISTINCT ?subVal; separator="</br>") as ?results) (GROUP_CONCAT(DISTINCT ?datedSubVal; separator="</br>") as ?datedresults) (GROUP_CONCAT(?year; separator=" </br> ") as ?dates) (GROUP_CONCAT(DISTINCT ?Typelabels; separator=";") as ?Type) (GROUP_CONCAT(DISTINCT ?material) as ?Materials) ?sp ?spName ?spWKT
WHERE {{

    {{SELECT DISTINCT ?s2neighbor WHERE {{
        ?s2cell rdf:type kwg-ont:S2Cell_Level13 ;
                kwg-ont:sfContains ?facility.
        ?facility fio:ofIndustry ?industryGroup;
                  fio:ofIndustry ?industryCode;
                  spatial:connectedTo ?county .
        {region_filter}
        ?industryCode a naics:NAICS-IndustryCode;
                      fio:subcodeOf ?industryGroup;
                      rdfs:label ?industryName.
        {industry_values}
        {industry_hierarchy}
        ?s2neighbor kwg-ont:sfTouches|owl:sameAs ?s2cell.
        ?s2neighbor rdf:type kwg-ont:S2Cell_Level13 .
    }} }}

    ?sp rdf:type coso:SamplePoint;
        spatial:connectedTo ?s2neighbor;
        rdfs:label ?spName;
        geo:hasGeometry/geo:asWKT ?spWKT.
    ?observation rdf:type coso:ContaminantObservation;
        coso:observedAtSamplePoint ?sp;
        coso:ofSubstance ?substance1;
        coso:observedTime ?time;
        coso:analyzedSample ?sample;
        coso:hasResult ?result.
    ?sample rdfs:label ?sampleLabel;
            coso:sampleOfMaterialType/rdfs:label ?material.
    {{SELECT ?sample (GROUP_CONCAT(DISTINCT ?sampleClassLabel; separator=";") as ?Typelabels) WHERE {{
        ?sample a ?sampleClass.
        ?sampleClass rdfs:label ?sampleClassLabel.
        VALUES ?sampleClass {{coso:WaterSample coso:AnimalMaterialSample coso:PlantMaterialSample coso:SolidMaterialSample}}
    }} GROUP BY ?sample }}
    ?result coso:measurementValue ?result_value;
            coso:measurementUnit ?unit.
    OPTIONAL {{ ?result qudt:quantityValue/qudt:numericValue ?numericResult }}
    {nondetect_fragment}
    ?substance1 rdfs:label ?substance.
    ?unit qudt:symbol ?unit_sym.
    {concentration_filter}
    BIND(SUBSTR(?time, 1, 7) as ?year)
    BIND(CONCAT('<b>',str(?result_value), '</b>', " ", ?unit_sym, " ", ?substance) as ?subVal)
    BIND(CONCAT(?year, ' <b> ',str(?result_value), '</b>', " ", ?unit_sym, " ", ?substance) as ?datedSubVal)
}} GROUP BY ?sp ?spName ?spWKT
ORDER BY DESC(?max)
"""
    
    print("--- Query 2: Fetching samples near facilities ---")
    samples_result = execute_sparql_query(ENDPOINTS['federation'], samples_query, timeout=300)
    samples_df = parse_sparql_results(samples_result)
    
    if not samples_df.empty:
        print(f"   > Found {len(samples_df)} sample points")
        samples_df = _normalize_samples_df(samples_df)
    else:
        print("   > No samples found near facilities")
    
    print(f"\n{'='*60}")
    print(f"ANALYSIS COMPLETE")
    print(f"  - Facilities: {len(facilities_df)}")
    print(f"  - Sample points nearby: {len(samples_df)}")
    print(f"{'='*60}\n")
    
    return facilities_df, samples_df


    return facilities_df, samples_df
