"""
SOCKG Queries
Fetch SOCKG locations and nearby facilities with optional state filtering.
"""
from __future__ import annotations

from typing import Optional
import pandas as pd
import requests
import streamlit as st

from core.sparql import ENDPOINT_URLS, parse_sparql_results


def _execute_sparql_query(query: str, timeout: int = 120) -> Optional[dict]:
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    try:
        response = requests.post(
            ENDPOINT_URLS["federation"],
            data={"query": query},
            headers=headers,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        print(f"SOCKG query error: {exc}")
        return None


def get_sockg_state_codes() -> pd.DataFrame:
    """
    Return states that have SOCKG locations.

    Returns:
        DataFrame with columns: ar1 (state URI), fips_code (2-digit)
    """
    query = """
PREFIX sockg: <https://idir.uta.edu/sockg-ontology#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT DISTINCT ?ar1 WHERE {
    ?ar1 rdf:type kwg-ont:AdministrativeRegion_1 .
    FILTER EXISTS {
        ?location a sockg:Location ;
                  dcterms:identifier ?locationId ;
                  spatial:connectedTo ?s2 .
        ?s2 a kwg-ont:Cell ;
            spatial:connectedTo ?ar1 .
    }
    FILTER(STRSTARTS(STR(?ar1), "http://stko-kwg.geog.ucsb.edu")).
}
"""
    results = _execute_sparql_query(query)
    df = parse_sparql_results(results) if results else pd.DataFrame()
    if df.empty:
        return pd.DataFrame(columns=["ar1", "fips_code"])

    df["fips_code"] = df["ar1"].str.extract(r"administrativeRegion\.USA\.(\d+)")
    df["fips_code"] = df["fips_code"].astype(str).str.zfill(2)
    df = df.dropna(subset=["fips_code"]).drop_duplicates(subset=["fips_code"])
    return df[["ar1", "fips_code"]].reset_index(drop=True)


def get_sockg_locations(state_code: Optional[str] = None) -> pd.DataFrame:
    """
    Fetch SOCKG locations (optionally filtered by state).

    Args:
        state_code: Optional 2-digit FIPS state code (e.g., "19" for Iowa)
    """
    # Build state filter - URIs use zero-padded 2-digit codes (e.g., USA.01 not USA.1)
    state_filter = ""
    if state_code:
        code = str(state_code).strip().zfill(2)  # Ensure 2-digit padded code
        state_filter = f"?s2 spatial:connectedTo kwgr:administrativeRegion.USA.{code} ."
        print(f"SOCKG locations: Filtering for state code '{code}' (USA.{code})")

    query = f"""
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX sockg: <https://idir.uta.edu/sockg-ontology#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>

SELECT DISTINCT ?location ?locationGeometry ?locationId ?locationDescription
WHERE {{
    ?location a sockg:Location ;
              geo:hasGeometry/geo:asWKT ?locationGeometry ;
              dcterms:identifier ?locationId ;
              dcterms:description ?locationDescription ;
              spatial:connectedTo ?s2 .
    ?s2 a kwg-ont:Cell .
    {state_filter}
}}
"""
    results = _execute_sparql_query(query)
    df = parse_sparql_results(results) if results else pd.DataFrame()
    if df.empty:
        return pd.DataFrame(columns=["location", "locationGeometry", "locationId", "locationDescription"])
    return df.reset_index(drop=True)


def get_sockg_facilities(state_code: Optional[str] = None) -> pd.DataFrame:
    """
    Fetch facilities near SOCKG locations (optionally filtered by state).

    Args:
        state_code: Optional 2-digit FIPS state code (e.g., "19" for Iowa)
    """
    # Build state filter - URIs use zero-padded 2-digit codes (e.g., USA.01 not USA.1)
    state_filter = ""
    if state_code:
        code = str(state_code).strip().zfill(2)  # Ensure 2-digit padded code
        state_filter = f"?s2 spatial:connectedTo kwgr:administrativeRegion.USA.{code} ."
        print(f"SOCKG facilities: Filtering for state code '{code}' (USA.{code})")

    query = f"""
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX sockg: <https://idir.uta.edu/sockg-ontology#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX fio: <http://w3id.org/fio/v1/fio#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX fio-pfas:  <http://w3id.org/fio/v1/pfas#>

SELECT DISTINCT ?facility ?facilityName ?facWKT ?PFASusing ?industrySector ?industrySubsector
       (GROUP_CONCAT(DISTINCT ?industry; SEPARATOR='; ') as ?industries)
       (GROUP_CONCAT(DISTINCT ?locationId; SEPARATOR='; ') as ?locations)
WHERE {{
    ?location a sockg:Location ;
              dcterms:identifier ?locationId ;
              spatial:connectedTo ?s2 .
    ?s2 a kwg-ont:Cell .
    {state_filter}
    ?s2 spatial:connectedTo|owl:sameAs ?s2n .
    ?s2n a kwg-ont:S2Cell_Level13 ;
         spatial:connectedTo|owl:sameAs ?s2neighbor .
    ?s2neighbor a kwg-ont:Cell ;
                kwg-ont:sfContains ?facility .
    ?facility a fio:Facility ;
              rdfs:label ?facilityName ;
              fio:ofIndustry ?industryCode ;
              geo:hasGeometry/geo:asWKT ?facWKT .
    ?industryCode a naics:NAICS-IndustryCode ;
                  rdfs:label ?industry ;
                  fio:subcodeOf ?industrySubsectorCode .
    ?industrySubsectorCode rdf:type naics:NAICS-IndustrySubsector ;
                           rdfs:label ?industrySubsector .
    ?industrySubsectorCode fio:subcodeOf ?industrySectorCode .
    ?industrySectorCode rdf:type naics:NAICS-IndustrySector ;
                        rdfs:label ?industrySector .
    OPTIONAL {{
        ?pfasList fio:hasMember ?industryCode ;
                  rdfs:subClassOf fio-pfas:IndustryCollectionByPFASContaminationConcern .
    }}
    BIND(BOUND(?pfasList) as ?PFASusing)
}}
GROUP BY ?facility ?facilityName ?facWKT ?PFASusing ?industrySector ?industrySubsector
"""
    results = _execute_sparql_query(query)
    df = parse_sparql_results(results) if results else pd.DataFrame()
    if df.empty:
        return pd.DataFrame(
            columns=[
                "facility",
                "facilityName",
                "facWKT",
                "PFASusing",
                "industrySector",
                "industrySubsector",
                "industries",
                "locations",
            ]
        )
    return df.reset_index(drop=True)


# Cached versions for use in app
@st.cache_data(ttl=3600)
def get_sockg_state_code_set() -> set:
    """Get FIPS state codes that have SOCKG locations."""
    df = get_sockg_state_codes()
    if df.empty:
        return set()
    return set(df["fips_code"].astype(str).str.zfill(2).tolist())
