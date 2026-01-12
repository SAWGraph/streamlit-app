"""
Concentration Utilities
SPARQL queries to get concentration bounds based on region/substance/material filters
"""
from typing import Optional
import requests


# SPARQL Endpoint
FEDERATION_ENDPOINT = "https://frink.apps.renci.org/federation/sparql"


def _parse_max_value(results: dict) -> Optional[float]:
    if not results or "results" not in results:
        return None
    bindings = results["results"].get("bindings", [])
    if not bindings:
        return None
    max_val = None
    for binding in bindings:
        if isinstance(binding, dict) and "max" in binding:
            max_val = binding.get("max", {}).get("value")
            break
    try:
        return float(max_val) if max_val is not None else None
    except (TypeError, ValueError):
        return None


def _execute_sparql_query(query: str, timeout: int = 60) -> Optional[dict]:
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    try:
        response = requests.post(
            FEDERATION_ENDPOINT,
            data={"query": query},
            headers=headers,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        print(f"SPARQL query error (max concentration): {exc}")
        return None


def get_max_concentration(
    region_code: str,
    is_subdivision: bool = False,
    substance_uri: Optional[str] = None,
    material_uri: Optional[str] = None,
) -> Optional[float]:
    """
    Get the maximum concentration (ng/L) for the selected region and filters.

    Args:
        region_code: FIPS code for county or subdivision geoId
        is_subdivision: True if region_code is a subdivision geoId
        substance_uri: Optional substance URI to filter
        material_uri: Optional material type URI to filter

    Returns:
        Maximum concentration value (float) or None if unavailable
    """
    if not region_code:
        return None

    if is_subdivision:
        region_pattern = (
            f"?sp rdf:type coso:SamplePoint ;"
            f" kwg-ont:sfWithin|kwg-ont:sfTouches <https://datacommons.org/browser/geoId/{region_code}> ."
        )
    else:
        region_pattern = (
            f"?sp rdf:type coso:SamplePoint ;"
            f" kwg-ont:sfWithin|kwg-ont:sfTouches ?ar3 .\n"
            f"?ar3 rdf:type kwg-ont:AdministrativeRegion_3 ;"
            f" kwg-ont:administrativePartOf <http://stko-kwg.geog.ucsb.edu/lod/resource/administrativeRegion.USA.{region_code}> ."
        )

    substance_filter = f"VALUES ?substance {{<{substance_uri}>}}" if substance_uri else ""
    material_filter = f"VALUES ?matType {{<{material_uri}>}}" if material_uri else ""

    query = f"""
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX qudt: <http://qudt.org/schema/qudt/>

SELECT (MAX(?result_value) as ?max) WHERE {{
    {region_pattern}
    ?observation rdf:type coso:ContaminantObservation ;
                coso:observedAtSamplePoint ?sp ;
                coso:ofSubstance ?substance ;
                coso:analyzedSample ?sample ;
                coso:hasResult ?result .
    ?sample coso:sampleOfMaterialType ?matType .
    ?result coso:measurementValue ?result_value ;
            coso:measurementUnit ?unit .
    VALUES ?unit {{<http://qudt.org/vocab/unit/NanoGM-PER-L>}}
    {substance_filter}
    {material_filter}
}}
"""

    results = _execute_sparql_query(query, timeout=120)
    return _parse_max_value(results)
