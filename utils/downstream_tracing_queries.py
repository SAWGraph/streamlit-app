"""
PFAS Downstream Tracing Query Functions
=======================================

Implements a 3-step pipeline that mirrors the upstream tracing workflow, but
in the opposite hydrological direction:

    Step 1: Find contaminated sample points in a selected region (federation)
    Step 2: Trace downstream through the HY_FlowPath network (federation)
    Step 3: Find contaminated sample points in downstream S2 cells (sawgraph)

This module is designed to be imported by the Streamlit UI without modifying
existing upstream/nearby analysis logic.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

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


def convertS2ListToQueryString(s2_list: list[str]) -> str:
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


def execute_downstream_step1_query(
    substance_uri: Optional[str],
    material_uri: Optional[str],
    min_conc: float,
    max_conc: float,
    region_code: str,
    timeout: int = 180,
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """
    Step 1: Find contaminated sample points in the user-selected region and
    return sample point + S2 cell identifiers for downstream tracing.
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
    Step 2: Trace downstream S2 cells (and a limited number of flowline WKTs for
    mapping) starting from the contaminated S2 cells from Step 1.

    Returns:
        downstream_s2_df: DataFrame with column 's2cell'
        downstream_flowlines_df: DataFrame with column 'downstream_flowlineWKT'
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

    # Expand to neighboring cells (same pattern used in upstream combined query)
    ?s2neighbor rdf:type kwg-ont:S2Cell_Level13 ;
                kwg-ont:sfTouches | owl:sameAs ?s2start .

    # Start flowlines connected to the starting (or touching) cells
    ?start_flowline rdf:type hyf:HY_FlowPath ;
                    spatial:connectedTo ?s2neighbor .

    # Trace downstream from the start flowline(s)
    ?start_flowline hyf:downstreamFlowPathTC ?downstream_flowline .

    # Collect downstream S2 cells connected to downstream flowlines
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


def execute_downstream_samples_query(
    downstream_s2_df: pd.DataFrame,
    substance_uri: Optional[str],
    material_uri: Optional[str],
    min_conc: float,
    max_conc: float,
    max_s2_cells: int = 200,
    timeout: int = 180,
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """
    Step 3: Find contaminated sample points within downstream S2 cells.
    """
    if downstream_s2_df is None or downstream_s2_df.empty or "s2cell" not in downstream_s2_df.columns:
        return pd.DataFrame(), None, {"info": "No downstream S2 cells to search."}

    s2_list = downstream_s2_df["s2cell"].dropna().unique().tolist()
    if not s2_list:
        return pd.DataFrame(), None, {"info": "No downstream S2 cells to search."}

    if len(s2_list) > max_s2_cells:
        s2_list = s2_list[:max_s2_cells]

    s2_values_string = convertS2ListToQueryString(s2_list)

    substance_filter = f"VALUES ?substance {{<{substance_uri}>}}" if substance_uri else ""
    material_filter = f"VALUES ?matType {{<{material_uri}>}}" if material_uri else ""

    query = f"""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>

SELECT DISTINCT (COUNT(DISTINCT ?subVal) as ?resultCount) (MAX(?result_value) as ?max)
                ?sp ?spWKT
WHERE {{
    VALUES ?s2cell {{ {s2_values_string} }}

    ?sp rdf:type coso:SamplePoint ;
        geo:hasGeometry/geo:asWKT ?spWKT ;
        spatial:connectedTo ?s2cell .

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
GROUP BY ?sp ?spWKT
"""

    results_json, error, debug_info = _post_sparql(ENDPOINT_URLS["sawgraph"], query, timeout=timeout)
    if error or not results_json:
        return pd.DataFrame(), error, debug_info

    df = parse_sparql_results(results_json)
    return df, None, debug_info

