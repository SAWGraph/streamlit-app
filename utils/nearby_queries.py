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

# SPARQL Endpoints
ENDPOINTS = {
    'fio': "https://frink.apps.renci.org/fiokg/sparql",
    'spatial': "https://frink.apps.renci.org/spatialkg/sparql", 
    'sawgraph': "https://frink.apps.renci.org/sawgraph/sparql",
    'federation': "https://frink.apps.renci.org/federation/sparql"
}

# Common NAICS industry codes for the dropdown
# Includes sector codes (2 digits), subsector codes (3 digits), industry groups (4 digits), and specific industries (5-6 digits)
NAICS_INDUSTRIES = {
    # ===========================================
    # 2-DIGIT SECTOR CODES (Official NAICS from U.S. Census Bureau)
    # Source: https://www.census.gov/naics/
    # Note: 31-33 = Manufacturing, 44-45 = Retail Trade, 48-49 = Transportation
    # ===========================================
    "11": "Agriculture, Forestry, Fishing and Hunting",
    "21": "Mining, Quarrying, and Oil and Gas Extraction",
    "22": "Utilities",
    "23": "Construction",
    "31": "Manufacturing (31-33 range)",
    "32": "Manufacturing (31-33 range)",
    "33": "Manufacturing (31-33 range)",
    "42": "Wholesale Trade",
    "44": "Retail Trade (44-45 range)",
    "45": "Retail Trade (44-45 range)",
    "48": "Transportation and Warehousing (48-49 range)",
    "49": "Transportation and Warehousing (48-49 range)",
    "51": "Information",
    "52": "Finance and Insurance",
    "53": "Real Estate and Rental and Leasing",
    "54": "Professional, Scientific, and Technical Services",
    "55": "Management of Companies and Enterprises",
    "56": "Administrative and Support and Waste Management and Remediation Services",
    "61": "Educational Services",
    "62": "Health Care and Social Assistance",
    "71": "Arts, Entertainment, and Recreation",
    "72": "Accommodation and Food Services",
    "81": "Other Services (except Public Administration)",
    "92": "Public Administration",
    
    # ===========================================
    # Water & Waste Management (Subsectors and Industries)
    # ===========================================
    "221310": "Water Supply and Irrigation Systems",
    "221320": "Sewage Treatment Facilities",
    "562111": "Solid Waste Collection",
    "562112": "Hazardous Waste Collection",
    "562211": "Hazardous Waste Treatment and Disposal",
    "562212": "Solid Waste Landfill",
    "562213": "Solid Waste Combustors and Incinerators",
    "562219": "Other Nonhazardous Waste Treatment and Disposal",
    "562910": "Remediation Services",
    "5622": "Waste Treatment and Disposal (All)",
    
    # Chemical Manufacturing
    "3251": "Basic Chemical Manufacturing",
    "3252": "Resin, Rubber, and Artificial Fibers",
    "3253": "Pesticide, Fertilizer, and Other Agricultural Chemical",
    "3254": "Pharmaceutical and Medicine Manufacturing",
    "3255": "Paint, Coating, and Adhesive Manufacturing",
    "3256": "Soap, Cleaning Compound, and Toilet Preparation",
    "325": "Chemical Manufacturing (All)",
    
    # Paper and Textiles
    "3221": "Pulp, Paper, and Paperboard Mills",
    "3222": "Converted Paper Product Manufacturing",
    "322": "Paper Manufacturing (All)",
    "3131": "Fiber, Yarn, and Thread Mills",
    "3132": "Fabric Mills",
    "3133": "Textile and Fabric Finishing and Coating",
    "313": "Textile Mills (All)",
    
    # Plastics and Rubber
    "3261": "Plastics Product Manufacturing",
    "3262": "Rubber Product Manufacturing",
    "326": "Plastics and Rubber Products (All)",
    
    # Metal Manufacturing
    "3311": "Iron and Steel Mills and Ferroalloy",
    "3312": "Steel Product Manufacturing from Purchased Steel",
    "3313": "Alumina and Aluminum Production and Processing",
    "3314": "Nonferrous Metal (except Aluminum) Production",
    "3315": "Foundries",
    "331": "Primary Metal Manufacturing (All)",
    "3321": "Forging and Stamping",
    "3322": "Cutlery and Handtool Manufacturing",
    "3323": "Architectural and Structural Metals",
    "3324": "Boiler, Tank, and Shipping Container",
    "3325": "Hardware Manufacturing",
    "3326": "Spring and Wire Product Manufacturing",
    "3327": "Machine Shops and Threaded Product",
    "3328": "Coating, Engraving, Heat Treating",
    "3329": "Other Fabricated Metal Product",
    "332": "Fabricated Metal Product Manufacturing (All)",
    
    # Transportation Equipment
    "3361": "Motor Vehicle Manufacturing",
    "3362": "Motor Vehicle Body and Trailer",
    "3363": "Motor Vehicle Parts Manufacturing",
    "3364": "Aerospace Product and Parts Manufacturing",
    "3365": "Railroad Rolling Stock Manufacturing",
    "3366": "Ship and Boat Building",
    "336": "Transportation Equipment (All)",
    
    # Electronics and Electrical
    "3341": "Computer and Peripheral Equipment",
    "3342": "Communications Equipment Manufacturing",
    "3343": "Audio and Video Equipment",
    "3344": "Semiconductor and Other Electronic Component",
    "3345": "Navigational and Electromedical Instruments",
    "334": "Computer and Electronic Product (All)",
    "3351": "Electric Lighting Equipment",
    "3352": "Household Appliance Manufacturing",
    "3353": "Electrical Equipment Manufacturing",
    "3359": "Other Electrical Equipment and Component",
    "335": "Electrical Equipment and Appliance (All)",
    
    # Petroleum and Coal
    "3241": "Petroleum and Coal Products Manufacturing",
    "324": "Petroleum and Coal Products (All)",
    
    # Food and Beverage
    "3111": "Animal Food Manufacturing",
    "3112": "Grain and Oilseed Milling",
    "3113": "Sugar and Confectionery Product",
    "3114": "Fruit and Vegetable Preserving",
    "3115": "Dairy Product Manufacturing",
    "3116": "Animal Slaughtering and Processing",
    "3117": "Seafood Product Preparation and Packaging",
    "3118": "Bakeries and Tortilla Manufacturing",
    "3119": "Other Food Manufacturing",
    "311": "Food Manufacturing (All)",
    "3121": "Beverage Manufacturing",
    "312": "Beverage and Tobacco Product (All)",
    
    # Wood Products
    "3211": "Sawmills and Wood Preservation",
    "3212": "Veneer, Plywood, and Engineered Wood",
    "3219": "Other Wood Product Manufacturing",
    "321": "Wood Product Manufacturing (All)",
    
    # Printing
    "3231": "Printing and Related Support Activities",
    "323": "Printing and Related (All)",
    
    # Services with PFAS
    "812320": "Drycleaning and Laundry Services",
    "561740": "Carpet and Upholstery Cleaning Services",
    "488119": "Other Airport Operations",
    "48811": "Airport Operations",
    
    # Military and Government
    "928110": "National Security",
    "92811": "National Security (All)",
}


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


def parse_sparql_results(results: dict) -> pd.DataFrame:
    """Convert SPARQL JSON results to DataFrame"""
    if not results or 'results' not in results or 'bindings' not in results['results']:
        return pd.DataFrame()
    
    bindings = results['results']['bindings']
    if not bindings:
        return pd.DataFrame()
    
    data = []
    for binding in bindings:
        row = {}
        for key, value in binding.items():
            row[key] = value.get('value', '')
        data.append(row)
    
    return pd.DataFrame(data)


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


def _build_industry_values_clause(naics_codes: list[str]) -> str:
    """
    Build a VALUES clause for industry filtering (notebook style).
    Returns empty string if no codes provided.
    """
    if not naics_codes:
        return ""
    
    # For now, take the first code (UI typically selects one)
    code = naics_codes[0]
    
    # Industry codes > 4 digits are specific codes, <= 4 are groups
    if len(code) > 4:
        return f"VALUES ?industryCode {{naics:NAICS-{code}}}."
    else:
        return f"VALUES ?industryGroup {{naics:NAICS-{code}}}."


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
    industry_values = _build_industry_values_clause(naics_codes)
    
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
    # Desired behavior:
    # - include_nondetects=False: only keep detected numeric results within [min,max]
    # - include_nondetects=True: keep (detected numeric within [min,max]) OR (non-detect flagged)
    concentration_filter = ""
    if include_nondetects:
        concentration_filter = (
            f"FILTER( ?isNonDetect || (BOUND(?numericValue) && ?numericValue >= {min_concentration} && ?numericValue <= {max_concentration}) )"
        )
    else:
        concentration_filter = "\n".join(
            [
                "FILTER(!?isNonDetect)",
                "FILTER(BOUND(?numericValue))",
                "FILTER(?numericValue > 0)",
                f"FILTER (?numericValue >= {min_concentration} && ?numericValue <= {max_concentration})",
            ]
        )
    
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
        ?s2neighbor kwg-ont:sfTouches|owl:sameAs ?s2cell.
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
    OPTIONAL {{ ?result qudt:enumeratedValue ?enumDetected }}
    # Non-detect detection: enumeratedValue OR explicit "non-detect" value (string/URI)
    BIND(
      (BOUND(?enumDetected) || LCASE(STR(?result_value)) = "non-detect" || STR(?result_value) = STR(coso:non-detect))
      as ?isNonDetect
    )
    # Numeric value for detected results (best-effort). For non-detects, numericValue is 0 (but we still keep them via ?isNonDetect).
    # IMPORTANT: if this is a non-detect row, force numericValue=0 even if numericResult exists
    # (numericResult can sometimes represent detection limit / other non-detect quantities).
    BIND(
      IF(
        ?isNonDetect,
        0,
        COALESCE(xsd:decimal(?numericResult), xsd:decimal(?result_value))
      ) as ?numericValue
    )
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


def _execute_fallback_analysis(
    naics_code: str | list[str],
    region_code: Optional[str],
    min_concentration: float,
    max_concentration: float,
    include_nondetects: bool = False
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fallback: Execute analysis in separate steps if consolidated query fails.
    This matches the original notebook workflow more closely.
    """
    print("--- Fallback: Running separate queries ---")
    # Adjust min_concentration for nondetects
    effective_min = 0 if include_nondetects else min_concentration
    
    # Step 1: Get facilities in the region
    facilities_df = _get_facilities_in_region(naics_code, region_code)
    
    if facilities_df.empty:
        print("No facilities found in region.")
        return pd.DataFrame(), pd.DataFrame()
    
    # Step 2: Get S2 cells for these facilities and expand to neighbors
    s2_cells = _get_s2_cells_with_neighbors(naics_code, region_code)
    
    if s2_cells.empty:
        print("No S2 cells found.")
        return facilities_df, pd.DataFrame()
    
    # Step 3: Find samples in those S2 cells
    samples_df = _get_samples_in_cells(
        s2_cells,
        effective_min,
        max_concentration,
        include_nondetects=include_nondetects,
    )
    
    print(f"\n{'='*60}")
    print(f"FALLBACK ANALYSIS COMPLETE")
    print(f"  - Facilities in region: {len(facilities_df)}")
    print(f"  - Contaminated samples nearby: {len(samples_df)}")
    print(f"{'='*60}\n")
    
    return facilities_df, samples_df


def _get_facilities_in_region(naics_code: str | list[str], region_code: Optional[str]) -> pd.DataFrame:
    """Get facilities of specified industry type within a region"""
    naics_codes = _normalize_naics_codes(naics_code)
    industry_filter = _build_industry_filter(naics_codes)
    
    region_filter = ""
    if region_code:
        sanitized_region = str(region_code).strip()
        if sanitized_region:
            region_filter = (
                f"?s2cell spatial:connectedTo "
                f"kwgr:administrativeRegion.USA.{sanitized_region} ."
            )
    
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
    ?s2cell rdf:type kwg-ont:S2Cell_Level13;
            kwg-ont:sfContains ?facility.
    {region_filter}
    
    ?facility fio:ofIndustry ?industryGroup;
              fio:ofIndustry ?industryCode;
              geo:hasGeometry/geo:asWKT ?facWKT;
              rdfs:label ?facilityName.
    ?industryCode a naics:NAICS-IndustryCode;
                  fio:subcodeOf ?industryGroup;
                  rdfs:label ?industryName.
    {industry_filter}
}}
"""
    
    industry_label = ", ".join(naics_codes) if naics_codes else "ALL industries"
    region_label = str(region_code).strip() if region_code else "ALL regions"
    print(f"   > Finding facilities for {industry_label} in region {region_label}...")
    results = execute_sparql_query(ENDPOINTS['fio'], query)
    df = parse_sparql_results(results)
    
    if not df.empty:
        print(f"   > Found {len(df)} facilities")
    
    return df


def _get_s2_cells_with_neighbors(naics_code: str | list[str], region_code: Optional[str]) -> pd.DataFrame:
    """Get S2 cells containing facilities AND their neighboring cells in region"""
    naics_codes = _normalize_naics_codes(naics_code)
    industry_filter = _build_industry_filter(naics_codes)
    
    region_filter = ""
    if region_code:
        sanitized_region = str(region_code).strip()
        if sanitized_region:
            region_filter = (
                f"?s2cellFacility spatial:connectedTo "
                f"kwgr:administrativeRegion.USA.{sanitized_region} ."
            )
    
    query = f"""
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fio: <http://w3id.org/fio/v1/fio#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

SELECT DISTINCT ?s2cellNeighbor WHERE {{
    # Find S2 cells with facilities of this industry in the region
    ?s2cellFacility rdf:type kwg-ont:S2Cell_Level13;
                    kwg-ont:sfContains ?facility.
    {region_filter}
    
    ?facility fio:ofIndustry ?industryGroup;
              fio:ofIndustry ?industryCode.
    ?industryCode a naics:NAICS-IndustryCode;
                  fio:subcodeOf ?industryGroup.
    {industry_filter}
    
    # Expand to neighbors
    ?s2cellFacility kwg-ont:sfTouches|owl:sameAs ?s2cellNeighbor.
}}
"""
    
    print("   > Finding S2 cells and neighbors...")
    results = execute_sparql_query(ENDPOINTS['fio'], query, timeout=120)
    df = parse_sparql_results(results)
    
    if not df.empty:
        # Rename column to match expected format
        df = df.rename(columns={'s2cellNeighbor': 's2cell'})
        print(f"   > Found {len(df)} S2 cells (including neighbors)")
    
    return df


def _get_samples_in_cells(
    s2_cells_df: pd.DataFrame,
    min_concentration: float,
    max_concentration: float,
    include_nondetects: bool = False,
) -> pd.DataFrame:
    """Find contaminated samples within specified S2 cells"""
    if s2_cells_df.empty:
        return pd.DataFrame()
    
    # Convert to VALUES string (limit to prevent timeout)
    s2_list = s2_cells_df['s2cell'].tolist()[:200]  # Limit to 200 cells
    s2_list_prefixed = [uri.replace("http://stko-kwg.geog.ucsb.edu/lod/resource/", "kwgr:") for uri in s2_list]
    s2_values_string = " ".join(s2_list_prefixed)
    
    concentration_filter = (
        f"FILTER( ?isNonDetect || (BOUND(?numericValue) && ?numericValue >= {min_concentration} && ?numericValue <= {max_concentration}) )"
        if include_nondetects
        else "\n".join(
            [
                "FILTER(!?isNonDetect)",
                "FILTER(BOUND(?numericValue))",
                "FILTER(?numericValue > 0)",
                f"FILTER (?numericValue >= {min_concentration} && ?numericValue <= {max_concentration})",
            ]
        )
    )
    query = f"""
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX qudt: <http://qudt.org/schema/qudt/>

SELECT
    ?sp ?spName ?spWKT
    (COUNT(DISTINCT ?observation) as ?resultCount)
    (MAX(?numericValue) as ?max)
    (GROUP_CONCAT(DISTINCT ?subVal; separator="</br>") as ?results)
    (GROUP_CONCAT(DISTINCT ?datedSubVal; separator="</br>") as ?datedresults)
    (GROUP_CONCAT(?year; separator=" </br> ") as ?dates)
    (GROUP_CONCAT(DISTINCT ?Typelabels; separator=";") as ?Type)
    (GROUP_CONCAT(DISTINCT ?material; separator=";") as ?Materials)
WHERE {{
    ?sp rdf:type coso:SamplePoint;
        spatial:connectedTo ?s2cell;
        rdfs:label ?spName;
        geo:hasGeometry/geo:asWKT ?spWKT.
    VALUES ?s2cell {{{s2_values_string}}}

    ?observation rdf:type coso:ContaminantObservation;
        coso:observedAtSamplePoint ?sp;
        coso:ofSubstance ?substance1 ;
        coso:observedTime ?time ;
        coso:analyzedSample ?sample ;
        coso:hasResult ?result .
    ?sample rdfs:label ?sampleLabel;
      coso:sampleOfMaterialType/rdfs:label ?material.
    {{
      SELECT ?sample (GROUP_CONCAT(DISTINCT ?sampleClassLabel; separator=";") as ?Typelabels) WHERE {{
        ?sample a ?sampleClass.
        ?sampleClass rdfs:label ?sampleClassLabel.
        VALUES ?sampleClass {{
          coso:WaterSample coso:AnimalMaterialSample coso:PlantMaterialSample coso:SolidMaterialSample
        }}
      }} GROUP BY ?sample
    }}
    ?result coso:measurementValue ?result_value;
        coso:measurementUnit ?unit .
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
    ?substance1 rdfs:label ?substance.
    ?unit qudt:symbol ?unit_sym.
    {concentration_filter}
    BIND(SUBSTR(?time, 1, 7) as ?year)
    BIND(CONCAT('<b>',str(?result_value), '</b>', " ", ?unit_sym, " ", ?substance) as ?subVal)
    BIND(CONCAT(?year, ' <b> ',str(?result_value), '</b>', " ", ?unit_sym, " ", ?substance) as ?datedSubVal)
}}
GROUP BY ?sp ?spName ?spWKT
"""
    
    print(f"   > Finding samples with concentration {min_concentration}-{max_concentration} ng/L...")
    results = execute_sparql_query(ENDPOINTS['sawgraph'], query, timeout=120)
    df = parse_sparql_results(results)
    
    if not df.empty:
        print(f"   > Found {len(df)} contaminated sample points")
        df = _normalize_samples_df(df)
    
    return df
