"""
Nearby Samples Queries - Consolidated Version
Finds contaminated samples near specific facility types (by NAICS code)
using a single federated SPARQL query.

Similar to upstream tracing, this uses the federation endpoint to combine
data from multiple knowledge graphs in one query.
"""

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
# Includes both specific industry codes (5-6 digits) and industry groups (3-4 digits)
NAICS_INDUSTRIES = {
    # Water & Waste Management
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


def execute_nearby_analysis(
    naics_code: str,
    region_code: str,
    min_concentration: float = 0.0,
    max_concentration: float = 500.0
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Execute the complete "Samples Near Facilities" analysis in ONE consolidated query.
    
    This is similar to how the upstream tracing works - one big federated query
    that combines facility data, spatial relationships, and contamination data.
    
    The query finds:
    1. Facilities of the specified NAICS industry type
    2. S2 cells containing/neighboring those facilities
    3. Contaminated samples in those S2 cells
    4. All filtered to the specified region
    
    Args:
        naics_code: NAICS industry code to search for
        region_code: FIPS region code (state or county)
        min_concentration: Minimum contamination threshold (ng/L)
        max_concentration: Maximum contamination threshold (ng/L)
    
    Returns:
        Tuple of (facilities_df, samples_df) - S2 cells are internal only
    """
    print(f"\n{'='*60}")
    print(f"NEARBY ANALYSIS: NAICS {naics_code} in region {region_code}")
    print(f"Concentration range: {min_concentration}-{max_concentration} ng/L")
    print(f"{'='*60}\n")
    
    # Determine industry filter based on code length
    is_specific_code = len(naics_code) > 4
    if is_specific_code:
        industry_filter = f'VALUES ?industryCode {{naics:NAICS-{naics_code}}}.'
    else:
        industry_filter = f'VALUES ?industryGroup {{naics:NAICS-{naics_code}}}.'
    
    # Sanitize region code for URI
    sanitized_region = str(region_code).lstrip('0') or '0'
    
    # =========================================================================
    # CONSOLIDATED QUERY: Facilities + Neighboring S2 Cells + Samples
    # This single query does what previously required 5 separate queries
    # =========================================================================
    
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
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT DISTINCT 
    ?facility ?facWKT ?facilityName ?industryCode ?industryName
    ?sp ?spWKT 
    (MAX(?result_value) as ?maxConcentration)
    (GROUP_CONCAT(DISTINCT ?substance; separator=", ") as ?substances)
    (GROUP_CONCAT(DISTINCT ?matTypeLabel; separator=", ") as ?materials)
WHERE {{
    # Step 1: Find facilities of the specified industry type
    ?facility fio:ofIndustry ?industryGroup;
              fio:ofIndustry ?industryCode;
              geo:hasGeometry/geo:asWKT ?facWKT;
              rdfs:label ?facilityName.
    ?industryCode a naics:NAICS-IndustryCode;
                  fio:subcodeOf ?industryGroup;
                  rdfs:label ?industryName.
    {industry_filter}
    
    # Step 2: Find S2 cells that contain these facilities
    ?s2cellFacility rdf:type kwg-ont:S2Cell_Level13;
                    kwg-ont:sfContains ?facility.
    
    # Step 3: Filter S2 cells to the specified region AND get neighboring cells
    ?s2cellFacility spatial:connectedTo kwgr:administrativeRegion.USA.{sanitized_region}.
    ?s2cellFacility kwg-ont:sfTouches|owl:sameAs ?s2cellNeighbor.
    
    # Step 4: Find contaminated samples in those S2 cells (facility cell + neighbors)
    ?sp rdf:type coso:SamplePoint;
        spatial:connectedTo ?s2cellNeighbor;
        geo:hasGeometry/geo:asWKT ?spWKT.
    
    # Step 5: Get contamination observations at those sample points
    ?observation rdf:type coso:ContaminantObservation;
        coso:observedAtSamplePoint ?sp;
        coso:ofSubstance ?substance1;
        coso:analyzedSample ?sample;
        coso:hasResult ?result.
    
    ?sample coso:sampleOfMaterialType ?matType.
    ?matType rdfs:label ?matTypeLabel.
    
    ?result coso:measurementValue ?result_value;
            coso:measurementUnit ?unit.
    
    # Filter by concentration range
    FILTER (?result_value >= {min_concentration} && ?result_value <= {max_concentration})
    
    # Extract substance name
    BIND(STRAFTER(str(?substance1), "#parameter.") as ?substance)
}}
GROUP BY ?facility ?facWKT ?facilityName ?industryCode ?industryName ?sp ?spWKT
"""
    
    print("--- Running consolidated federated query ---")
    print("   > Querying facilities, neighbors, and samples in one query...")
    
    # Try the consolidated query first (federation endpoint)
    results = execute_sparql_query(ENDPOINTS['federation'], query, timeout=300)
    combined_df = parse_sparql_results(results)
    
    if not combined_df.empty:
        print(f"   > Consolidated query returned {len(combined_df)} results")
        
        # Extract facilities (unique facilities)
        facility_cols = ['facility', 'facWKT', 'facilityName', 'industryCode', 'industryName']
        facilities_df = combined_df[facility_cols].drop_duplicates(subset=['facility'])
        
        # Extract samples (unique samples)
        sample_cols = ['sp', 'spWKT', 'maxConcentration', 'substances', 'materials']
        samples_df = combined_df[sample_cols].drop_duplicates(subset=['sp'])
        
        # Convert numeric columns
        if 'maxConcentration' in samples_df.columns:
            samples_df['maxConcentration'] = pd.to_numeric(samples_df['maxConcentration'], errors='coerce')
        
        print(f"\n{'='*60}")
        print(f"ANALYSIS COMPLETE")
        print(f"  - Facilities in region: {len(facilities_df)}")
        print(f"  - Contaminated samples nearby: {len(samples_df)}")
        print(f"{'='*60}\n")
        
        return facilities_df, samples_df
    
    # If consolidated query fails or returns empty, try fallback approach
    print("   > Consolidated query returned no results, trying fallback...")
    return _execute_fallback_analysis(naics_code, region_code, min_concentration, max_concentration)


def _execute_fallback_analysis(
    naics_code: str,
    region_code: str,
    min_concentration: float,
    max_concentration: float
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fallback: Execute analysis in separate steps if consolidated query fails.
    This matches the original notebook workflow more closely.
    """
    print("--- Fallback: Running separate queries ---")
    
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
    samples_df = _get_samples_in_cells(s2_cells, min_concentration, max_concentration)
    
    print(f"\n{'='*60}")
    print(f"FALLBACK ANALYSIS COMPLETE")
    print(f"  - Facilities in region: {len(facilities_df)}")
    print(f"  - Contaminated samples nearby: {len(samples_df)}")
    print(f"{'='*60}\n")
    
    return facilities_df, samples_df


def _get_facilities_in_region(naics_code: str, region_code: str) -> pd.DataFrame:
    """Get facilities of specified industry type within a region"""
    is_specific_code = len(naics_code) > 4
    if is_specific_code:
        industry_filter = f'VALUES ?industryCode {{naics:NAICS-{naics_code}}}.'
    else:
        industry_filter = f'VALUES ?industryGroup {{naics:NAICS-{naics_code}}}.'
    
    sanitized_region = str(region_code).lstrip('0') or '0'
    
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
            kwg-ont:sfContains ?facility;
            spatial:connectedTo kwgr:administrativeRegion.USA.{sanitized_region}.
    
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
    
    print(f"   > Finding facilities for NAICS {naics_code} in region {region_code}...")
    results = execute_sparql_query(ENDPOINTS['fio'], query)
    df = parse_sparql_results(results)
    
    if not df.empty:
        print(f"   > Found {len(df)} facilities")
    
    return df


def _get_s2_cells_with_neighbors(naics_code: str, region_code: str) -> pd.DataFrame:
    """Get S2 cells containing facilities AND their neighboring cells in region"""
    is_specific_code = len(naics_code) > 4
    if is_specific_code:
        industry_filter = f'VALUES ?industryCode {{naics:NAICS-{naics_code}}}.'
    else:
        industry_filter = f'VALUES ?industryGroup {{naics:NAICS-{naics_code}}}.'
    
    sanitized_region = str(region_code).lstrip('0') or '0'
    
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
                    kwg-ont:sfContains ?facility;
                    spatial:connectedTo kwgr:administrativeRegion.USA.{sanitized_region}.
    
    ?facility fio:ofIndustry ?industryGroup;
              fio:ofIndustry ?industryCode.
    ?industryCode a naics:NAICS-IndustryCode;
                  fio:subcodeOf ?industryGroup.
    {industry_filter}
    
    # Expand to neighbors
    ?s2cellFacility kwg-ont:sfTouches|owl:sameAs ?s2cellNeighbor.
}}
"""
    
    print(f"   > Finding S2 cells and neighbors...")
    results = execute_sparql_query(ENDPOINTS['fio'], query, timeout=120)
    df = parse_sparql_results(results)
    
    if not df.empty:
        # Rename column to match expected format
        df = df.rename(columns={'s2cellNeighbor': 's2cell'})
        print(f"   > Found {len(df)} S2 cells (including neighbors)")
    
    return df


def _get_samples_in_cells(s2_cells_df: pd.DataFrame, min_concentration: float, max_concentration: float) -> pd.DataFrame:
    """Find contaminated samples within specified S2 cells"""
    if s2_cells_df.empty:
        return pd.DataFrame()
    
    # Convert to VALUES string (limit to prevent timeout)
    s2_list = s2_cells_df['s2cell'].tolist()[:200]  # Limit to 200 cells
    s2_list_prefixed = [uri.replace("http://stko-kwg.geog.ucsb.edu/lod/resource/", "kwgr:") for uri in s2_list]
    s2_values_string = " ".join(s2_list_prefixed)
    
    query = f"""
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX qudt: <http://qudt.org/schema/qudt/>

SELECT 
    ?sp ?spWKT 
    (COUNT(DISTINCT ?subVal) as ?resultCount) 
    (MAX(?result_value) as ?maxConcentration) 
    (GROUP_CONCAT(DISTINCT ?substance; separator=", ") as ?substances) 
    (GROUP_CONCAT(DISTINCT ?matTypeLabel; separator=", ") as ?materials)
WHERE {{
    ?sp rdf:type coso:SamplePoint;
        spatial:connectedTo ?s2cell;
        geo:hasGeometry/geo:asWKT ?spWKT.
    VALUES ?s2cell {{{s2_values_string}}}
    
    ?observation rdf:type coso:ContaminantObservation;
        coso:observedAtSamplePoint ?sp;
        coso:ofSubstance ?substance1;
        coso:analyzedSample ?sample;
        coso:hasResult ?result.
    
    ?sample rdfs:label ?sampleLabel;
            coso:sampleOfMaterialType ?matType.
    ?matType rdfs:label ?matTypeLabel.
    
    ?result coso:measurementValue ?result_value;
            coso:measurementUnit ?unit.
    FILTER (?result_value >= {min_concentration} && ?result_value <= {max_concentration})
    
    ?unit qudt:symbol ?unit_sym.
    BIND((CONCAT(str(?result_value), " ", ?unit_sym)) as ?subVal)
    BIND(STRAFTER(str(?substance1), "#parameter.") as ?substance)
}} 
GROUP BY ?sp ?spWKT
"""
    
    print(f"   > Finding samples with concentration {min_concentration}-{max_concentration} ng/L...")
    results = execute_sparql_query(ENDPOINTS['sawgraph'], query, timeout=120)
    df = parse_sparql_results(results)
    
    if not df.empty:
        print(f"   > Found {len(df)} contaminated sample points")
        # Convert numeric columns
        if 'maxConcentration' in df.columns:
            df['maxConcentration'] = pd.to_numeric(df['maxConcentration'], errors='coerce')
        if 'resultCount' in df.columns:
            df['resultCount'] = pd.to_numeric(df['resultCount'], errors='coerce')
    
    return df
