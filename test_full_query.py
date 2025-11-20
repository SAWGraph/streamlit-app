#!/usr/bin/env python3
"""Test FULL upstream tracing query for Maine"""

import requests
import json
import time

# Test with a SMALLER region first - Penobscot County
region_code = "23019"  # Penobscot County - smaller than all of Maine
substance_uri = "http://sawgraph.spatialai.org/v1/me-egad#parameter.PFOA_A"
material_uri = "http://sawgraph.spatialai.org/v1/me-egad-data#sampleMaterialType.GW"
min_conc = 0
max_conc = 500

def build_values_clause(var_name, uri_value):
    if not uri_value:
        return ""
    return f"VALUES ?{var_name} {{<{uri_value}>}}"

substance_filter = build_values_clause("substance", substance_uri)
material_filter = build_values_clause("matType", material_uri)

# Build region pattern
if len(region_code) > 5:
    region_pattern = f"VALUES ?ar3 {{<https://datacommons.org/browser/geoId/{region_code}>}}"
else:
    region_pattern = f"?ar3 rdf:type kwg-ont:AdministrativeRegion_3 ; kwg-ont:administrativePartOf+ kwgr:administrativeRegion.USA.{region_code} ."

# FULL query with upstream tracing
query = f"""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX fio: <http://w3id.org/fio/v1/fio#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX me_egad: <http://sawgraph.spatialai.org/v1/me-egad#>
PREFIX me_egad_data: <http://sawgraph.spatialai.org/v1/me-egad-data#>

SELECT DISTINCT ?sp ?spWKT ?upstream_flowlineWKT ?facility ?facWKT ?facilityName 
                ?industryName ?industryGroup ?industryGroupName ?industrySubsector ?industrySubsectorName
                ?substance ?sample ?matType ?result_value ?unit
WHERE {{
    ?sp rdf:type coso:SamplePoint ;
        geo:hasGeometry/geo:asWKT ?spWKT ;
        spatial:connectedTo ?ar3 ;
        spatial:connectedTo ?s2 .
    
    {region_pattern}
    
    ?s2 rdf:type kwg-ont:S2Cell_Level13 .
    
    ?s2cell rdf:type kwg-ont:S2Cell_Level13 ;
             kwg-ont:sfTouches | owl:sameAs ?s2 .
    
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
    FILTER (?result_value >= {min_conc})
    FILTER (?result_value <= {max_conc})
    
    ?downstream_flowline rdf:type hyf:HY_FlowPath ;
                         spatial:connectedTo ?s2cell .
    
    ?upstream_flowline hyf:downstreamFlowPathTC ?downstream_flowline ;
                       geo:hasGeometry/geo:asWKT ?upstream_flowlineWKT .
    
    ?s2cellus spatial:connectedTo ?upstream_flowline ;
              rdf:type kwg-ont:S2Cell_Level13 .
    
    OPTIONAL {{
        ?s2cellus kwg-ont:sfContains ?facility .
        ?facility fio:ofIndustry ?industryCode, ?industryGroup, ?industrySubsector ;
                  geo:hasGeometry/geo:asWKT ?facWKT ;
                  rdfs:label ?facilityName .
        
        ?industryCode a naics:NAICS-IndustryCode ;
                      rdfs:label ?industryName ;
                      fio:subcodeOf ?industryGroup .
        
        ?industryGroup a naics:NAICS-IndustryGroup ;
                       rdfs:label ?industryGroupName ;
                       fio:subcodeOf ?industrySubsector .
        
        ?industrySubsector a naics:NAICS-IndustrySubsector ;
                           rdfs:label ?industrySubsectorName .
    }}
}}
LIMIT 100
"""

print("=" * 80)
print("TESTING FULL UPSTREAM TRACING QUERY")
print("=" * 80)
print(f"\nRegion: Penobscot County (code {region_code})")
print(f"Substance: PFOA")
print(f"Material: Groundwater")
print(f"Concentration: {min_conc}-{max_conc} ng/L")
print(f"\nQuery length: {len(query)} characters")
print("\n" + "=" * 80)

endpoint = "https://frink.apps.renci.org/federation/sparql"
headers = {
    "Accept": "application/sparql-results+json",
    "Content-Type": "application/x-www-form-urlencoded"
}

print("Executing query...")
start_time = time.time()
try:
    response = requests.post(
        endpoint,
        data={"query": query},
        headers=headers,
        timeout=120
    )
    elapsed = time.time() - start_time
    
    print(f"Response status: {response.status_code}")
    print(f"Query took: {elapsed:.2f} seconds")
    
    if response.status_code == 200:
        results = response.json()
        bindings = results.get('results', {}).get('bindings', [])
        print(f"\n✅ SUCCESS! Found {len(bindings)} rows (sample+flowline+facility combinations)")
        
        if bindings:
            # Count unique items
            unique_samples = set()
            unique_facilities = set()
            for binding in bindings:
                if 'sp' in binding:
                    unique_samples.add(binding['sp']['value'])
                if 'facility' in binding:
                    unique_facilities.add(binding['facility']['value'])
            
            print(f"\nUnique sample points: {len(unique_samples)}")
            print(f"Unique facilities: {len(unique_facilities)}")
            
            print("\nFirst result:")
            for key, value in bindings[0].items():
                val_str = str(value.get('value', 'N/A'))
                print(f"  {key}: {val_str[:100]}")
        else:
            print("\n⚠️  Query succeeded but returned 0 results")
    else:
        print(f"\n❌ ERROR: {response.status_code}")
        print(f"Response: {response.text[:1000]}")
        
except Exception as e:
    elapsed = time.time() - start_time
    print(f"\n❌ EXCEPTION after {elapsed:.2f}s: {e}")

print("\n" + "=" * 80)
