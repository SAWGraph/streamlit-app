#!/usr/bin/env python3
"""Test SPARQL query for Maine to debug issues"""

import requests
import json

# Test with Maine (state code 23)
region_code = "23"
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

SELECT DISTINCT ?sp ?spWKT
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
    
    VALUES ?unit {{<http://qudt.org/vocab/unit/NanoGM-PER-L>}}
    {substance_filter}
    {material_filter}
    FILTER (?result_value >= {min_conc})
    FILTER (?result_value <= {max_conc})
}}
LIMIT 10
"""

print("=" * 80)
print("TESTING SIMPLE QUERY FOR CONTAMINATED SAMPLES IN MAINE")
print("=" * 80)
print(f"\nRegion: Maine (code {region_code})")
print(f"Substance: PFOA")
print(f"Material: Groundwater")
print(f"Concentration: {min_conc}-{max_conc} ng/L")
print(f"\nQuery length: {len(query)} characters")
print("\n" + "=" * 80)
print("QUERY:")
print("=" * 80)
print(query)
print("=" * 80)

endpoint = "https://frink.apps.renci.org/federation/sparql"
headers = {
    "Accept": "application/sparql-results+json",
    "Content-Type": "application/x-www-form-urlencoded"
}

print("\nExecuting query...")
try:
    response = requests.post(
        endpoint,
        data={"query": query},
        headers=headers,
        timeout=60
    )
    print(f"Response status: {response.status_code}")
    
    if response.status_code == 200:
        results = response.json()
        bindings = results.get('results', {}).get('bindings', [])
        print(f"\n✅ SUCCESS! Found {len(bindings)} sample points")
        
        if bindings:
            print("\nFirst few results:")
            for i, binding in enumerate(bindings[:3]):
                print(f"\n  Sample {i+1}:")
                for key, value in binding.items():
                    print(f"    {key}: {value.get('value', 'N/A')[:80]}")
        else:
            print("\n⚠️  Query succeeded but returned 0 results")
            print("This might indicate:")
            print("  - No contaminated samples in Maine matching criteria")
            print("  - Wrong substance/material URIs")
            print("  - Wrong concentration range")
    else:
        print(f"\n❌ ERROR: {response.status_code}")
        print(f"Response: {response.text[:500]}")
        
except Exception as e:
    print(f"\n❌ EXCEPTION: {e}")

print("\n" + "=" * 80)
