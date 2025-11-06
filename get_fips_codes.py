"""
Script to query all US administrative regions with their labels and FIPS codes
and save the results to a CSV file.
"""
import sys
import os
import pandas as pd

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.sparql_helpers import get_sparql_wrapper, convertToDataframe

def get_all_fips_codes():
    """
    Query all unique combinations of labels and FIPS codes for US administrative regions
    """
    
    query = '''
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

# Get all unique combinations of labels and FIPS codes
SELECT DISTINCT ?label ?fipsCode
WHERE {
  ?s rdfs:label ?label .
  
  # 1. Find all URIs that are US admin regions
  FILTER( CONTAINS( STR(?s), "administrativeRegion.USA.") )
  
  # 2. Extract the FIPS code after the ".USA." part
  BIND( STRAFTER(STR(?s), ".USA.") AS ?fipsCode )
}
# No LIMIT, we want all of them!
'''
    
    print("Connecting to SPARQL endpoint...")
    sparql = get_sparql_wrapper('spatial')
    
    print("Executing query...")
    sparql.setQuery(query)
    result = sparql.query()
    
    print("Converting results to DataFrame...")
    df = convertToDataframe(result)
    
    return df


def main():
    try:
        # Execute query
        df = get_all_fips_codes()
        
        # Display results summary
        print(f"\n✅ Query successful!")
        print(f"Found {len(df)} administrative regions")
        print("\nFirst few results:")
        print(df.head(10))
        
        # Save to CSV
        output_file = "us_administrative_regions_fips.csv"
        df.to_csv(output_file, index=False)
        print(f"\n💾 Results saved to: {output_file}")
        
        # Display statistics
        print(f"\nStatistics:")
        print(f"- Total regions: {len(df)}")
        print(f"- Unique labels: {df['label'].nunique()}")
        print(f"- Unique FIPS codes: {df['fipsCode'].nunique()}")
        
        # Show some sample FIPS code patterns
        if len(df) > 0:
            print(f"\nSample FIPS codes:")
            for fips in df['fipsCode'].head(10).values:
                label = df[df['fipsCode'] == fips]['label'].values[0]
                print(f"  {fips}: {label}")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

