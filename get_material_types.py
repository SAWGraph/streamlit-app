"""
Script to query all sample material types from the SPARQL endpoint
and save the results to a CSV file.
"""
import requests
import pandas as pd
import sys
import os

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def get_all_material_types():
    """
    Query all unique sample material types from the knowledge graph
    """
    
    query = """
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?matType ?shortName ?label
WHERE {
  # 1. Find a sample and its material type
  ?sample coso:sampleOfMaterialType ?matType .

  # 2. Get the part after the '#'
  BIND( STRAFTER(STR(?matType), "#") AS ?afterHash )

  # 3. Filter: Only keep ones that are 'sampleMaterialType'
  FILTER( STRSTARTS(?afterHash, "sampleMaterialType.") )

  # 4. Get the short code after the '.'
  BIND( STRAFTER(?afterHash, ".") AS ?shortName )

  # 5. (Optional) Try to get a nice label for it
  OPTIONAL { ?matType rdfs:label ?label . }
}
"""
    
    # SPARQL endpoint URL
    sparql_endpoint = "https://frink.apps.renci.org/federation/sparql"
    
    headers = {
        "Accept": "application/sparql-results+json"
    }
    
    print("Connecting to SPARQL endpoint...")
    print(f"Endpoint: {sparql_endpoint}")
    
    print("Executing query...")
    
    try:
        response = requests.get(sparql_endpoint, params={"query": query}, headers=headers)
        
        if response.status_code == 200:
            results = response.json()
            # Extract variable names from the results
            variables = results['head']['vars']
            # Extract bindings (rows) from the results
            bindings = results['results']['bindings']
            
            # Create a list of dictionaries for DataFrame
            data = []
            for binding in bindings:
                row = {}
                for var in variables:
                    if var in binding:
                        row[var] = binding[var]['value']
                    else:
                        row[var] = None  # Handle optional fields
                data.append(row)
            
            df = pd.DataFrame(data)
            return df
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"A network error occurred: {e}")
        return None


def main():
    try:
        # Execute query
        df = get_all_material_types()
        
        if df is not None and not df.empty:
            # Display results summary
            print(f"\n✅ Query successful!")
            print(f"Found {len(df)} material types")
            
            # Sort by shortName for better readability
            df = df.sort_values('shortName')
            
            print("\nMaterial types found:")
            for idx, row in df.iterrows():
                label_text = f" ({row['label']})" if row['label'] else ""
                print(f"  {row['shortName']}{label_text}")
            
            # Save to CSV
            output_file = "sample_material_types.csv"
            df.to_csv(output_file, index=False)
            print(f"\n💾 Results saved to: {output_file}")
            
            # Display statistics
            print(f"\nStatistics:")
            print(f"- Total unique material types: {len(df)}")
            print(f"- Material types with labels: {df['label'].notna().sum()}")
            print(f"- Material types without labels: {df['label'].isna().sum()}")
            
            # Show sample data
            print(f"\nSample data:")
            print(df.head(10).to_string(index=False))
        else:
            print("❌ No data returned from query")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

