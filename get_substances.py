"""
Script to query all PFAS substances from the SPARQL endpoint
and save the results to a CSV file.
"""
import requests
import pandas as pd
import sys
import os

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def get_all_substances():
    """
    Query all unique PFAS substances from ContaminantObservations
    """
    
    query = """
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT DISTINCT ?substance ?shortName
WHERE {
  ?obs rdf:type coso:ContaminantObservation .
  ?obs coso:ofSubstance ?substance .

  # 1. Get the part after the '#'
  # (e.g., "parameter.PFOS_A")
  BIND( STRAFTER(STR(?substance), "#") AS ?afterHash )

  # 2. Get the part after the '.'
  # (e.g., "PFOS_A")
  BIND( STRAFTER(?afterHash, ".") AS ?tempName )

  # 3. Replace the "_A" with nothing
  # (e.g., "PFOS")
  BIND( REPLACE(?tempName, "_A", "") AS ?shortName )
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
            data.append(row)
        
        df = pd.DataFrame(data)
        return df
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None


def main():
    try:
        # Execute query
        df = get_all_substances()
        
        if df is not None and not df.empty:
            # Display results summary
            print(f"\n✅ Query successful!")
            print(f"Found {len(df)} substances")
            
            # Sort by shortName for better readability
            df = df.sort_values('shortName')
            
            print("\nSubstances found:")
            print(df['shortName'].to_string(index=False))
            
            # Save to CSV
            output_file = "pfas_substances.csv"
            df.to_csv(output_file, index=False)
            print(f"\n💾 Results saved to: {output_file}")
            
            # Display statistics
            print(f"\nStatistics:")
            print(f"- Total unique substances: {len(df)}")
            print(f"- Unique short names: {df['shortName'].nunique()}")
            
            # Show sample data
            print(f"\nSample substances (first 10):")
            for idx, row in df.head(10).iterrows():
                print(f"  {row['shortName']}: {row['substance']}")
        else:
            print("❌ No data returned from query")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

