# """
# SPARQL Query Playground
# Test and explore different queries on the RENCI Federation endpoint
# """

# import streamlit as st
# import pandas as pd
# from SPARQLWrapper import SPARQLWrapper, JSON, GET
# import time

# # Page configuration
# st.set_page_config(
#     page_title="Query Playground",
#     page_icon="🧪",
#     layout="wide",
#     initial_sidebar_state="expanded"
# )

# # Title
# st.title("🧪 SPARQL Query Playground")
# st.markdown("Test queries on the RENCI Federation endpoint and see what data structure it returns.")

# # Helper function
# def execute_test_query(query_string):
#     """Execute a test query and return results"""
#     try:
#         sparql = SPARQLWrapper('https://frink.apps.renci.org/federation/sparql')
#         sparql.setMethod(GET)
#         sparql.setReturnFormat(JSON)
#         sparql.setTimeout(60)
        
#         start_time = time.time()
#         sparql.setQuery(query_string)
#         results = sparql.query()
#         query_time = time.time() - start_time
        
#         json_results = results.convert()
        
#         # Extract results
#         if "results" in json_results and "bindings" in json_results["results"]:
#             bindings = json_results["results"]["bindings"]
            
#             # Convert to DataFrame
#             data = []
#             for binding in bindings:
#                 row = {}
#                 for var, value in binding.items():
#                     row[var] = value['value']
#                 data.append(row)
            
#             df = pd.DataFrame(data)
#             return True, df, query_time, len(bindings), json_results
#         else:
#             return True, pd.DataFrame(), query_time, 0, json_results
            
#     except Exception as e:
#         return False, None, 0, 0, str(e)

# # Sidebar with pre-built test queries
# st.sidebar.header("📚 Sample Queries")

# test_queries = {
#     "1. Test Basic Connectivity": """
# PREFIX me_egad: <http://w3id.org/sawgraph/v1/me-egad#>

# SELECT (COUNT(*) as ?count)
# WHERE {
#   ?obs a me_egad:EGAD-PFAS-Observation .
# }
# LIMIT 1
# """,
    
#     "2. List Available Services": """
# # This might not work on all endpoints
# SELECT DISTINCT ?service
# WHERE {
#   ?service a <http://www.w3.org/ns/sparql-service-description#Service> .
# }
# LIMIT 10
# """,
    
#     "3. Sample Observations (10)": """
# PREFIX me_egad: <http://w3id.org/sawgraph/v1/me-egad#>
# PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
# PREFIX sosa: <http://www.w3.org/ns/sosa/>

# SELECT ?obs ?substance ?sp
# WHERE {
#   GRAPH <http://w3id.org/sawgraph/v1/me-egad-data#Observations> {
#     ?obs a me_egad:EGAD-PFAS-Observation ;
#          coso:ofDatasetSubstance ?substance ;
#          sosa:hasFeatureOfInterest ?sp .
#   }
# }
# LIMIT 10
# """,
    
#     "4. Test Spatial SERVICE": """
# PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
# PREFIX geo: <http://www.opengis.net/ont/geosparql#>
# PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

# SELECT ?county ?name
# WHERE {
#   SERVICE <repository:Spatial> {
#     ?county geo:hasGeometry ?geom ;
#             rdfs:label ?name .
#   }
# }
# LIMIT 10
# """,
    
#     "5. Test FIO SERVICE (Facilities)": """
# PREFIX fio: <http://w3id.org/fio/v1/fio#>
# PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

# SELECT ?facility ?name ?industry
# WHERE {
#   SERVICE <repository:FIO> {
#     ?facility fio:ofIndustry ?industry ;
#               rdfs:label ?name .
#   }
# }
# LIMIT 10
# """,
    
#     "6. Test Hydrology SERVICE": """
# PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>
# PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

# SELECT ?flowpath
# WHERE {
#   SERVICE <repository:Hydrology> {
#     ?flowpath rdf:type hyf:HY_FlowPath .
#   }
# }
# LIMIT 10
# """,
    
#     "7. Federated Query (Samples + Facilities)": """
# PREFIX me_egad: <http://w3id.org/sawgraph/v1/me-egad#>
# PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
# PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
# PREFIX fio: <http://w3id.org/fio/v1/fio#>

# SELECT ?obs ?facility
# WHERE {
#   # Get observations from SAWGraph
#   GRAPH <http://w3id.org/sawgraph/v1/me-egad-data#Observations> {
#     ?obs a me_egad:EGAD-PFAS-Observation .
#   }
  
#   # Get facilities from FIO in the same query!
#   SERVICE <repository:FIO> {
#     ?facility fio:ofIndustry ?industry .
#   }
# }
# LIMIT 5
# """
# }

# # Query selector
# selected_query_name = st.sidebar.selectbox(
#     "Choose a test query:",
#     list(test_queries.keys())
# )

# if st.sidebar.button("📋 Load Selected Query", use_container_width=True):
#     st.session_state.current_query = test_queries[selected_query_name]

# st.sidebar.markdown("---")
# st.sidebar.info("""
# 💡 **Tips:**
# - Start with Test 1 to verify connectivity
# - Try each SERVICE individually (4, 5, 6)
# - Then try federated query (7)
# - Modify queries and experiment!
# """)

# # Main query editor
# st.subheader("✏️ SPARQL Query Editor")

# # Initialize query in session state
# if 'current_query' not in st.session_state:
#     st.session_state.current_query = test_queries["1. Test Basic Connectivity"]

# # Query text area
# query = st.text_area(
#     "Enter your SPARQL query:",
#     value=st.session_state.current_query,
#     height=300,
#     help="Write or modify your SPARQL query here"
# )

# # Store the query
# st.session_state.current_query = query

# # Execute button
# col1, col2, col3 = st.columns([1, 1, 2])
# with col1:
#     execute_button = st.button("🚀 Execute Query", type="primary", use_container_width=True)
# with col2:
#     if st.button("🔄 Clear Results", use_container_width=True):
#         if 'query_results' in st.session_state:
#             del st.session_state.query_results

# # Execute query
# if execute_button:
#     with st.spinner("Executing query..."):
#         success, df, query_time, num_results, raw_results = execute_test_query(query)
        
#         if success:
#             st.session_state.query_results = {
#                 'df': df,
#                 'time': query_time,
#                 'count': num_results,
#                 'raw': raw_results
#             }
#         else:
#             st.error(f"❌ Query failed!")
#             st.code(df, language="text")  # df contains error message in this case

# # Display results
# if 'query_results' in st.session_state:
#     results = st.session_state.query_results
    
#     st.markdown("---")
#     st.subheader("📊 Query Results")
    
#     # Summary metrics
#     col1, col2, col3 = st.columns(3)
#     col1.metric("Execution Time", f"{results['time']:.2f}s")
#     col2.metric("Results Returned", results['count'])
#     col3.metric("Columns", len(results['df'].columns) if not results['df'].empty else 0)
    
#     # Display DataFrame
#     if not results['df'].empty:
#         st.success(f"✅ Query successful! Returned {len(results['df'])} rows")
        
#         # Show DataFrame
#         st.dataframe(results['df'], use_container_width=True)
        
#         # Download option
#         csv = results['df'].to_csv(index=False)
#         st.download_button(
#             label="📥 Download Results (CSV)",
#             data=csv,
#             file_name="query_results.csv",
#             mime="text/csv"
#         )
        
#         # Show column info
#         with st.expander("📋 Column Information"):
#             st.write("**Columns in result:**")
#             for col in results['df'].columns:
#                 st.write(f"- `{col}` ({results['df'][col].dtype})")
#                 # Show sample values
#                 sample_vals = results['df'][col].head(3).tolist()
#                 st.caption(f"  Sample values: {sample_vals}")
        
#         # Show raw JSON
#         with st.expander("🔍 Raw JSON Response"):
#             st.json(results['raw'])
#     else:
#         st.warning("⚠️ Query returned 0 results")
#         st.info("The query executed successfully but found no matching data.")
        
#         # Show raw response
#         with st.expander("🔍 Raw JSON Response"):
#             st.json(results['raw'])

# # Information
# with st.expander("ℹ️ About Federation Queries"):
#     st.markdown("""
#     ### RENCI Federation Endpoint
    
#     **URL:** `https://frink.apps.renci.org/federation/sparql`
    
#     ### Available Services
    
#     Use `SERVICE` clauses to query different repositories:
    
#     ```sparql
#     SERVICE <repository:SAWGraph> {
#       # PFAS observations, samples, results
#     }
    
#     SERVICE <repository:Spatial> {
#       # S2 cells, administrative regions
#     }
    
#     SERVICE <repository:Hydrology> {
#       # Water flow networks
#     }
    
#     SERVICE <repository:FIO> {
#       # Facilities and industries
#     }
#     ```
    
#     ### Named Graphs (SAWGraph)
    
#     Within SAWGraph service, data is in named graphs:
#     - `<http://w3id.org/sawgraph/v1/me-egad-data#Observations>`
#     - `<http://w3id.org/sawgraph/v1/me-egad-data#Samples>`
#     - `<http://w3id.org/sawgraph/v1/me-egad-data#SamplingPoints>`
#     - `<http://w3id.org/sawgraph/v1/me-egad-data#Facilities>`
    
#     ### Tips
    
#     1. **Start simple** - Test each SERVICE separately first
#     2. **Check what works** - Not all services may be available
#     3. **Look at structure** - Examine column names and values
#     4. **Build up complexity** - Combine services once you understand each
#     """)



"""
SPARQL Query Playground
Test and explore different queries on the RENCI Federation endpoint
"""

import time
import pandas as pd
import streamlit as st
from SPARQLWrapper import SPARQLWrapper, JSON, GET, POST

# -----------------------------
# Page configuration
# -----------------------------
st.set_page_config(
    page_title="Query Playground",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🧪 SPARQL Query Playground")
st.markdown("Run SPARQL against the **RENCI Federation** and explore results.")

# -----------------------------
# FRINK federation settings
# -----------------------------
FRINK_BASE = "https://frink.apps.renci.org/federation/sparql"
REPOSITORIES = ["SAWGraph", "Spatial", "Hydrology", "FIO"]  # Must match FRINK config

st.sidebar.header("🧭 Federation Scope")
selected_repos = st.sidebar.multiselect(
    "Include repositories (sources[] parameter)",
    REPOSITORIES,
    default=["SAWGraph"],  # start with SAWGraph; add others as needed
    help="This constrains which backends the federation queries. Most SAWGraph PFAS queries only need SAWGraph."
)

st.sidebar.markdown("---")

# -----------------------------
# Helper: execute query
# -----------------------------
def execute_test_query(query_string: str, repos: list[str]):
    """
    Execute a SPARQL query against the FRINK federation endpoint,
    constraining the scope with sources[]=... for each selected repository.
    """
    try:
        sources = repos if repos else ["federation"]

        # Auto POST for long queries (safer with proxies; avoids URL length limits)
        method = POST if len(query_string) > 1800 else GET

        sparql = SPARQLWrapper(FRINK_BASE)
        sparql.setMethod(method)
        sparql.setReturnFormat(JSON)
        sparql.setTimeout(120)
        sparql.setQuery(query_string)
        for source in sources:
            sparql.addParameter("sources[]", source)

        start = time.time()
        results = sparql.query()
        elapsed = time.time() - start

        json_results = results.convert()

        # Normalize results to a DataFrame
        bindings = json_results.get("results", {}).get("bindings", [])
        rows = []
        for b in bindings:
            row = {var: val.get("value") for var, val in b.items()}
            rows.append(row)
        df = pd.DataFrame(rows)

        return True, df, elapsed, len(bindings), json_results

    except Exception as e:
        return False, str(e), 0.0, 0, str(e)

# -----------------------------
# Pre-built example queries
# -----------------------------
test_queries = {
    "1) SAWGraph • Count PFAS Observations": """
PREFIX me_egad: <http://w3id.org/sawgraph/v1/me-egad#>

SELECT (COUNT(*) AS ?n)
WHERE {
  GRAPH <http://w3id.org/sawgraph/v1/me-egad-data#Observations> {
    ?obs a me_egad:EGAD-PFAS-Observation .
  }
}
""",
    "2) SAWGraph • Sample PFAS Observations (10)": """
PREFIX me_egad: <http://w3id.org/sawgraph/v1/me-egad#>
PREFIX coso:   <http://w3id.org/coso/v1/contaminoso#>
PREFIX sosa:   <http://www.w3.org/ns/sosa/>

SELECT ?obs ?substance ?sp
WHERE {
  GRAPH <http://w3id.org/sawgraph/v1/me-egad-data#Observations> {
    ?obs a me_egad:EGAD-PFAS-Observation ;
         coso:ofDatasetSubstance ?substance ;
         sosa:hasFeatureOfInterest ?sp .
  }
}
LIMIT 10
""",
    "3) Spatial SERVICE • List a few administrative regions": """
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX geo:  <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?county ?name
WHERE {
  SERVICE <repository:Spatial> {
    ?county geo:hasGeometry ?geom ;
            rdfs:label ?name .
  }
}
LIMIT 10
""",
    "4) FIO SERVICE • Example facilities": """
PREFIX fio:  <http://w3id.org/fio/v1/fio#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

# Ensure 'FIO' is checked in the sidebar
SELECT ?facility ?name ?industry
WHERE {
  SERVICE <repository:FIO> {
    ?facility fio:ofIndustry ?industry ;
              rdfs:label ?name .
  }
}
LIMIT 10
""",
    "5) Hydrology SERVICE • Flow paths": """
PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

# Ensure 'Hydrology' is checked in the sidebar
SELECT ?flowpath
WHERE {
  SERVICE <repository:Hydrology> {
    ?flowpath rdf:type hyf:HY_FlowPath .
  }
}
LIMIT 10
""",
    "6) Federated • SAWGraph observations + FIO facilities (toy)": """
PREFIX me_egad: <http://w3id.org/sawgraph/v1/me-egad#>
PREFIX fio:     <http://w3id.org/fio/v1/fio#>

# Check both 'SAWGraph' and 'FIO' in the sidebar
SELECT ?someObs ?someFacility
WHERE {
  GRAPH <http://w3id.org/sawgraph/v1/me-egad-data#Observations> {
    ?someObs a me_egad:EGAD-PFAS-Observation .
  }

  SERVICE <repository:FIO> {
    ?someFacility fio:ofIndustry ?industry .
  }
}
LIMIT 5
"""
}

# -----------------------------
# Query picker and editor
# -----------------------------
st.sidebar.header("📚 Sample Queries")
selected_query_name = st.sidebar.selectbox(
    "Choose an example:",
    list(test_queries.keys())
)

if st.sidebar.button("📋 Load Selected Query", use_container_width=True):
    st.session_state.current_query = test_queries[selected_query_name]

st.sidebar.info("""
Tips:
- Start with **SAWGraph** examples (1–2).
- Enable **Spatial/FIO/Hydrology** and run the corresponding SERVICE queries (3–5).
- Try a federated example (6) once basics work.
""")

st.subheader("✏️ SPARQL Query Editor")

if 'current_query' not in st.session_state:
    st.session_state.current_query = test_queries["1) SAWGraph • Count PFAS Observations"]

query = st.text_area(
    "Enter your SPARQL query:",
    value=st.session_state.current_query,
    height=300,
    help="Write or modify your SPARQL query here."
)
st.session_state.current_query = query

# -----------------------------
# Execute / Clear controls
# -----------------------------
col1, col2, col3 = st.columns([1, 1, 2])
with col1:
    run_clicked = st.button("🚀 Execute Query", type="primary", use_container_width=True)
with col2:
    clear_clicked = st.button("🔄 Clear Results", use_container_width=True)

if clear_clicked and 'query_results' in st.session_state:
    del st.session_state['query_results']

# -----------------------------
# Run the query
# -----------------------------
if run_clicked:
    with st.spinner("Executing query against the RENCI federation…"):
        ok, df_or_err, secs, nrows, raw = execute_test_query(query, selected_repos)

        if ok:
            st.session_state['query_results'] = {
                "df": df_or_err,
                "secs": secs,
                "nrows": nrows,
                "raw": raw
            }
        else:
            # Clear old results on failure
            if 'query_results' in st.session_state:
                del st.session_state['query_results']
            st.error("❌ Query failed.")
            st.code(df_or_err, language="text")  # df_or_err contains error text

# -----------------------------
# Show results
# -----------------------------
if 'query_results' in st.session_state:
    res = st.session_state['query_results']
    st.markdown("---")
    st.subheader("📊 Query Results")

    c1, c2, c3 = st.columns(3)

    secs_value = res.get('secs')
    if isinstance(secs_value, (int, float)):
        secs_label = f"{secs_value:.2f} s"
    else:
        secs_label = "—"
    c1.metric("Execution Time", secs_label)

    nrows_value = res.get('nrows')
    c2.metric("Rows", nrows_value if isinstance(nrows_value, (int, float)) else "—")

    df_result = res.get('df', pd.DataFrame())
    num_cols = 0 if df_result.empty else len(df_result.columns)
    c3.metric("Columns", num_cols)
    
    if df_result.empty:
        st.warning("Query returned **0 rows**. The query executed correctly but matched no data.")
    else:
        st.success(f"✅ Returned {len(df_result)} rows")
        st.dataframe(df_result, use_container_width=True)

        # Download
        csv = df_result.to_csv(index=False)
        st.download_button(
            label="📥 Download Results (CSV)",
            data=csv,
            file_name="query_results.csv",
            mime="text/csv",
            use_container_width=True
        )

        with st.expander("📋 Column Information"):
            st.write("**Columns:**")
            for col in df_result.columns:
                st.write(f"- `{col}` — dtype: {df_result[col].dtype}")
                st.caption(f"  Examples: {df_result[col].head(3).tolist()}")

    with st.expander("🔍 Raw JSON Response"):
        st.json(res.get('raw', {}))

# -----------------------------
# Federation help
# -----------------------------
with st.expander("ℹ️ About the RENCI Federation & SERVICE usage "):
    st.markdown("""
**Endpoint:** `https://frink.apps.renci.org/federation/sparql`  
**Scope control:** This app appends `sources[]=` for each selected repository, e.g.  
`sources[]=SAWGraph&sources[]=FIO` — so FRINK knows which backends to query.

**Typical usage:**
- SAWGraph data lives in named graphs (e.g., `<http://w3id.org/sawgraph/v1/me-egad-data#Observations>`).
- Spatial/Hydrology/FIO are accessed with `SERVICE <repository:Spatial|Hydrology|FIO>` blocks (be sure the repo is selected in the sidebar).
- Use **POST** automatically for long queries (handled here).

If a query returns 0 rows, try:
1. Limiting to a single backend (e.g., just SAWGraph),
2. Removing filters or lowering LIMITs,
3. Testing the corresponding SERVICE with a very simple pattern first.
""")
