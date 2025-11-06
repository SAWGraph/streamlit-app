"""
SAWGraph PFAS Analysis - Home Page
Multi-page Streamlit application for analyzing PFAS contamination data
"""

import streamlit as st
from utils.sparql_helpers import test_connection, ENDPOINTS

# Page configuration
st.set_page_config(
    page_title="SAWGraph PFAS Analysis",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Header
st.title("🌊 SAWGraph PFAS Analysis Suite")
st.markdown("""
Welcome to the SAWGraph PFAS Analysis application. Analyze PFAS contamination data, 
trace potential sources, and identify facilities using integrated knowledge graphs.
""")

st.info("👈 **Get Started:** Click on **PFAS Analysis** in the sidebar, then select your analysis type from the dropdown.")

# Overview section  
st.markdown("")  # Add spacing
col1, col2 = st.columns(2)

with col1:
    st.subheader("📊 What You Can Do")
    st.markdown("""
    **Current Features:**
    - 🌊 **Upstream Tracing**: Find contamination sources through water flow analysis
    - 🗺️ **Interactive Maps**: Visualize contamination and facilities spatially
    - 📊 **Data Export**: Download results as CSV files
    - 🔧 **Flexible Filtering**: Filter by substance, region, material type, concentration
    
    **Coming Soon:**
    - Proximity analysis, regional statistics, facility risk scoring
    """)

with col2:
    st.subheader("🗄️ Data Sources")
    st.markdown("""
    This application queries multiple knowledge graphs:
    
    - **SAWGraph**: PFAS contamination observations
    - **Spatial KG**: Administrative boundaries
    - **Hydrology KG**: Water flow networks
    - **FIO KG**: Industrial facilities
    """)

st.markdown("---")

# Quick Start Guide
with st.expander("📖 How to Use This Application"):
    st.markdown("""
    ### 5 Simple Steps
    
    1. **Navigate** - Click **PFAS Analysis** in the sidebar
    2. **Select** - Choose your analysis type from the dropdown
    3. **Configure** - Set your parameters (substance, region, concentration, etc.)
    4. **Execute** - Click the "Execute Query" button
    5. **Analyze** - View results on interactive maps, download data as CSV
    
    ### Available Analysis Types
    
    **🌊 PFAS Upstream Tracing** *(Active)*
    - Identifies potential contamination sources through hydrological flow path analysis
    
    **🏭 Samples Near Facilities** *(Coming Soon)*
    - Analyzes contamination proximity to specific industry types
    
    **📊 Regional Overview** *(Coming Soon)*  
    - Provides statistical summary of contamination in a region
    
    **⚠️ Facility Risk Assessment** *(Coming Soon)*
    - Assesses facilities based on contamination risk factors
    """)

# About section
with st.expander("ℹ️ About This Application"):
    st.markdown("""
    ### SAWGraph: Semantic Analysis of Water
    
    This application leverages **SAWGraph**, a comprehensive knowledge graph integrating:
    - 🔬 PFAS contamination observations from EPA monitoring
    - 🗺️ Spatial relationships and administrative boundaries  
    - 💧 Hydrological flow networks (NHDPlus V2)
    - 🏭 Industrial facility data with NAICS classification
    
    **Technology Stack:**
    - **Backend**: SPARQL endpoints hosted by FRINK/RENCI
    - **Frontend**: Streamlit with Folium maps
    - **Data**: Real-time queries to federated knowledge graphs
    """)

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; padding: 30px; font-size: 14px;'>
    <b>SAWGraph PFAS Analysis Suite</b> | Built with Streamlit | Data from <a href="https://frink.apps.renci.org" style="color: #1f77b4; text-decoration: none;">FRINK/RENCI</a>
    </div>
    """,
    unsafe_allow_html=True
)
