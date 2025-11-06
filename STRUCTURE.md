# SAWGraph PFAS Analysis - Multi-Page App Structure

## 📁 Directory Structure

```
streamlit/
├── app.py                          # Home page - landing, navigation, connection tests
├── pages/                          # Analysis pages (auto-discovered by Streamlit)
│   ├── 1_🔍_PFAS_Upstream_Analysis.py   # Your original analysis
│   └── _template.py                # Template for new pages
├── utils/                          # Shared utilities
│   ├── __init__.py
│   └── sparql_helpers.py           # SPARQL connection & query helpers
├── requirements.txt
└── README.md
```

## 🚀 How to Run

```bash
streamlit run app.py
```

The app will open in your browser with:
- **Home page**: Overview, connection tests, quick start guide
- **Sidebar navigation**: Automatically includes all pages from `pages/` directory

## ➕ Adding New Query Pages

1. **Copy the template**:
   ```bash
   cp pages/_template.py "pages/2_🌊_Your_New_Analysis.py"
   ```

2. **File naming convention**:
   - Start with a number (for ordering): `1_`, `2_`, `3_`, etc.
   - Add an emoji (shows in sidebar): `🔍`, `🌊`, `🏭`, `📊`, etc.
   - Use underscores for spaces: `My_Analysis`
   - Example: `2_📊_Water_Quality_Trends.py`

3. **Edit the new file**:
   - Update the title and description
   - Add your parameters in the sidebar
   - Write your SPARQL query
   - Customize the results display

4. **Streamlit will automatically**:
   - Discover the new page
   - Add it to the sidebar navigation
   - Handle routing

## 🔧 Shared Utilities

### `utils/sparql_helpers.py`

Common functions available to all pages:

```python
from utils.sparql_helpers import get_sparql_wrapper, convertToDataframe

# Get a configured SPARQL wrapper
sparql = get_sparql_wrapper('sawgraph')  # or 'spatial', 'hydrology', 'fio'
sparql.setQuery(your_query)
result = sparql.query()

# Convert results to DataFrame
df = convertToDataframe(result)
```

### Available Endpoints

- `'sawgraph'`: PFAS contamination observations
- `'spatial'`: Administrative boundaries and spatial relationships  
- `'hydrology'`: Water flow networks (NHDPlus V2)
- `'fio'`: Industrial facilities (NAICS data)

## 📝 Page Template Structure

Every analysis page should have:

1. **Imports** (including utils)
2. **Page config** (`st.set_page_config()`)
3. **Title & description**
4. **Sidebar parameters**
5. **Run button** with query execution
6. **Results display** (maps, tables, charts)
7. **Export options** (CSV downloads)
8. **Info expander** (documentation)

## 🎨 Emoji Reference for Pages

- 🔍 Search/Query
- 🌊 Water/Hydrology
- 🏭 Facilities/Industrial
- 📊 Statistics/Charts
- 🗺️ Maps/Geographic
- 🔬 Analysis/Science
- 📈 Trends/Time Series
- 🎯 Targeted Analysis
- 🌡️ Measurements/Values
- ⚠️ Alerts/Warnings

## 💡 Tips

1. **Keep pages focused**: Each page should answer one specific question
2. **Reuse utilities**: Don't duplicate code - add shared functions to `utils/`
3. **Test connections**: Use the home page to verify SPARQL endpoints
4. **Progressive disclosure**: Use expanders for debug info and advanced options
5. **Clear feedback**: Show progress bars, status messages, and helpful errors

## 🔐 Credentials

Currently using DIGEST authentication with `setHTTPAuth(DIGEST)`.
If you need to add credentials:

```python
sparql = get_sparql_wrapper('sawgraph')
sparql.setCredentials('username', 'password')
```

Consider storing credentials in:
- Environment variables
- `.streamlit/secrets.toml` (don't commit to git!)
- External config file

