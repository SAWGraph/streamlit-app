# Region & Substance Selector Guide

## Overview
The Region & Substance Selector is a Streamlit page that provides cascading dropdown menus for selecting PFAS substances and US administrative regions (Substance → State → County → Subdivision).

## Files Created

1. **`pages/region_selector.py`** - The main Streamlit page with cascading dropdowns
2. **`pfas_substances.csv`** - Dataset containing 94 PFAS substances with short names and URIs
3. **`us_administrative_regions_fips.csv`** - Dataset containing 41,788 administrative regions with FIPS codes
4. **`get_substances.py`** - Script to regenerate the substances CSV from SPARQL endpoint
5. **`get_fips_codes.py`** - Script to regenerate the regions CSV from SPARQL endpoint

## How to Use

### Running the Application

```bash
cd "/Users/hashimniane/Project Dev/streamlit"
streamlit run pages/region_selector.py
```

Or if you're running the main app, it will appear as a page in the sidebar navigation.

### Using the Cascading Dropdowns

1. **Select PFAS Substance (Optional)**
   - Choose from 69 unique PFAS compounds
   - Example: "PFOS", "PFOA", "PFHPA", "PFBS"
   - Can leave as "-- All Substances --" to analyze all PFAS

2. **Select State (Required)** 
   - Choose from 51 US states and territories
   - Example: "Alabama", "Maine", "California", "Texas"

3. **Select County (Optional)**
   - Dropdown is filtered based on selected state
   - Shows counties that have subdivision data
   - Can leave as "-- All Counties --" to select the entire state

4. **Select Subdivision (Optional)**
   - Dropdown is filtered based on selected county
   - Shows census county divisions (CCDs) or other subdivisions
   - Can leave as "-- All Subdivisions --" to select the entire county

## Features

### Data Summary
- Shows total counts of PFAS substances, states, counties, and subdivisions
- Located at the top of the page
- 69 unique PFAS compounds available (94 total substance entries including variants)

### Selection Display
- Shows your current selection with:
  - PFAS substance name and URI (if selected)
  - State name and FIPS code
  - County name (if selected)
  - Subdivision name and FIPS code (if selected)
- Provides code snippets for using both substance and region in SPARQL queries

### Region Statistics
- When county is selected: Shows number of subdivisions
- When only state is selected: Shows all counties in that state
- Expandable tables with detailed information

### Export Options
- Download button to save selection as CSV
- File includes all selected region information

## FIPS Code Structure

- **2 digits**: State code (e.g., `23` = Maine)
- **5 digits**: County code (e.g., `23005` = Cumberland County, Maine)
- **10+ digits**: Subdivision code (e.g., `2300502655` = Baldwin town)

## Integration with Existing Code

To use the selected FIPS code in your SPARQL queries:

```python
# In the region selector, the FIPS code is displayed
regionCode = "23005"  # Example: Cumberland County, Maine

# Use in SPARQL query (like in working_version.py):
kwgr:administrativeRegion.USA.{regionCode}
```

## Example: Maine Data

- **State**: Maine (FIPS: 23)
- **Counties**: 16 counties with data (e.g., Cumberland County, Penobscot County)
- **Subdivisions**: 561 total subdivisions across all counties
  - Example: Cumberland County has 29 subdivisions (towns like Portland, Brunswick, etc.)

## Updating the Data

### Update PFAS Substances

If you need to refresh the substances data:

```bash
python get_substances.py
```

This will:
1. Query the federation SPARQL endpoint
2. Retrieve all PFAS substances from ContaminantObservations
3. Extract short names (e.g., PFOS, PFOA)
4. Save to `pfas_substances.csv`

### Update Administrative Regions

If you need to refresh the administrative regions data:

```bash
python get_fips_codes.py
```

This will:
1. Query the spatial SPARQL endpoint
2. Retrieve all US administrative regions
3. Save to `us_administrative_regions_fips.csv`

## Technical Details

### Data Processing
- Filters out duplicate "Geometry of" entries
- Handles county name matching for both formats
- Sorts all dropdowns alphabetically
- Uses Streamlit session state to maintain selections

### Performance
- Data is cached using `@st.cache_data`
- Fast filtering with pandas operations
- Responsive UI even with 41K+ entries

## Troubleshooting

### CSV File Not Found
If you see an error about missing CSV file:
1. Make sure `us_administrative_regions_fips.csv` is in the project root
2. Run `python get_fips_codes.py` to regenerate it

### No Counties Showing
Some states may have limited or no subdivision data in the knowledge graph. This is expected behavior.

### Dropdown Not Updating
Make sure you've selected a state before trying to select a county, and selected a county before trying to select a subdivision.

## Future Enhancements

Possible improvements:
- Add a map visualization of selected region
- Show population or area statistics
- Direct integration with the PFAS analysis workflow
- Ability to select multiple regions at once
- Save favorite regions for quick access

