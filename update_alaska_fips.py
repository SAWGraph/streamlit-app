
import pandas as pd
import csv

# Raw Alaska data from Census Bureau
alaska_data = """AK,02,013,Aleutians East Borough,01598,Aleutians East census subarea,S
AK,02,016,Aleutians West Census Area,01615,Aleutians West census subarea,S
AK,02,020,Anchorage Municipality,03050,Anchorage census subarea,S
AK,02,050,Bethel Census Area,03580,Aniak census subarea,S
AK,02,050,Bethel Census Area,45510,Lower Kuskokwim census subarea,S
AK,02,060,Bristol Bay Borough,09050,Bristol Bay census subarea,S
AK,02,068,Denali Borough,18765,Denali census subarea,S
AK,02,070,Dillingham Census Area,19000,Dillingham census subarea,S
AK,02,090,Fairbanks North Star Borough,24450,Fairbanks North Star census subarea,S
AK,02,100,Haines Borough,31160,Haines census subarea,S
AK,02,105,Hoonah-Angoon Census Area,33390,Hoonah-Angoon census subarea,S
AK,02,105,Hoonah-Angoon Census Area,40560,Klukwan census subarea,S
AK,02,110,Juneau City and Borough,36450,Juneau census subarea,S
AK,02,122,Kenai Peninsula Borough,38460,Kenai-Cook Inlet census subarea,S
AK,02,122,Kenai Peninsula Borough,68610,Seward-Hope census subarea,S
AK,02,130,Ketchikan Gateway Borough,39010,Ketchikan census subarea,S
AK,02,150,Kodiak Island Borough,41200,Kodiak Island census subarea,S
AK,02,164,Lake and Peninsula Borough,42780,Lake and Peninsula census subarea,S
AK,02,170,Matanuska-Susitna Borough,47440,Matanuska-Susitna census subarea,S
AK,02,180,Nome Census Area,54970,Nome census subarea,S
AK,02,185,North Slope Borough,55970,North Slope census subarea,S
AK,02,188,Northwest Arctic Borough,56270,Northwest Arctic census subarea,S
AK,02,195,Petersburg Census Area,60360,Petersburg census subarea,S
AK,02,198,Prince of Wales-Hyder Census Area,34575,Hyder census subarea,S
AK,02,198,Prince of Wales-Hyder Census Area,48873,Metlakatla Indian Community census subarea,S
AK,02,198,Prince of Wales-Hyder Census Area,64310,Prince of Wales census subarea,S
AK,02,220,Sitka City and Borough,70590,Sitka census subarea,S
AK,02,230,Skagway Municipality,70810,Skagway census subarea,S
AK,02,240,Southeast Fairbanks Census Area,72030,Southeast Fairbanks census subarea,S
AK,02,261,Valdez-Cordova Census Area,14420,Chugach census subarea,S
AK,02,261,Valdez-Cordova Census Area,17350,Copper River census subarea,S
AK,02,270,Wade Hampton Census Area,82700,Wade Hampton census subarea,S
AK,02,275,Wrangell City and Borough,86420,Wrangell census subarea,S
AK,02,282,Yakutat City and Borough,86498,Yakutat census subarea,S
AK,02,290,Yukon-Koyukuk Census Area,42080,Koyukuk-Middle Yukon census subarea,S
AK,02,290,Yukon-Koyukuk Census Area,46060,McGrath-Holy Cross census subarea,S
AK,02,290,Yukon-Koyukuk Census Area,86690,Yukon Flats census subarea,S"""

csv_path = "/Users/hashimniane/Project Dev/streamlit/us_administrative_regions_fips.csv"

# Parse the data
new_rows = []
processed_counties = set()

lines = alaska_data.split('\n')
for line in lines:
    if not line.strip():
        continue
    parts = line.split(',')
    # Format: State, StateFP, CountyFP, CountyName, CousubFP, CousubName, ClassFP
    state_fp = parts[1]
    county_fp = parts[2]
    county_name = parts[3]
    cousub_fp = parts[4]
    cousub_name = parts[5]
    
    # 1. Add County Level Entry (if not already added)
    county_full_fips = f"{state_fp}{county_fp}"
    if county_full_fips not in processed_counties:
        # Format: "CountyName, Alaska",FIPS
        new_rows.append([f"{county_name}, Alaska", county_full_fips])
        processed_counties.add(county_full_fips)
        
    # 2. Add Subdivision Level Entry
    # Format: "Geometry of SubdivisionName, CountyName, Alaska",FIPS
    subdivision_full_fips = f"{state_fp}{county_fp}{cousub_fp}"
    label = f"Geometry of {cousub_name}, {county_name}, Alaska"
    new_rows.append([label, subdivision_full_fips])

# Append to CSV
with open(csv_path, 'a', newline='') as f:
    writer = csv.writer(f)
    for row in new_rows:
        writer.writerow(row)

print(f"Successfully appended {len(new_rows)} rows to {csv_path}")
