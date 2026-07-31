import pandas as pd
import requests
import time

# create a new DataFrame
all_years_data = pd.DataFrame()

# loop through the years and fetch data for each year
years = range(2014, 2025) 

for year in years:
    print(f"Fetching population data for year {year}...")
    try:
        # ACS 5-Year API 
        url = f"https://api.census.gov/data/{year}/acs/acs5?get=NAME,B01003_001E&for=zip%20code%20tabulation%20area:*"
        
        response = requests.get(url)
        
        # if the request is successful, process the data
        if response.status_code == 200:
            data = response.json()
            # convert to DataFrame
            df_year = pd.DataFrame(data[1:], columns=data[0])
            
            # clean and rename columns
            df_year = df_year.rename(columns={'B01003_001E': 'Population', 'zip code tabulation area': 'ZIP_Code'})
            df_year['ZIP_Code'] = df_year['NAME'].str.replace('ZCTA5 ', '')
            df_year['Year'] = year 
            
            # keep only relevant columns
            df_year = df_year[['ZIP_Code', 'Year', 'Population']]
            
            # merge with the main DataFrame
            all_years_data = pd.concat([all_years_data, df_year], ignore_index=True)
            
    except Exception as e:
        print(f"Could not fetch data for {year}: {e}")
    
    # pause to avoid hitting API rate limits
    time.sleep(1)

# convert Population to numeric and filter out invalid entries
all_years_data['Population'] = pd.to_numeric(all_years_data['Population'], errors='coerce')
all_years_data = all_years_data.dropna(subset=['Population'])
all_years_data = all_years_data[all_years_data['Population'] > 0]

# export as the final CSV
all_years_data.to_csv('historical_zip_population_10yrs.csv', index=False)
print("Data fetching complete. Saved to historical_zip_population_10yrs.csv")