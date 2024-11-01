import os
import pandas as pd
import glob

# Base directory where the CSV files are stored
base_directory = 'F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\CommodityDataAnalysisProject'

# Define the years you want to process
# years = ['2023','2022','2021','2020','2019','2018','2017','2016','2015','2014','2013','2012','2011','2010','2009','2008','2007','2006','2005','2004','2003']  # Add more
years = ['2024'] 
# Define the commodities you want to process
commodities = ['Tomato']

# Define the market you want to filter
market_name = 'Azadpur'  # Specify the market name you want

# Load the calendar CSV file
calendar_path = os.path.join(base_directory, 'calendar.csv')
calendar_df = pd.read_csv(calendar_path)

# Optimize calendar data types
calendar_df['Date_Key'] = calendar_df['Date_Key'].astype('int32')

# Define the output filename for the single aggregated CSV file
output_filename = f'aggregated_daily_data_{market_name}_{commodities[0]}_commodity2024.csv'
output_path = os.path.join(base_directory, output_filename)

# List to store results for incremental concatenation
results = []

# Loop through the specified years
for year in years:
    # Construct the path for the year folder
    year_folder_path = os.path.join(base_directory, 'Silver', year)
    
    # Recursively find all CSV files in the year folder
    csv_files = glob.glob(os.path.join(year_folder_path, '**', '*.csv'), recursive=True)
    
    # Process each CSV file incrementally
    for file in csv_files:
        for chunk in pd.read_csv(file, chunksize=500000):  # Process in chunks of 500,000 rows
            # Ensure 'Arrival_Date' column is in datetime format
            chunk['Arrival_Date'] = pd.to_datetime(chunk['Arrival_Date'])
            
            # Filter data for the specified commodities and market
            chunk = chunk.query("Commodity in @commodities and Market == @market_name")
            
            # Merge with calendar data to get the correct date key
            merged_chunk = pd.merge(chunk, calendar_df, left_on='Arrival_Date_Key', right_on='Date_Key', how='left')
            
            # Group by year, Arrival_Date, State, District, Market, Commodity, and Variety for daily aggregation
            daily_chunk = merged_chunk.groupby([
                'year', 'Arrival_Date', 'State', 'District', 'Market', 'Commodity', 'Variety', 'Grade'
            ]).agg({
                'Min_Price': 'mean',
                'Max_Price': 'mean',
                'Modal_Price': 'mean'
            }).reset_index()
            
            # Append the result for this chunk to the results list
            results.append(daily_chunk)

# Concatenate all chunks into a single DataFrame
final_data = pd.concat(results, ignore_index=True)

# Write the aggregated data to the CSV file
final_data.to_csv(output_path, index=False)

print(f"Aggregated CSV file generated successfully for {market_name}.")
