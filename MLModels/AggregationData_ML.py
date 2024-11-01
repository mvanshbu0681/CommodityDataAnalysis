import os
import pandas as pd
import glob

# Base directory where the CSV files are stored
base_directory = 'F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\CommodityDataAnalysisProject'

# Define the years you want to process
years = ['2023', '2022','2021','2020','2019','2018']  # Add more years as needed

# Define the commodities you want to process
commodities = ['Tomato']  # Add more commodities as needed

# Define the markets you want to process
markets = ['Azadpur']  # Add more markets as needed

# Load the calendar CSV file
calendar_path = os.path.join(base_directory, 'calendar.csv')
calendar_df = pd.read_csv(calendar_path)

# Optimize calendar data types
calendar_df['Date_Key'] = calendar_df['Date_Key'].astype('int32')
calendar_df['week'] = calendar_df['week'].astype('int8')

# Define the output filename for the single aggregated CSV file
output_filename = 'aggregated_weekly_data_multiple_commodities_filtered_by_market.csv'
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
            chunk['Arrival_Date'] = pd.to_datetime(chunk['Arrival_Date'], errors='coerce')
            
            # Filter data for the specified commodities and markets
            chunk = chunk.query("Commodity in @commodities and Market in @markets")
            
            # Merge with calendar data to get the correct week numbers
            merged_chunk = pd.merge(chunk, calendar_df, left_on='Arrival_Date_Key', right_on='Date_Key', how='left')
            
            # Remove exact duplicates if any
            merged_chunk = merged_chunk.drop_duplicates()
            
            # Group by year, week, State, District, Market, Commodity, Variety, and Grade
            weekly_chunk = merged_chunk.groupby([
                'year', 'week', 'State', 'District', 'Market', 'Commodity'
            ], as_index=False).agg({
                'Modal_Price': 'mean'
            })
            
            # Append the result for this chunk to the results list
            results.append(weekly_chunk)

# Concatenate all chunks into a single DataFrame
final_data = pd.concat(results, ignore_index=True)

# Write the aggregated data to the CSV file
final_data.to_csv(output_path, index=False)

print("Aggregated weekly CSV file generated successfully for multiple commodities and specific markets.")
