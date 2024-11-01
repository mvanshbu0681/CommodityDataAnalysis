import pandas as pd
import glob
import os

# Define the root folder where historical data is stored
root_folder = 'F://Education//COLLEGE//PROGRAMING//Python//PROJECTS//CommodityDataAnalysisProject//Bronze//'

# Use glob to match all CSV files in the format year/month/day/commoditydata_*.csv
all_files = glob.glob(os.path.join(root_folder, '*', '*', '*', 'commoditydata_*.csv'), recursive=True)

# Empty list to store all market data from different files
all_data = []

# Loop through each file and extract unique markets
for file_path in all_files:
    print(f"Processing file: {file_path}")
    
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        
        # Extract unique market data from this file
        if 'Market' in df.columns and 'District' in df.columns and 'State' in df.columns:
            unique_markets = df[['Market', 'District', 'State']].drop_duplicates()
            all_data.append(unique_markets)
    
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

# Combine all unique markets into a single DataFrame
if all_data:
    combined_data = pd.concat(all_data, ignore_index=True).drop_duplicates().reset_index(drop=True)

    combined_data.rename(columns={
        'State': 'market_state',
        'District': 'market_district',
        'Market': 'market_name'
    }, inplace=True)

    # Add a unique market_id column
    combined_data['market_id'] = range(1, len(combined_data) + 1)

    # Reorder the columns to have market_id first
    combined_data = combined_data[['market_id', 'market_name', 'market_district', 'market_state']]

    # Print the count of unique markets
    print(f"Count of unique markets: {len(combined_data)}")

    # Save the combined unique markets to a CSV file
    output_file = 'F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\CommodityDataAnalysisProject\\Dim_MarketDetails.csv'
    combined_data.to_csv(output_file, index=False)

    print(f"Script has completed successfully. The unique markets with IDs are saved in '{output_file}'.")
else:
    print("No valid market data found.")
