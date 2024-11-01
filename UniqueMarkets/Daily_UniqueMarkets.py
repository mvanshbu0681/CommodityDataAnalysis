import pandas as pd
import glob
import os
from datetime import datetime
from UniqueMarketsSqlTransfer import unique_markets_SqlTransfer


def check_for_new_market(date):
   
    # Define the root folder where the data is stored
    root_folder = 'F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\CommodityDataAnalysisProject\\Bronze\\'
    year = date.strftime("%Y")
    month = str(date.month).zfill(1)  
    day = str(date.day).zfill(1)      

    daily_files = glob.glob(os.path.join(root_folder, year, month, day, 'commoditydata_*.csv'), recursive=True)
    if not daily_files:
        print(f"No files found for {date.strftime('%Y-%m-%d')}.")
        return
    
    # Load the existing market details CSV if it exists
    output_file = 'F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\CommodityDataAnalysisProject\\Dim_MarketDetails.csv'
    
    try:
        # Load existing data from the CSV to avoid re-adding duplicates
        existing_markets = pd.read_csv(output_file)
    except FileNotFoundError:
        # If the file doesn't exist, initialize an empty DataFrame
        existing_markets = pd.DataFrame(columns=['market_id', 'market_name', 'market_district', 'market_state'])

    # Process each daily file found
    all_new_data = []
    
    for file_path in daily_files:
        print(f"Processing file: {file_path}")
        try:
            # Read the CSV file into a DataFrame
            df = pd.read_csv(file_path)

            # Extract unique market data from this file
            if 'Market' in df.columns and 'District' in df.columns and 'State' in df.columns:
                unique_markets = df[['Market', 'District', 'State']].drop_duplicates()

                # Rename columns to match output CSV format
                unique_markets.rename(columns={
                    'State': 'market_state',
                    'District': 'market_district',
                    'Market': 'market_name'
                }, inplace=True)

                # Check for new unique markets that are not in the existing market details
                new_markets = unique_markets[~unique_markets[['market_name', 'market_district', 'market_state']]
                                             .apply(tuple, axis=1)
                                             .isin(existing_markets[['market_name', 'market_district', 'market_state']]
                                                   .apply(tuple, axis=1))]

                if not new_markets.empty:
                    print(f"Found {len(new_markets)} new markets.")

                    all_new_data.append(new_markets)

        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    if all_new_data:
        # Concatenate all new unique markets and append them to the existing file
        new_markets_combined = pd.concat(all_new_data, ignore_index=True)

        # Add new market IDs
        new_market_ids = range(existing_markets['market_id'].max() + 1, existing_markets['market_id'].max() + 1 + len(new_markets_combined))
        new_markets_combined['market_id'] = new_market_ids

        # Append new markets to the existing file and save
        updated_markets = pd.concat([existing_markets, new_markets_combined], ignore_index=True)

        updated_markets.to_csv(output_file, index=False)
        print(f"Updated market details saved to '{output_file}'.")
        unique_markets_SqlTransfer()
    else:
        print("No new unique markets found.")

# # Example usage
# if __name__ == "__main__":
#     # Replace with the date you want to process
#     process_date = datetime(2024, 10, 17)  # For example, Oct 11, 2024
#     process_daily_file(process_date)
