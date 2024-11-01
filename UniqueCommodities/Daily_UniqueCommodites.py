import pandas as pd
import os
import glob
from datetime import datetime
from UniqueCommoditiesSqlTransfer import unique_commodities_SqlTransfer

def check_for_new_commodity(date):
    """
    This function processes commodity data files for a specific date and updates the unique commodities CSV
    if new unique commodities are found.
    
    :param date: A datetime object representing the date for the file to be processed.
    """
    # Define the root folder where daily commodity data is stored
    root_folder = 'F://Education//COLLEGE//PROGRAMING//Python//PROJECTS//CommodityDataAnalysisProject//Bronze//'
    # Format year, month, day for folder structure
    year = date.strftime("%Y")
    month = str(date.month).zfill(1)  # Two-digit format
    day = str(date.day).zfill(1)      # Two-digit format

    # Path pattern to match CSV files in the daily folder
    file_pattern = os.path.join(root_folder, year, month, day, '*.csv')  # Match any CSV files
    daily_files = glob.glob(file_pattern, recursive=True)

    if not daily_files:
        print(f"No files found for {date.strftime('%Y-%m-%d')}.")
        return

    # Load existing commodity details CSV if it exists
    output_file = 'F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\CommodityDataAnalysisProject\\Dim_CommodityDetails.csv'
    
    try:
        # Load existing commodities if the file exists
        existing_commodities = pd.read_csv(output_file)
    except FileNotFoundError:
        # If the file doesn't exist, initialize an empty DataFrame
        existing_commodities = pd.DataFrame(columns=['commodity_id', 'commodity_name', 'commodity_variety', 'commodity_grade'])

    # Process each daily file found
    all_new_data = []
    
    for file_path in daily_files:
        print(f"Processing file: {file_path}")
        try:
            # Read the CSV file into a DataFrame
            df = pd.read_csv(file_path)

            # Extract unique commodity data from this file
            if 'Commodity' in df.columns and 'Variety' in df.columns and 'Grade' in df.columns:
                unique_commodities = df[['Commodity', 'Variety', 'Grade']].drop_duplicates()

                # Rename columns to match output CSV format
                unique_commodities.rename(columns={
                    'Commodity': 'commodity_name',
                    'Variety': 'commodity_variety',
                    'Grade': 'commodity_grade'
                }, inplace=True)

                # Check for new unique commodities that are not in the existing commodity details
                new_commodities = unique_commodities[~unique_commodities[['commodity_name', 'commodity_variety', 'commodity_grade']]
                                                     .apply(tuple, axis=1)
                                                     .isin(existing_commodities[['commodity_name', 'commodity_variety', 'commodity_grade']]
                                                           .apply(tuple, axis=1))]

                if not new_commodities.empty:
                    print(f"Found {len(new_commodities)} new commodities.")
                    all_new_data.append(new_commodities)

        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    if all_new_data:
        # Concatenate all new unique commodities and append them to the existing file
        new_commodities_combined = pd.concat(all_new_data, ignore_index=True)

        # Add new commodity IDs
        new_commodity_ids = range(existing_commodities['commodity_id'].max() + 1, 
                                  existing_commodities['commodity_id'].max() + 1 + len(new_commodities_combined))
        new_commodities_combined['commodity_id'] = new_commodity_ids

        # Append new commodities to the existing file and save
        updated_commodities = pd.concat([existing_commodities, new_commodities_combined], ignore_index=True)

        updated_commodities.to_csv(output_file, index=False)
        print(f"Updated commodity details saved to '{output_file}'.")
        unique_commodities_SqlTransfer()

    else:
        print("No new unique commodities found.")

# # Example usage
# if __name__ == "__main__":
#     # Replace with the date you want to process
#     process_date = datetime(2024, 10, 7)  # Example: Oct 11, 2024
#     check_for_new_commodity(process_date)
