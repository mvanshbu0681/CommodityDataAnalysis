import pandas as pd
import glob
import os

# Define the root folder where historical commodity data is stored
root_folder = 'F://Education//COLLEGE//PROGRAMING//Python//PROJECTS//CommodityDataAnalysisProject//Bronze//'

def process_historical_commodities():
    """
    This function processes all historical CSV files to extract and update unique commodities across all files.
    """
    # Use glob to match all CSV files in the format year/month/day/*.csv
    all_files = glob.glob(os.path.join(root_folder, '*', '*', '*', '*.csv'), recursive=True)

    # Empty list to store all commodity data from different files
    all_data = []

    # Process each file
    for file_path in all_files:
        print(f"Processing file: {file_path}")
        try:
            # Read the CSV file into a DataFrame
            df = pd.read_csv(file_path)

            # Extract relevant commodity columns and drop duplicates
            if 'Commodity' in df.columns and 'Variety' in df.columns and 'Grade' in df.columns:
                unique_commodities = df[['Commodity', 'Variety', 'Grade']].drop_duplicates()
                all_data.append(unique_commodities)

        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    if all_data:
        # Combine all unique commodities into a single DataFrame
        combined_data = pd.concat(all_data, ignore_index=True).drop_duplicates().reset_index(drop=True)

        # Rename columns to match desired format
        combined_data.rename(columns={
            'Commodity': 'commodity_name',
            'Variety': 'commodity_variety',
            'Grade': 'commodity_grade'
        }, inplace=True)

        # Add a unique commodity_id column
        combined_data['commodity_id'] = range(1, len(combined_data) + 1)

        # Reorder columns to have commodity_id first
        combined_data = combined_data[['commodity_id', 'commodity_name', 'commodity_variety', 'commodity_grade']]

        # Save the unique commodities to a CSV file
        output_file = 'F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\CommodityDataAnalysisProject\\Dim_CommodityDetails.csv'
        combined_data.to_csv(output_file, index=False)

        print(f"Script has completed successfully. The unique commodities with IDs are saved in '{output_file}'.")
    else:
        print("No valid commodity data found.")

# Example usage for historical data processing
if __name__ == "__main__":
    process_historical_commodities()
