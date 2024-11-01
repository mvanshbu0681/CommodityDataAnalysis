import pandas as pd
import os
import glob
from datetime import datetime, timedelta

# Function to get dates from the MySQL tables

class Gold:
    # Function to process files from Silver to Gold with joins
    def data_transformation(date):
        path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/CommodityDataAnalysisProject'
        
        # Load unique markets and commodities CSVs
        unique_markets = pd.read_csv('F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\CommodityDataAnalysisProject\\Dim_MarketDetails.csv')
        unique_commodities = pd.read_csv('F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\CommodityDataAnalysisProject\\Dim_CommodityDetails.csv')
        
        # Increment start_date by 1 day before starting the processing
        
        year = date.strftime("%Y")
        month = str(date.month)
        day = str(date.day)
        
        silver_path = os.path.join(path, 'Silver', year, month, day)
        gold_path = os.path.join(path, 'Gold', year, month, day)
        
        if not os.path.exists(gold_path):
            os.makedirs(gold_path)

        csv_files = glob.glob(silver_path + "/*.csv")

        for file in csv_files:
            df = pd.read_csv(file)

            # Perform joins with unique markets and commodities
            df = pd.merge(df, unique_markets, left_on=['State', 'District', 'Market'], right_on=['market_state', 'market_district', 'market_name'], how='left')
            df = pd.merge(df, unique_commodities, left_on=['Commodity', 'Variety', 'Grade'], right_on=['commodity_name', 'commodity_variety', 'commodity_grade'], how='left')
            
            # df['Mean_Price'] = df[['Min_Price', 'Max_Price', 'Modal_Price']].mean(axis=1)

            # Remove redundant columns after joins
            df.drop(columns=['market_state' ,'State','District','Commodity','Market','Variety', 'Grade', 'market_district', 'market_name', 'commodity_name', 'commodity_variety', 'commodity_grade'], inplace=True)

            # Example transformations for Gold layer (e.g., calculating mean price)

            # Save the processed file to the Gold directory
            original_filename = os.path.basename(file).replace('Silver_', '')
            new_filename = f"Gold_{original_filename}"

            output_file = os.path.join(gold_path, new_filename)

            df.to_csv(output_file, index=False)

        print(f"Processed {len(csv_files)} files and saved to {gold_path}")


    # Get dates from the database
    # start_date, end_date = get_dates_from_db()

    # Process files from Silver to Gold with joins between the incremented start date and the end date
    # process_silver_to_gold_with_joins(start_date, end_date)
