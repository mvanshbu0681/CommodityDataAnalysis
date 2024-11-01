import pandas as pd
import os
import glob
from datetime import datetime, timedelta

# Function to get dates from the MySQL tables


class Silver: 
    # Function to process files within a date range
    def data_cleaning(date):
        path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/CommodityDataAnalysisProject'
        
        # Increment start_date by 1 day before starting the processing


        year = date.strftime("%Y")
        month = str(date.month)
        day = str(date.day)
        
        input_path = os.path.join(path, 'Bronze', year, month, day)
        output_path = os.path.join(path, 'Silver', year, month, day)
        
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        csv_files = glob.glob(input_path + "/*.csv")

        for file in csv_files:
            df = pd.read_csv(file)
            
            if 'Arrival_Date' in df.columns:
                df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'], format='%d/%m/%Y')
                df['Arrival_Date_String'] = df['Arrival_Date'].dt.strftime('%d-%b-%Y')
                df['Arrival_Date_Key'] = df['Arrival_Date'].dt.strftime('%Y%m%d').astype(int)
            
            # Display missing values after filling
            print("Missing values after filling with 0:")
            print(df.isnull().sum())
            if 'Commodity_Code' in df.columns:
                df_cleaned = df.drop(columns=['Commodity_Code'])
            else:
                df_cleaned = df

            column_mapping = {
                'Min_x0020_Price': 'Min_Price',
                'Max_x0020_Price': 'Max_Price',
                'Modal_x0020_Price': 'Modal_Price',
                'Min_Price': 'Min_Price',
                'Max_Price': 'Max_Price',
                'Modal_Price': 'Modal_Price'
            }
            df.rename(columns=column_mapping, inplace=True)
            df['Min_Price'].fillna(0, inplace=True)
            df['Max_Price'].fillna(0, inplace=True)
            df['Modal_Price'].fillna(0, inplace=True)
            df = df[(df['Min_Price'] <= 100000) & (df['Max_Price'] <= 100000) & (df['Modal_Price'] <= 100000)]
            original_filename = os.path.basename(file)
            new_filename = f"Silver_{original_filename}"
            output_file = os.path.join(output_path, new_filename)

            df_cleaned.to_csv(output_file, index=False)

        print(f"Processed {len(csv_files)} files and saved to {output_path}")

            # Move to the next day

    # Get dates from the database
    # start_date, end_date = get_dates_from_db()

    # Process files between the incremented start date and the end date
    # process_files(start_date, end_date)

# Silver.data_cleaning(datetime(2024, 8, 9))
