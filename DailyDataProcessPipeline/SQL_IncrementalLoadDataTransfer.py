import datetime
import glob
import mysql.connector
import pandas as pd

def load_data_to_mysql(date):
    today = datetime.date.today()
    
    year = date.strftime("%Y")
    month = str(date.month)
    day = str(date.day)
    path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/CommodityDataAnalysisProject'
    
    # Use a different table if the date is today
    if date == today:
        table_name = 'fact_indiancommoditymarketdata'
    else:
        table_name = 'fact_indiancommoditymarketdata_' + year
    
    # Now you can use the incremented date for further processing
    input_path = f"{path}/Gold/{year}/{month}/{day}"
    input_files = glob.glob(input_path + "/*.csv")

    # Database connection and processing logic
    db_config = {
        'user': 'root',      # Replace with your MySQL username
        'password': 'admin',  # Replace with your MySQL password
        'host': 'localhost',          # Replace with your MySQL host
        'database': 'commoditydataanaylsis'   # Replace with your MySQL database name
    }

    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    batch_size = 1000  # Adjust batch size as needed

    # Process each CSV file
    for file in input_files:
        # Read CSV into DataFrame
        final_df = pd.read_csv(file)
        print("Checking for NaN values in the DataFrame...")
        print(final_df.isnull().sum())
        final_df = final_df.where(pd.notnull(final_df), None)
        
        # Prepare the SQL insert query
        insert_query = f'INSERT INTO {table_name} ({", ".join(final_df.columns)}) VALUES ({", ".join(["%s" for _ in final_df.columns])})'

        # Iterate over the DataFrame in batches
        for start in range(0, len(final_df), batch_size):
            end = start + batch_size
            batch_data = final_df.iloc[start:end].values.tolist()
            cursor.executemany(insert_query, batch_data)
            connection.commit()

    # Close the cursor and connection
    cursor.close()
    connection.close()

# Example call
# load_data_to_mysql(datetime.date(2024, 10, 14))  # Assuming today's date is 2024-10-14
