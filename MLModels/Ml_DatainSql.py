import mysql.connector
import pandas as pd

def unique_commodities_SqlTransfer():
    # Database configuration
    db_config = {
        'user': 'root',      # Replace with your MySQL username
        'password': 'admin',  # Replace with your MySQL password
        'host': 'localhost',  # Replace with your MySQL host
        'database': 'commoditydataanaylsis'  # Replace with your MySQL database name
    }

    # Path to the CSV file
    csv_file_path = r'F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\CommodityDataAnalysisProject\forecast_results.csv'
    
    # Read the entire CSV file at once
    final_df = pd.read_csv(csv_file_path)

    # Establish database connection
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    # Table name
    table_name = 'Fact_CommodityForecast'
    
    # Generate the dynamic SQL delete query
    delete_query = f'DELETE FROM {table_name}'
    
    # Execute the delete query
    cursor.execute(delete_query)
    connection.commit()

    # Remove duplicates based on relevant columns
    print("Original count:", final_df.count())
    final_df = final_df.drop_duplicates(subset=['Predicted_Date_Key', 'Predicted_Price'], keep='first')
    print("Count after duplicates removed:", final_df.count())

    # Insert data in chunks
    chunk_size = 100000  # Adjust this size as necessary
    for start in range(0, len(final_df), chunk_size):
        end = start + chunk_size
        chunk = final_df.iloc[start:end]  # Get a chunk of data
        # Generate the dynamic SQL insert query
        insert_query = f'INSERT INTO {table_name} ({", ".join(chunk.columns)}) VALUES ({", ".join(["%s" for _ in chunk.columns])})'
        print(insert_query)

        # Convert the chunk DataFrame to a list of tuples
        data = chunk.values.tolist()

        # Execute the insert query for each row in the chunk
        cursor.executemany(insert_query, data)
        connection.commit()
        del insert_query, data, chunk  # Clear variables to free up memory

    # Close the cursor and connection
    cursor.close()
    connection.close()

    print("Data successfully inserted into the unique_commodities table.")

unique_commodities_SqlTransfer()
