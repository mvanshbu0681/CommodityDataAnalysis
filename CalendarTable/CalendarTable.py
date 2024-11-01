import pandas as pd
import mysql.connector

# MySQL connection details
db_config = {
        'user': 'root',      # Replace with your MySQL username
        'password': 'admin',  # Replace with your MySQL password
        'host': 'localhost',          # Replace with your MySQL host
        'database': 'commoditydataanaylsis'   # Replace with your MySQL database name
    }

# Read the CSV file
csv_file = "F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\CommodityDataAnalysisProject\dim_calendar.csv"  # Replace with your CSV file path
df = pd.read_csv(csv_file)

# Establish a connection to the MySQL database
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Create insert statement for the MySQL table
insert_query = """
    INSERT INTO dim_calendar (word_date, date, year, quarter, month, day_of_month, week, day_of_week, weekday, Date_Key)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Insert data row by row
for index, row in df.iterrows():
    cursor.execute(insert_query, tuple(row))

# Commit the transaction
conn.commit()

# Close the database connection
cursor.close()
conn.close()

print("Data transfer complete!")
