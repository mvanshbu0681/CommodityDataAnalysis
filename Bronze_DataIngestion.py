import pandas as pd
import requests
import os
from datetime import date, timedelta
import mysql.connector
import sys
from dotenv import load_dotenv

def create_db_connection():
    """Establishes and returns a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='admin',
            database='commoditydataanaylsis'
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        sys.exit()

def get_start_date():
    """Fetches the start date from the LastRun table."""
    connection = create_db_connection()
    cursor = connection.cursor()

    cursor.execute("SELECT run_date FROM LastRun ORDER BY id DESC LIMIT 1")
    result = cursor.fetchone()
    connection.close()

    if result:
        return result[0]
    else:
        return None

def update_start_date(new_date):
    """Updates the LastRun table with the new date."""
    connection = create_db_connection()
    cursor = connection.cursor()

    cursor.execute("UPDATE LastRun SET run_date = %s WHERE id = 1", (new_date,))
    connection.commit()
    connection.close()

def getData(start_date, end_date):
    """Fetches and saves data from the API within the given date range."""
    output_path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/CommodityDataAnalysisProject/Bronze'
    
    load_dotenv()

    base_url_past = os.getenv('BASE_URL_PAST')
    base_url_today = os.getenv('BASE_URL_TODAY')
    api_key = os.getenv('API_KEY')

    current_date = start_date
    
    while current_date <= end_date:
        formatted_date = current_date.strftime('%d/%m/%Y')
        
        if current_date == date.today():
            url = f'{base_url_today}?api-key={api_key}&format=csv&limit=100000'
        else:
            url = f'{base_url_past}?api-key={api_key}&format=csv&limit=10000&filters%5BArrival_Date%5D={formatted_date}'
        
        output_directory = os.path.join(output_path, str(current_date.year), str(current_date.month), str(current_date.day))
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        
        try:
            with requests.Session() as session:
                response = session.get(url)
                response.raise_for_status()  # Check if the request was successful
                csv_content = response.content
                
                filename = f'commoditydata_{formatted_date.replace("/", "")}.csv'
                file_path = os.path.join(output_directory, filename)
                
                with open(file_path, 'wb') as file:
                    file.write(csv_content)
                    
                print(f"Data saved to {file_path}")
        
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
        
        current_date += timedelta(days=1)
    
    update_start_date(end_date)

# Fetch start date from the LastRun table from the database
start_date = get_start_date()

if not start_date:
    print("No start date found in the database. Exiting the program.")
    sys.exit()

start_date += timedelta(days=1)
end_date = date.today()

getData(start_date, end_date)
