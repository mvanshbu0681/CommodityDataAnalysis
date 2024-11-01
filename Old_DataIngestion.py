import pandas as pd
import requests
import os
from datetime import datetime, timedelta

def getData(start_date, end_date):
    
    base_url = 'https://api.data.gov.in/resource/35985678-0d79-46b4-9ed6-6f13308a1d24'
    api_key = '579b464db66ec23bdd000001c448725136334a8c46b2f7e597535cc1'
    
    # Define the output path
    output_path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/CommodityDataAnalysisProject/Bronze'
    
    # Initialize date
    current_date = start_date
    
    while current_date <= end_date:
        formatted_date = current_date.strftime('%d/%m/%Y')
        url = f'{base_url}?api-key={api_key}&format=csv&limit=10000&filters%5BArrival_Date%5D={formatted_date}'
        
        output_directory = os.path.join(output_path, str(current_date.year), str(current_date.month), str(current_date.day))
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        
        # Fetch and save data
        try:
            with requests.Session() as session:
                response = session.get(url)
                response.raise_for_status()  # Check if the request was successful
                csv_content = response.content
                
                # Generate the filename with date and time
                filename = f'commoditydata_{formatted_date.replace("/", "")}.csv'
                file_path = os.path.join(output_directory, filename)
                
                with open(file_path, 'wb') as file:
                    file.write(csv_content)
                    
                print(f"Data saved to {file_path}")
        
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
        
        # Move to the next day
        current_date += timedelta(days=1)

# Define start and end dates
start_date = datetime(2024, 8, 11)
end_date = datetime(2024, 8, 12)

getData(start_date, end_date)
