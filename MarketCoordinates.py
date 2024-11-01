import requests
import pandas as pd
import time

# Define the latitude and longitude bounds for India
INDIA_LATITUDE_BOUNDS = (6.0, 37.0)
INDIA_LONGITUDE_BOUNDS = (68.0, 97.0)

# Function to check if the coordinates are within India's bounds
def is_within_india(lat, lon):
    return INDIA_LATITUDE_BOUNDS[0] <= lat <= INDIA_LATITUDE_BOUNDS[1] and INDIA_LONGITUDE_BOUNDS[0] <= lon <= INDIA_LONGITUDE_BOUNDS[1]

# Function to fetch coordinates using geocoding API
def get_coordinates(location_name):
    base_url = "https://geocode.maps.co/search"
    params = {"q": location_name, "api_key": "66d2270352b57895083413hcwfd7859"}
    response = requests.get(base_url, params=params)

    # Check for successful response
    if response.status_code == 200:
        try:
            data = response.json()
            if data:
                # Choose the first result or apply logic to select the best one
                location = data[0]
                lat = float(location['lat'])
                lon = float(location['lon'])
                if is_within_india(lat, lon):
                    return lat, lon
                else:
                    print(f"Coordinates for {location_name} are outside India: ({lat}, {lon})")
                    return None, None
            else:
                print(f"No results found for location: {location_name}")
                return None, None
        except ValueError:
            print(f"Error decoding JSON for location: {location_name}")
            return None, None
    else:
        print(f"Error: Received status code {response.status_code} for location: {location_name}")
        return None, None

# Function to apply market and district logic for coordinates
def get_coordinates_with_fallback(row):
    market_name = row['market_name']
    district_name = row['market_district']

    # Try fetching market-level coordinates
    lat, lon = get_coordinates(market_name)
    
    # If market-level coordinates are not found, try district-level coordinates
    if lat is None or lon is None:
        print(f"Market coordinates not found for {market_name}, trying district: {district_name}")
        lat, lon = get_coordinates(district_name)
    
    # If both market and district-level coordinates are unavailable, return None
    if lat is None or lon is None:
        print(f"Both market and district coordinates not found for {market_name}")
    
    # Return latitude and longitude as a pandas Series
    return pd.Series([lat, lon])

# Load the CSV file
df = pd.read_csv('F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\CommodityDataAnalysisProject\\Dim_MarketDetails.csv')

# Apply the function with rate limiting
def get_coordinates_with_delay(row):
    result = get_coordinates_with_fallback(row)
    time.sleep(1)  # Add a delay of 1 second between requests
    return result

# Apply the function and fetch coordinates
df[['latitude', 'longitude']] = df.apply(get_coordinates_with_delay, axis=1)

# Save the updated DataFrame to a new CSV file
df.to_csv('F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\CommodityDataAnalysisProject\\market_data_with_coordinates.csv', index=False)

print("Coordinates added and CSV file saved successfully.")
