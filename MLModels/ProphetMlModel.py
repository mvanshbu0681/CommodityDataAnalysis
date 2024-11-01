import pandas as pd
import numpy as np
from prophet import Prophet
from datetime import datetime
import matplotlib.pyplot as plt
import glob
import os
from scipy import stats
import matplotlib.dates as mdates  # Import mdates

# Define the base directory
base_dir = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/CommodityDataAnalysisProject/Gold'

# Get all CSV files
csv_files = glob.glob(os.path.join(base_dir, '2018/*/*/*.csv')) + \
            glob.glob(os.path.join(base_dir, '2019/*/*/*.csv')) + \
            glob.glob(os.path.join(base_dir, '2020/*/*/*.csv')) + \
            glob.glob(os.path.join(base_dir, '2021/*/*/*.csv')) + \
            glob.glob(os.path.join(base_dir, '2022/*/*/*.csv')) + \
            glob.glob(os.path.join(base_dir, '2023/*/*/*.csv'))

# Read all CSV files into a DataFrame
df = pd.concat([pd.read_csv(file) for file in csv_files], ignore_index=True)

# Ensure Arrival_Date is a datetime object and filter for market_id and commodity_id
df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'], format='%Y-%m-%d')

# Define inflation data
inflation_data = {
    'date': ['2018-12-31', '2019-12-31', '2020-12-31', '2021-12-31', '2022-12-31'],
    'inflation_rate': [3.9388, 3.7295, 6.6234, 5.1314, 6.699]
}
inflation_df = pd.DataFrame(inflation_data)
inflation_df['date'] = pd.to_datetime(inflation_df['date'])

# Extrapolate inflation rates
def extrapolate_inflation(start_date, end_date, inflation_df):
    x = mdates.date2num(inflation_df['date'])
    y = inflation_df['inflation_rate']
    slope, intercept, _, _, _ = stats.linregress(x, y)
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    x_extrapolate = mdates.date2num(date_range)
    y_extrapolate = slope * x_extrapolate + intercept
    return pd.DataFrame({'ds': date_range, 'inflation_rate': y_extrapolate})

# Forecast function for each market and commodity combination
def forecast_for_combination(market_id, commodity_id, df, extrapolated_inflation):
    try:
        # Filter the data
        df_filtered = df[(df['market_id'] == market_id) & (df['commodity_id'] == commodity_id)]

        # Check if there are enough data points for Prophet
        if len(df_filtered) < 2:
            print(f"Not enough data for market_id {market_id} and commodity_id {commodity_id}")
            return

        # Ensure 'Arrival_Date' column is a datetime
        df_filtered.loc[:, 'Arrival_Date'] = pd.to_datetime(df_filtered['Arrival_Date'], format='%Y-%m-%d')

        # Prepare data for Prophet
        prophet_df = df_filtered[['Arrival_Date', 'Modal_Price']].rename(columns={'Arrival_Date': 'ds', 'Modal_Price': 'y'})

        # Merge with inflation data
        prophet_df = prophet_df.merge(extrapolated_inflation, on='ds', how='left')

        # Check for non-NaN data
        if prophet_df[['y']].dropna().shape[0] < 2:
            print(f"Insufficient non-NaN data for market_id {market_id} and commodity_id {commodity_id}")
            return

        # Create and fit the Prophet model
        model = Prophet(yearly_seasonality=True, weekly_seasonality=True, daily_seasonality=False)
        model.add_regressor('inflation_rate')
        model.fit(prophet_df)

        # Create future dates for prediction
        future_dates = pd.date_range(start='2024-01-01', end='2024-12-31')
        future_df = pd.DataFrame({'ds': future_dates})
        future_df = future_df.merge(extrapolated_inflation, on='ds', how='left')

        # Make predictions
        forecast = model.predict(future_df)

        # Create final forecast DataFrame
        final_forecast = forecast[['ds', 'yhat']].rename(columns={'ds': 'Predicted_Date', 'yhat': 'Predicted_Price'})
        final_forecast['Predicted_Price'] = final_forecast['Predicted_Price'].round(2)
        final_forecast['market_id'] = market_id
        final_forecast['commodity_id'] = commodity_id

        # Add date string and key columns
        final_forecast['Predicted_Date_String'] = final_forecast['Predicted_Date'].astype(str)
        final_forecast['Predicted_Date_Key'] = final_forecast['Predicted_Date'].dt.strftime('%Y%m%d').astype(int)

        # Append to CSV
        final_forecast.to_csv('forecast_results.csv', mode='a', header=not os.path.exists('forecast_results.csv'), index=False)
        print(f"Forecast for market_id {market_id} and commodity_id {commodity_id} saved.")

    except Exception as e:
        # Log the error and continue
        print(f"Error processing market_id {market_id} and commodity_id {commodity_id}: {e}")
        with open('F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\CommodityDataAnalysisProject\error_log.txt', 'a') as f:
            f.write(f"Error for market_id {market_id}, commodity_id {commodity_id}: {e}\n")

# Function to load already processed combinations
def load_processed_combinations():
    if os.path.exists('forecast_results.csv'):
        processed_df = pd.read_csv('forecast_results.csv')
        processed_combinations = processed_df[['market_id', 'commodity_id']].drop_duplicates()
        return processed_combinations
    else:
        return pd.DataFrame(columns=['market_id', 'commodity_id'])

# Extrapolate inflation data
start_date = df['Arrival_Date'].min()
end_date = pd.to_datetime('2024-12-31')
extrapolated_inflation = extrapolate_inflation(start_date, end_date, inflation_df)

# Load already processed combinations
processed_combinations = load_processed_combinations()

# Get remaining combinations to process
unique_combinations = df[['market_id', 'commodity_id']].drop_duplicates()
remaining_combinations = pd.merge(unique_combinations, processed_combinations, on=['market_id', 'commodity_id'], how='left', indicator=True)
remaining_combinations = remaining_combinations[remaining_combinations['_merge'] == 'left_only'].drop(columns=['_merge'])

# Loop through all remaining unique combinations of market_id and commodity_id
for _, row in remaining_combinations.iterrows():
    market_id = row['market_id']
    commodity_id = row['commodity_id']
    print(f"Processing market_id {market_id} and commodity_id {commodity_id}...")
    forecast_for_combination(market_id, commodity_id, df, extrapolated_inflation)
