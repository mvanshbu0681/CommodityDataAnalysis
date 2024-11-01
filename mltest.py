from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestRegressor
import pandas as pd
from sklearn.metrics import mean_squared_error
from google.colab import files

# Load the CSV file
df = pd.read_csv('F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\CommodityDataAnalysisProject\\aggregated_daily_data_Azadpur_Onion_commodity2023-2018.csv')

# Convert 'Date' column to datetime
df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'])

# Create new columns 'Day', 'Month', 'Year', and 'DayOfWeek'
df['Day'] = df['Arrival_Date'].dt.day
df['Month'] = df['Arrival_Date'].dt.month
df['Year'] = df['Arrival_Date'].dt.year
df['DayOfWeek'] = df['Arrival_Date'].dt.dayofweek

# Add lag features (previous day's prices)
df['Prev_Min_Price'] = df['Min_Price'].shift(1)
df['Prev_Max_Price'] = df['Max_Price'].shift(1)
df['Prev_Modal_Price'] = df['Modal_Price'].shift(1)

# Drop rows with NaN values caused by shifting
df.dropna(inplace=True)

# Define features and target
X = df[['Day', 'Month', 'Year', 'DayOfWeek', 'Prev_Min_Price', 'Prev_Max_Price', 'Prev_Modal_Price']]
y = df[['Min_Price', 'Max_Price', 'Modal_Price']]

# Split data into training and testing sets
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Define the Random Forest Regressor model
model = RandomForestRegressor(random_state=42)

# Set up parameter grid for Grid Search
param_grid = {
    'n_estimators': [100, 200, 300],
    'max_depth': [None, 10, 20, 30],
    'min_samples_split': [2, 5, 10],
    'min_samples_leaf': [1, 2, 4]
}

# Perform Grid Search
grid_search = GridSearchCV(estimator=model, param_grid=param_grid, cv=5, scoring='neg_mean_squared_error', n_jobs=-1, verbose=2)
grid_search.fit(X_train, y_train)

# Get the best model
best_model = grid_search.best_estimator_

# Make predictions
predictions = best_model.predict(X_test)

# Calculate and print Mean Squared Error for each target
for i, target in enumerate(['Min_Price', 'Max_Price', 'Modal_Price']):
    mse = mean_squared_error(y_test.iloc[:, i], predictions[:, i])
    print(f'MSE for {target}: {mse}')

# Create a new dataframe for predictions from 2024-01-01 to 2024-09-08
future_dates = pd.date_range(start='2024-01-01', end='2024-09-08')
future_days = future_dates.day
future_months = future_dates.month
future_years = [2024] * len(future_days)
future_day_of_week = future_dates.dayofweek

# Assuming previous prices are similar to the last known prices for simplicity
prev_min_price = df['Min_Price'].iloc[-1]
prev_max_price = df['Max_Price'].iloc[-1]
prev_modal_price = df['Modal_Price'].iloc[-1]

future_data = pd.DataFrame({
    'Day': future_days,
    'Month': future_months,
    'Year': future_years,
    'DayOfWeek': future_day_of_week,
    'Prev_Min_Price': prev_min_price,
    'Prev_Max_Price': prev_max_price,
    'Prev_Modal_Price': prev_modal_price
})

# Make predictions for future dates
future_predictions = best_model.predict(future_data)

# Create a new dataframe for the predicted prices
predicted_prices = pd.DataFrame({
    'Date': future_dates,
    # 'Predicted_Min_Price': future_predictions[:, 0],
    # 'Predicted_Max_Price': future_predictions[:, 1],
    'Predicted_Modal_Price': future_predictions[:, 2]
})

# Save the predicted prices to a CSV file
predicted_prices.to_csv('F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\CommodityDataAnalysisProject\\predicted_prices.csv', index=False)

# Download the CSV file
# files.download('predicted_prices.csv')
