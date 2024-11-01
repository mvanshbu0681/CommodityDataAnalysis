import pandas as pd

# Define the start and end dates
start_date = "2002-01-01"
end_date = "2030-12-31"

# Generate a date range using pandas
date_range = pd.date_range(start=start_date, end=end_date)

# Create a DataFrame with columns similar to your CSV
calendar_df = pd.DataFrame({
    'word_date': date_range.strftime('%d-%b-%y'),          # Format as '1-Jan-02'
    'date': date_range,                                    # Full date
    'year': date_range.year,                               # Year
    'quarter': date_range.quarter,                         # Quarter of the year
    'month': date_range.month,                             # Month number
    'day_of_month': date_range.day,                        # Day of the month
    'week': date_range.isocalendar().week,                 # Week of the year
    'day_of_week': date_range.strftime('%a'),              # Day of the week (e.g., Mon, Tue)
    'weekday': date_range.weekday + 1,                     # Weekday (Monday=1, ..., Sunday=7)
    'Date_Key': date_range.strftime('%Y%m%d').astype(int)  # Date Key in 'YYYYMMDD' format
})

# Save the complete DataFrame to a new CSV file
output_file = "F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\CommodityDataAnalysisProject\complete_calendar_data_2002_to_2030.csv"
calendar_df.to_csv(output_file, index=False)

print(f"CSV file '{output_file}' created successfully!")
