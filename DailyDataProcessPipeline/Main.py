from datetime import datetime, timedelta, date
import mysql.connector
from Silver_DataCleansing import Silver
from Gold_DataTransformation import Gold
from SQL_IncrementalLoadDataTransfer import load_data_to_mysql
import sys

sys.path.append('F:\\Education\\COLLEGE\\PROGRAMING\\Python\\Codes\\CommodityDataAnalysis\\Production\\UniqueCommodities')
from Daily_UniqueCommodites import check_for_new_commodity

sys.path.append('F:\\Education\\COLLEGE\\PROGRAMING\\Python\\Codes\\CommodityDataAnalysis\\Production\\UniqueMarkets')
from Daily_UniqueMarkets import check_for_new_market

def get_dates_from_db():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="admin",
        database="commoditydataanaylsis"
    )
    cursor = connection.cursor()

    # Get the last processed date
    cursor.execute("SELECT run_date FROM LastProcessed ORDER BY id DESC LIMIT 1")
    start_date = cursor.fetchone()[0]

    # Get the last run date
    cursor.execute("SELECT run_date FROM Lastrun ORDER BY id DESC LIMIT 1")
    end_date = cursor.fetchone()[0]

    cursor.close()
    connection.close()

    return start_date, end_date

def update_run_date_in_db(date, table_name):
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="admin",
        database="commoditydataanaylsis"
    )
    cursor = connection.cursor()

    # Update the run_date in the specified table
    cursor.execute(f"UPDATE {table_name} SET run_date = %s ORDER BY id DESC LIMIT 1", (date,))
    connection.commit()

    cursor.close()
    connection.close()

# Fetch the start and end dates
start_date, end_date = get_dates_from_db()

# Increment start_date by 1 day to start from the next date
start_date += timedelta(days=1)

date = start_date
try:
    while date <= end_date:
        check_for_new_commodity(date)
        check_for_new_market(date)
        Silver.data_cleaning(date)
        Gold.data_transformation(date)
        load_data_to_mysql(date)
        date += timedelta(days=1)

    # If everything succeeds, update the LastProcessed table with the final processed date
    date -= timedelta(days=1)
    update_run_date_in_db(date, "LastProcessed")

except Exception as e:
    print(f"Error occurred: {e}")
    # Roll back the last processed date in case of an error
    date -= timedelta(days=1)
    update_run_date_in_db(date, "LastProcessed")
    print(f"LastProcessed updated to {date} due to error.")
