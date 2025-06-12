import requests
import os
from datetime import datetime
from zoneinfo import ZoneInfo
import mysql.connector
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

STATIONS = os.getenv("STATIONS").split(",")

# MySQL setup
mysql_db = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
    port=os.getenv("DB_PORT")
)
mysql_cursor = mysql_db.cursor()

API = os.getenv("PARKING_DATA_API")
data = requests.get(API).json()
df = pd.DataFrame(data)

# IST time
now_ist = datetime.now(ZoneInfo("Asia/Kolkata")).replace(microsecond=0).replace(tzinfo=None)
print("IST time:", now_ist)
df['timestamp'] = now_ist

df = df[['timestamp','stationName', 'parkingAreaName', 'twoWheelerCapacity','threeNFourWheelerCapacity',
      'twoWheelerOccupied', 'threeNFourWheelerOccupied','twoWheelerAvailable','threeNFourWheelerAvailable']]

print(df[['timestamp','stationName']].drop_duplicates())
records = df.to_dict(orient='records')

insert_query = """
INSERT INTO availability (
    timestamp, stationName, parkingAreaName, twoWheelerCapacity,
    threeNFourWheelerCapacity, twoWheelerOccupied, threeNFourWheelerOccupied,
    twoWheelerAvailable, threeNFourWheelerAvailable
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

records_to_insert = []

for doc in records:
    try:
        # Extract data from MongoDB document (skip _id field)
        timestamp = doc.get('timestamp')
        station_name = doc.get('stationName')
        parking_area_name = doc.get('parkingAreaName')
        two_wheeler_capacity = doc.get('twoWheelerCapacity')
        three_four_wheeler_capacity = doc.get('threeNFourWheelerCapacity')
        two_wheeler_occupied = doc.get('twoWheelerOccupied')
        three_four_wheeler_occupied = doc.get('threeNFourWheelerOccupied')
        two_wheeler_available = doc.get('twoWheelerAvailable')
        three_four_wheeler_available = doc.get('threeNFourWheelerAvailable')
        
        # Handle timestamp conversion if needed
        if isinstance(timestamp, str):
            # If timestamp is string, parse it
            try:
                timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                # Try alternative format if needed
                print(f"Warning: Could not parse timestamp: {timestamp}")
                timestamp = None
        
        # Prepare record for insertion
        record = (
            timestamp,
            station_name,
            parking_area_name,
            two_wheeler_capacity,
            three_four_wheeler_capacity,
            two_wheeler_occupied,
            three_four_wheeler_occupied,
            two_wheeler_available,
            three_four_wheeler_available
        )
        
        records_to_insert.append(record)
        
    except Exception as e:
        print(f"Error processing document {doc.get('_id', 'unknown')}: {e}")
        failed_inserts += 1

# Bulk insert all records
try:
    if records_to_insert:
        mysql_cursor.executemany(insert_query, records_to_insert)
        mysql_db.commit()
        print(f"Successfully inserted {len(records_to_insert)} records into MySQL")
    else:
        print("No records to insert")
        
except mysql.connector.Error as e:
    print(f"MySQL Error during bulk insert: {e}")
    mysql_db.rollback()
    
    # Try individual inserts if bulk insert fails
    print("Attempting individual inserts...")
    for i, record in enumerate(records_to_insert):
        successful_inserts = 0
        failed_inserts = 0
        try:
            mysql_cursor.execute(insert_query, record)
            mysql_db.commit()
            successful_inserts += 1
        except mysql.connector.Error as individual_error:
            print(f"Failed to insert record {i+1}: {individual_error}")
            print(f"Record data: {record}")
            failed_inserts += 1
            mysql_db.rollback()
        print(f"Successful inserts: {successful_inserts}, Failed inserts: {failed_inserts}")

finally:
    # Close the MySQL connection
    if mysql_cursor:
        mysql_cursor.close()
    if mysql_db:
        mysql_db.close()
    print("MySQL connection closed.")