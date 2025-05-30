import requests
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from pymongo import MongoClient
from urllib.parse import quote_plus # Import quote_plus
import pandas as pd

# Connect to MongoDB
MONGO_URI = os.getenv("MONGO_URI")

# Connect to MongoDB
client = MongoClient(MONGO_URI, tls=True)
db = client["parking"]
collection = db["availability"]

url = "https://commuters-dataapi.chennaimetrorail.org/api/ParkingArea/getParkingAreaAvailability"
data = requests.get(url).json()
#print(data)
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
df = pd.DataFrame(data)

# IST time
now_ist = datetime.now(ZoneInfo("Asia/Kolkata"))
print("IST time:", now_ist)
df['timestamp'] = now_ist

df = df[['timestamp','stationName', 'parkingAreaName', 'twoWheelerCapacity','threeNFourWheelerCapacity',
      'twoWheelerOccupied', 'threeNFourWheelerOccupied','twoWheelerAvailable','threeNFourWheelerAvailable']]
print(df.columns, now)
df = df[df['stationName'].isin(['OTA - Nanganallur Road','Arignar Anna Alandur ','Guindy', 'Little Mount',])]
print(df.head())
records = df.to_dict(orient='records')

if records:
    collection.insert_many(records)
    print(f"Inserted {len(records)} records.")
else:
    print("No records to insert.")