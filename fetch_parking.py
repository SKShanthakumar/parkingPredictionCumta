import requests
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Connect to MongoDB
MONGO_URI = os.getenv("MONGO_URI")
STATIONS = os.getenv("STATIONS").split(",")

# Connect to MongoDB
client = MongoClient(MONGO_URI, tls=True)
db = client["parking"]
collection = db["availability"]

API = os.getenv("PARKING_DATA_API")
data = requests.get(API).json()
df = pd.DataFrame(data)

# IST time
now_ist = datetime.now(ZoneInfo("Asia/Kolkata")).replace(microsecond=0).replace(tzinfo=None)
print("IST time:", now_ist)
df['timestamp'] = now_ist

df = df[['timestamp','stationName', 'parkingAreaName', 'twoWheelerCapacity','threeNFourWheelerCapacity',
      'twoWheelerOccupied', 'threeNFourWheelerOccupied','twoWheelerAvailable','threeNFourWheelerAvailable']]

df = df[df['stationName'].isin(STATIONS)]

print(df[['timestamp','stationName']].drop_duplicates())
records = df.to_dict(orient='records')

if records:
    collection.insert_many(records)
    print(f"Inserted {len(records)} records.")
else:
    print("No records to insert.")