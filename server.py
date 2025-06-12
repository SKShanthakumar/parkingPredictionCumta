import mysql.connector
import os
from fastapi import FastAPI
from pydantic import BaseModel
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
PERIODS = int(os.getenv("PERIODS", 6))

# MySQL setup
try:
    mysql_db = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    cursor = mysql_db.cursor()
    print("MySQL connection established successfully.")

except mysql.connector.Error as e:
    print(f"MySQL connection error: {e}")

app = FastAPI()

class ForecastRequest(BaseModel):
    station_name: str
    vehicle_type: int  # 0 for twoWheeler, 1 for threeNFourWheeler

def get_latest_forecast(station_name, vehicle_type):
    """Retrieve the latest forecast for a station and vehicle type"""
    try:        
        query = """
        SELECT f.timestamp as forecast_timestamp, 
               f.predicted_availability
        FROM forecast_batches fb
        JOIN forecast f ON fb.id = f.batch_id
        WHERE fb.station_name = %s AND fb.vehicle_type = %s
        ORDER BY fb.timestamp DESC, f.timestamp ASC
        LIMIT %s
        """
        
        # if limit:
        #     query += f" LIMIT {limit}"
        
        cursor.execute(query, (station_name, vehicle_type, PERIODS))
        predictions = cursor.fetchall()
        result = []

        for timestamp, predicted_availability in predictions:
            result.append({"Timestamp": timestamp, "predicted_availability": predicted_availability})

        return result
        
    except mysql.connector.Error as e:
        print(f"MySQL Error in get_latest_forecast: {e}")
        return None

@app.get("/")
def read_root():
    return {"message": "Welcome to the Parking Forecast API"}

@app.post("/forecast")
def forecast_parking(req: ForecastRequest):
    station_name = req.station_name.lower().strip()
    vehicle_type = 'twoWheelerAvailable' if int(req.vehicle_type) == 0 else 'threeNFourWheelerAvailable'
    print(f"Fetching forecast for station: {station_name}, vehicle type: {vehicle_type}")
    data = get_latest_forecast(station_name, vehicle_type)

    if data:
        return {"forecast": data, "message": "forecast data for this station"}
    else:
        return {"forecast": [], "message": "No forecast found for this station"}
