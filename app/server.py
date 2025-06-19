import mysql.connector
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import pandas as pd
from typing import List
from schemas import ForecastRequest, AvailabilityRequest

load_dotenv()
PERIODS = int(os.getenv("PERIODS", 6))

# MySQL setup
def get_mysql_connection():
    try:
        mysql_db = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            port=os.getenv("DB_PORT")
        )
        cursor = mysql_db.cursor()
        print("MySQL connection established successfully.")
        return mysql_db

    except mysql.connector.Error as e:
        print(f"MySQL connection error: {e}")
        exit(1)

app = FastAPI()

origins = os.getenv("CORS_ORIGINS", "*").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,              # Allows requests from these domains
    allow_credentials=True,             # Allows cookies, authorization headers
    allow_methods=["*"],                # Allows all HTTP methods: GET, POST, etc.
    allow_headers=["*"],                # Allows all headers
)



def get_latest_forecast(station_name, vehicle_type):
    """Retrieve the latest forecast for a station and vehicle type"""
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()     
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
    """Retrieve the latest forecast for a specific station and vehicle type"""
    
    station_name = req.station_name.lower().strip()
    vehicle_type = 'twoWheelerAvailable' if int(req.vehicle_type) == 0 else 'threeNFourWheelerAvailable'
    print(f"Fetching forecast for station: {station_name}, vehicle type: {vehicle_type}")
    data = get_latest_forecast(station_name, vehicle_type)

    if data:
        return {"forecast": data, "message": "forecast data for this station"}
    else:
        return {"forecast": [], "message": "No forecast found for this station"}


@app.post("/available")
def get_availability(req: AvailabilityRequest):
    """Retrieve availability for a list of stations and vehicle type for past N days"""

    def helper(station_name, vehicle_type, days):
        """Helper function to get availability for a single station and days"""
    
        station_name = station_name.lower().strip()
        data_vehicle_type = 'Two Wheeler' if int(vehicle_type) == 0 else 'Three and Four Wheeler'
        vehicle_type = 'twoWheelerAvailable' if int(vehicle_type) == 0 else 'threeNFourWheelerAvailable'
        
        query = f"""
        SELECT timestamp, SUM(`{vehicle_type}`)
        FROM availability
        WHERE stationName = %s
        AND DATE(timestamp) >= CURDATE() - INTERVAL %s DAY
        GROUP BY timestamp
        ORDER BY timestamp
        """
        cursor.execute(query, (station_name, days - 1))
        result = cursor.fetchall()
        
        if result:
            res = []
            for x in result:
                res.append({
                    "timestamp": x[0],
                    "available": x[1]
                })
            return {"station": station_name, "vehicle": data_vehicle_type, "availability": res}
        else:
            return {"station": station_name, "vehicle": data_vehicle_type, "availability": "No data found for this station"}

    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        stations = req.stations
        days = req.days
        res = []
        for entry in stations:
            # Pass entry.days to helper
            res.append(helper(entry.station_name, entry.vehicle_type, days))
        
        cursor.close()
        conn.close()
    
        return {"availability": res, "message": "Availability data for the requested stations"}
    
    except mysql.connector.Error as e:
        print(f"MySQL Error in get_availability: {e}")
        return None