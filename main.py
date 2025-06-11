# main.py
import pymongo
import os
from fastapi import FastAPI
from pydantic import BaseModel

# mongo setup
MONGO_URI = os.getenv("MONGO_URI")
client = pymongo.MongoClient(MONGO_URI, tls=True) # Change if needed
db = client["parking"]  # Replace with your DB name
collection = db["forecast"]  # Replace with your collection name

app = FastAPI()

class ForecastRequest(BaseModel):
    station_name: str

@app.get("/")
def read_root():
    return {"message": "Welcome to the Parking Forecast API"}

@app.post("/forecast")
def forecast_parking(req: ForecastRequest):
    station_name = req.station_name.lower().strip()

    doc = collection.find_one({"station_name": station_name})
    
    if doc and "predictions" in doc:
        return {"forecast": doc["predictions"]}
    else:
        return {"forecast": [], "message": "No forecast found for this station"}
