import pymongo
import pandas as pd
from neuralprophet import NeuralProphet
import os   
from datetime import datetime
from zoneinfo import ZoneInfo
import argparse
from dotenv import load_dotenv

load_dotenv()

# Argument parsing
parser = argparse.ArgumentParser()
parser.add_argument("--station", type=str, required=True, help="Station name")
parser.add_argument("--vehicle", type=int, required=True, help="Vehicle type (0 for twoWheeler, 1 for threeNFourWheeler)", choices=[0, 1])
args = parser.parse_args()

station_name = args.station
vehicle_type = 'twoWheelerAvailable' if args.vehicle == 0 else 'threeNFourWheelerAvailable'
periods = os.getenv("PERIODS")
periods = int(periods)

# MongoDB setup
MONGO_URI = os.getenv("MONGO_URI")
client = pymongo.MongoClient(MONGO_URI, tls=True) # Change if needed
db = client["parking"]  # Replace with your DB name

def train_model():
    availability_collection = db["availability"]  # Replace with your collection name
    cursor = availability_collection.find()  # Fetch all documents

    raw_df = pd.DataFrame(list(cursor))     # Convert to DataFrame
    raw_df.drop(columns=['_id'], inplace=True, errors='ignore')  # Drop MongoDB's default _id field if not needed
    
    # Display the DataFrame
    print(f"Shape of raw data: {raw_df.shape}")

    # group by timestamp and stationName
    grouped_df = raw_df.groupby(['timestamp', 'stationName']).agg({
        'parkingAreaName': lambda x: list(x),
        'twoWheelerCapacity': 'sum',
        'threeNFourWheelerCapacity': 'sum',
        'twoWheelerOccupied': 'sum',
        'threeNFourWheelerOccupied': 'sum',
        'twoWheelerAvailable': 'sum',
        'threeNFourWheelerAvailable': 'sum'
    }).reset_index()

    cutoff_ts = datetime.now() #'2025-06-04 23:00:00.000'
    df = grouped_df[(grouped_df['stationName']==station_name)]
    df = df.sort_values('timestamp')
    print(f"Shape of {station_name} data: {df.shape}")

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')

    # Prophet expects columns: ds (date), y (target)
    df_prophet = df[df['timestamp']<=cutoff_ts].reset_index()[['timestamp', vehicle_type]]
    df_prophet.columns = ['ds', 'y']

    df_prophet = df_prophet.dropna()
    df_prophet = df_prophet[df_prophet['y'] >= 0]

    m = NeuralProphet(daily_seasonality=True, learning_rate=1.0)
    m.fit(df_prophet, freq="15min")
    
    print(f"{station_name} - {vehicle_type} Model Trained Successfully")
    return m, df_prophet

def forecast_parking():
    model, history_df = train_model()
    future = model.make_future_dataframe(history_df, periods=periods)

    # Predict
    forecast = model.predict(future)

    # Return the forecasted part only
    forecast_tail = forecast.tail(periods)

    result = [
        {
            "timestamp": row['ds'].strftime('%Y-%m-%d %H:%M:%S'),
            "predicted_availability": round(row['yhat1'], 2)
        }
        for _, row in forecast_tail.iterrows()
    ]
    print("Forecasting completed")

    # mongo collection object
    forecast_collection = db["forecast"]

    now_ist = datetime.now(ZoneInfo("Asia/Kolkata")).replace(microsecond=0).replace(tzinfo=None)

    # Insert in MongoDB
    doc = {
        "station_name": station_name.lower().strip(),
        "predictions": result,
        "timestamp": now_ist,
        "vehicle_type": vehicle_type
    }
    forecast_collection.insert_one(doc)

    print("data inserted in MongoDB")
    return result

if __name__ == "__main__":
    forecast = forecast_parking()
    print(f"Forecast for {station_name} ({vehicle_type}):")
    for entry in forecast:
        print(f"Timestamp: {entry['timestamp']}, Predicted Availability: {entry['predicted_availability']}")