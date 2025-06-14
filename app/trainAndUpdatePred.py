import mysql.connector
import pandas as pd
from neuralprophet import NeuralProphet
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import argparse

load_dotenv()

# Argument parsing
parser = argparse.ArgumentParser()
parser.add_argument("--station", type=str, required=True, help="Station name")
parser.add_argument("--vehicle", type=int, required=True, help="Vehicle type (0 for twoWheeler, 1 for threeNFourWheeler)", choices=[0, 1])
args = parser.parse_args()

station_name = args.station
vehicle_type = 'twoWheelerAvailable' if args.vehicle == 0 else 'threeNFourWheelerAvailable'
periods = int(os.getenv("PERIODS", 6))

# MySQL database configuration
DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'database': os.getenv("DB_NAME"),
    'port': os.getenv("DB_PORT")
}

def get_db_connection():
    """Create and return a MySQL database connection"""
    return mysql.connector.connect(**DB_CONFIG)

def train_model():
    """Train the NeuralProphet model using data from MySQL availability table"""
    try:
        # Connect to MySQL
        connection = get_db_connection()
        
        # Query to fetch all availability data
        query = """
        SELECT timestamp, stationName, parkingAreaName, 
               twoWheelerCapacity, threeNFourWheelerCapacity,
               twoWheelerOccupied, threeNFourWheelerOccupied,
               twoWheelerAvailable, threeNFourWheelerAvailable
        FROM availability
        ORDER BY timestamp
        """
        
        # Load data into pandas DataFrame
        raw_df = pd.read_sql(query, connection)
        connection.close()
        
        print(f"Shape of raw data: {raw_df.shape}")
        
        # Group by timestamp and stationName
        grouped_df = raw_df.groupby(['timestamp', 'stationName']).agg({
            'parkingAreaName': lambda x: list(x),
            'twoWheelerCapacity': 'sum',
            'threeNFourWheelerCapacity': 'sum',
            'twoWheelerOccupied': 'sum',
            'threeNFourWheelerOccupied': 'sum',
            'twoWheelerAvailable': 'sum',
            'threeNFourWheelerAvailable': 'sum'
        }).reset_index()
        
        # Filter data for specific station
        cutoff_ts = datetime.now()
        df = grouped_df[grouped_df['stationName'] == station_name]
        df = df.sort_values('timestamp')
        
        print(f"Shape of {station_name} data: {df.shape}")
        
        # Ensure timestamp is datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        # Prepare data for Prophet (expects columns: ds (date), y (target))
        df_prophet = df[df['timestamp'] <= cutoff_ts].reset_index()[['timestamp', vehicle_type]]
        df_prophet.columns = ['ds', 'y']
        df_prophet = df_prophet.dropna()
        df_prophet = df_prophet[df_prophet['y'] >= 0]
        
        # Train the model
        m = NeuralProphet(daily_seasonality=True, learning_rate=1.0)
        m.fit(df_prophet, freq="15min")
        
        print(f"{station_name} - {vehicle_type} Model Trained Successfully")
        return m, df_prophet
        
    except mysql.connector.Error as e:
        print(f"MySQL Error in train_model: {e}")
        raise
    except Exception as e:
        print(f"Error in train_model: {e}")
        raise

def forecast_parking():
    """Generate forecast and save to MySQL database"""
    try:
        # Train the model
        model, history_df = train_model()
        
        # Generate future dataframe
        future = model.make_future_dataframe(history_df, periods=periods)
        
        # Make predictions
        forecast = model.predict(future)
        
        # Get only the forecasted part
        forecast_tail = forecast.tail(periods)
        
        # Prepare result data
        result = []
        for _, row in forecast_tail.iterrows():
            result.append({
                "timestamp": row['ds'].strftime('%Y-%m-%d %H:%M:%S'),
                "predicted_availability": round(row['yhat1'], 2)
            })
        
        print("Forecasting completed")
        
        # Save to MySQL database
        save_forecast_to_mysql(result)
        
        return result
        
    except Exception as e:
        print(f"Error in forecast_parking: {e}")
        raise

def save_forecast_to_mysql(predictions):
    """Save forecast results to MySQL database"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Current IST timestamp
        now_ist = datetime.now(ZoneInfo("Asia/Kolkata")).replace(microsecond=0).replace(tzinfo=None)
        
        # Insert into forecast_batches table
        insert_batch_query = """
        INSERT INTO forecast_batches (station_name, vehicle_type, timestamp)
        VALUES (%s, %s, %s)
        """
        
        batch_data = (station_name, vehicle_type, now_ist)
        cursor.execute(insert_batch_query, batch_data)
        
        # Get the inserted batch ID
        batch_id = cursor.lastrowid
        
        # Prepare data for forecast table
        forecast_records = []
        for prediction in predictions:
            forecast_records.append((
                batch_id,
                prediction["timestamp"],
                prediction["predicted_availability"]
            ))
        
        # Insert into forecast table
        insert_forecast_query = """
        INSERT INTO forecast (batch_id, timestamp, predicted_availability)
        VALUES (%s, %s, %s)
        """
        
        cursor.executemany(insert_forecast_query, forecast_records)
        
        # Commit the transaction
        connection.commit()
        
        print(f"Data inserted in MySQL - Batch ID: {batch_id}")
        print(f"Inserted {len(forecast_records)} forecast records")
        
        cursor.close()
        connection.close()
        
    except mysql.connector.Error as e:
        print(f"MySQL Error in save_forecast_to_mysql: {e}")
        if connection:
            connection.rollback()
        raise
    except Exception as e:
        print(f"Error in save_forecast_to_mysql: {e}")
        if connection:
            connection.rollback()
        raise

if __name__ == "__main__":
    try:
        # Generate forecast
        forecast = forecast_parking()
        
        print(f"\nForecast for {station_name} ({vehicle_type}):")
        for entry in forecast:
            print(f"Timestamp: {entry['timestamp']}, Predicted Availability: {entry['predicted_availability']}")
            
    except Exception as e:
        print(f"Error in main execution: {e}")
        exit(1)