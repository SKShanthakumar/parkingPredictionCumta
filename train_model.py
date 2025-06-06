import pymongo
import pandas as pd
import matplotlib.pyplot as plt
from neuralprophet import NeuralProphet
import pickle
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error
import numpy as np
import os 
# After saving model to 'parking_forecast_model.pkl'
from supabase import create_client

MONGO_URI = os.getenv("MONGO_URI")
client = pymongo.MongoClient(MONGO_URI, tls=True) # Change if needed
db = client["parking"]  # Replace with your DB name
collection = db["availability"]  # Replace with your collection name
# Fetch all documents
cursor = collection.find()
# Convert to DataFrame
raw_df = pd.DataFrame(list(cursor))
# (Optional) Drop MongoDB's default _id field if not needed
raw_df.drop(columns=['_id'], inplace=True, errors='ignore')
# Display the DataFrame
print(raw_df.shape)

station = 'Guindy'
parking_area = 'SGU P1'
vehicle_type = 'twoWheelerAvailable'
cutoff_ts = '2025-06-04 23:00:00.000'
df = raw_df[(raw_df['stationName']==station)& (raw_df['parkingAreaName']==parking_area)]
df = df.sort_values('timestamp')
print(station, parking_area, df.shape)
df['timestamp'] = pd.to_datetime(df['timestamp'])
# # Sort and set index
df = df.sort_values('timestamp')

# Resample to regular interval (optional but helpful)
# df = df.set_index('timestamp').resample('15min').mean().interpolate()

# Prophet expects columns: ds (date), y (target)
df_prophet = df[df['timestamp']<=cutoff_ts].reset_index()[['timestamp', vehicle_type]]
df_prophet.columns = ['ds', 'y']

df_prophet = df_prophet.dropna()
df_prophet = df_prophet[df_prophet['y'] >= 0]

m = NeuralProphet(daily_seasonality=True)
metrics = m.fit(df_prophet, freq="15min")
future = m.make_future_dataframe(df_prophet, periods=24)
forecast = m.predict(future)

# Save model and history together
with open("parking_forecast_model.pkl", "wb") as f:
    pickle.dump({"model": m, "history_df": df_prophet}, f)

# Round timestamps to the nearest minute
actual = df[df['timestamp']>=cutoff_ts][['timestamp', 'twoWheelerAvailable']]
actual.columns = ['ds', 'y']
actual['ds_rounded'] = actual['ds'].dt.round('1min')
forecast['ds_rounded'] = forecast['ds'].dt.round('1min')

# Merge on rounded timestamps
merged = pd.merge(actual, forecast, left_on='ds_rounded', right_on='ds_rounded')
merged = merged.sort_values('ds_rounded')

mae = mean_absolute_error(merged['y_x'], merged['yhat1'])
rmse = np.sqrt(mean_squared_error(merged['y_x'], merged['yhat1']))
mape = mean_absolute_percentage_error(merged['y_x'], merged['yhat1']) * 100

print(f"📊 Accuracy Report:")
print(f"MAE :  {mae:.2f}")
print(f"RMSE:  {rmse:.2f}")
print(f"MAPE:  {mape:.2f}%")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if SUPABASE_URL and SUPABASE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    bucket = supabase.storage.from_("models")
    try:
        bucket.remove(["parking_forecast_model.pkl"])
        print("Old model removed.")
    except Exception:
        print("No previous model found or already deleted.")

    with open("parking_forecast_model.pkl", "rb") as f:
        bucket.upload(
            path="parking_forecast_model.pkl",
            file=f,
            file_options={"content-type": "application/octet-stream"}
        )
    print("✅ Model uploaded to Supabase")
else:
    print("⚠️ Supabase credentials not found. Skipping upload.")
