from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from io import BytesIO
import pandas as pd
import pickle
import os
import traceback
from supabase import create_client, Client

# Initialize FastAPI app
app = FastAPI(title="Parking Availability Forecast API")

# Supabase config (store in Render's Environment Variables)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

MODEL_BUCKET = "models"  # change if different
MODEL_FILENAME = "parking_forecast_model.pkl"

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Global variable for model
model = None

# Define input data format
class ForecastRequest(BaseModel):
    periods: int = 48  # number of 15-min steps (i.e., 12 = next 3 hours)
    

# Health check
@app.get("/")
def read_root():
    return {"message": "🚀 Forecast API is running"}

@app.on_event("startup")
def load_model_once():
    global model
    model_path = "/tmp/parking_forecast_model.pkl"
    if not os.path.exists(model_path):
        with open(model_path, "wb") as f:
            res = supabase.storage.from_(MODEL_BUCKET).download(MODEL_FILENAME)
            f.write(res)
    with open(model_path, "rb") as f:
        model = pickle.load(f)
    print("✅ Model loaded at startup.")

# ...existing code...

@app.post("/forecast")
def get_forecast(data: ForecastRequest):
    try:
        global model
        if model is None:
            raise Exception("Model not loaded.")
        prophet_model = model['model']
        history_df = model['history_df']
        future = prophet_model.make_future_dataframe(history_df, periods=data.periods)
        forecast = prophet_model.predict(future)
        forecast_tail = forecast.tail(data.periods)
        result = [
            {
                "timestamp": row['ds'].strftime('%Y-%m-%d %H:%M:%S'),
                "predicted_availability": round(row['yhat1'], 2)
            }
            for _, row in forecast_tail.iterrows()
        ]
        return {"forecast": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating forecast: {e}")
# ...existing code...